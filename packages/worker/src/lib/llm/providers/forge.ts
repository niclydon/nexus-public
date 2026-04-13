/**
 * Forge local LLM provider adapter.
 *
 * Calls the Forge unified LLM API running on Dev-Server (gateway).
 * Forge routes requests to Primary-Server (GPU compute node) for heavy
 * inference and falls back to Dev-Server MLX for Apple Silicon-specific tasks.
 * Uses the OpenAI SDK since Forge exposes OpenAI-compatible endpoints.
 * Zero cost -- all inference runs on local home lab hardware.
 *
 * Uses streaming to avoid Cloudflare's 100s idle timeout — tokens flow
 * continuously, keeping the connection alive for long generations.
 */
import OpenAI from 'openai';

let client: OpenAI | null = null;

function getClient(): OpenAI {
  if (!client) {
    if (!process.env.FORGE_API_KEY) throw new Error('FORGE_API_KEY not set');
    if (!process.env.FORGE_BASE_URL) throw new Error('FORGE_BASE_URL not set');
    client = new OpenAI({
      apiKey: process.env.FORGE_API_KEY,
      baseURL: process.env.FORGE_BASE_URL, // e.g., https://forge.example.io/v1
      timeout: 130_000, // Just above router's 120s timeout — AbortController handles the real cutoff
    });
  }
  return client;
}

export interface LLMResponse {
  text: string;
  inputTokens: number;
  outputTokens: number;
}

export async function callForge(params: {
  model: string;
  systemPrompt: string;
  userMessage: string;
  maxTokens: number;
  signal?: AbortSignal;
}): Promise<LLMResponse> {
  // Use streaming to keep connection alive through Cloudflare Tunnel.
  // Tokens flow continuously — no idle timeout.
  // AbortSignal ensures the connection is actually torn down on timeout,
  // not just orphaned by Promise.race.
  // Build request body with chat_template_kwargs to disable Qwen thinking mode.
  // Use raw fetch instead of OpenAI SDK to include the llama-server extension field.
  const body = JSON.stringify({
    model: params.model,
    max_tokens: params.maxTokens,
    messages: [
      { role: 'system', content: params.systemPrompt },
      { role: 'user', content: params.userMessage },
    ],
    stream: true,
    stream_options: { include_usage: true },
    chat_template_kwargs: { enable_thinking: false },
  });

  const baseURL = process.env.FORGE_BASE_URL!;
  const apiKey = process.env.FORGE_API_KEY!;
  const resp = await fetch(`${baseURL}/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
    body,
    signal: params.signal,
  });

  if (!resp.ok) {
    const errText = await resp.text().catch(() => '');
    throw new Error(`Forge ${resp.status}: ${errText.slice(0, 200)}`);
  }

  // Parse SSE stream
  const reader = resp.body?.getReader();
  if (!reader) throw new Error('No response body');
  const decoder = new TextDecoder();

  let text = '';
  let inputTokens = 0;
  let outputTokens = 0;
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      if (!line.startsWith('data: ')) continue;
      const data = line.slice(6).trim();
      if (data === '[DONE]') continue;
      try {
        const chunk = JSON.parse(data);
        const delta = chunk.choices?.[0]?.delta?.content;
        if (delta) text += delta;
        if (chunk.usage) {
          inputTokens = chunk.usage.prompt_tokens ?? 0;
          outputTokens = chunk.usage.completion_tokens ?? 0;
        }
      } catch { /* skip malformed chunks */ }
    }
  }

  // Guard: if Forge returned empty content, throw explicitly so the circuit breaker
  // gets actionable context instead of silently propagating an empty string.
  if (!text && (params.systemPrompt.length + params.userMessage.length) > 0) {
    throw new Error('Forge returned empty content (possible thinking mode issue)');
  }

  // Estimate tokens if llama-server didn't report usage stats (returns 0 sometimes)
  if (inputTokens === 0 && text.length > 0) {
    inputTokens = Math.ceil((params.systemPrompt.length + params.userMessage.length) / 3.5);
  }
  if (outputTokens === 0 && text.length > 0) {
    outputTokens = Math.ceil(text.length / 3.5);
  }

  return { text, inputTokens, outputTokens };
}
