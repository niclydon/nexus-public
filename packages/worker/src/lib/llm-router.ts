import { createLogger } from '@nexus/core';

const logger = createLogger('llm-router');

export interface LlmRequest {
  systemPrompt: string;
  userMessage: string;
  agentId: string;
  model?: string;
  maxTokens?: number;
  temperature?: number;
  forgeUrl?: string;
  /** Prior messages for multi-step conversation history */
  history?: Array<{ role: 'user' | 'assistant'; content: string }>;
}

export interface LlmResponse {
  actions: ActionRequest[];
  [key: string]: unknown;
}

export interface ActionRequest {
  action: string;
  params: Record<string, unknown>;
  reason?: string;
}

export async function callLlm(req: LlmRequest): Promise<LlmResponse | null> {
  const forgeUrl = req.forgeUrl ?? process.env.FORGE_PRIORITY_URL ?? process.env.FORGE_URL ?? 'http://localhost:8088';
  const forgeKey = process.env.FORGE_API_KEY ?? '';
  const model = req.model ?? 'qwen3.5-35b-a3b';
  const maxTokens = req.maxTokens ?? 2000;
  const temperature = req.temperature ?? 0.1;

  const url = `${forgeUrl}/v1/chat/completions`;
  const messages: Array<{ role: string; content: string }> = [
    { role: 'system', content: req.systemPrompt },
  ];
  // Include conversation history for multi-step cycles
  if (req.history) {
    for (const h of req.history) {
      messages.push({ role: h.role, content: h.content });
    }
  }
  messages.push({ role: 'user', content: req.userMessage });

  const body = {
    model,
    messages,
    max_tokens: maxTokens,
    temperature,
    // Only Qwen3/3.5 models support the thinking mode toggle
    // Qwen2.5 and Llama models ignore or break on this parameter
    ...(model.includes('qwen3') ? { chat_template_kwargs: { enable_thinking: false } } : {}),
  };

  logger.logVerbose('LLM request to:', url, 'model:', model, 'agent:', req.agentId);
  const stop = logger.time('llm-call');

  try {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (forgeKey) headers['Authorization'] = `Bearer ${forgeKey}`;
    headers['X-Agent-Id'] = req.agentId;

    const response = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });

    stop();

    if (!response.ok) {
      const text = await response.text();
      logger.logMinimal('LLM error:', response.status, text.slice(0, 500));
      return null;
    }

    const data = await response.json() as {
      choices: Array<{ message: { content: string } }>;
    };

    const content = data.choices?.[0]?.message?.content;
    if (!content) {
      logger.logMinimal('LLM returned empty content');
      return null;
    }

    logger.logDebug('LLM raw response:', content.slice(0, 500));

    // Parse JSON from response — handle markdown code blocks
    let jsonStr = content.trim();
    if (jsonStr.startsWith('```')) {
      const start = jsonStr.indexOf('\n') + 1;
      const end = jsonStr.lastIndexOf('```');
      jsonStr = jsonStr.slice(start, end).trim();
    }

    const parsed = JSON.parse(jsonStr) as LlmResponse;
    if (!Array.isArray(parsed.actions)) {
      parsed.actions = [];
    }

    logger.logVerbose('LLM returned', parsed.actions.length, 'actions for', req.agentId);
    return parsed;
  } catch (err) {
    stop();
    logger.logMinimal('LLM call failed:', (err as Error).message);
    return null;
  }
}
