/**
 * Anthropic Claude provider adapter.
 */
import Anthropic from '@anthropic-ai/sdk';

let client: Anthropic | null = null;

function getClient(): Anthropic {
  if (!client) {
    if (!process.env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not set');
    // maxRetries: 0 — let the router-level circuit breaker handle retries with
    // proper fallback chains. Without this, the SDK retries 2x on transient
    // errors, each with up to 120s timeout, causing 6+ minute hangs that blow
    // past the chain's per-model timeout. We saw real life-narration calls
    // taking 422 seconds because of this.
    // timeout: 60_000 — generous enough for legitimate slow generation, short
    // enough that the router AbortController can catch hung calls.
    client = new Anthropic({
      apiKey: process.env.ANTHROPIC_API_KEY,
      timeout: 60_000,
      maxRetries: 0,
    });
  }
  return client;
}

export interface LLMResponse {
  text: string;
  inputTokens: number;
  outputTokens: number;
}

export async function callAnthropic(params: {
  model: string;
  systemPrompt: string;
  userMessage: string;
  maxTokens: number;
  signal?: AbortSignal;
}): Promise<LLMResponse> {
  const response = await getClient().messages.create({
    model: params.model,
    max_tokens: params.maxTokens,
    system: params.systemPrompt,
    messages: [{ role: 'user', content: params.userMessage }],
  }, { signal: params.signal });

  const text = response.content
    .filter((b): b is Anthropic.TextBlock => b.type === 'text')
    .map((b) => b.text)
    .join('');

  return {
    text,
    inputTokens: response.usage.input_tokens,
    outputTokens: response.usage.output_tokens,
  };
}

/**
 * Call Anthropic via the Message Batches API (50% cost savings).
 * Submits a single-request batch, polls until complete, returns the same interface.
 * Typically completes in seconds to minutes for single requests.
 */
export async function callAnthropicBatch(params: {
  model: string;
  systemPrompt: string;
  userMessage: string;
  maxTokens: number;
}): Promise<LLMResponse> {
  const client = getClient();

  // Submit a single-request batch
  const batch = await client.messages.batches.create({
    requests: [
      {
        custom_id: `aria-${Date.now()}`,
        params: {
          model: params.model,
          max_tokens: params.maxTokens,
          system: params.systemPrompt,
          messages: [{ role: 'user' as const, content: params.userMessage }],
        },
      },
    ],
  });

  // Poll until complete (check every 5 seconds, timeout after 30 minutes)
  const POLL_INTERVAL_MS = 5_000;
  const MAX_WAIT_MS = 30 * 60 * 1000;
  const startTime = Date.now();

  let batchResult = batch;
  while (batchResult.processing_status === 'in_progress') {
    if (Date.now() - startTime > MAX_WAIT_MS) {
      // Cancel the batch and fall back
      try { await client.messages.batches.cancel(batch.id); } catch { /* best effort */ }
      throw new Error(`Batch ${batch.id} timed out after 30 minutes`);
    }

    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
    batchResult = await client.messages.batches.retrieve(batch.id);
  }

  // Retrieve results
  if (batchResult.processing_status !== 'ended') {
    throw new Error(`Batch ${batch.id} ended with status: ${batchResult.processing_status}`);
  }

  // Stream results from the results URL
  const results: Array<{
    custom_id: string;
    result: {
      type: string;
      message?: {
        content: Array<{ type: string; text?: string }>;
        usage: { input_tokens: number; output_tokens: number };
      };
      error?: { type: string; message: string };
    };
  }> = [];

  const resultsStream = await client.messages.batches.results(batch.id);
  for await (const result of resultsStream) {
    results.push(result as typeof results[number]);
  }

  if (results.length === 0) {
    throw new Error(`Batch ${batch.id} returned no results`);
  }

  const firstResult = results[0];
  if (firstResult.result.type === 'errored') {
    throw new Error(`Batch request failed: ${firstResult.result.error?.message ?? 'unknown error'}`);
  }

  if (firstResult.result.type !== 'succeeded' || !firstResult.result.message) {
    throw new Error(`Batch request returned unexpected type: ${firstResult.result.type}`);
  }

  const message = firstResult.result.message;
  const text = message.content
    .filter((b): b is { type: 'text'; text: string } => b.type === 'text')
    .map((b) => b.text)
    .join('');

  return {
    text,
    inputTokens: message.usage.input_tokens,
    outputTokens: message.usage.output_tokens,
  };
}
