import { observedLlmCall } from '../../packages/worker/src/lib/llm/observability.js';

export async function observedScriptLlm<T>(
  params: {
    name: string;
    provider: string;
    model?: string;
    userMessage?: string;
    metadata?: Record<string, unknown>;
  },
  fn: () => Promise<T>,
): Promise<T> {
  return observedLlmCall(
    {
      name: params.name,
      provider: params.provider,
      model: params.model ?? 'unknown',
      userMessage: params.userMessage,
      metadata: { source: 'script', ...(params.metadata ?? {}) },
    },
    fn,
  );
}
