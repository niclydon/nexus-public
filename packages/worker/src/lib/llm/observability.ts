import Langfuse from 'langfuse';

let client: Langfuse | null = null;

function getLangfuse(): Langfuse | null {
  if (process.env.LANGFUSE_ENABLED !== 'true') return null;
  if (!process.env.LANGFUSE_PUBLIC_KEY || !process.env.LANGFUSE_SECRET_KEY) return null;
  if (!client) {
    client = new Langfuse({
      publicKey: process.env.LANGFUSE_PUBLIC_KEY,
      secretKey: process.env.LANGFUSE_SECRET_KEY,
      baseUrl: process.env.LANGFUSE_BASE_URL ?? 'http://localhost:3010',
      flushAt: 20,
      flushInterval: 5000,
    });
  }
  return client;
}

export async function observedLlmCall<T>(
  params: {
    name: string;
    provider: string;
    model: string;
    systemPrompt?: string;
    userMessage?: string;
    metadata?: Record<string, unknown>;
  },
  fn: () => Promise<T>,
): Promise<T> {
  const lf = getLangfuse();
  const input = {
    system_prompt_chars: params.systemPrompt?.length ?? 0,
    user_message_chars: params.userMessage?.length ?? 0,
  };
  const trace = lf?.trace({
    name: params.name,
    input,
    metadata: { provider: params.provider, model: params.model, ...params.metadata },
  });
  const generation = trace?.generation({
    name: params.provider,
    model: params.model,
    input,
  });
  try {
    const result = await fn();
    generation?.end({ output: { type: 'result' } });
    trace?.update({ output: { type: 'result' } });
    lf?.flushAsync();
    return result;
  } catch (err) {
    generation?.end({ level: 'ERROR', statusMessage: (err as Error).message });
    lf?.flushAsync();
    throw err;
  }
}
