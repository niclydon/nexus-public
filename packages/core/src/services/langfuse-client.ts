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

export async function traceForgeFetch<T extends Response>(
  params: {
    name: string;
    url: string;
    model: string;
    input?: Record<string, unknown>;
    metadata?: Record<string, unknown>;
  },
  fn: () => Promise<T>,
): Promise<T> {
  const lf = getLangfuse();
  const trace = lf?.trace({
    name: params.name,
    input: params.input,
    metadata: { provider: 'forge', model: params.model, endpoint: params.url, ...params.metadata },
  });
  const generation = trace?.generation({
    name: params.name,
    model: params.model,
    input: params.input,
  });
  try {
    const response = await fn();
    const output = { ok: response.ok, status: response.status };
    generation?.end({ output });
    trace?.update({ output });
    lf?.flushAsync();
    return response;
  } catch (err) {
    generation?.end({ level: 'ERROR', statusMessage: (err as Error).message });
    lf?.flushAsync();
    throw err;
  }
}
