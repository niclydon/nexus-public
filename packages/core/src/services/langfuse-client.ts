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
    summarizeResponse?: (response: T) => Promise<Record<string, unknown>>;
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
    const output = params.summarizeResponse
      ? await params.summarizeResponse(response)
      : await summarizeResponse(response);
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

async function summarizeResponse(response: Response): Promise<Record<string, unknown>> {
  const output: Record<string, unknown> = { ok: response.ok, status: response.status };
  try {
    const data = await response.clone().json() as {
      choices?: Array<{ message?: { content?: string | null } }>;
      data?: Array<{ embedding?: number[] }>;
      embeddings?: number[][];
      results?: unknown[];
      usage?: Record<string, unknown>;
    };
    const text = data.choices?.[0]?.message?.content;
    if (typeof text === 'string') output.content_chars = text.length;
    if (Array.isArray(data.data)) {
      output.embedding_count = data.data.length;
      output.dimensions = data.data[0]?.embedding?.length ?? 0;
      output.vector_contents = 'suppressed';
    }
    if (Array.isArray(data.embeddings)) {
      output.embedding_count = data.embeddings.length;
      output.dimensions = data.embeddings[0]?.length ?? 0;
      output.vector_contents = 'suppressed';
    }
    if (Array.isArray(data.results)) output.result_count = data.results.length;
    if (data.usage) output.usage = data.usage;
  } catch {
    // Non-JSON responses are still audited by status.
  }
  return output;
}
