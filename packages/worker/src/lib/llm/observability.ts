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
    summarize?: (result: T) => Record<string, unknown> | Promise<Record<string, unknown>>;
    usage?: (result: T) => Record<string, unknown> | undefined;
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
    const output = params.summarize ? await params.summarize(result) : await summarizeResult(result);
    const usage = params.usage?.(result) ?? summarizeUsage(result);
    generation?.end({ output, usage });
    trace?.update({ output });
    lf?.flushAsync();
    return result;
  } catch (err) {
    generation?.end({ level: 'ERROR', statusMessage: (err as Error).message });
    lf?.flushAsync();
    throw err;
  }
}

async function summarizeResult(result: unknown): Promise<Record<string, unknown>> {
  if (result instanceof Response) {
    return summarizeResponse(result);
  }
  const value = result as {
    choices?: Array<{ message?: { content?: string | null } }>;
    content?: Array<{ type?: string; text?: string }>;
    text?: string;
    usageMetadata?: unknown;
  };
  if (Array.isArray(value.choices)) {
    const text = value.choices[0]?.message?.content ?? '';
    return { content_chars: text.length, choice_count: value.choices.length };
  }
  if (Array.isArray(value.content)) {
    const text = value.content.filter((b) => b.type === 'text').map((b) => b.text ?? '').join('');
    return { content_chars: text.length, block_count: value.content.length };
  }
  if (typeof value.text === 'string') {
    return { content_chars: value.text.length };
  }
  return { type: 'result' };
}

async function summarizeResponse(response: Response): Promise<Record<string, unknown>> {
  const output: Record<string, unknown> = { ok: response.ok, status: response.status };
  try {
    const data = await response.clone().json() as {
      choices?: Array<{ message?: { content?: string | null } }>;
      data?: Array<{ embedding?: number[] }>;
      embedding?: { values?: number[] };
      usage?: Record<string, unknown>;
    };
    const text = data.choices?.[0]?.message?.content;
    if (typeof text === 'string') output.content_chars = text.length;
    if (Array.isArray(data.data)) {
      output.embedding_count = data.data.length;
      output.dimensions = data.data[0]?.embedding?.length ?? 0;
      output.vector_contents = 'suppressed';
    }
    if (data.embedding?.values) {
      output.embedding_count = 1;
      output.dimensions = data.embedding.values.length;
      output.vector_contents = 'suppressed';
    }
    if (data.usage) output.usage = data.usage;
  } catch {
    // Non-JSON responses are still audited by status.
  }
  return output;
}

function summarizeUsage(result: unknown): Record<string, unknown> | undefined {
  const value = result as {
    usage?: Record<string, unknown>;
    usageMetadata?: {
      promptTokenCount?: number;
      candidatesTokenCount?: number;
      totalTokenCount?: number;
    };
  };
  if (value.usage) return value.usage;
  if (value.usageMetadata) {
    return {
      prompt_tokens: value.usageMetadata.promptTokenCount,
      completion_tokens: value.usageMetadata.candidatesTokenCount,
      total_tokens: value.usageMetadata.totalTokenCount,
    };
  }
  return undefined;
}
