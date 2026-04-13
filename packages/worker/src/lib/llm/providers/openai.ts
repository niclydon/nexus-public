/**
 * OpenAI GPT provider adapter.
 */
import OpenAI from 'openai';

let client: OpenAI | null = null;

function getClient(): OpenAI {
  if (!client) {
    if (!process.env.OPENAI_API_KEY) throw new Error('OPENAI_API_KEY not set');
    client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY, timeout: 120_000 });
  }
  return client;
}

export interface LLMResponse {
  text: string;
  inputTokens: number;
  outputTokens: number;
}

export async function callOpenAI(params: {
  model: string;
  systemPrompt: string;
  userMessage: string;
  maxTokens: number;
  signal?: AbortSignal;
}): Promise<LLMResponse> {
  const response = await getClient().chat.completions.create({
    model: params.model,
    max_tokens: params.maxTokens,
    messages: [
      { role: 'system', content: params.systemPrompt },
      { role: 'user', content: params.userMessage },
    ],
  }, { signal: params.signal });

  const choice = response.choices[0];
  const text = choice?.message?.content ?? '';

  return {
    text,
    inputTokens: response.usage?.prompt_tokens ?? 0,
    outputTokens: response.usage?.completion_tokens ?? 0,
  };
}
