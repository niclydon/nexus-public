/**
 * Google Gemini provider adapter.
 */
import { GoogleGenAI } from '@google/genai';

let genai: GoogleGenAI | null = null;

function getClient(): GoogleGenAI {
  if (!genai) {
    const key = process.env.GOOGLE_GENAI_API_KEY;
    if (!key) throw new Error('GOOGLE_GENAI_API_KEY not set');
    genai = new GoogleGenAI({ apiKey: key });
  }
  return genai;
}

export interface LLMResponse {
  text: string;
  inputTokens: number;
  outputTokens: number;
}

export async function callGemini(params: {
  model: string;
  systemPrompt: string;
  userMessage: string;
  maxTokens: number;
  signal?: AbortSignal;
}): Promise<LLMResponse> {
  const client = getClient();

  const response = await client.models.generateContent({
    model: params.model,
    contents: params.userMessage,
    config: {
      systemInstruction: params.systemPrompt,
      maxOutputTokens: params.maxTokens,
      abortSignal: params.signal,
    },
  });

  const text = response.text ?? '';
  const usage = response.usageMetadata;

  return {
    text,
    inputTokens: usage?.promptTokenCount ?? 0,
    outputTokens: usage?.candidatesTokenCount ?? 0,
  };
}
