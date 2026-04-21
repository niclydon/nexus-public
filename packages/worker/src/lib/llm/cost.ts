/**
 * LLM cost estimation.
 *
 * Per-model token pricing (cents per 1M tokens).
 * Updated when model pricing changes.
 */

interface ModelPricing {
  inputCentsPerMillion: number;
  outputCentsPerMillion: number;
}

const PRICING: Record<string, ModelPricing> = {
  // Anthropic
  'claude-sonnet-4-6':          { inputCentsPerMillion: 300,  outputCentsPerMillion: 1500 },
  'claude-haiku-4-5-20251001':  { inputCentsPerMillion: 80,   outputCentsPerMillion: 400 },

  // Google
  'gemini-2.5-flash-lite':      { inputCentsPerMillion: 0,    outputCentsPerMillion: 0 },   // free tier
  'gemini-2.0-flash':           { inputCentsPerMillion: 10,   outputCentsPerMillion: 40 },

  // OpenAI
  'gpt-4o':                     { inputCentsPerMillion: 250,  outputCentsPerMillion: 1000 },
  'gpt-4o-mini':                { inputCentsPerMillion: 15,   outputCentsPerMillion: 60 },

  // Forge → local inference (zero cost)
  'qwen3-next-chat-80b':            { inputCentsPerMillion: 0,    outputCentsPerMillion: 0 },
  'qwen3-code-30b-a3b':            { inputCentsPerMillion: 0,    outputCentsPerMillion: 0 },
  'qwen3-vl-8b':                { inputCentsPerMillion: 0,    outputCentsPerMillion: 0 },
  'qwen3-vl-32b':               { inputCentsPerMillion: 0,    outputCentsPerMillion: 0 },
  'nomic':                      { inputCentsPerMillion: 0,    outputCentsPerMillion: 0 },
};

/**
 * Estimate cost in cents for a given model invocation.
 */
export function estimateCost(model: string, inputTokens: number, outputTokens: number): number {
  const pricing = PRICING[model];
  if (!pricing) return 0;

  const inputCost = (inputTokens / 1_000_000) * pricing.inputCentsPerMillion;
  const outputCost = (outputTokens / 1_000_000) * pricing.outputCentsPerMillion;
  return Math.round((inputCost + outputCost) * 1000) / 1000; // 3 decimal places
}
