/**
 * LLM Decision Engine -- Router
 *
 * Central entry point for all LLM calls across the platform.
 * Loads model chains from the `llm_model_config` database table,
 * caches them in memory (5 min TTL), handles fallback across
 * providers, circuit-breaks dead models, and logs every call.
 *
 * Model configuration is database-driven -- ARIA can update her
 * own model preferences via db_write without code deploys.
 *
 * Usage:
 *   const result = await routeRequest({
 *     handler: 'daily-briefing',
 *     taskTier: 'generation',
 *     systemPrompt: '...',
 *     userMessage: '...',
 *     maxTokens: 1200,
 *   });
 */
import { getPool } from '@nexus/core';
import { callAnthropic, callAnthropicBatch } from './providers/anthropic.js';
import { callForge } from './providers/forge.js';
import { callGemini } from './providers/google.js';
import { callOpenAI } from './providers/openai.js';
import { estimateCost } from './cost.js';

export type TaskTier = 'reasoning' | 'generation' | 'classification';
export type Provider = 'anthropic' | 'google' | 'openai' | 'forge';

export class LLMRouterError extends Error {
  retryable: boolean;
  retryAfterMs: number;
  constructor(message: string, retryable: boolean, retryAfterMs: number = 0) {
    super(message);
    this.name = 'LLMRouterError';
    this.retryable = retryable;
    this.retryAfterMs = retryAfterMs;
  }
}

// ── Circuit Breaker ──────────────────────────────────────────────

interface ModelHealth {
  failureCount: number;
  disabledUntil: number;
  lastError: string;
  permanent: boolean;
}

const modelHealth = new Map<string, ModelHealth>();

const RATE_LIMIT_COOLDOWN_MS = 60 * 1000;
const TRANSIENT_COOLDOWN_MS = 5 * 60 * 1000;
const DEPRECATION_COOLDOWN_MS = 24 * 60 * 60 * 1000;

function isModelDisabled(model: string): boolean {
  const health = modelHealth.get(model);
  if (!health) return false;
  if (health.permanent) return true;
  return Date.now() < health.disabledUntil;
}

function isRateLimitError(error: string): boolean {
  return error.includes('429') || error.includes('quota') || error.includes('rate limit') || error.includes('rate_limit');
}

function isOverloadedError(error: string): boolean {
  return error.includes('503') || error.includes('529') || error.includes('overloaded');
}

function recordModelFailure(model: string, error: string): void {
  const existing = modelHealth.get(model) || { failureCount: 0, disabledUntil: 0, lastError: '', permanent: false };
  existing.failureCount++;
  existing.lastError = error;

  const isDeprecation = error.includes('no longer available') ||
    error.includes('deprecated') ||
    error.includes('not found') ||
    (error.includes('404') && (error.includes('model') || error.includes('Model')));

  if (isDeprecation) {
    existing.permanent = true;
    existing.disabledUntil = Date.now() + DEPRECATION_COOLDOWN_MS;
    console.warn(`[llm-router] MODEL DEPRECATED: ${model} -- disabled for 24h. Error: ${error}`);
  } else if (isRateLimitError(error) || isOverloadedError(error)) {
    const backoff = Math.min(RATE_LIMIT_COOLDOWN_MS * Math.pow(2, existing.failureCount - 1), 10 * 60 * 1000);
    existing.disabledUntil = Date.now() + backoff;
    console.warn(`[llm-router] Model ${model} rate-limited, disabled for ${Math.round(backoff / 1000)}s after ${existing.failureCount} failure(s)`);
  } else {
    const backoff = Math.min(TRANSIENT_COOLDOWN_MS * Math.pow(2, existing.failureCount - 1), 60 * 60 * 1000);
    existing.disabledUntil = Date.now() + backoff;
    console.warn(`[llm-router] Model ${model} disabled for ${Math.round(backoff / 1000)}s after ${existing.failureCount} failure(s)`);
  }

  modelHealth.set(model, existing);
}

function recordModelSuccess(model: string): void {
  const health = modelHealth.get(model);
  if (health && !health.permanent) {
    modelHealth.delete(model);
  }
}

/**
 * Periodic health recovery — clear expired disable states.
 * Called every 5 minutes to ensure models don't stay permanently stuck.
 */
let lastHealthSweep = 0;
const HEALTH_SWEEP_INTERVAL_MS = 5 * 60 * 1000;

function sweepModelHealth(): void {
  const now = Date.now();
  if (now - lastHealthSweep < HEALTH_SWEEP_INTERVAL_MS) return;
  lastHealthSweep = now;

  for (const [model, health] of modelHealth.entries()) {
    if (health.permanent) continue;
    if (now >= health.disabledUntil) {
      console.log(`[llm-router] Model ${model} re-enabled after cooldown (was disabled for: ${health.lastError?.slice(0, 80)})`);
      modelHealth.delete(model);
    }
  }
}

// ── Budget Enforcement ──────────────────────────────────────────

let cachedDailySpend: { cents: number; expiresAt: number } | null = null;
let cachedDailyBudget: { cents: number; expiresAt: number } | null = null;

const SPEND_CACHE_TTL_MS = 60_000;   // 60 seconds
const BUDGET_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

async function getDailySpendCents(): Promise<number> {
  if (cachedDailySpend && Date.now() < cachedDailySpend.expiresAt) {
    return cachedDailySpend.cents;
  }
  try {
    const pool = getPool();
    const { rows } = await pool.query<{ total: string }>(
      `SELECT COALESCE(SUM(estimated_cost_cents), 0) as total FROM llm_usage WHERE created_at >= CURRENT_DATE`
    );
    const cents = parseFloat(rows[0]?.total || '0');
    cachedDailySpend = { cents, expiresAt: Date.now() + SPEND_CACHE_TTL_MS };
    return cents;
  } catch (err) {
    console.error('[llm-router] Failed to query daily spend:', err instanceof Error ? err.message : err);
    return 0;
  }
}

async function getDailyBudgetCents(): Promise<number> {
  if (cachedDailyBudget && Date.now() < cachedDailyBudget.expiresAt) {
    return cachedDailyBudget.cents;
  }
  try {
    const pool = getPool();
    const { rows } = await pool.query<{ value: string }>(
      `SELECT value FROM settings WHERE key = 'llm_daily_budget_cents'`
    );
    const cents = parseInt(rows[0]?.value || '500', 10);
    cachedDailyBudget = { cents, expiresAt: Date.now() + BUDGET_CACHE_TTL_MS };
    return cents;
  } catch (err) {
    console.error('[llm-router] Failed to query daily budget:', err instanceof Error ? err.message : err);
    return 500; // default $5
  }
}

/**
 * Returns true when daily LLM spend >= daily budget limit.
 */
export async function isOverDailyBudget(): Promise<boolean> {
  const [spend, budget] = await Promise.all([getDailySpendCents(), getDailyBudgetCents()]);
  return spend >= budget;
}

// ── Database-Driven Model Config ─────────────────────────────────

interface ModelConfig {
  model: string;
  provider: Provider;
  call: (params: { model: string; systemPrompt: string; userMessage: string; maxTokens: number; signal?: AbortSignal }) => Promise<{ text: string; inputTokens: number; outputTokens: number }>;
  timeoutMs: number;
}

const PROVIDER_CALLERS: Record<Provider, ModelConfig['call']> = {
  anthropic: callAnthropic,
  forge: callForge,
  google: callGemini,
  openai: callOpenAI,
};

// Hardcoded defaults -- used if DB is unreachable or table is empty.
// All three tiers route to qwen3-next-chat-80b → Forge backend primary-server-text → port
// 8080 (the BULK slot, RPC tensor-split with Secondary-Server). The priority slot
// (port 8088, qwen3-next-chat-80b) is reserved for agent cycles + interactive
// chat, both of which use agent_config table directly and bypass this router.
// Cloud providers are fallbacks.
const DEFAULT_CHAINS: Record<TaskTier, ModelConfig[]> = {
  reasoning: [
    { model: 'qwen3-next-chat-80b', provider: 'forge', call: callForge, timeoutMs: 180_000 },
    { model: 'claude-sonnet-4-6', provider: 'anthropic', call: callAnthropic, timeoutMs: 30_000 },
    { model: 'gpt-4o', provider: 'openai', call: callOpenAI, timeoutMs: 20_000 },
    { model: 'claude-haiku-4-5-20251001', provider: 'anthropic', call: callAnthropic, timeoutMs: 15_000 },
  ],
  generation: [
    { model: 'qwen3-next-chat-80b', provider: 'forge', call: callForge, timeoutMs: 180_000 },
    { model: 'claude-haiku-4-5-20251001', provider: 'anthropic', call: callAnthropic, timeoutMs: 15_000 },
    { model: 'gemini-2.0-flash', provider: 'google', call: callGemini, timeoutMs: 10_000 },
    { model: 'gpt-4o-mini', provider: 'openai', call: callOpenAI, timeoutMs: 10_000 },
  ],
  classification: [
    { model: 'qwen3-next-chat-80b', provider: 'forge', call: callForge, timeoutMs: 180_000 },
    { model: 'gemini-2.5-flash-lite', provider: 'google', call: callGemini, timeoutMs: 10_000 },
    { model: 'claude-haiku-4-5-20251001', provider: 'anthropic', call: callAnthropic, timeoutMs: 5_000 },
    { model: 'gpt-4o-mini', provider: 'openai', call: callOpenAI, timeoutMs: 5_000 },
  ],
};

// In-memory cache of DB-loaded config (5 min TTL)
let cachedChains: Record<TaskTier, ModelConfig[]> | null = null;
let cacheExpiresAt = 0;
const CACHE_TTL_MS = 5 * 60 * 1000;

/**
 * Load model chains from the llm_model_config table.
 * Returns hardcoded defaults if the DB query fails or returns nothing.
 */
async function loadModelChains(): Promise<Record<TaskTier, ModelConfig[]>> {
  // Return cache if fresh
  if (cachedChains && Date.now() < cacheExpiresAt) {
    return cachedChains;
  }

  try {
    const pool = getPool();
    const { rows } = await pool.query<{
      tier: TaskTier;
      chain_order: number;
      model: string;
      provider: Provider;
      timeout_ms: number;
      is_enabled: boolean;
    }>(
      `SELECT tier, chain_order, model, provider, timeout_ms, is_enabled
       FROM llm_model_config
       WHERE is_enabled = true
       ORDER BY tier, chain_order`
    );

    if (rows.length === 0) {
      console.warn('[llm-router] No enabled models in llm_model_config -- using hardcoded defaults');
      cachedChains = DEFAULT_CHAINS;
      cacheExpiresAt = Date.now() + CACHE_TTL_MS;
      return DEFAULT_CHAINS;
    }

    const chains: Record<string, ModelConfig[]> = {};
    for (const row of rows) {
      const caller = PROVIDER_CALLERS[row.provider];
      if (!caller) {
        console.warn(`[llm-router] Unknown provider "${row.provider}" for model ${row.model} -- skipping`);
        continue;
      }
      if (!chains[row.tier]) chains[row.tier] = [];
      chains[row.tier].push({
        model: row.model,
        provider: row.provider,
        call: caller,
        timeoutMs: row.timeout_ms,
      });
    }

    // Fill any missing tiers with defaults
    for (const tier of ['reasoning', 'generation', 'classification'] as TaskTier[]) {
      if (!chains[tier] || chains[tier].length === 0) {
        console.warn(`[llm-router] No models configured for ${tier} tier -- using defaults`);
        chains[tier] = DEFAULT_CHAINS[tier];
      }
    }

    cachedChains = chains as Record<TaskTier, ModelConfig[]>;
    cacheExpiresAt = Date.now() + CACHE_TTL_MS;
    console.log(`[llm-router] Loaded model config from DB: ${rows.length} models across ${Object.keys(chains).length} tiers`);
    return cachedChains;
  } catch (err) {
    console.error('[llm-router] Failed to load model config from DB -- using hardcoded defaults:', err instanceof Error ? err.message : err);
    cachedChains = DEFAULT_CHAINS;
    cacheExpiresAt = Date.now() + 60_000; // Retry in 1 min on error
    return DEFAULT_CHAINS;
  }
}

/**
 * Force a cache refresh (called after db_write updates to llm_model_config).
 */
export function invalidateModelConfigCache(): void {
  cachedChains = null;
  cacheExpiresAt = 0;
}

/**
 * Get current model health state — for Model Ops agent visibility.
 */
export function getModelHealth(): Array<{ model: string; failureCount: number; disabledUntil: number; lastError: string; permanent: boolean; isDisabled: boolean }> {
  const now = Date.now();
  const results: Array<{ model: string; failureCount: number; disabledUntil: number; lastError: string; permanent: boolean; isDisabled: boolean }> = [];
  for (const [model, health] of modelHealth.entries()) {
    results.push({
      model,
      failureCount: health.failureCount,
      disabledUntil: health.disabledUntil,
      lastError: health.lastError,
      permanent: health.permanent,
      isDisabled: health.permanent || now < health.disabledUntil,
    });
  }
  return results;
}

/**
 * Reset model health — clear circuit breaker for a specific model or all.
 * Used by Model Ops agent when it verifies a model is healthy again.
 */
export function resetModelHealth(model?: string): number {
  if (model) {
    const had = modelHealth.has(model);
    modelHealth.delete(model);
    console.log(`[llm-router] Model health reset for ${model} (by Model Ops)`);
    return had ? 1 : 0;
  }
  const count = modelHealth.size;
  modelHealth.clear();
  console.log(`[llm-router] All model health reset (${count} entries cleared by Model Ops)`);
  return count;
}

// ── Public Interface ─────────────────────────────────────────────

export interface RouteRequestParams {
  handler: string;
  taskTier: TaskTier;
  systemPrompt: string;
  userMessage: string;
  maxTokens: number;
  preferredModel?: string;
  /** Use the Anthropic Batch API for 50% cost savings. Only for non-time-sensitive jobs. */
  useBatch?: boolean;
}

export interface RouteResult {
  text: string;
  model: string;
  provider: Provider;
  inputTokens: number;
  outputTokens: number;
  estimatedCostCents: number;
  latencyMs: number;
  wasFallback: boolean;
  reason: string;
}

function isProviderAvailable(provider: Provider): boolean {
  switch (provider) {
    case 'anthropic': return !!process.env.ANTHROPIC_API_KEY;
    case 'forge': return !!process.env.FORGE_API_KEY && !!process.env.FORGE_BASE_URL;
    case 'google': return !!process.env.GOOGLE_GENAI_API_KEY;
    case 'openai': return !!process.env.OPENAI_API_KEY;
  }
}

async function callWithTimeout(
  config: ModelConfig,
  params: { systemPrompt: string; userMessage: string; maxTokens: number },
  useBatch?: boolean
): Promise<{ text: string; inputTokens: number; outputTokens: number }> {
  // Use batch API for Anthropic models when requested (50% cost savings)
  if (useBatch && config.provider === 'anthropic') {
    // Batch has its own 30-min timeout, no need for race
    return callAnthropicBatch({ model: config.model, ...params });
  }

  // AbortController ensures the underlying HTTP connection is torn down on timeout.
  // Plain Promise.race leaves streaming connections orphaned forever.
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), config.timeoutMs);

  try {
    const result = await config.call({ model: config.model, ...params, signal: controller.signal });
    return result;
  } catch (err) {
    if (controller.signal.aborted) {
      throw new Error(`Timeout after ${config.timeoutMs}ms`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

async function logUsage(params: {
  model: string;
  provider: Provider;
  handler: string;
  taskTier: TaskTier;
  inputTokens: number;
  outputTokens: number;
  estimatedCostCents: number;
  latencyMs: number;
  wasFallback: boolean;
  fallbackReason: string | null;
  modelSelectedReason: string;
}): Promise<void> {
  try {
    const pool = getPool();
    await pool.query(
      `INSERT INTO llm_usage
       (model, provider, handler, task_tier, input_tokens, output_tokens,
        estimated_cost_cents, latency_ms, was_fallback, fallback_reason, model_selected_reason)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
      [
        params.model, params.provider, params.handler, params.taskTier,
        params.inputTokens, params.outputTokens, params.estimatedCostCents,
        params.latencyMs, params.wasFallback, params.fallbackReason,
        params.modelSelectedReason,
      ]
    );
  } catch (err) {
    console.error('[llm-router] Failed to log usage:', err instanceof Error ? err.message : err);
  }
}

/**
 * Route an LLM request through the decision engine.
 */
export async function routeRequest(params: RouteRequestParams): Promise<RouteResult> {
  const { handler, systemPrompt, userMessage, maxTokens, preferredModel, useBatch } = params;
  let { taskTier } = params;
  const startTime = Date.now();

  // Periodic health sweep — re-enable models whose cooldown has expired
  sweepModelHealth();

  // Budget enforcement: downgrade to classification tier when over budget
  if (taskTier !== 'classification' && await isOverDailyBudget()) {
    console.warn(`[llm-router] Daily budget exceeded, downgrading to classification tier (was: ${taskTier})`);
    taskTier = 'classification';
  }

  // Load model chain from DB (cached 5 min)
  const allChains = await loadModelChains();
  let chain = [...(allChains[taskTier] || [])];
  if (chain.length === 0) {
    throw new Error(`Unknown task tier: ${taskTier}`);
  }

  // Filter to available providers and non-disabled models
  chain = chain.filter((c) => {
    if (!isProviderAvailable(c.provider)) return false;
    if (isModelDisabled(c.model)) {
      const health = modelHealth.get(c.model);
      console.log(`[llm-router] Skipping disabled model ${c.model}: ${health?.lastError?.slice(0, 100)}`);
      return false;
    }
    return true;
  });
  if (chain.length === 0) {
    throw new Error(`No providers available for task tier: ${taskTier} (some models may be temporarily disabled)`);
  }

  // Preferred model override
  if (preferredModel) {
    const preferred = chain.find((c) => c.model === preferredModel);
    if (preferred) {
      chain = [preferred, ...chain.filter((c) => c.model !== preferredModel)];
    }
  }

  let lastError: Error | null = null;

  for (let i = 0; i < chain.length; i++) {
    const config = chain[i];
    const isFallback = i > 0;
    const batchLabel = useBatch && config.provider === 'anthropic' ? ' [batch -50%]' : '';
    const reason = isFallback
      ? `Fallback ${i}: ${lastError?.message ?? 'previous model failed'}${batchLabel}`
      : `Primary model for ${taskTier} tier${batchLabel}`;

    try {
      const result = await callWithTimeout(config, { systemPrompt, userMessage, maxTokens }, useBatch);
      const latencyMs = Date.now() - startTime;
      const costCents = estimateCost(config.model, result.inputTokens, result.outputTokens);
      const batchDiscount = useBatch && config.provider === 'anthropic' ? 0.5 : 1;
      const adjustedCostCents = costCents * batchDiscount;

      await logUsage({
        model: config.model,
        provider: config.provider,
        handler,
        taskTier,
        inputTokens: result.inputTokens,
        outputTokens: result.outputTokens,
        estimatedCostCents: adjustedCostCents,
        latencyMs,
        wasFallback: isFallback,
        fallbackReason: isFallback ? (lastError?.message ?? null) : null,
        modelSelectedReason: reason,
      });

      recordModelSuccess(config.model);

      if (isFallback) {
        console.log(`[llm-router] ${handler}: Fallback to ${config.model} (${config.provider}) succeeded. Reason: ${lastError?.message}`);
      }

      return {
        text: result.text,
        model: config.model,
        provider: config.provider,
        inputTokens: result.inputTokens,
        outputTokens: result.outputTokens,
        estimatedCostCents: adjustedCostCents,
        latencyMs,
        wasFallback: isFallback,
        reason,
      };
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      console.error(`[llm-router] ${handler}: ${config.model} (${config.provider}) failed: ${lastError.message}`);
      recordModelFailure(config.model, lastError.message);
    }
  }

  const lastMsg = lastError?.message ?? 'unknown';
  const retryable = isRateLimitError(lastMsg) || isOverloadedError(lastMsg) || lastMsg.includes('Timeout');
  const retryAfterMs = retryable ? 60_000 : 0;
  throw new LLMRouterError(
    `[llm-router] All models failed for ${handler} (${taskTier} tier). Last error: ${lastMsg}`,
    retryable,
    retryAfterMs
  );
}
