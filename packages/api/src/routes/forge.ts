import { Router } from 'express';
import { createLogger } from '@nexus/core';

const logger = createLogger('routes/forge');
const router = Router();

// FORGE_BASE_URL is shared with other code that uses it as an OpenAI-
// compatible base (so it may end with `/v1`). Our targets (`/api/usage`,
// `/health/backends`, `/v1/models`) live at the domain root, so strip any
// trailing `/v1` or `/` to get the canonical root.
const FORGE_BASE_URL = (process.env.FORGE_BASE_URL || 'http://localhost:8642')
  .replace(/\/v1\/?$/, '')
  .replace(/\/$/, '');
const FORGE_API_KEY = process.env.FORGE_API_KEY || '';

/**
 * Internal helper: GET a Forge endpoint with API-key auth and return the
 * parsed JSON. Errors are normalized to `{ error, status }` shape.
 */
async function forgeFetch(path: string): Promise<{ ok: boolean; data?: unknown; error?: string; status?: number }> {
  const url = `${FORGE_BASE_URL}${path}`;
  try {
    const headers: Record<string, string> = {};
    if (FORGE_API_KEY) headers.Authorization = `Bearer ${FORGE_API_KEY}`;
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(10_000) });
    const text = await res.text();
    let data: unknown = null;
    try {
      data = JSON.parse(text);
    } catch {
      // Non-JSON response (shouldn't happen for these endpoints)
      data = { raw: text.slice(0, 500) };
    }
    if (!res.ok) {
      return { ok: false, status: res.status, error: `Forge ${res.status}`, data };
    }
    return { ok: true, data };
  } catch (err: any) {
    return { ok: false, error: err?.message || 'fetch failed' };
  }
}

/**
 * GET /v1/forge/usage?period=24h|7d|30d
 * Proxies /api/usage on Forge with the internal API key.
 */
router.get('/usage', async (req, res) => {
  const period = typeof req.query.period === 'string' ? req.query.period : '24h';
  const allowed = new Set(['24h', '7d', '30d']);
  if (!allowed.has(period)) {
    res.status(400).json({ error: 'period must be one of 24h, 7d, 30d' });
    return;
  }
  const result = await forgeFetch(`/api/usage?period=${encodeURIComponent(period)}`);
  if (!result.ok) {
    logger.logMinimal(`GET /v1/forge/usage failed: ${result.error}`);
    res.status(result.status ?? 502).json({ error: result.error, detail: result.data });
    return;
  }
  res.json(result.data);
});

/**
 * GET /v1/forge/backends
 * Proxies /health/backends — no auth needed on Forge side but we proxy
 * anyway so Chancery has one consistent entry point for all Forge data.
 */
router.get('/backends', async (_req, res) => {
  const result = await forgeFetch('/health/backends');
  if (!result.ok) {
    res.status(result.status ?? 502).json({ error: result.error, detail: result.data });
    return;
  }
  res.json(result.data);
});

/**
 * GET /v1/forge/models
 * Proxies /v1/models.
 */
router.get('/models', async (_req, res) => {
  const result = await forgeFetch('/v1/models');
  if (!result.ok) {
    res.status(result.status ?? 502).json({ error: result.error, detail: result.data });
    return;
  }
  res.json(result.data);
});

export default router;
