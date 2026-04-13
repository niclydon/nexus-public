/**
 * Looki Daily Sync — pulls yesterday's moments from the Looki API.
 * Runs once per day. Stores in looki_moments table.
 * Self-chains every 24 hours.
 */
import { getPool, createLogger } from '@nexus/core';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/looki-daily-sync');

const INTERVAL_SECONDS = 24 * 60 * 60; // 24 hours

export async function handleLookiDailySync(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const apiKey = process.env.LOOKI_API_KEY;
  const baseUrl = process.env.LOOKI_BASE_URL || 'https://open.looki.ai/api/v1';

  if (!apiKey) {
    return { status: 'skipped', reason: 'LOOKI_API_KEY not configured' };
  }

  try {
    // Pull yesterday's date
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const dateStr = yesterday.toISOString().split('T')[0];

    logger.log(`Syncing Looki moments for ${dateStr}`);

    const resp = await fetch(`${baseUrl}/moments?on_date=${dateStr}`, {
      headers: { 'X-API-Key': apiKey, Accept: 'application/json' },
      signal: AbortSignal.timeout(15_000),
    });

    if (!resp.ok) {
      logger.logMinimal(`Looki API error: ${resp.status}`);
      return { error: `HTTP ${resp.status}`, checked: true };
    }

    const data = await resp.json() as { code: number; data?: Record<string, unknown>[] };
    if (data.code !== 0) {
      return { error: `API code ${data.code}`, checked: true };
    }

    const moments = data.data ?? [];
    let inserted = 0;

    for (const m of moments) {
      const mid = String(m.id ?? '');
      if (!mid) continue;

      const cover = m.cover_file as Record<string, unknown> | undefined;
      const location = cover?.location ? String(cover.location) : null;

      let duration: number | null = null;
      if (m.start_time && m.end_time) {
        try {
          const s = new Date(String(m.start_time)).getTime();
          const e = new Date(String(m.end_time)).getTime();
          duration = Math.round((e - s) / 1000);
        } catch { /* ignore */ }
      }

      const { rowCount } = await pool.query(
        `INSERT INTO looki_moments (id, title, description, start_time, duration_seconds, location, synced_at)
         VALUES ($1, $2, $3, $4, $5, $6, NOW())
         ON CONFLICT (id) DO UPDATE SET
           title = COALESCE(EXCLUDED.title, looki_moments.title),
           description = COALESCE(EXCLUDED.description, looki_moments.description),
           synced_at = NOW()`,
        [mid, m.title ?? '', m.description ?? '', m.start_time ?? null, duration, location ? JSON.stringify({ raw: location }) : null],
      );
      if (rowCount && rowCount > 0) inserted++;
    }

    logger.log(`Looki daily sync: ${inserted} moments for ${dateStr}`);

    return {
      checked: true,
      date: dateStr,
      moments_found: moments.length,
      inserted,
    };
  } catch (err) {
    logger.logMinimal('Looki daily sync error:', (err as Error).message);
    return { error: (err as Error).message, checked: false };
  } finally {
    // Self-chain with pile-up guard — skip if one is already pending.
    try {
      const existing = await pool.query(
        `SELECT id FROM tempo_jobs WHERE job_type = 'looki-daily-sync' AND status = 'pending' LIMIT 1`,
      );
      if (existing.rows.length === 0) {
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
           VALUES ('looki-daily-sync', '{}', 'nexus', 1, 3, NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
        );
      }
    } catch (err) {
      logger.logMinimal('Failed to self-chain:', (err as Error).message);
    }
  }
}
