/**
 * Platform Mode service.
 *
 * Single source of truth for the platform-wide mode flag stored in
 * `platform_modes` (singleton row id=1). Used by the worker loops,
 * log-monitor, nexus-api, Chancery, and the health watchdog to decide
 * whether the platform is currently in Creative Mode (always-on inference
 * services intentionally shut down for image/video generation work).
 *
 * Behavior:
 *   - Caches the result for CACHE_TTL_MS (15s) to avoid hammering Postgres
 *     from poll loops. Creative mode transitions are human-triggered events
 *     that happen minutes/hours apart, so 15s of staleness is fine.
 *   - Silently degrades if the table doesn't exist (pre-migration 046),
 *     returning mode='normal'. Callers never have to handle errors.
 *   - isCreativeMode() is the fast-path shortcut for the common case.
 */

import { getPool } from '../db.js';
import { createLogger } from '../logger.js';

const logger = createLogger('platform-mode');

export type PlatformModeName = 'normal' | 'creative';

export interface PlatformModeSnapshot {
  mode: PlatformModeName;
  started_at: Date | null;
  started_by: string | null;
  reason: string | null;
  /** Milliseconds since the current mode started. null if started_at is unknown. */
  elapsed_ms: number | null;
}

export interface PlatformModeHistoryRow {
  id: number;
  mode: PlatformModeName;
  started_at: Date;
  ended_at: Date | null;
  started_by: string | null;
  reason: string | null;
  duration_sec: number | null;
}

const CACHE_TTL_MS = 15_000;
let cached: { snapshot: PlatformModeSnapshot; fetchedAt: number } | null = null;

const NORMAL: PlatformModeSnapshot = {
  mode: 'normal',
  started_at: null,
  started_by: null,
  reason: null,
  elapsed_ms: null,
};

/**
 * Get the current platform mode. Cached for 15 seconds.
 * Falls back to 'normal' if the platform_modes table is missing or the
 * query fails — callers should never have to catch.
 */
export async function getPlatformMode(options?: { bypassCache?: boolean }): Promise<PlatformModeSnapshot> {
  const now = Date.now();
  if (!options?.bypassCache && cached && now - cached.fetchedAt < CACHE_TTL_MS) {
    return cached.snapshot;
  }

  try {
    const pool = getPool();
    const r = await pool.query<{
      mode: PlatformModeName;
      started_at: Date | null;
      started_by: string | null;
      reason: string | null;
    }>(`SELECT mode, started_at, started_by, reason FROM platform_modes WHERE id = 1`);

    if (r.rows.length === 0) {
      cached = { snapshot: NORMAL, fetchedAt: now };
      return NORMAL;
    }

    const row = r.rows[0];
    const snapshot: PlatformModeSnapshot = {
      mode: row.mode,
      started_at: row.started_at,
      started_by: row.started_by,
      reason: row.reason,
      elapsed_ms: row.started_at ? now - new Date(row.started_at).getTime() : null,
    };
    cached = { snapshot, fetchedAt: now };
    return snapshot;
  } catch (e: any) {
    // Table missing (pre-migration 046) — behave as 'normal' and don't spam logs.
    if (!/relation .*platform_modes.* does not exist/i.test(e.message || '')) {
      logger.logMinimal('getPlatformMode failed, defaulting to normal:', e.message);
    }
    cached = { snapshot: NORMAL, fetchedAt: now };
    return NORMAL;
  }
}

/** Fast-path check for the common case. */
export async function isCreativeMode(): Promise<boolean> {
  const m = await getPlatformMode();
  return m.mode === 'creative';
}

/** Clear the cache. Use this immediately after a mode flip. */
export function invalidatePlatformModeCache(): void {
  cached = null;
}

/**
 * Recent history entries (most-recent first). For the Chancery banner and
 * the /v1/platform/mode endpoint. Safe against missing tables.
 */
export async function getPlatformModeHistory(limit = 20): Promise<PlatformModeHistoryRow[]> {
  try {
    const pool = getPool();
    const r = await pool.query<PlatformModeHistoryRow>(
      `SELECT id, mode, started_at, ended_at, started_by, reason, duration_sec
       FROM platform_mode_history
       ORDER BY started_at DESC
       LIMIT $1`,
      [limit],
    );
    return r.rows;
  } catch (e: any) {
    if (!/relation .*platform_mode_history.* does not exist/i.test(e.message || '')) {
      logger.logMinimal('getPlatformModeHistory failed:', e.message);
    }
    return [];
  }
}

/**
 * Format an elapsed-ms value for human-readable display.
 * 45s -> "45s"   150s -> "2m"   3700s -> "1h 1m"
 */
export function formatElapsed(ms: number | null): string {
  if (ms == null) return 'unknown';
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  const remM = m % 60;
  return remM > 0 ? `${h}h ${remM}m` : `${h}h`;
}
