/**
 * Data source health check — monitors sync recency and Google OAuth token expiry.
 */
import { query, getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/data-source-health');

interface SourceRow {
  source_key: string;
  display_name: string;
  status: string;
  last_sync_at: string | null;
  avg_sync_interval_sec: number | null;
  sync_method: string | null;
  target_table: string | null;
  auth_status: string;
  enabled: boolean;
}

const GOOGLE_SOURCES = ['gmail', 'google_calendar', 'google_voice'];

export async function handleDataSourceHealth(job: TempoJob): Promise<Record<string, unknown>> {
  logger.log(`Starting data-source-health check (${job.id.slice(0, 8)})`);

  const { rows: sources } = await query<SourceRow>(
    `SELECT source_key, display_name, status, last_sync_at, avg_sync_interval_sec,
            sync_method, target_table, auth_status, enabled
     FROM data_source_registry
     WHERE enabled = true`,
  );

  logger.logVerbose(`Checking ${sources.length} enabled data sources`);

  let degradedCount = 0;
  let errorCount = 0;
  let activeCount = 0;
  const issues: string[] = [];

  for (const src of sources) {
    if (!src.avg_sync_interval_sec || !src.last_sync_at) {
      if (src.target_table && !src.last_sync_at) {
        await checkTargetTableRecency(src, issues);
      }
      continue;
    }

    const elapsedSec = (Date.now() - new Date(src.last_sync_at).getTime()) / 1000;
    const ratio = elapsedSec / src.avg_sync_interval_sec;

    if (ratio >= 10) {
      const reason = `No sync in ${formatDuration(elapsedSec)} (expected every ${formatDuration(src.avg_sync_interval_sec)})`;
      await query(
        `UPDATE data_source_registry
         SET status = 'error', last_error = $2, last_error_at = NOW(), updated_at = NOW()
         WHERE source_key = $1 AND enabled = true AND status != 'disabled'`,
        [src.source_key, reason],
      );
      errorCount++;
      issues.push(`${src.display_name}: ${reason}`);
    } else if (ratio >= 3) {
      const reason = `Sync overdue: ${formatDuration(elapsedSec)} since last (expected every ${formatDuration(src.avg_sync_interval_sec)})`;
      await query(
        `UPDATE data_source_registry
         SET status = CASE WHEN status = 'error' THEN 'error' ELSE 'degraded' END,
             last_error = $2, last_error_at = NOW(), updated_at = NOW()
         WHERE source_key = $1 AND enabled = true AND status != 'disabled'`,
        [src.source_key, reason],
      );
      degradedCount++;
      issues.push(`${src.display_name}: ${reason}`);
    } else {
      activeCount++;
    }
  }

  await checkGoogleAuth(sources, issues);

  // Reset daily counters
  await query(
    `UPDATE data_source_registry
     SET items_synced_today = 0, items_synced_today_reset_at = CURRENT_DATE
     WHERE items_synced_today_reset_at < CURRENT_DATE`,
  );

  const summary = `active=${activeCount}, degraded=${degradedCount}, error=${errorCount}`;
  logger.log(`Complete: ${summary}`);

  await logEvent({
    action: `Data source health: ${summary}`,
    component: 'data-source-health',
    category: 'system',
    metadata: { active: activeCount, degraded: degradedCount, error: errorCount, issues },
  });

  return { active: activeCount, degraded: degradedCount, error: errorCount, issues, checked_at: new Date().toISOString() };
}

async function checkTargetTableRecency(src: SourceRow, issues: string[]): Promise<void> {
  if (!src.target_table) return;
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(src.target_table)) return;

  try {
    const { rows } = await query<{ latest: string | null }>(
      `SELECT MAX(created_at) as latest FROM "${src.target_table}"`,
    );

    if (rows[0]?.latest) {
      const hoursAgo = (Date.now() - new Date(rows[0].latest).getTime()) / 3_600_000;
      if (hoursAgo < 24) {
        await query(
          `UPDATE data_source_registry
           SET status = 'active', last_sync_at = $2, updated_at = NOW()
           WHERE source_key = $1 AND status = 'unknown'`,
          [src.source_key, rows[0].latest],
        );
        logger.logVerbose(`${src.source_key}: bootstrapped from ${src.target_table}`);
      }
    }
  } catch {
    // Table might not have created_at — non-fatal
  }
}

async function checkGoogleAuth(sources: SourceRow[], issues: string[]): Promise<void> {
  const googleSources = sources.filter(s => GOOGLE_SOURCES.includes(s.source_key));
  if (googleSources.length === 0) return;

  try {
    const { rows } = await query<{ expiry_date: string | null }>(
      `SELECT expiry_date FROM google_tokens LIMIT 1`,
    );

    if (rows.length === 0) {
      for (const src of googleSources) {
        await query(
          `UPDATE data_source_registry SET auth_status = 'expired', updated_at = NOW() WHERE source_key = $1`,
          [src.source_key],
        );
      }
      issues.push('Google OAuth: no tokens found');
      return;
    }

    const expiry = rows[0].expiry_date ? new Date(rows[0].expiry_date) : null;
    const isExpired = expiry ? expiry.getTime() < Date.now() : false;

    for (const src of googleSources) {
      const newStatus = isExpired ? 'expired' : 'valid';
      if (src.auth_status !== newStatus) {
        await query(
          `UPDATE data_source_registry SET auth_status = $2, auth_expires_at = $3, updated_at = NOW() WHERE source_key = $1`,
          [src.source_key, newStatus, expiry],
        );
        if (isExpired) issues.push(`${src.display_name}: Google OAuth token expired`);
      }
    }
  } catch {
    // Non-fatal
  }
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
  return `${(seconds / 86400).toFixed(1)}d`;
}
