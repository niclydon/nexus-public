/**
 * Context Accumulate handler — Gate 1.
 *
 * Checks all data sources for changes, creates context_snapshots, then
 * automatically chains to Gate 2 (significance-check) if changes found.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { accumulateContext } from '../lib/proactive/context-accumulator.js';
import {
  getFastTrackSettings,
  shouldRegenerate,
  buildContextSummary,
  regenerateAdaptivePatterns,
} from '../lib/proactive/adaptive-fast-track.js';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('context-accumulate');

export async function handleContextAccumulate(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { sources?: string[] };
  logger.log(`Starting context accumulation...`);
  await jobLog(job.id, 'Scanning data sources for changes...');

  const result = await accumulateContext(payload.sources);
  const pool = getPool();

  // If changes detected, enqueue one significance check per snapshot (Gate 2)
  // Each source gets its own Gate 2 job so that attribution is clean —
  // Gate 3 only sees the specific source that was significant, not an
  // unrelated bundle of changes that happened in the same 5-minute window.
  if (result.snapshot_ids.length > 0) {
    for (const snapshotId of result.snapshot_ids) {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
         VALUES ($1, $2, $3, $4)`,
        [
          'significance-check',
          JSON.stringify({ snapshot_ids: [snapshotId] }),
          1, // Higher priority than regular jobs
          2,
        ]
      );
    }
    logger.log(`Enqueued ${result.snapshot_ids.length} significance check(s) (1 per source)`);
    await jobLog(job.id, `${result.changed} source(s) changed → ${result.snapshot_ids.length} snapshot(s) → ${result.snapshot_ids.length} Gate 2 job(s)`);
  } else {
    await jobLog(job.id, `Checked ${result.checked} source(s), no changes detected`);
  }

  // Regenerate adaptive fast-track patterns if needed
  try {
    const ftSettings = await getFastTrackSettings(pool);
    if (shouldRegenerate(ftSettings)) {
      const contextSummary = await buildContextSummary(pool);
      await regenerateAdaptivePatterns(pool, contextSummary);
      logger.log('Regenerated adaptive fast-track patterns');
    }
  } catch (err) {
    logger.logMinimal('Fast-track pattern regeneration failed:', err instanceof Error ? err.message : err);
  }

  // Self-chain: run again in 5 minutes
  try {
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
       VALUES ('context-accumulate', '{}', 'nexus', 1, 3, NOW() + INTERVAL '300 seconds')`,
    );
  } catch (err) {
    logger.logMinimal('Failed to self-chain context-accumulate:', (err as Error).message);
  }

  return {
    checked: result.checked,
    changed: result.changed,
    snapshot_ids: result.snapshot_ids,
  };
}
