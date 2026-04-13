/**
 * Significance Check handler — Gate 2.
 *
 * Uses Gemini Flash-Lite to classify snapshot significance, then
 * automatically chains to Gate 3 (anticipation-analyze) for
 * significant/urgent changes.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { classifySignificance } from '../lib/proactive/gemini-gate.js';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('significance-check');

export async function handleSignificanceCheck(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { snapshot_ids: string[]; fast_tracked?: boolean };
  logger.log(`Classifying ${payload.snapshot_ids.length} snapshot(s)...`);
  await jobLog(job.id, `Classifying ${payload.snapshot_ids.length} snapshot(s) via Gemini Flash-Lite`);

  const results = await classifySignificance(payload.snapshot_ids);

  const pool = getPool();

  // Filter for significant/urgent — these go to Gate 3
  const significant = results.filter(
    r => r.significance === 'significant' || r.significance === 'urgent'
  );

  if (significant.length > 0) {
    // Load current risk level for the engine
    const { rows } = await pool.query(
      `SELECT value FROM proactive_settings WHERE key = 'risk_level'`
    );
    const riskLevel = (rows[0]?.value as string) ?? 'medium';

    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
       VALUES ($1, $2, $3, $4)`,
      [
        'anticipation-analyze',
        JSON.stringify({
          snapshot_ids: significant.map(s => s.snapshot_id),
          risk_level: riskLevel,
          ...(payload.fast_tracked && { fast_tracked: true }),
        }),
        payload.fast_tracked ? 3 : 2,
        2,
      ]
    );
    logger.log(`Enqueued analysis for ${significant.length} significant snapshot(s)`);
    await jobLog(job.id, `${significant.length}/${results.length} significant → chained to Gate 3`);
  } else {
    await jobLog(job.id, `0/${results.length} significant — nothing to escalate`);
  }

  // Notable-tier accumulation: track per-source notable counts over rolling window.
  // When a source accumulates 3+ notables in 7 days, promote to full analysis.
  const notables = results.filter(r => r.significance === 'notable');
  if (notables.length > 0) {
    for (const notable of notables) {
      // Count recent notables from same source
      const { rows: countRows } = await pool.query<{ cnt: number }>(`
        SELECT COUNT(*)::int as cnt FROM context_snapshots
        WHERE source = (SELECT source FROM context_snapshots WHERE id = $1)
          AND significance_level = 'notable'
          AND created_at > NOW() - INTERVAL '7 days'
      `, [notable.snapshot_id]);

      const recentNotables = countRows[0]?.cnt ?? 0;

      if (recentNotables >= 3) {
        // Promote: this source has accumulated enough notables to warrant full analysis
        const { rows: sourceRows } = await pool.query<{ source: string }>(
          `SELECT source FROM context_snapshots WHERE id = $1`, [notable.snapshot_id],
        );
        const source = sourceRows[0]?.source ?? 'unknown';

        // Get the recent notable snapshot IDs for this source
        const { rows: snapIds } = await pool.query<{ id: string }>(`
          SELECT id FROM context_snapshots
          WHERE source = $1 AND significance_level = 'notable'
            AND created_at > NOW() - INTERVAL '7 days'
          ORDER BY created_at DESC LIMIT 5
        `, [source]);

        const { rows: riskRows } = await pool.query(
          `SELECT value FROM proactive_settings WHERE key = 'risk_level'`
        );

        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
           VALUES ('anticipation-analyze', $1, 1, 2)`,
          [JSON.stringify({
            snapshot_ids: snapIds.map(r => r.id),
            risk_level: (riskRows[0]?.value as string) ?? 'medium',
            promoted_from_notable: true,
          })],
        );

        logger.log(`Notable promotion: source "${source}" accumulated ${recentNotables} notables in 7d → Gate 3`);
        await jobLog(job.id, `Notable promotion: "${source}" (${recentNotables} notables) → Gate 3`);
      }
    }
  }

  return {
    classified: results.length,
    insignificant: results.filter(r => r.significance === 'insignificant').length,
    notable: notables.length,
    significant: significant.length,
  };
}
