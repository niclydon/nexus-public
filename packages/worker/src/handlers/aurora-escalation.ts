/**
 * AURORA escalation handler (fast lane).
 * Re-checks critical anomalies and sends push notification if they persist.
 */
import { query, getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/aurora-escalation');

export async function handleAuroraEscalation(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as {
    trigger?: string;
    metric?: string;
    deviation_sigma?: number;
  };

  if (!payload?.trigger || !payload?.metric || payload.deviation_sigma === undefined) {
    logger.logMinimal('Missing required payload fields');
    await jobLog(job.id, 'Missing payload', 'error');
    return { status: 'error', message: 'Missing required payload fields' };
  }

  logger.log(`Escalation: ${payload.trigger} — ${payload.metric} at ${payload.deviation_sigma.toFixed(1)}σ`);

  let stillAnomalous = false;
  let currentZ = 0;

  try {
    const [dimension] = payload.metric.split('/');
    const { rows } = await query<{ deviation_from_baseline: unknown }>(
      `SELECT deviation_from_baseline FROM aurora_signatures
       WHERE dimension = $1 AND deviation_from_baseline IS NOT NULL
       ORDER BY date DESC LIMIT 1`,
      [dimension],
    );

    if (rows.length > 0) {
      const metricKey = payload.metric.split('/')[1];
      const deviation = typeof rows[0].deviation_from_baseline === 'string'
        ? JSON.parse(rows[0].deviation_from_baseline)
        : rows[0].deviation_from_baseline;

      if (deviation[metricKey]) {
        currentZ = deviation[metricKey].z_score;
        stillAnomalous = Math.abs(currentZ) >= 2.5;
      }
    }
  } catch (err) {
    logger.logMinimal(`Re-check failed: ${(err as Error).message}`);
  }

  let notified = false;
  if (stillAnomalous) {
    logger.log(`Anomaly persists: ${payload.metric} at ${currentZ.toFixed(1)}σ — sending notification`);
    try {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
         VALUES ('push-notification', $1, 8, 1)`,
        [JSON.stringify({
          title: 'AURORA Anomaly Alert',
          body: `${payload.metric.replace('/', ': ')} is ${Math.abs(currentZ).toFixed(1)}σ from baseline (${payload.trigger})`,
          category: 'aurora_anomaly',
          data: { metric: payload.metric, z_score: currentZ, trigger: payload.trigger },
        })],
      );
      notified = true;
    } catch (err) {
      logger.logMinimal(`Push notification failed: ${(err as Error).message}`);
    }
  } else {
    logger.log(`Anomaly resolved: ${payload.metric} now at ${currentZ.toFixed(1)}σ`);
  }

  await jobLog(job.id, `${payload.metric} ${stillAnomalous ? 'persists' : 'resolved'} at ${currentZ.toFixed(1)}σ${notified ? ', notification sent' : ''}`);

  return {
    trigger: payload.trigger,
    metric: payload.metric,
    original_sigma: payload.deviation_sigma,
    current_sigma: currentZ,
    still_anomalous: stillAnomalous,
    notified,
  };
}
