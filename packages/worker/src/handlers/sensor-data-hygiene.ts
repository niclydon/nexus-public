/**
 * Sensor Data Hygiene + Data Retention handler.
 *
 * Daily audit and cleanup of:
 *
 * Sensor data (useless entries):
 *   - device_activity: rows where all activity fields are false (unknown state)
 *   - device_location: rows with zero/null coordinates or very low accuracy
 *   - health_data: rows with null/zero values
 *
 * Audit/log table retention (when auto_clean=true):
 *   - job_log: 7-day retention
 *   - tempo_jobs (completed/failed): 7-day retention
 *   - event_log: 30-day retention
 *   - llm_usage: 30-day retention
 *   - core_memory (superseded): 90-day post-supersession retention
 *   - proactive_insights (expired): delete on expiry
 *
 * Generates a report summarizing findings and cleanup actions.
 * Delivered via the unified report system.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { deliverReport } from '../lib/report-delivery.js';

const logger = createLogger('sensor-hygiene');

interface HygieneFindings {
  unknownActivity: { count: number; oldestDate: string | null; newestDate: string | null };
  badLocations: { count: number; zeroCoords: number; lowAccuracy: number };
  emptyHealth: { count: number; byType: Record<string, number> };
  cleaned: { activity: number; location: number; health: number };
  retention: {
    jobLog: number;
    tempoJobs: number;
    eventLog: number;
    llmUsage: number;
    coreMemory: number;
    proactiveInsights: number;
  };
}

export async function handleSensorDataHygiene(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const autoClean = job.payload?.auto_clean === true;
  logger.log(`Starting audit (auto_clean=${autoClean})`);

  const findings: HygieneFindings = {
    unknownActivity: { count: 0, oldestDate: null, newestDate: null },
    badLocations: { count: 0, zeroCoords: 0, lowAccuracy: 0 },
    emptyHealth: { count: 0, byType: {} },
    cleaned: { activity: 0, location: 0, health: 0 },
    retention: { jobLog: 0, tempoJobs: 0, eventLog: 0, llmUsage: 0, coreMemory: 0, proactiveInsights: 0 },
  };

  // 1. Check device_activity for unknown entries (all activity fields false)
  try {
    const { rows } = await pool.query<{ cnt: string; oldest: string; newest: string }>(
      `SELECT COUNT(*) as cnt,
              MIN(timestamp) as oldest,
              MAX(timestamp) as newest
       FROM device_activity
       WHERE stationary = false AND walking = false AND running = false
         AND cycling = false AND automotive = false`
    );
    findings.unknownActivity.count = parseInt(rows[0]?.cnt ?? '0', 10);
    findings.unknownActivity.oldestDate = rows[0]?.oldest ?? null;
    findings.unknownActivity.newestDate = rows[0]?.newest ?? null;

    // Auto-clean: delete unknown entries older than 7 days
    if (autoClean && findings.unknownActivity.count > 0) {
      const { rowCount } = await pool.query(
        `DELETE FROM device_activity
         WHERE stationary = false AND walking = false AND running = false
           AND cycling = false AND automotive = false
           AND timestamp < NOW() - INTERVAL '7 days'`
      );
      findings.cleaned.activity = rowCount ?? 0;
    }
  } catch (err) {
    logger.logMinimal('Failed to audit device_activity:', err);
  }

  // 2. Check device_location for bad entries
  try {
    const { rows } = await pool.query<{ total: string; zero: string; low_acc: string }>(
      `SELECT
         COUNT(*) FILTER (WHERE latitude = 0 AND longitude = 0) as zero,
         COUNT(*) FILTER (WHERE accuracy > 1000) as low_acc,
         COUNT(*) FILTER (WHERE latitude = 0 AND longitude = 0 OR accuracy > 1000) as total
       FROM device_location`
    );
    findings.badLocations.count = parseInt(rows[0]?.total ?? '0', 10);
    findings.badLocations.zeroCoords = parseInt(rows[0]?.zero ?? '0', 10);
    findings.badLocations.lowAccuracy = parseInt(rows[0]?.low_acc ?? '0', 10);

    if (autoClean && findings.badLocations.count > 0) {
      const { rowCount } = await pool.query(
        `DELETE FROM device_location
         WHERE (latitude = 0 AND longitude = 0 OR accuracy > 1000)
           AND created_at < NOW() - INTERVAL '7 days'`
      );
      findings.cleaned.location = rowCount ?? 0;
    }
  } catch (err) {
    logger.logMinimal('Failed to audit device_location:', err);
  }

  // 3. Check health_data for empty/zero values
  try {
    const { rows } = await pool.query<{ data_type: string; cnt: string }>(
      `SELECT data_type, COUNT(*) as cnt
       FROM health_data
       WHERE value IS NULL OR value::text = '0' OR value::text = ''
       GROUP BY data_type`
    );
    for (const r of rows) {
      findings.emptyHealth.byType[r.data_type] = parseInt(r.cnt, 10);
      findings.emptyHealth.count += parseInt(r.cnt, 10);
    }

    if (autoClean && findings.emptyHealth.count > 0) {
      const { rowCount } = await pool.query(
        `DELETE FROM health_data
         WHERE (value IS NULL OR value::text = '0' OR value::text = '')
           AND created_at < NOW() - INTERVAL '7 days'`
      );
      findings.cleaned.health = rowCount ?? 0;
    }
  } catch (err) {
    logger.logMinimal('Failed to audit health_data:', err);
  }

  // 4. Audit/log table retention (always runs when auto_clean is true)
  if (autoClean) {
    logger.logVerbose('Running audit table retention...');

    // job_log: 7-day retention
    try {
      const { rowCount } = await pool.query(
        `DELETE FROM job_log WHERE created_at < NOW() - INTERVAL '7 days'`
      );
      findings.retention.jobLog = rowCount ?? 0;
      if (findings.retention.jobLog > 0) logger.log(`Pruned ${findings.retention.jobLog} job_log rows (>7d)`);
    } catch (err) { logger.logMinimal('Failed to prune job_log:', err); }

    // tempo_jobs completed/failed: 7-day retention
    try {
      const { rowCount } = await pool.query(
        `DELETE FROM tempo_jobs WHERE status IN ('completed', 'failed') AND COALESCE(completed_at, failed_at, created_at) < NOW() - INTERVAL '7 days'`
      );
      findings.retention.tempoJobs = rowCount ?? 0;
      if (findings.retention.tempoJobs > 0) logger.log(`Pruned ${findings.retention.tempoJobs} tempo_jobs rows (>7d)`);
    } catch (err) { logger.logMinimal('Failed to prune tempo_jobs:', err); }

    // event_log: 30-day retention
    try {
      const { rowCount } = await pool.query(
        `DELETE FROM event_log WHERE created_at < NOW() - INTERVAL '30 days'`
      );
      findings.retention.eventLog = rowCount ?? 0;
      if (findings.retention.eventLog > 0) logger.log(`Pruned ${findings.retention.eventLog} event_log rows (>30d)`);
    } catch (err) { logger.logMinimal('Failed to prune event_log:', err); }

    // llm_usage: 30-day retention
    try {
      const { rowCount } = await pool.query(
        `DELETE FROM llm_usage WHERE created_at < NOW() - INTERVAL '30 days'`
      );
      findings.retention.llmUsage = rowCount ?? 0;
      if (findings.retention.llmUsage > 0) logger.log(`Pruned ${findings.retention.llmUsage} llm_usage rows (>30d)`);
    } catch (err) { logger.logMinimal('Failed to prune llm_usage:', err); }

    // core_memory superseded: 90-day post-supersession retention
    try {
      const { rowCount } = await pool.query(
        `DELETE FROM core_memory WHERE superseded_by IS NOT NULL AND updated_at < NOW() - INTERVAL '90 days'`
      );
      findings.retention.coreMemory = rowCount ?? 0;
      if (findings.retention.coreMemory > 0) logger.log(`Pruned ${findings.retention.coreMemory} superseded core_memory rows (>90d)`);
    } catch (err) { logger.logMinimal('Failed to prune core_memory:', err); }

    // proactive_insights expired: delete on expiry
    try {
      const { rowCount } = await pool.query(
        `DELETE FROM proactive_insights WHERE expires_at IS NOT NULL AND expires_at < NOW()`
      );
      findings.retention.proactiveInsights = rowCount ?? 0;
      if (findings.retention.proactiveInsights > 0) logger.log(`Pruned ${findings.retention.proactiveInsights} expired proactive_insights`);
    } catch (err) { logger.logMinimal('Failed to prune proactive_insights:', err); }
  }

  // 5. Build report
  const totalIssues = findings.unknownActivity.count + findings.badLocations.count + findings.emptyHealth.count;
  const totalCleaned = findings.cleaned.activity + findings.cleaned.location + findings.cleaned.health;
  const totalRetention = Object.values(findings.retention).reduce((a, b) => a + b, 0);

  const lines: string[] = [];
  lines.push(`Sensor Data Hygiene & Retention Report`);
  lines.push(`Sensor issues found: ${totalIssues}`);
  if (autoClean) lines.push(`Sensor records cleaned (>7 days old): ${totalCleaned}`);
  if (autoClean) lines.push(`Audit/log records pruned: ${totalRetention}`);
  lines.push('');

  // Activity
  lines.push(`DEVICE ACTIVITY`);
  if (findings.unknownActivity.count > 0) {
    lines.push(`  ${findings.unknownActivity.count} "unknown" entries (all activity fields false)`);
    if (findings.unknownActivity.oldestDate) lines.push(`  Oldest: ${new Date(findings.unknownActivity.oldestDate).toLocaleDateString()}`);
    if (findings.unknownActivity.newestDate) lines.push(`  Newest: ${new Date(findings.unknownActivity.newestDate).toLocaleDateString()}`);
    if (findings.cleaned.activity > 0) lines.push(`  Cleaned: ${findings.cleaned.activity} old records`);
  } else {
    lines.push('  Clean -- no unknown entries');
  }
  lines.push('');

  // Location
  lines.push(`DEVICE LOCATION`);
  if (findings.badLocations.count > 0) {
    lines.push(`  ${findings.badLocations.count} problematic entries`);
    if (findings.badLocations.zeroCoords > 0) lines.push(`  Zero coordinates: ${findings.badLocations.zeroCoords}`);
    if (findings.badLocations.lowAccuracy > 0) lines.push(`  Low accuracy (>1km): ${findings.badLocations.lowAccuracy}`);
    if (findings.cleaned.location > 0) lines.push(`  Cleaned: ${findings.cleaned.location} old records`);
  } else {
    lines.push('  Clean -- no problematic entries');
  }
  lines.push('');

  // Health
  lines.push(`HEALTH DATA`);
  if (findings.emptyHealth.count > 0) {
    lines.push(`  ${findings.emptyHealth.count} empty/zero entries`);
    for (const [type, count] of Object.entries(findings.emptyHealth.byType)) {
      lines.push(`  ${type}: ${count}`);
    }
    if (findings.cleaned.health > 0) lines.push(`  Cleaned: ${findings.cleaned.health} old records`);
  } else {
    lines.push('  Clean -- no empty entries');
  }

  // Retention
  if (autoClean && totalRetention > 0) {
    lines.push('');
    lines.push('DATA RETENTION');
    if (findings.retention.jobLog > 0) lines.push(`  job_log: ${findings.retention.jobLog} rows pruned (>7d)`);
    if (findings.retention.tempoJobs > 0) lines.push(`  tempo_jobs: ${findings.retention.tempoJobs} rows pruned (>7d)`);
    if (findings.retention.eventLog > 0) lines.push(`  event_log: ${findings.retention.eventLog} rows pruned (>30d)`);
    if (findings.retention.llmUsage > 0) lines.push(`  llm_usage: ${findings.retention.llmUsage} rows pruned (>30d)`);
    if (findings.retention.coreMemory > 0) lines.push(`  core_memory: ${findings.retention.coreMemory} superseded rows pruned (>90d)`);
    if (findings.retention.proactiveInsights > 0) lines.push(`  proactive_insights: ${findings.retention.proactiveInsights} expired rows pruned`);
  }

  const reportBody = lines.join('\n');
  const title = totalIssues > 0 || totalRetention > 0
    ? `Data Hygiene: ${totalIssues} sensor issue${totalIssues !== 1 ? 's' : ''}${totalRetention > 0 ? `, ${totalRetention} audit rows pruned` : ''}`
    : 'Data Hygiene: All clean';

  // Deliver report
  const deliveredVia = await deliverReport({
    reportType: 'sensor-data-hygiene',
    title,
    body: reportBody,
    category: 'sensor_hygiene',
    metadata: findings as unknown as Record<string, unknown>,
  });

  logger.log(`Report delivered via: ${deliveredVia.join(', ') || 'none'}`);

  return {
    delivered_via: deliveredVia,
    total_issues: totalIssues,
    total_cleaned: totalCleaned,
    total_retention: totalRetention,
    findings,
  };
}
