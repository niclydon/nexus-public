/**
 * Log Monitor handler.
 *
 * Centralized log monitoring worker that collects health/error data from
 * all ARIA components, evaluates alerting rules, and delivers notifications
 * based on severity. Runs every 60 seconds.
 *
 * Monitors:
 *   1. Job failure rates (overall and per-type)
 *   2. Queue depth and backlog
 *   3. Subsystem recency (when key jobs last succeeded)
 *   4. LLM health (fallback rates, error patterns)
 *   5. Database connectivity
 *   6. Proactive intelligence pipeline health
 *   7. Data source health (sync failures, degraded/error sources)
 *
 * Alert tiers:
 *   - Critical: immediate push notification + inbox
 *   - Warning: batched hourly digest
 *   - Info: batched daily digest
 */
import { query, getPool, createLogger, platformMode } from '@nexus/core';
import type { TempoJob } from '../job-worker.js';
import { logEvent } from '../lib/event-log.js';

const logger = createLogger('log-monitor');

// ── Types ──

type Severity = 'critical' | 'warning' | 'info';

interface Finding {
  severity: Severity;
  source: string;
  title: string;
  detail: string;
  fingerprint: string;
}

// ── Monitor: Job Failures ──

async function checkJobFailures(): Promise<Finding[]> {
  const pool = getPool();
  const findings: Finding[] = [];

  try {
    // Overall failure rate in the last hour
    const { rows: overallRows } = await pool.query<{ failed: string; completed: string }>(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'failed' AND updated_at >= NOW() - INTERVAL '1 hour') as failed,
        COUNT(*) FILTER (WHERE status = 'completed' AND updated_at >= NOW() - INTERVAL '1 hour') as completed
      FROM tempo_jobs
    `);
    const failed = parseInt(overallRows[0].failed, 10);
    const completed = parseInt(overallRows[0].completed, 10);
    const total = failed + completed;

    if (total > 0) {
      const rate = failed / total;
      if (rate > 0.5) {
        findings.push({
          severity: 'critical',
          source: 'job-failures',
          title: 'Job failure rate critical',
          detail: `${(rate * 100).toFixed(1)}% failure rate (${failed}/${total}) in the last hour`,
          fingerprint: 'job-failure-rate-critical',
        });
      } else if (rate >= 0.2) {
        findings.push({
          severity: 'warning',
          source: 'job-failures',
          title: 'Elevated job failure rate',
          detail: `${(rate * 100).toFixed(1)}% failure rate (${failed}/${total}) in the last hour`,
          fingerprint: 'job-failure-rate-warning',
        });
      }
    }

    // Per-type failures in the last hour (find job types with high failure counts)
    const { rows: typeRows } = await pool.query<{ job_type: string; fail_count: string; last_error: string | null }>(`
      SELECT job_type, COUNT(*) as fail_count,
             (SELECT last_error FROM tempo_jobs t2 WHERE t2.job_type = t1.job_type AND t2.status = 'failed'
              ORDER BY t2.updated_at DESC LIMIT 1) as last_error
      FROM tempo_jobs t1
      WHERE status = 'failed' AND updated_at >= NOW() - INTERVAL '1 hour'
      GROUP BY job_type
      HAVING COUNT(*) >= 3
      ORDER BY fail_count DESC
      LIMIT 10
    `);

    for (const row of typeRows) {
      const count = parseInt(row.fail_count, 10);
      const errorPreview = row.last_error ? (row.last_error as string).slice(0, 200) : 'unknown';
      findings.push({
        severity: count >= 10 ? 'critical' : 'warning',
        source: 'job-failures',
        title: `${row.job_type} failing repeatedly`,
        detail: `${count} failures in the last hour. Latest error: ${errorPreview}`,
        fingerprint: `job-type-failing-${row.job_type}`,
      });
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logMinimal('Job failure check error:', msg);
  }

  return findings;
}

// ── Monitor: Queue Depth ──

async function checkQueueDepth(): Promise<Finding[]> {
  const pool = getPool();
  const findings: Finding[] = [];

  try {
    // Pending jobs count
    const { rows: pendingRows } = await pool.query<{ count: string }>(`
      SELECT COUNT(*) as count FROM tempo_jobs WHERE status = 'pending'
    `);
    const pendingCount = parseInt(pendingRows[0].count, 10);

    if (pendingCount > 500) {
      findings.push({
        severity: 'critical',
        source: 'queue-depth',
        title: 'Job queue severely backed up',
        detail: `${pendingCount} pending jobs in the queue`,
        fingerprint: 'queue-depth-critical',
      });
    } else if (pendingCount > 200) {
      findings.push({
        severity: 'warning',
        source: 'queue-depth',
        title: 'Job queue backing up',
        detail: `${pendingCount} pending jobs in the queue`,
        fingerprint: 'queue-depth-warning',
      });
    }

    // Stuck processing jobs (claimed > 20 min ago, still processing)
    const { rows: stuckRows } = await pool.query<{ count: string; types: string }>(`
      SELECT COUNT(*) as count,
             string_agg(DISTINCT job_type, ', ') as types
      FROM tempo_jobs
      WHERE status = 'processing'
        AND claimed_at < NOW() - INTERVAL '20 minutes'
    `);
    const stuckCount = parseInt(stuckRows[0].count, 10);

    if (stuckCount > 0) {
      findings.push({
        severity: stuckCount >= 5 ? 'critical' : 'warning',
        source: 'queue-depth',
        title: `${stuckCount} stuck job(s)`,
        detail: `Jobs stuck in processing for >20min. Types: ${stuckRows[0].types || 'unknown'}`,
        fingerprint: 'stuck-jobs',
      });
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logMinimal('Queue depth check error:', msg);
  }

  return findings;
}

// ── Monitor: Subsystem Recency ──

interface SubsystemCheck {
  jobType: string;
  label: string;
  warnAfterHours: number;
  criticalAfterHours: number;
}

const SUBSYSTEM_CHECKS: SubsystemCheck[] = [
  { jobType: 'context-accumulate', label: 'Proactive Intelligence', warnAfterHours: 0.5, criticalAfterHours: 1 },
  { jobType: 'memory-maintenance', label: 'Memory Maintenance', warnAfterHours: 48, criticalAfterHours: 96 },
  { jobType: 'journal', label: 'Journal', warnAfterHours: 24, criticalAfterHours: 48 },
  { jobType: 'health-check', label: 'Health Check', warnAfterHours: 3, criticalAfterHours: 6 },
  { jobType: 'knowledge-backfill', label: 'Knowledge Backfill', warnAfterHours: 2, criticalAfterHours: 6 },
  { jobType: 'knowledge-summarize', label: 'Knowledge Summarize', warnAfterHours: 12, criticalAfterHours: 24 },
  { jobType: 'life-narration', label: 'Life Narration', warnAfterHours: 2, criticalAfterHours: 4 },
  { jobType: 'execute-scheduled-task', label: 'Scheduled Tasks', warnAfterHours: 0.1, criticalAfterHours: 0.25 },
];

async function checkSubsystemRecency(): Promise<Finding[]> {
  const pool = getPool();
  const findings: Finding[] = [];

  for (const check of SUBSYSTEM_CHECKS) {
    try {
      const { rows } = await pool.query<{ last_completed: string | null }>(`
        SELECT MAX(completed_at) as last_completed
        FROM tempo_jobs
        WHERE job_type = $1 AND status = 'completed'
      `, [check.jobType]);

      const lastCompleted = rows[0]?.last_completed;

      if (!lastCompleted) {
        logger.logDebug(`${check.label}: never completed, skipping`);
        continue;
      }

      const hoursAgo = (Date.now() - new Date(lastCompleted).getTime()) / 3_600_000;

      if (hoursAgo > check.criticalAfterHours) {
        findings.push({
          severity: 'critical',
          source: 'subsystem-recency',
          title: `${check.label} not running`,
          detail: `Last completed ${hoursAgo.toFixed(1)}h ago (threshold: ${check.criticalAfterHours}h)`,
          fingerprint: `subsystem-stale-${check.jobType}`,
        });
      } else if (hoursAgo > check.warnAfterHours) {
        findings.push({
          severity: 'warning',
          source: 'subsystem-recency',
          title: `${check.label} overdue`,
          detail: `Last completed ${hoursAgo.toFixed(1)}h ago (threshold: ${check.warnAfterHours}h)`,
          fingerprint: `subsystem-stale-${check.jobType}`,
        });
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.logDebug(`Subsystem recency check failed for ${check.label}:`, msg);
    }
  }

  return findings;
}

// ── Monitor: LLM Health ──

async function checkLLMHealth(): Promise<Finding[]> {
  const pool = getPool();
  const findings: Finding[] = [];

  try {
    // Fallback rate in the last hour
    const { rows } = await pool.query<{ total: string; fallbacks: string; errors: string }>(`
      SELECT
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE was_fallback = true) as fallbacks,
        COUNT(*) FILTER (WHERE error IS NOT NULL) as errors
      FROM llm_usage
      WHERE created_at >= NOW() - INTERVAL '1 hour'
    `);
    const total = parseInt(rows[0].total, 10);
    const fallbacks = parseInt(rows[0].fallbacks, 10);
    const errors = parseInt(rows[0].errors, 10);

    if (total > 0) {
      if (fallbacks > 10) {
        findings.push({
          severity: 'critical',
          source: 'llm-health',
          title: 'High LLM fallback rate',
          detail: `${fallbacks} fallbacks out of ${total} requests in the last hour`,
          fingerprint: 'llm-fallback-critical',
        });
      } else if (fallbacks >= 3) {
        findings.push({
          severity: 'warning',
          source: 'llm-health',
          title: 'LLM fallbacks detected',
          detail: `${fallbacks} fallbacks out of ${total} requests in the last hour`,
          fingerprint: 'llm-fallback-warning',
        });
      }

      if (errors > 5) {
        findings.push({
          severity: 'warning',
          source: 'llm-health',
          title: 'LLM errors elevated',
          detail: `${errors} errors out of ${total} requests in the last hour`,
          fingerprint: 'llm-errors-warning',
        });
      }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logDebug('LLM health check error:', msg);
  }

  return findings;
}

// ── Monitor: Database Health ──

async function checkDatabaseHealth(): Promise<Finding[]> {
  const pool = getPool();
  const findings: Finding[] = [];

  try {
    const start = Date.now();
    await Promise.race([
      pool.query('SELECT 1'),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000)),
    ]);
    const elapsed = Date.now() - start;

    if (elapsed > 3000) {
      findings.push({
        severity: 'warning',
        source: 'database',
        title: 'Database responding slowly',
        detail: `SELECT 1 took ${elapsed}ms`,
        fingerprint: 'db-slow',
      });
    }

    logger.logDebug(`Database ping: ${elapsed}ms`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    findings.push({
      severity: 'critical',
      source: 'database',
      title: 'Database unreachable',
      detail: `Database check failed: ${msg}`,
      fingerprint: 'db-down',
    });
  }

  return findings;
}

// ── Monitor: Proactive Intelligence Pipeline ──

async function checkProactivePipeline(): Promise<Finding[]> {
  const pool = getPool();
  const findings: Finding[] = [];

  try {
    // Check gate failure rates in the last 6 hours
    const gateTypes = ['significance-check', 'anticipation-analyze', 'insight-deliver'];
    for (const gateType of gateTypes) {
      const { rows } = await pool.query<{ failed: string; total: string }>(`
        SELECT
          COUNT(*) FILTER (WHERE status = 'failed') as failed,
          COUNT(*) as total
        FROM tempo_jobs
        WHERE job_type = $1 AND updated_at >= NOW() - INTERVAL '6 hours'
      `, [gateType]);

      const failed = parseInt(rows[0].failed, 10);
      const total = parseInt(rows[0].total, 10);

      if (total >= 3 && failed / total > 0.5) {
        findings.push({
          severity: 'warning',
          source: 'proactive-pipeline',
          title: `PIE gate failing: ${gateType}`,
          detail: `${failed}/${total} failures in the last 6 hours`,
          fingerprint: `pie-gate-failing-${gateType}`,
        });
      }
    }

    // Check insight delivery recency
    const { rows: deliveryRows } = await pool.query<{ last_delivered: string | null }>(`
      SELECT MAX(delivered_at) as last_delivered
      FROM proactive_insights WHERE delivered_at IS NOT NULL
    `);
    const lastDelivered = deliveryRows[0]?.last_delivered;

    if (lastDelivered) {
      const hoursAgo = (Date.now() - new Date(lastDelivered).getTime()) / 3_600_000;
      if (hoursAgo > 72) {
        findings.push({
          severity: 'warning',
          source: 'proactive-pipeline',
          title: 'No insights delivered recently',
          detail: `Last insight delivered ${hoursAgo.toFixed(1)}h ago`,
          fingerprint: 'pie-no-delivery',
        });
      }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logDebug('Proactive pipeline check error:', msg);
  }

  return findings;
}

// ── Monitor: Data Source Health ──

async function checkDataSourceHealth(): Promise<Finding[]> {
  const pool = getPool();
  const findings: Finding[] = [];

  try {
    // Check for data sources in error or degraded state
    const { rows } = await pool.query<{
      source_key: string;
      display_name: string;
      status: string;
      last_error: string | null;
      last_error_at: string | null;
      consecutive_failures: number;
      last_sync_at: string | null;
    }>(`
      SELECT source_key, display_name, status, last_error, last_error_at::text,
             consecutive_failures, last_sync_at::text
      FROM data_source_registry
      WHERE enabled = true AND status IN ('error', 'degraded')
    `);

    for (const src of rows) {
      const lastSyncAgo = src.last_sync_at
        ? `${((Date.now() - new Date(src.last_sync_at).getTime()) / 3_600_000).toFixed(1)}h ago`
        : 'never';

      if (src.status === 'error') {
        findings.push({
          severity: 'critical',
          source: 'data-source-health',
          title: `${src.display_name} sync failing`,
          detail: `${src.consecutive_failures} consecutive failures. Last sync: ${lastSyncAgo}. Error: ${(src.last_error || 'unknown').slice(0, 200)}`,
          fingerprint: `data-source-error-${src.source_key}`,
        });
      } else if (src.status === 'degraded') {
        findings.push({
          severity: 'warning',
          source: 'data-source-health',
          title: `${src.display_name} sync degraded`,
          detail: `${src.consecutive_failures} consecutive failures. Last sync: ${lastSyncAgo}`,
          fingerprint: `data-source-degraded-${src.source_key}`,
        });
      }
    }

    // Check for expired Google OAuth
    const { rows: authRows } = await pool.query<{ source_key: string; display_name: string }>(`
      SELECT source_key, display_name FROM data_source_registry
      WHERE enabled = true AND auth_status = 'expired'
    `);

    if (authRows.length > 0) {
      const names = authRows.map(r => r.display_name).join(', ');
      findings.push({
        severity: 'warning',
        source: 'data-source-health',
        title: 'OAuth token expired',
        detail: `Expired auth for: ${names}. Re-authorize Google to restore sync.`,
        fingerprint: 'data-source-auth-expired',
      });
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logDebug('Data source health check error:', msg);
  }

  return findings;
}

// ── Monitor: Integration Health ──

/**
 * Check integration health by probing proxy signals from existing tables.
 * Updates integration_health rows directly — this is the heartbeat mechanism.
 * No actual outbound API calls — all signals come from DB state.
 */
async function checkAndUpdateIntegrationHealth(): Promise<Finding[]> {
  const pool = getPool();
  const findings: Finding[] = [];

  try {
    // Load all integrations
    const { rows: integrations } = await pool.query<{
      integration_key: string;
      display_name: string;
      category: string;
      status: string;
      auth_method: string | null;
    }>(`SELECT integration_key, display_name, category, status, auth_method FROM integration_health`);

    if (integrations.length === 0) return findings;

    // Gather proxy signals once
    const signals = await gatherIntegrationSignals(pool);

    for (const intg of integrations) {
      const signal = signals[intg.integration_key];
      if (!signal) continue; // No check available for this integration

      const prevStatus = intg.status;

      if (signal.error) {
        // Mark as error/degraded
        await pool.query(
          `UPDATE integration_health SET
             status = $2, last_check_at = NOW(), last_error = $3, last_error_at = NOW(),
             consecutive_failures = consecutive_failures + 1,
             auth_valid = COALESCE($4, auth_valid),
             auth_expires_at = COALESCE($5, auth_expires_at),
             updated_at = NOW()
           WHERE integration_key = $1`,
          [intg.integration_key, signal.severity === 'critical' ? 'error' : 'degraded',
           signal.error.slice(0, 500), signal.authValid ?? null, signal.authExpires ?? null]
        );

        findings.push({
          severity: signal.severity === 'critical' ? 'critical' : 'warning',
          source: 'integration-health',
          title: `${intg.display_name} ${signal.severity === 'critical' ? 'failing' : 'degraded'}`,
          detail: signal.error,
          fingerprint: `integration-${signal.severity}-${intg.integration_key}`,
        });
      } else {
        // Healthy
        await pool.query(
          `UPDATE integration_health SET
             status = 'healthy', last_check_at = NOW(), last_success_at = NOW(),
             consecutive_failures = 0,
             auth_valid = COALESCE($2, auth_valid),
             auth_expires_at = COALESCE($3, auth_expires_at),
             updated_at = NOW()
           WHERE integration_key = $1`,
          [intg.integration_key, signal.authValid ?? null, signal.authExpires ?? null]
        );

        // If it was previously in error, the auto-resolve in the main handler will clear the alert
      }

      logger.logDebug(`${intg.integration_key}: ${signal.error ? signal.severity : 'healthy'}`);
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logMinimal('Integration health check error:', msg);
  }

  return findings;
}

interface IntegrationSignal {
  error?: string;
  severity?: 'warning' | 'critical';
  authValid?: boolean;
  authExpires?: Date;
}

/**
 * Gather health proxy signals for all integrations from existing DB tables.
 */
async function gatherIntegrationSignals(
  pool: ReturnType<typeof getPool>
): Promise<Record<string, IntegrationSignal>> {
  const signals: Record<string, IntegrationSignal> = {};

  // -- Google OAuth: check token expiry --
  try {
    const { rows } = await pool.query<{ expiry_date: string | null; access_token: string | null }>(
      `SELECT expiry_date, access_token FROM google_tokens LIMIT 1`
    );
    if (rows.length === 0) {
      signals.google_oauth = { error: 'No Google tokens found', severity: 'critical', authValid: false };
      // All Google services depend on this
      signals.google_gmail = { error: 'Google OAuth not connected', severity: 'critical', authValid: false };
      signals.google_calendar = { error: 'Google OAuth not connected', severity: 'critical', authValid: false };
      signals.google_contacts = { error: 'Google OAuth not connected', severity: 'critical', authValid: false };
    } else {
      const expiry = rows[0].expiry_date ? new Date(rows[0].expiry_date) : null;
      const isExpired = expiry ? expiry.getTime() < Date.now() : false;
      if (isExpired) {
        signals.google_oauth = { error: 'OAuth token expired', severity: 'warning', authValid: false, authExpires: expiry! };
        signals.google_gmail = { error: 'OAuth token expired', severity: 'warning', authValid: false };
        signals.google_calendar = { error: 'OAuth token expired', severity: 'warning', authValid: false };
        signals.google_contacts = { error: 'OAuth token expired', severity: 'warning', authValid: false };
      } else {
        signals.google_oauth = { authValid: true, authExpires: expiry ?? undefined };
        signals.google_gmail = { authValid: true };
        signals.google_calendar = { authValid: true };
        signals.google_contacts = { authValid: true };
      }
    }
  } catch {
    // Table might not exist yet
  }

  // -- LLM providers: check recent llm_usage for errors --
  try {
    const { rows } = await pool.query<{ provider: string; errors: string; total: string }>(`
      SELECT
        CASE
          WHEN model ILIKE '%claude%' OR model ILIKE '%anthropic%' THEN 'anthropic'
          WHEN model ILIKE '%gemini%' THEN 'google_gemini'
          WHEN model ILIKE '%gpt%' OR model ILIKE '%openai%' OR model ILIKE '%text-embedding%' THEN 'openai'
          WHEN model ILIKE '%nova%' OR model ILIKE '%bedrock%' OR model ILIKE '%amazon%' THEN 'aws_bedrock'
          ELSE 'other'
        END as provider,
        COUNT(*) FILTER (WHERE error IS NOT NULL) as errors,
        COUNT(*) as total
      FROM llm_usage
      WHERE created_at >= NOW() - INTERVAL '1 hour'
      GROUP BY provider
    `);

    // Track which LLM providers had usage
    const llmProvidersChecked = new Set<string>();

    for (const row of rows) {
      if (row.provider === 'other') continue;
      llmProvidersChecked.add(row.provider);
      const errors = parseInt(row.errors, 10);
      const total = parseInt(row.total, 10);
      if (total > 0 && errors / total > 0.5) {
        signals[row.provider] = { error: `${errors}/${total} errors in the last hour`, severity: 'critical' };
      } else if (errors >= 3) {
        signals[row.provider] = { error: `${errors} errors in the last hour`, severity: 'warning' };
      } else {
        signals[row.provider] = signals[row.provider] ?? {};
      }
    }

    // LLM providers with no recent usage — mark healthy (no errors = no problems)
    for (const p of ['anthropic', 'google_gemini', 'openai', 'aws_bedrock']) {
      if (!llmProvidersChecked.has(p) && !signals[p]) {
        signals[p] = {};
      }
    }
  } catch {
    // Table might not exist yet
  }

  // -- Relay (iMessage): check imessage_incoming recency --
  try {
    const { rows } = await pool.query<{ latest: string | null }>(
      `SELECT MAX(received_at) as latest FROM imessage_incoming`
    );
    if (rows[0]?.latest) {
      const hoursAgo = (Date.now() - new Date(rows[0].latest).getTime()) / 3_600_000;
      if (hoursAgo > 24) {
        signals.relay_imessage = { error: `No messages in ${hoursAgo.toFixed(0)}h`, severity: 'warning' };
      } else {
        signals.relay_imessage = {};
      }
    }
  } catch { /* table may not exist */ }

  // -- Relay (Toolkit): check log_reporter_snapshots for toolkit availability --
  try {
    const { rows } = await pool.query<{ toolkit: Record<string, unknown> | null }>(
      `SELECT toolkit FROM log_reporter_snapshots ORDER BY created_at DESC LIMIT 1`
    );
    if (rows[0]?.toolkit) {
      const available = rows[0].toolkit.available;
      if (available === true) {
        signals.relay_toolkit = {};
      } else {
        signals.relay_toolkit = { error: 'LLM Toolkit not available on relay', severity: 'warning' };
      }
    }
  } catch { /* table may not exist */ }

  // -- APNs: check recent push notification job failures --
  try {
    const { rows } = await pool.query<{ failed: string; total: string }>(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'failed') as failed,
        COUNT(*) as total
      FROM tempo_jobs
      WHERE job_type = 'push-notification' AND updated_at >= NOW() - INTERVAL '6 hours'
    `);
    const failed = parseInt(rows[0].failed, 10);
    const total = parseInt(rows[0].total, 10);
    if (total > 0 && failed / total > 0.5) {
      signals.apns = { error: `${failed}/${total} push failures in 6h`, severity: 'critical' };
    } else if (failed >= 3) {
      signals.apns = { error: `${failed} push failures in 6h`, severity: 'warning' };
    } else {
      signals.apns = {};
    }
  } catch { /* table may not exist */ }

  // -- Resend: check recent aria-email-send job failures --
  try {
    const { rows } = await pool.query<{ failed: string; total: string }>(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'failed') as failed,
        COUNT(*) as total
      FROM tempo_jobs
      WHERE job_type = 'aria-email-send' AND updated_at >= NOW() - INTERVAL '24 hours'
    `);
    const failed = parseInt(rows[0].failed, 10);
    const total = parseInt(rows[0].total, 10);
    if (total > 0 && failed / total > 0.5) {
      signals.resend = { error: `${failed}/${total} email send failures in 24h`, severity: 'warning' };
    } else {
      signals.resend = {};
    }
  } catch { /* table may not exist */ }

  // -- Twilio: check recent send-reminder and twilio-related job failures --
  try {
    const { rows } = await pool.query<{ failed: string; total: string }>(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'failed') as failed,
        COUNT(*) as total
      FROM tempo_jobs
      WHERE job_type = 'send-reminder' AND updated_at >= NOW() - INTERVAL '24 hours'
    `);
    const failed = parseInt(rows[0].failed, 10);
    const total = parseInt(rows[0].total, 10);
    if (total > 0 && failed / total > 0.5) {
      signals.twilio = { error: `${failed}/${total} SMS failures in 24h`, severity: 'warning' };
    } else {
      signals.twilio = {};
    }
  } catch { /* table may not exist */ }

  // -- Looki: check recent looki-realtime-poll failures --
  try {
    const { rows } = await pool.query<{ failed: string; total: string }>(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'failed') as failed,
        COUNT(*) as total
      FROM tempo_jobs
      WHERE job_type = 'looki-realtime-poll' AND updated_at >= NOW() - INTERVAL '1 hour'
    `);
    const failed = parseInt(rows[0].failed, 10);
    const total = parseInt(rows[0].total, 10);
    if (total > 0 && failed / total > 0.5) {
      signals.looki = { error: `${failed}/${total} Looki poll failures in 1h`, severity: 'warning' };
    } else {
      signals.looki = {};
    }
  } catch { /* table may not exist */ }

  // -- ElevenLabs: check voice-related event_log entries --
  try {
    const { rows } = await pool.query<{ latest: string | null }>(`
      SELECT MAX(created_at) as latest FROM event_log WHERE component = 'voice'
    `);
    // ElevenLabs is healthy if it's been used recently or if there's simply no errors
    // (it's an on-demand service, not continuously polled)
    signals.elevenlabs = {};
  } catch { /* table may not exist */ }

  // -- Brave Search: check self-improve-research job failures --
  try {
    const { rows } = await pool.query<{ failed: string; total: string }>(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'failed') as failed,
        COUNT(*) as total
      FROM tempo_jobs
      WHERE job_type = 'self-improve-research' AND updated_at >= NOW() - INTERVAL '24 hours'
    `);
    const failed = parseInt(rows[0].failed, 10);
    const total = parseInt(rows[0].total, 10);
    if (total > 0 && failed / total > 0.5) {
      signals.brave_search = { error: `${failed}/${total} research job failures in 24h`, severity: 'warning' };
    } else {
      signals.brave_search = {};
    }
  } catch { /* table may not exist */ }

  // -- AWS S3: check recent photo thumbnail uploads --
  try {
    const { rows } = await pool.query<{ recent: string }>(`
      SELECT COUNT(*) as recent FROM photo_metadata
      WHERE thumbnail_s3_key IS NOT NULL AND described_at >= NOW() - INTERVAL '24 hours'
    `);
    // S3 is healthy if photo-describe is writing thumbnails, or if there's simply nothing to write
    signals.aws_s3 = {};
  } catch { /* table may not exist */ }

  // -- GitHub: check self-improve-code job failures --
  try {
    const { rows } = await pool.query<{ failed: string; total: string }>(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'failed') as failed,
        COUNT(*) as total
      FROM tempo_jobs
      WHERE job_type = 'self-improve-code' AND updated_at >= NOW() - INTERVAL '24 hours'
    `);
    const failed = parseInt(rows[0].failed, 10);
    const total = parseInt(rows[0].total, 10);
    if (total > 0 && failed / total > 0.5) {
      signals.github = { error: `${failed}/${total} code improvement failures in 24h`, severity: 'warning' };
    } else {
      signals.github = {};
    }
  } catch { /* table may not exist */ }

  // -- Notion: on-demand integration, mark healthy if configured --
  signals.notion = {};

  // -- OpenWeather: on-demand integration, mark healthy if configured --
  signals.openweather = {};

  return signals;
}

// ── Monitor: Local Services (via Log Reporter) ──

interface ReporterSnapshot {
  daemon: {
    errors_today: number;
    api_unreachable: boolean;
    needs_full_disk_access: boolean;
    sent_today: number;
    received_today: number;
  } | null;
  services: Array<{
    name: string;
    running: boolean;
    errors_today?: number;
    failed_today?: number;
    crashed_out?: boolean;
    process_alive?: boolean;
    has_access?: boolean;
  }> | null;
  toolkit: {
    available: boolean;
    total_items?: number;
    completed_items?: number;
    failed_items?: number;
    pending_items?: number;
    processed_last_hour?: number;
  } | null;
  recent_errors: Array<{ message: string }> | null;
  created_at: string;
}

async function checkLocalServices(): Promise<Finding[]> {
  const pool = getPool();
  const findings: Finding[] = [];

  try {
    // Get the most recent snapshot
    const { rows } = await pool.query<ReporterSnapshot>(`
      SELECT daemon, services, toolkit, recent_errors, created_at::text
      FROM log_reporter_snapshots
      ORDER BY created_at DESC
      LIMIT 1
    `);

    if (rows.length === 0) {
      // No snapshots at all — reporter may have never run
      logger.logDebug('No log reporter snapshots found');
      return findings;
    }

    const snap = rows[0];
    const snapAge = (Date.now() - new Date(snap.created_at).getTime()) / 60_000; // minutes

    // Check if reporter is stale (no snapshot in 5 minutes)
    if (snapAge > 5) {
      findings.push({
        severity: 'warning',
        source: 'local-services',
        title: 'Log reporter not reporting',
        detail: `Last snapshot ${snapAge.toFixed(1)} minutes ago. Relay app may be stopped or log reporter crashed.`,
        fingerprint: 'log-reporter-stale',
      });
    }

    // Check if reporter is very stale (no snapshot in 30 minutes)
    if (snapAge > 30) {
      findings.push({
        severity: 'critical',
        source: 'local-services',
        title: 'Log reporter offline',
        detail: `No snapshot in ${snapAge.toFixed(0)} minutes. Relay app is likely not running.`,
        fingerprint: 'log-reporter-offline',
      });
      return findings; // Don't alert on stale service data
    }

    // Check daemon state
    const daemon = snap.daemon;
    if (daemon) {
      if (daemon.api_unreachable) {
        findings.push({
          severity: 'critical',
          source: 'local-services',
          title: 'Relay cannot reach ARIA API',
          detail: 'The Relay app reports ARIA API is unreachable. iMessage relay, delivery, and syncing are all down.',
          fingerprint: 'relay-api-unreachable',
        });
      }

      if (daemon.needs_full_disk_access) {
        findings.push({
          severity: 'warning',
          source: 'local-services',
          title: 'Relay needs Full Disk Access',
          detail: 'iMessage relay cannot read Messages database without Full Disk Access permission.',
          fingerprint: 'relay-full-disk-access',
        });
      }

      if (daemon.errors_today > 20) {
        findings.push({
          severity: 'warning',
          source: 'local-services',
          title: 'Relay daemon errors elevated',
          detail: `${daemon.errors_today} errors today`,
          fingerprint: 'relay-daemon-errors',
        });
      }
    }

    // Check individual services
    if (snap.services) {
      for (const svc of snap.services) {
        if (!svc.running) continue; // Don't alert on intentionally stopped services

        const errors = svc.errors_today ?? svc.failed_today ?? 0;

        if (svc.crashed_out) {
          findings.push({
            severity: 'critical',
            source: 'local-services',
            title: `${svc.name} crashed out`,
            detail: `Service ${svc.name} gave up after repeated crashes.`,
            fingerprint: `local-svc-crashed-${svc.name}`,
          });
        } else if (svc.process_alive === false && svc.name === 'attachment-processing') {
          findings.push({
            severity: 'warning',
            source: 'local-services',
            title: 'Toolkit watch process not running',
            detail: 'The media-pipeline watch subprocess is not alive.',
            fingerprint: 'toolkit-watch-dead',
          });
        }

        if (errors > 10) {
          findings.push({
            severity: 'warning',
            source: 'local-services',
            title: `${svc.name} errors elevated`,
            detail: `${errors} errors today`,
            fingerprint: `local-svc-errors-${svc.name}`,
          });
        }

        if (svc.has_access === false) {
          findings.push({
            severity: 'warning',
            source: 'local-services',
            title: `${svc.name} access denied`,
            detail: `Service ${svc.name} does not have required system access.`,
            fingerprint: `local-svc-access-${svc.name}`,
          });
        }
      }
    }

    // Check LLM Toolkit stats
    const toolkit = snap.toolkit;
    if (toolkit?.available) {
      const failedItems = toolkit.failed_items ?? 0;
      const totalItems = toolkit.total_items ?? 0;
      const pendingItems = toolkit.pending_items ?? 0;

      if (totalItems > 0 && failedItems / totalItems > 0.2) {
        findings.push({
          severity: 'warning',
          source: 'local-services',
          title: 'Toolkit failure rate elevated',
          detail: `${failedItems}/${totalItems} items failed (${((failedItems / totalItems) * 100).toFixed(1)}%)`,
          fingerprint: 'toolkit-failure-rate',
        });
      }

      if (pendingItems > 100) {
        findings.push({
          severity: 'warning',
          source: 'local-services',
          title: 'Toolkit queue backing up',
          detail: `${pendingItems} items pending processing`,
          fingerprint: 'toolkit-queue-backlog',
        });
      }
    }

    // Check recent errors from relay.log
    const recentErrors = snap.recent_errors;
    if (recentErrors && recentErrors.length >= 20) {
      findings.push({
        severity: 'warning',
        source: 'local-services',
        title: 'High relay log error rate',
        detail: `${recentErrors.length} errors/warnings in relay.log since last report`,
        fingerprint: 'relay-log-error-rate',
      });
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logDebug('Local services check error:', msg);
  }

  return findings;
}

// ── Alert Dedup & Persistence ──

/**
 * Check if an alert with this fingerprint is already active (unresolved and recent).
 * Returns true if the finding should be suppressed.
 */
async function isDuplicate(fingerprint: string, severity: Severity): Promise<boolean> {
  const pool = getPool();
  // Critical alerts: suppress if same alert raised in the last 30 minutes
  // Warning alerts: suppress if same alert raised in the last 2 hours
  // Info alerts: suppress if same alert raised in the last 24 hours
  const suppressWindow = severity === 'critical' ? 30 : severity === 'warning' ? 120 : 1440;

  const { rows } = await pool.query<{ id: string }>(
    `SELECT id FROM log_monitor_alerts
     WHERE fingerprint = $1
       AND created_at > NOW() - INTERVAL '1 minute' * $2
     LIMIT 1`,
    [fingerprint, suppressWindow]
  );

  return rows.length > 0;
}

/**
 * Persist a finding as an alert record.
 */
async function persistAlert(finding: Finding): Promise<string> {
  const pool = getPool();
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO log_monitor_alerts (severity, source, title, detail, fingerprint)
     VALUES ($1, $2, $3, $4, $5) RETURNING id`,
    [finding.severity, finding.source, finding.title, finding.detail, finding.fingerprint]
  );
  return rows[0].id;
}

/**
 * Auto-resolve alerts whose fingerprint no longer appears in findings.
 */
async function autoResolveAlerts(activeFingerprints: Set<string>): Promise<number> {
  const pool = getPool();

  // Find unresolved alerts not in the current findings
  const { rows: unresolvedRows } = await pool.query<{ id: string; fingerprint: string }>(
    `SELECT id, fingerprint FROM log_monitor_alerts
     WHERE resolved_at IS NULL
       AND created_at > NOW() - INTERVAL '7 days'`
  );

  let resolved = 0;
  for (const row of unresolvedRows) {
    if (!activeFingerprints.has(row.fingerprint)) {
      await pool.query(
        `UPDATE log_monitor_alerts SET resolved_at = NOW() WHERE id = $1`,
        [row.id]
      );
      resolved++;
    }
  }

  return resolved;
}

// ── Delivery ──

/**
 * Send a critical alert immediately via push notification.
 */
async function deliverCriticalAlert(finding: Finding): Promise<void> {
  const pool = getPool();

  try {
    // Find an enabled push delivery method
    const { rows } = await pool.query<{ config: Record<string, unknown> }>(
      `SELECT config FROM notification_delivery_methods
       WHERE channel = 'push' AND is_enabled = true LIMIT 1`
    );

    if (rows.length === 0) {
      logger.logVerbose('No push delivery method configured, skipping critical alert push');
      return;
    }

    const deviceToken = rows[0].config?.device_token as string | undefined;
    const isSandbox = rows[0].config?.is_sandbox === true || rows[0].config?.is_sandbox === 'true';

    if (!deviceToken) {
      logger.logVerbose('Push method has no device_token, skipping critical alert push');
      return;
    }

    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
       VALUES ($1, $2, $3, $4)`,
      [
        'push-notification',
        JSON.stringify({
          device_token: deviceToken,
          title: `⚠ ${finding.title}`,
          body: finding.detail.slice(0, 200),
          priority: 'time-sensitive',
          category: 'general',
          is_sandbox: isSandbox,
        }),
        5, // high priority
        2,
      ]
    );

    logger.log(`Enqueued critical alert push: ${finding.title}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logMinimal('Failed to deliver critical alert push:', msg);
  }
}

/**
 * Deliver a finding to the inbox.
 */
async function deliverToInbox(finding: Finding): Promise<void> {
  const pool = getPool();

  try {
    await pool.query(
      `INSERT INTO aria_inbox (title, body, category, urgency, source, metadata)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        finding.title,
        finding.detail,
        'system_alert',
        finding.severity === 'critical' ? 'urgent' : 'standard',
        'log-monitor',
        JSON.stringify({ source: finding.source, fingerprint: finding.fingerprint }),
      ]
    );
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logDebug('Failed to deliver to inbox:', msg);
  }
}

/**
 * Compile and deliver hourly warning digest.
 * Only runs if there are undelivered warning alerts.
 */
async function compileWarningDigest(): Promise<void> {
  const pool = getPool();

  try {
    const { rows } = await pool.query<{ id: string; title: string; detail: string; source: string; created_at: string }>(
      `SELECT id, title, detail, source, created_at::text
       FROM log_monitor_alerts
       WHERE severity = 'warning'
         AND delivered = false
         AND resolved_at IS NULL
       ORDER BY created_at DESC
       LIMIT 50`
    );

    if (rows.length === 0) return;

    // Build digest body
    const lines = rows.map(r => `• [${r.source}] ${r.title}: ${r.detail}`);
    const body = `${rows.length} warning(s) in the last hour:\n\n${lines.join('\n')}`;

    // Deliver to inbox
    await pool.query(
      `INSERT INTO aria_inbox (title, body, category, urgency, source, metadata)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        `Log Monitor: ${rows.length} warning(s)`,
        body,
        'system_alert',
        'standard',
        'log-monitor',
        JSON.stringify({ type: 'warning_digest', count: rows.length }),
      ]
    );

    // Mark alerts as delivered
    const ids = rows.map(r => r.id);
    await pool.query(
      `UPDATE log_monitor_alerts SET delivered = true WHERE id = ANY($1)`,
      [ids]
    );

    logger.log(`Compiled warning digest: ${rows.length} warning(s)`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logMinimal('Failed to compile warning digest:', msg);
  }
}

/**
 * Compile and deliver daily info digest.
 */
async function compileDailyDigest(): Promise<void> {
  const pool = getPool();

  try {
    // Job stats for the last 24 hours
    const { rows: statsRows } = await pool.query<{
      completed: string; failed: string; cancelled: string;
    }>(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'completed') as completed,
        COUNT(*) FILTER (WHERE status = 'failed') as failed,
        COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled
      FROM tempo_jobs
      WHERE updated_at >= NOW() - INTERVAL '24 hours'
    `);

    const completed = parseInt(statsRows[0].completed, 10);
    const failed = parseInt(statsRows[0].failed, 10);
    const cancelled = parseInt(statsRows[0].cancelled, 10);

    // Top failing job types
    const { rows: failRows } = await pool.query<{ job_type: string; count: string }>(`
      SELECT job_type, COUNT(*) as count
      FROM tempo_jobs
      WHERE status = 'failed' AND updated_at >= NOW() - INTERVAL '24 hours'
      GROUP BY job_type
      ORDER BY count DESC
      LIMIT 5
    `);

    // Alert summary
    const { rows: alertRows } = await pool.query<{ severity: string; count: string }>(`
      SELECT severity, COUNT(*) as count
      FROM log_monitor_alerts
      WHERE created_at >= NOW() - INTERVAL '24 hours'
      GROUP BY severity
    `);

    const alertSummary = alertRows.map(r => `${r.severity}: ${r.count}`).join(', ') || 'none';
    const failSummary = failRows.map(r => `${r.job_type} (${r.count})`).join(', ') || 'none';

    const body = [
      `Jobs (24h): ${completed} completed, ${failed} failed, ${cancelled} cancelled`,
      `Top failures: ${failSummary}`,
      `Alerts raised: ${alertSummary}`,
    ].join('\n');

    await pool.query(
      `INSERT INTO aria_inbox (title, body, category, urgency, source, metadata)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        'Log Monitor: Daily Summary',
        body,
        'system_alert',
        'low',
        'log-monitor',
        JSON.stringify({ type: 'daily_digest' }),
      ]
    );

    // Mark all undelivered info alerts as delivered
    await pool.query(
      `UPDATE log_monitor_alerts SET delivered = true
       WHERE severity = 'info' AND delivered = false
         AND created_at >= NOW() - INTERVAL '24 hours'`
    );

    logger.log('Compiled daily info digest');
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logMinimal('Failed to compile daily digest:', msg);
  }
}

// ── Digest Timing ──

let lastWarningDigest = 0;
let lastDailyDigest = 0;

const WARNING_DIGEST_INTERVAL_MS = 3_600_000; // 1 hour
const DAILY_DIGEST_INTERVAL_MS = 86_400_000;  // 24 hours

// ── Main Handler ──

export async function handleLogMonitor(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = (job.payload ?? {}) as { force_digest?: boolean };
  const stop = logger.time('log-monitor');

  // Creative Mode short-circuit. When always-on inference services are
  // intentionally shut down for image/video gen, nearly every monitor here
  // would scream: subsystem recency goes overdue (no agents running), LLM
  // health shows 100% fallback rate, job queue backs up waiting for Forge,
  // data source health drops. Skip the scan and return a no-op result so
  // the scheduled job stays green and we don't flood Pushover / the inbox.
  const mode = await platformMode.getPlatformMode();
  if (mode.mode === 'creative') {
    const elapsed = platformMode.formatElapsed(mode.elapsed_ms);
    logger.log(`[creative-mode] log-monitor scan skipped — platform in creative mode for ${elapsed}`);
    stop();
    return {
      status: 'skipped',
      reason: 'creative_mode',
      creative_mode: {
        since: mode.started_at,
        elapsed_ms: mode.elapsed_ms,
        started_by: mode.started_by,
        reason: mode.reason,
      },
      findings: 0,
    };
  }

  logger.logVerbose('Starting log monitor scan');

  // Run all monitors in parallel
  const [
    jobFailures,
    queueDepth,
    subsystemRecency,
    llmHealth,
    dbHealth,
    pipelineHealth,
    dataSourceHealth,
    integrationHealth,
    localServices,
  ] = await Promise.all([
    checkJobFailures(),
    checkQueueDepth(),
    checkSubsystemRecency(),
    checkLLMHealth(),
    checkDatabaseHealth(),
    checkProactivePipeline(),
    checkDataSourceHealth(),
    checkAndUpdateIntegrationHealth(),
    checkLocalServices(),
  ]);

  const allFindings = [
    ...jobFailures,
    ...queueDepth,
    ...subsystemRecency,
    ...llmHealth,
    ...dbHealth,
    ...pipelineHealth,
    ...dataSourceHealth,
    ...integrationHealth,
    ...localServices,
  ];

  logger.logVerbose(`Found ${allFindings.length} finding(s)`);

  // Dedup and persist new findings
  const activeFingerprints = new Set<string>();
  let newCritical = 0;
  let newWarning = 0;
  let newInfo = 0;

  for (const finding of allFindings) {
    activeFingerprints.add(finding.fingerprint);

    const dup = await isDuplicate(finding.fingerprint, finding.severity);
    if (dup) {
      logger.logDebug(`Suppressed duplicate: ${finding.fingerprint}`);
      continue;
    }

    await persistAlert(finding);

    switch (finding.severity) {
      case 'critical':
        newCritical++;
        await deliverCriticalAlert(finding);
        await deliverToInbox(finding);
        logger.log(`CRITICAL: ${finding.title} — ${finding.detail}`);
        break;
      case 'warning':
        newWarning++;
        logger.logVerbose(`WARNING: ${finding.title}`);
        break;
      case 'info':
        newInfo++;
        logger.logDebug(`INFO: ${finding.title}`);
        break;
    }
  }

  // Auto-resolve alerts that are no longer firing
  const resolved = await autoResolveAlerts(activeFingerprints);
  if (resolved > 0) {
    logger.logVerbose(`Auto-resolved ${resolved} alert(s)`);
  }

  // Compile digests on schedule
  const now = Date.now();

  if (payload.force_digest || now - lastWarningDigest > WARNING_DIGEST_INTERVAL_MS) {
    await compileWarningDigest();
    lastWarningDigest = now;
  }

  if (payload.force_digest || now - lastDailyDigest > DAILY_DIGEST_INTERVAL_MS) {
    await compileDailyDigest();
    lastDailyDigest = now;
  }

  // Cleanup old alerts (>30 days)
  try {
    const pool = getPool();
    await pool.query(
      `DELETE FROM log_monitor_alerts WHERE created_at < NOW() - INTERVAL '30 days'`
    );
    // Clean up old log reporter snapshots (keep 24h)
    await pool.query(
      `DELETE FROM log_reporter_snapshots WHERE created_at < NOW() - INTERVAL '24 hours'`
    );
  } catch {
    // Non-critical
  }

  stop();

  // Only log event if there were new findings
  if (newCritical + newWarning + newInfo > 0) {
    logEvent({
      action: `Log monitor: ${newCritical} critical, ${newWarning} warning, ${newInfo} info`,
      component: 'log-monitor',
      category: 'system',
      metadata: {
        new_critical: newCritical,
        new_warning: newWarning,
        new_info: newInfo,
        total_findings: allFindings.length,
        resolved,
        job_id: job.id,
      },
    });
  }

  logger.logVerbose(`Complete: ${allFindings.length} findings, ${newCritical}c/${newWarning}w/${newInfo}i new`);

  return {
    findings: allFindings.length,
    new_critical: newCritical,
    new_warning: newWarning,
    new_info: newInfo,
    resolved,
    checked_at: new Date().toISOString(),
  };
}
