/**
 * Health check handler — ported from aria-tempo.
 * Checks 6 subsystems and reports overall platform health.
 */
import { query, getPool, createLogger } from '@nexus/core';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/health-check');

type SubsystemStatus = 'healthy' | 'degraded' | 'down';

interface SubsystemResult {
  status: SubsystemStatus;
  detail: string;
}

async function checkDatabase(): Promise<SubsystemResult> {
  try {
    const client = await getPool().connect();
    try {
      await Promise.race([
        client.query('SELECT 1'),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000)),
      ]);
      return { status: 'healthy', detail: 'Database responding normally' };
    } finally {
      client.release();
    }
  } catch (err) {
    return { status: 'down', detail: `Database check failed: ${(err as Error).message}` };
  }
}

async function checkTempo(): Promise<SubsystemResult> {
  try {
    const { rows } = await query<{ failed: string; completed: string }>(
      `SELECT
        COUNT(*) FILTER (WHERE status = 'failed' AND updated_at >= NOW() - INTERVAL '1 hour') as failed,
        COUNT(*) FILTER (WHERE status = 'completed' AND updated_at >= NOW() - INTERVAL '1 hour') as completed
      FROM tempo_jobs`,
    );
    const failed = parseInt(rows[0].failed, 10);
    const completed = parseInt(rows[0].completed, 10);
    const total = failed + completed;

    if (total === 0) return { status: 'healthy', detail: 'No jobs processed in the last hour' };
    const pct = ((failed / total) * 100).toFixed(1);
    if (failed / total > 0.5) return { status: 'down', detail: `Job failure rate ${pct}% (${failed}/${total})` };
    if (failed / total >= 0.2) return { status: 'degraded', detail: `Job failure rate ${pct}% (${failed}/${total})` };
    return { status: 'healthy', detail: `Job failure rate ${pct}% (${failed}/${total})` };
  } catch (err) {
    return { status: 'down', detail: `Tempo check failed: ${(err as Error).message}` };
  }
}

async function checkPIE(): Promise<SubsystemResult> {
  try {
    const { rows } = await query<{ last_delivered: string | null }>(
      `SELECT MAX(delivered_at) as last_delivered FROM proactive_insights WHERE delivered_at IS NOT NULL`,
    );
    const last = rows[0].last_delivered;
    if (!last) return { status: 'down', detail: 'No insights ever delivered' };
    const hoursAgo = (Date.now() - new Date(last).getTime()) / 3_600_000;
    if (hoursAgo > 72) return { status: 'down', detail: `Last insight ${hoursAgo.toFixed(1)}h ago` };
    if (hoursAgo > 24) return { status: 'degraded', detail: `Last insight ${hoursAgo.toFixed(1)}h ago` };
    return { status: 'healthy', detail: `Last insight ${hoursAgo.toFixed(1)}h ago` };
  } catch (err) {
    return { status: 'down', detail: `PIE check failed: ${(err as Error).message}` };
  }
}

async function checkMemory(): Promise<SubsystemResult> {
  try {
    const { rows } = await query<{ last_completed: string | null }>(
      `SELECT MAX(completed_at) as last_completed FROM tempo_jobs WHERE job_type = 'memory-maintenance' AND status = 'completed'`,
    );
    const last = rows[0].last_completed;
    if (!last) return { status: 'down', detail: 'Memory maintenance has never completed' };
    const hoursAgo = (Date.now() - new Date(last).getTime()) / 3_600_000;
    if (hoursAgo > 96) return { status: 'down', detail: `Last maintenance ${hoursAgo.toFixed(1)}h ago` };
    if (hoursAgo > 48) return { status: 'degraded', detail: `Last maintenance ${hoursAgo.toFixed(1)}h ago` };
    return { status: 'healthy', detail: `Last maintenance ${hoursAgo.toFixed(1)}h ago` };
  } catch (err) {
    return { status: 'down', detail: `Memory check failed: ${(err as Error).message}` };
  }
}

async function checkLLM(): Promise<SubsystemResult> {
  try {
    const { rows } = await query<{ fallback_count: string }>(
      `SELECT COUNT(*) as fallback_count FROM llm_usage WHERE was_fallback = true AND created_at >= NOW() - INTERVAL '1 hour'`,
    );
    const count = parseInt(rows[0].fallback_count, 10);
    if (count > 10) return { status: 'down', detail: `${count} LLM fallbacks in the last hour` };
    if (count >= 3) return { status: 'degraded', detail: `${count} LLM fallbacks in the last hour` };
    return { status: 'healthy', detail: `${count} LLM fallbacks in the last hour` };
  } catch (err) {
    return { status: 'down', detail: `LLM check failed: ${(err as Error).message}` };
  }
}

async function checkNexusWorker(): Promise<SubsystemResult> {
  try {
    const { rows } = await query<{ last_check_in: string | null }>(
      `SELECT MAX(last_check_in) as last_check_in FROM agent_registry WHERE executor = 'nexus' AND is_active = true`,
    );
    const last = rows[0].last_check_in;
    if (!last) return { status: 'degraded', detail: 'No Nexus agents have checked in' };
    const minsAgo = (Date.now() - new Date(last).getTime()) / 60_000;
    if (minsAgo > 30) return { status: 'degraded', detail: `Last agent check-in ${minsAgo.toFixed(0)}min ago` };
    return { status: 'healthy', detail: `Last agent check-in ${minsAgo.toFixed(0)}min ago` };
  } catch (err) {
    return { status: 'down', detail: `Nexus worker check failed: ${(err as Error).message}` };
  }
}

export async function handleHealthCheck(job: TempoJob): Promise<Record<string, unknown>> {
  logger.log(`Starting health check ${job.id.slice(0, 8)}`);

  const [database, tempo, pie, memory, llm, nexusWorker] = await Promise.all([
    checkDatabase(),
    checkTempo(),
    checkPIE(),
    checkMemory(),
    checkLLM(),
    checkNexusWorker(),
  ]);

  const subsystems = { database, tempo, pie, memory, llm, nexus_worker: nexusWorker };
  const statuses = Object.values(subsystems).map(s => s.status);
  const overall: SubsystemStatus = statuses.includes('down') ? 'down' : statuses.includes('degraded') ? 'degraded' : 'healthy';

  logger.log(`Health check complete: ${overall}`);

  return {
    status: overall,
    subsystems,
    checked_at: new Date().toISOString(),
  };
}
