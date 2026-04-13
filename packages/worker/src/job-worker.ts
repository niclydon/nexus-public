/**
 * Job queue worker — ported from aria-tempo's worker.ts.
 *
 * Polls tempo_jobs for pending jobs where executor='nexus', claims them
 * atomically with FOR UPDATE SKIP LOCKED, processes via handler registry.
 *
 * During migration, aria-tempo handles executor='tempo' jobs and nexus-worker
 * handles executor='nexus' jobs. Handlers migrate one at a time.
 */
import { query, getPool, createLogger } from '@nexus/core';

const logger = createLogger('job-worker');

export interface TempoJob {
  id: string;
  job_type: string;
  payload: Record<string, unknown>;
  status: string;
  priority: number;
  attempts: number;
  max_attempts: number;
  executor: string;
  claimed_at: Date | null;
  name: string | null;
  description: string | null;
  result: Record<string, unknown> | null;
  last_error: string | null;
  created_at: Date;
}

type JobHandler = (job: TempoJob) => Promise<Record<string, unknown> | void>;

const handlers = new Map<string, JobHandler>();

const DEFAULT_TIMEOUT_MS = 5 * 60 * 1000;
const JOB_TIMEOUTS: Record<string, number> = {
  'echo': 10_000,
  'heartbeat': 10_000,
  'health-check': 60_000,
  'send-imessage': 30_000,
  'send-reminder': 30_000,
  'push-notification': 30_000,
  'speak-homepod': 10_000,
  'appletv-control': 10_000,
  'execute-scheduled-task': 120_000,
  'data-source-health': 120_000,
  'aurora-escalation': 300_000,
  'aria-email-send': 30_000,
  'log-monitor': 120_000,
  'gmail-sync': 60_000,
  'calendar-sync': 60_000,
  'instagram-sync': 120_000,
  'photos-sync': 180_000,
  'imessage-sync': 60_000,
  'contacts-sync': 120_000,
  'reverse-geocode': 300_000,
  'music-sync': 120_000,
  'life-chapters': 1_200_000,
  'biographical-interview': 600_000,
  'topic-model': 5_400_000,  // 90 min — extract (30m) + python cluster (1m) + parallel labeling (10m) + buffer
  'knowledge-summarize': 600_000,
  'data-hygiene': 600_000,
  'cross-correlate': 600_000,
};

export function registerJobHandler(type: string, handler: JobHandler): void {
  handlers.set(type, handler);
  logger.logVerbose(`Registered job handler: ${type}`);
}

export function getRegisteredHandlers(): string[] {
  return Array.from(handlers.keys());
}

/**
 * Poll and process ONLY priority jobs. Runs on its own fast loop,
 * completely independent from the regular job loop.
 */
export async function pollPriorityJobs(): Promise<number> {
  const pool = getPool();

  const claimResult = await pool.query<TempoJob>(
    `UPDATE tempo_jobs
     SET status = 'processing', claimed_at = NOW(), attempts = attempts + 1
     WHERE id IN (
       SELECT id FROM tempo_jobs
       WHERE status = 'pending'
         AND executor = 'nexus'
         AND job_type = ANY($1)
         AND (next_run_at IS NULL OR next_run_at <= NOW())
         AND attempts < max_attempts
       ORDER BY priority DESC, created_at ASC
       FOR UPDATE SKIP LOCKED
       LIMIT 3
     )
     RETURNING *`,
    [PRIORITY_TYPES],
  );

  const jobs = claimResult.rows;
  if (jobs.length === 0) return 0;

  logger.log(`Priority claimed ${jobs.length}: ${jobs.map(j => `${j.id.slice(0, 8)}(${j.job_type})`).join(', ')}`);

  for (const job of jobs) {
    await processJob(job);
  }

  return jobs.length;
}

/**
 * Fast job types that must never be delayed by batch work.
 * These get reserved slots and are always claimed first.
 */
const PRIORITY_TYPES = [
  'send-pushover', 'push-notification', 'send-imessage', 'send-reminder',
  'speak-homepod', 'aria-email-send', 'execute-scheduled-task',
];
const RESERVED_PRIORITY_SLOTS = 2;

/**
 * Long-running LLM-heavy job types. These run on the bulk worker
 * so they never starve urgent jobs.
 */
export const BULK_JOB_TYPES = [
  'sentiment-backfill', 'embed-backfill', 'knowledge-backfill',
  'memory-summarize', 'drive-ingest', 'photo-describe', 'photo-describe-mcp',
  'gmail-backfill', 'google-photos-backfill', 'pkg-backfill',
  'reverse-geocode', 'photo-history-analyze', 'imessage-history-analyze',
  'aurora-nightly', 'aurora-weekly', 'aurora-monthly',
  'conversation-embed', 'data-import', 'knowledge-summarize', 'data-hygiene',
  'notes-backfill', 'music-backfill', 'gmail-transaction-extract',
  'people-enrich', 'biographical-interview', 'advanced-correlate',
  'topic-model',
];

/** Returns the worker mode from WORKER_MODE env var */
export function getWorkerMode(): 'primary' | 'bulk' | 'scaler' {
  return (process.env.WORKER_MODE ?? 'primary') as 'primary' | 'bulk' | 'scaler';
}

/**
 * Claim and process pending jobs where executor='nexus'.
 *
 * WORKER_MODE determines which jobs this worker claims:
 *   primary — priority delivery jobs + non-bulk jobs (agents, sync, monitoring)
 *   bulk    — only BULK_JOB_TYPES (backfill, embeddings, sentiment, etc.)
 *
 * This ensures long-running LLM jobs never starve urgent delivery or agent work.
 */
export async function pollAndProcessJobs(batchSize: number = 5): Promise<number> {
  const pool = getPool();
  const mode = getWorkerMode();

  if (mode === 'bulk') {
    // Bulk worker: only claim BULK_JOB_TYPES
    const result = await pool.query<TempoJob>(
      `UPDATE tempo_jobs
       SET status = 'processing', claimed_at = NOW(), attempts = attempts + 1
       WHERE id IN (
         SELECT id FROM tempo_jobs
         WHERE status = 'pending'
           AND executor = 'nexus'
           AND job_type = ANY($2)
           AND (next_run_at IS NULL OR next_run_at <= NOW())
           AND attempts < max_attempts
         ORDER BY priority DESC, created_at ASC
         FOR UPDATE SKIP LOCKED
         LIMIT $1
       )
       RETURNING *`,
      [batchSize, BULK_JOB_TYPES],
    );

    const jobs = result.rows;
    if (jobs.length === 0) return 0;
    logger.log(`[bulk] Claimed ${jobs.length}: ${jobs.map(j => `${j.id.slice(0, 8)}(${j.job_type})`).join(', ')}`);
    for (const job of jobs) { await processJob(job); }
    return jobs.length;
  }

  // Primary worker: priority lane + non-bulk jobs
  // Lane 1: Priority jobs (delivery, fast actions) — reserved slots
  const priorityResult = await pool.query<TempoJob>(
    `UPDATE tempo_jobs
     SET status = 'processing', claimed_at = NOW(), attempts = attempts + 1
     WHERE id IN (
       SELECT id FROM tempo_jobs
       WHERE status = 'pending'
         AND executor = 'nexus'
         AND job_type = ANY($2)
         AND (next_run_at IS NULL OR next_run_at <= NOW())
         AND attempts < max_attempts
       ORDER BY priority DESC, created_at ASC
       FOR UPDATE SKIP LOCKED
       LIMIT $1
     )
     RETURNING *`,
    [RESERVED_PRIORITY_SLOTS, PRIORITY_TYPES],
  );

  // Lane 2: Non-bulk regular jobs — fill remaining slots
  const remainingSlots = batchSize - priorityResult.rows.length;
  // Exclude both priority types and bulk types
  const excludeTypes = [...PRIORITY_TYPES, ...BULK_JOB_TYPES];
  let regularResult = { rows: [] as TempoJob[] };
  if (remainingSlots > 0) {
    regularResult = await pool.query<TempoJob>(
      `UPDATE tempo_jobs
       SET status = 'processing', claimed_at = NOW(), attempts = attempts + 1
       WHERE id IN (
         SELECT id FROM tempo_jobs
         WHERE status = 'pending'
           AND executor = 'nexus'
           AND NOT (job_type = ANY($2))
           AND (next_run_at IS NULL OR next_run_at <= NOW())
           AND attempts < max_attempts
         ORDER BY priority DESC, created_at ASC
         FOR UPDATE SKIP LOCKED
         LIMIT $1
       )
       RETURNING *`,
      [remainingSlots, excludeTypes],
    );
  }

  const claimResult = { rows: [...priorityResult.rows, ...regularResult.rows] };

  const jobs = claimResult.rows;
  if (jobs.length === 0) return 0;

  logger.log(`Claimed ${jobs.length} job(s): ${jobs.map(j => `${j.id.slice(0, 8)}(${j.job_type})`).join(', ')}`);

  for (const job of jobs) {
    await processJob(job);
  }

  return jobs.length;
}

async function processJob(job: TempoJob): Promise<void> {
  const handler = handlers.get(job.job_type);

  if (!handler) {
    logger.logMinimal(`No handler for job type: ${job.job_type}`);
    await query(
      `UPDATE tempo_jobs SET status = 'failed', failed_at = NOW(), last_error = $2 WHERE id = $1`,
      [job.id, `No handler registered for job type: ${job.job_type}`],
    );
    return;
  }

  const startTime = Date.now();
  const timeoutMs = JOB_TIMEOUTS[job.job_type] ?? DEFAULT_TIMEOUT_MS;

  try {
    const timeoutPromise = new Promise<never>((_, reject) => {
      const timer = setTimeout(() => {
        const elapsed = Math.round((Date.now() - startTime) / 1000);
        reject(new Error(`Job timed out after ${elapsed}s (limit: ${Math.round(timeoutMs / 1000)}s)`));
      }, timeoutMs);
      if (timer.unref) timer.unref();
    });

    const result = await Promise.race([handler(job), timeoutPromise]);

    const durationMs = Date.now() - startTime;
    await query(
      `UPDATE tempo_jobs SET status = 'completed', completed_at = NOW(), result = $2 WHERE id = $1`,
      [job.id, result ? JSON.stringify(result) : null],
    );

    logger.log(`Job ${job.id.slice(0, 8)} (${job.job_type}) completed in ${Math.round(durationMs / 1000)}s`);
  } catch (err) {
    const errorMsg = (err as Error).message;
    const durationMs = Date.now() - startTime;
    const canRetry = job.attempts < job.max_attempts;

    await query(
      `UPDATE tempo_jobs SET status = $2, last_error = $3, failed_at = CASE WHEN $2 = 'failed' THEN NOW() ELSE failed_at END WHERE id = $1`,
      [job.id, canRetry ? 'pending' : 'failed', errorMsg],
    );

    logger.logMinimal(`Job ${job.id.slice(0, 8)} (${job.job_type}) failed after ${Math.round(durationMs / 1000)}s (attempt ${job.attempts}/${job.max_attempts}): ${errorMsg}`);
  }
}

/**
 * Requeue stale jobs stuck in 'processing' for too long (worker crash recovery).
 */
export async function requeueStaleJobs(staleMinutes: number = 20): Promise<number> {
  const requeued = await query(
    `UPDATE tempo_jobs
     SET status = 'pending', claimed_at = NULL
     WHERE status = 'processing'
       AND executor = 'nexus'
       AND claimed_at < NOW() - INTERVAL '1 minute' * $1
       AND attempts < max_attempts`,
    [staleMinutes],
  );

  const failed = await query(
    `UPDATE tempo_jobs
     SET status = 'failed', failed_at = NOW(), last_error = 'Stuck in processing too long'
     WHERE status = 'processing'
       AND executor = 'nexus'
       AND claimed_at < NOW() - INTERVAL '1 minute' * $1
       AND attempts >= max_attempts`,
    [staleMinutes],
  );

  const total = (requeued.rowCount ?? 0) + (failed.rowCount ?? 0);
  if (total > 0) {
    logger.log(`Stale reaper: requeued ${requeued.rowCount ?? 0}, failed ${failed.rowCount ?? 0}`);
  }

  return total;
}
