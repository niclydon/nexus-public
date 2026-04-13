/**
 * Autoscaler for ephemeral bulk workers.
 *
 * Monitors the tempo_jobs queue depth for bulk job types and dynamically
 * spawns/kills bulk-runner processes to match demand. Inspired by the
 * voice-print autoscaler pattern.
 *
 * Scaling formula:
 *   desired = min(MAX_WORKERS, Math.ceil(pending / SCALE_THRESHOLD))
 *   if desired > alive → spawn workers
 *   if desired < alive → kill oldest workers
 *   if pending == 0 → kill all ephemeral workers
 *
 * Configuration via environment:
 *   AUTOSCALE_ENABLED=true         (default: false)
 *   AUTOSCALE_MIN_WORKERS=0        (default: 0 — scale to zero when idle)
 *   AUTOSCALE_MAX_WORKERS=4        (default: 4)
 *   AUTOSCALE_THRESHOLD=50         (default: 50 — 1 worker per N pending jobs)
 *   AUTOSCALE_POLL_INTERVAL=30000  (default: 30s)
 */

import { spawn, type ChildProcess } from 'node:child_process';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { hostname } from 'node:os';
import { query, createLogger } from '@nexus/core';
import { BULK_JOB_TYPES } from './job-worker.js';

const logger = createLogger('autoscaler');
const HOST = hostname().split('.')[0].toLowerCase(); // 'primary-server' / 'secondary-server'

/**
 * Best-effort event logger. Writes to autoscaler_events for the
 * /health dashboard. Never throws — observability must not break the
 * autoscaler itself.
 */
async function logEvent(
  eventType: 'spawn' | 'exit' | 'scale_up' | 'scale_down' | 'scale_to_zero',
  fields: { worker_pid?: number; exit_code?: number; alive_count: number; queue_depth?: number; detail?: string },
): Promise<void> {
  try {
    await query(
      `INSERT INTO autoscaler_events (host, event_type, worker_pid, exit_code, alive_count, queue_depth, detail)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        HOST,
        eventType,
        fields.worker_pid ?? null,
        fields.exit_code ?? null,
        fields.alive_count,
        fields.queue_depth ?? null,
        fields.detail ?? null,
      ],
    );
  } catch (err) {
    logger.logMinimal('autoscaler event log failed:', (err as Error).message);
  }
}

const ENABLED = process.env.AUTOSCALE_ENABLED === 'true';
const MIN_WORKERS = parseInt(process.env.AUTOSCALE_MIN_WORKERS ?? '0', 10);
const MAX_WORKERS = parseInt(process.env.AUTOSCALE_MAX_WORKERS ?? '4', 10);
const SCALE_THRESHOLD = parseInt(process.env.AUTOSCALE_THRESHOLD ?? '50', 10);
const POLL_INTERVAL = parseInt(process.env.AUTOSCALE_POLL_INTERVAL ?? '30000', 10);

interface WorkerInfo {
  process: ChildProcess;
  pid: number;
  spawnedAt: Date;
}

const workers: WorkerInfo[] = [];
let running = false;

function getBulkRunnerPath(): string {
  // Resolve path to bulk-runner.js relative to this file
  const __dirname = dirname(fileURLToPath(import.meta.url));
  return resolve(__dirname, 'bulk-runner.js');
}

function spawnWorker(): WorkerInfo | null {
  const runnerPath = getBulkRunnerPath();

  // detached: true puts each bulk-runner in its OWN process group (pgid = child.pid).
  // This is critical because:
  //   1. killAllWorkers() can use process.kill(-pid, 'SIGTERM') to signal the whole
  //      group, cleanly killing the runner and any descendants it spawns.
  //   2. It guarantees signals to the worker can never leak to sibling services
  //      (nexus-api, nexus-mcp) that systemd started in the same parent shell.
  // We do NOT call child.unref() — the parent needs to keep tracking the child
  // via its event handlers (exit, stdout, stderr).
  const child = spawn('node', [runnerPath, '--max-idle-cycles=10'], {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: {
      ...process.env,
      WORKER_MODE: 'bulk',
    },
    detached: true,
  });

  if (!child.pid) {
    logger.logMinimal('Failed to spawn bulk runner');
    return null;
  }

  const info: WorkerInfo = {
    process: child,
    pid: child.pid,
    spawnedAt: new Date(),
  };

  // Pipe child output through our logger.
  // Use `log` (normal level) not `logVerbose` so bulk-runner job activity is
  // actually visible at default log level. Without this, errors thrown inside
  // bulk handlers (data-hygiene auto-merge, etc.) silently disappear because
  // they go to the child's stdout but the parent filters them.
  child.stdout?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n');
    for (const line of lines) {
      if (line.trim()) logger.log(`[worker:${child.pid}] ${line.trim()}`);
    }
  });

  child.stderr?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n');
    for (const line of lines) {
      if (line.trim()) logger.logMinimal(`[worker:${child.pid}] ${line.trim()}`);
    }
  });

  // Remove from tracking when process exits
  child.on('exit', (code) => {
    const idx = workers.findIndex(w => w.pid === child.pid);
    if (idx >= 0) workers.splice(idx, 1);
    logger.log(`Worker pid=${child.pid} exited (code=${code}, alive=${workers.length})`);
    // Best-effort persistence. Fire and forget — the process is already gone.
    logEvent('exit', {
      worker_pid: child.pid,
      exit_code: code ?? undefined,
      alive_count: workers.length,
    });
  });

  workers.push(info);
  logger.log(`Spawned worker pid=${child.pid} (alive=${workers.length})`);
  logEvent('spawn', {
    worker_pid: child.pid,
    alive_count: workers.length,
  });
  return info;
}

/**
 * Kill a single worker by signalling its entire process group (negative pid).
 * Workers are spawned with detached: true so each one has its own pgid.
 * If the spawned-PID is no longer valid (process already exited and the kernel
 * cleaned up), process.kill throws ESRCH — we swallow that since it just means
 * the worker exited on its own before we could kill it.
 */
function killWorkerGroup(w: WorkerInfo): void {
  try {
    process.kill(-w.pid, 'SIGTERM');
  } catch (err) {
    const e = err as NodeJS.ErrnoException;
    if (e.code !== 'ESRCH') {
      logger.logMinimal(`Failed to kill worker pgid=${w.pid}: ${e.message}`);
    }
  }
}

function killOldestWorker(): void {
  if (workers.length === 0) return;

  // Kill the oldest worker (first spawned)
  const oldest = workers[0];
  logger.log(`Scaling down: killing worker pid=${oldest.pid} (alive=${workers.length} → ${workers.length - 1})`);
  killWorkerGroup(oldest);
}

function killAllWorkers(): void {
  if (workers.length === 0) return;
  logger.log(`Scaling to zero: killing ${workers.length} workers`);
  for (const w of [...workers]) {
    killWorkerGroup(w);
  }
}

async function getQueueDepth(): Promise<{ total: number; byType: Record<string, number> }> {
  const result = await query<{ job_type: string; count: number }>(
    `SELECT job_type, COUNT(*)::int as count
     FROM tempo_jobs
     WHERE status IN ('pending', 'processing')
       AND executor = 'nexus'
       AND job_type = ANY($1)
       AND (next_run_at IS NULL OR next_run_at <= NOW() OR status = 'processing')
     GROUP BY job_type`,
    [BULK_JOB_TYPES],
  );

  const byType: Record<string, number> = {};
  let total = 0;
  for (const row of result.rows) {
    byType[row.job_type] = row.count;
    total += row.count;
  }

  return { total, byType };
}

async function scaleCheck(): Promise<void> {
  // Remove dead workers from tracking
  for (let i = workers.length - 1; i >= 0; i--) {
    if (workers[i].process.exitCode !== null) {
      workers.splice(i, 1);
    }
  }

  const { total, byType } = await getQueueDepth();
  const alive = workers.length;
  const desired = Math.max(MIN_WORKERS, Math.min(MAX_WORKERS, Math.ceil(total / SCALE_THRESHOLD)));

  if (total > 0 || alive > 0) {
    const types = Object.entries(byType).map(([t, c]) => `${t}=${c}`).join(', ');
    logger.logVerbose(`Queue: ${total} pending+processing (${types}) | Workers: ${alive} alive, ${desired} desired`);
  }

  if (total === 0 && alive > MIN_WORKERS) {
    // Grace period: don't kill workers alive < 60s — they may still be mid-job
    const now = Date.now();
    const killable = workers.filter(w => now - w.spawnedAt.getTime() > 60_000);
    if (killable.length === 0) {
      logger.logVerbose(`Queue empty but ${alive} worker(s) still in grace period — skipping scale-down`);
      return;
    }

    // Backlog empty — scale to minimum
    if (MIN_WORKERS === 0) {
      logEvent('scale_to_zero', {
        alive_count: 0,
        queue_depth: total,
        detail: `was ${alive} alive, killing all (queue empty)`,
      });
      killAllWorkers();
    } else {
      while (workers.length > MIN_WORKERS) {
        killOldestWorker();
      }
    }
    return;
  }

  if (desired > alive) {
    // Scale up
    const toSpawn = desired - alive;
    logger.log(`Scaling up: ${alive} → ${desired} workers (${total} pending)`);
    logEvent('scale_up', {
      alive_count: alive,
      queue_depth: total,
      detail: `${alive} → ${desired} (${toSpawn} to spawn, threshold=${SCALE_THRESHOLD})`,
    });
    for (let i = 0; i < toSpawn; i++) {
      spawnWorker();
    }
  } else if (desired < alive) {
    // Scale down gradually — at most 1 per cycle
    logEvent('scale_down', {
      alive_count: alive,
      queue_depth: total,
      detail: `${alive} → ${alive - 1} (desired=${desired})`,
    });
    killOldestWorker();
  }
}

/** Start the autoscaler loop. Call from primary worker's main(). */
export async function startAutoscaler(): Promise<void> {
  if (!ENABLED) {
    logger.logVerbose('Autoscaler disabled (set AUTOSCALE_ENABLED=true to enable)');
    return;
  }

  logger.log(`Autoscaler starting | min=${MIN_WORKERS} max=${MAX_WORKERS} threshold=${SCALE_THRESHOLD} poll=${POLL_INTERVAL}ms`);
  running = true;

  while (running) {
    try {
      await scaleCheck();
    } catch (err) {
      logger.logMinimal('Autoscaler error:', (err as Error).message);
    }
    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
  }
}

/** Stop the autoscaler and kill all workers. */
export function stopAutoscaler(): void {
  running = false;
  killAllWorkers();
}

/** Get current autoscaler status for monitoring. */
export function getAutoscalerStatus(): { enabled: boolean; alive: number; workers: Array<{ pid: number; uptime: number }> } {
  return {
    enabled: ENABLED,
    alive: workers.length,
    workers: workers.map(w => ({
      pid: w.pid,
      uptime: Math.round((Date.now() - w.spawnedAt.getTime()) / 1000),
    })),
  };
}
