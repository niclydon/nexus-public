import { createLogger, query, shutdown, platformMode, assertShowcaseDemoEnabled, type Agent } from '@nexus/core';
import { runAgentCycle } from './agent-runner.js';
import { pollAndProcessJobs, pollPriorityJobs, requeueStaleJobs, getWorkerMode, getRegisteredHandlers } from './job-worker.js';
import { registerAllHandlers } from './handlers/index.js';
import { startEventListener, stopEventListener, markAgentBusy, markAgentFree } from './event-listener.js';
import { connectAllServers, disconnectAllServers } from './lib/mcp-client-manager.js';
import { startToolServer } from './tool-server.js';
import { startAutoscaler, stopAutoscaler } from './autoscaler.js';

const logger = createLogger('worker');
assertShowcaseDemoEnabled('nexus-worker');

const AGENT_POLL_INTERVAL_MS = 10_000;
const JOB_POLL_INTERVAL_MS = 5_000;
const STALE_REAPER_INTERVAL_MS = 60_000;
const REVIEW_DIGEST_INTERVAL_MS = 90 * 60 * 1000; // 90 minutes
const LOG_MONITOR_INTERVAL_MS = 60 * 1000;        // 60 seconds
// In Creative Mode, log the pause state at most once per this window so
// journalctl doesn't get a new line every poll interval.
const CREATIVE_LOG_INTERVAL_MS = 5 * 60 * 1000;
let running = true;
let lastCreativeModeLog = 0;

function maybeLogCreativeMode(scope: string, mode: Awaited<ReturnType<typeof platformMode.getPlatformMode>>): void {
  const now = Date.now();
  if (now - lastCreativeModeLog < CREATIVE_LOG_INTERVAL_MS) return;
  lastCreativeModeLog = now;
  const elapsed = platformMode.formatElapsed(mode.elapsed_ms);
  logger.log(`[creative-mode] ${scope} paused — platform in creative mode for ${elapsed} (by ${mode.started_by || 'unknown'}, reason: ${mode.reason || 'n/a'})`);
}

async function getAgentsDueForCycle(): Promise<Agent[]> {
  const result = await query<Agent>(
    `SELECT * FROM agent_registry
     WHERE is_active = true
       AND executor = 'nexus'
       AND schedule_interval_sec IS NOT NULL
       AND (
         last_check_in IS NULL
         OR last_check_in < NOW() - (schedule_interval_sec || ' seconds')::interval
       )
     ORDER BY last_check_in ASC NULLS FIRST`,
  );
  return result.rows;
}

async function agentLoop() {
  while (running) {
    try {
      // Creative Mode short-circuit: always-on LLM services are shut down
      // for image/video gen work, so agent cycles would just fail against a
      // dead llama-server. Skip the whole loop iteration instead. We'll
      // resume automatically when mode flips back to 'normal'.
      const mode = await platformMode.getPlatformMode();
      if (mode.mode === 'creative') {
        maybeLogCreativeMode('agent loop', mode);
        await new Promise(resolve => setTimeout(resolve, AGENT_POLL_INTERVAL_MS));
        continue;
      }

      const agents = await getAgentsDueForCycle();
      if (agents.length > 0) {
        logger.logVerbose('Agents due:', agents.map(a => a.agent_id).join(', '));
      }
      for (const agent of agents) {
        if (!running) break;
        try {
          markAgentBusy(agent.agent_id);
          const result = await runAgentCycle(agent.agent_id);
          logger.log(`Agent cycle: ${result.agentId} | ${result.steps} steps | ${result.actions.length} actions | ${result.durationMs}ms`);
        } catch (err) {
          logger.logMinimal(`Agent cycle failed for ${agent.agent_id}:`, (err as Error).message);
        } finally {
          markAgentFree(agent.agent_id);
        }
      }
    } catch (err) {
      logger.logMinimal('Agent poll error:', (err as Error).message);
    }
    await new Promise(resolve => setTimeout(resolve, AGENT_POLL_INTERVAL_MS));
  }
}

async function priorityJobLoop() {
  while (running) {
    try {
      // Priority lane carries interactive chat replies which require the
      // priority slot (qwen-35b on 8088) — also shut down in creative mode.
      const mode = await platformMode.getPlatformMode();
      if (mode.mode === 'creative') {
        await new Promise(resolve => setTimeout(resolve, 2_000));
        continue;
      }

      const processed = await pollPriorityJobs();
      if (processed > 0) {
        logger.log(`Priority: processed ${processed} job(s)`);
      }
    } catch (err) {
      logger.logMinimal('Priority job poll error:', (err as Error).message);
    }
    await new Promise(resolve => setTimeout(resolve, 2_000)); // 2s — fast poll for delivery
  }
}

async function jobLoop() {
  while (running) {
    try {
      const processed = await pollAndProcessJobs(5);
      if (processed > 0) {
        logger.logVerbose(`Processed ${processed} job(s)`);
      }
    } catch (err) {
      logger.logMinimal('Job poll error:', (err as Error).message);
    }
    await new Promise(resolve => setTimeout(resolve, JOB_POLL_INTERVAL_MS));
  }
}

async function staleReaperLoop() {
  while (running) {
    try {
      await requeueStaleJobs(20);
    } catch (err) {
      logger.logMinimal('Stale reaper error:', (err as Error).message);
    }
    await new Promise(resolve => setTimeout(resolve, STALE_REAPER_INTERVAL_MS));
  }
}

/**
 * Enqueue a recurring job if one isn't already pending/processing. Returns
 * true if a new job was inserted. Used by the recurring task loops to
 * prevent pile-up when a handler is slow or broken.
 */
async function enqueueIfAbsent(jobType: string, priority = 0, maxAttempts = 1): Promise<boolean> {
  const existing = await query<{ id: string }>(
    `SELECT id FROM tempo_jobs WHERE job_type = $1 AND status IN ('pending', 'processing') LIMIT 1`,
    [jobType],
  );
  if (existing.rows.length > 0) return false;
  await query(
    `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts, executor)
     VALUES ($1, '{}', 'pending', $2, $3, 'nexus')`,
    [jobType, priority, maxAttempts],
  );
  return true;
}

async function recurringTaskLoop() {
  // Wait 5 min before first digest (let agents run a few cycles first)
  await new Promise(resolve => setTimeout(resolve, 5 * 60 * 1000));

  while (running) {
    try {
      // Review-digest sends Pushover if there are unreviewed decisions.
      // Skip in creative mode — no LLM available, nothing to review.
      const mode = await platformMode.getPlatformMode();
      if (mode.mode !== 'creative') {
        if (await enqueueIfAbsent('review-digest')) {
          logger.logVerbose('Enqueued review-digest job');
        }
      }
    } catch (err) {
      logger.logMinimal('Review digest scheduling error:', (err as Error).message);
    }
    await new Promise(resolve => setTimeout(resolve, REVIEW_DIGEST_INTERVAL_MS));
  }
}

/**
 * Log-monitor scheduler. Runs every 60s to enqueue a fresh log-monitor
 * job if one isn't already in flight. Previously this was scheduled by
 * aria-tempo (retired 2026-04-07) and nothing picked up the duty, so
 * log-monitor hadn't run in 10 days as of 2026-04-10.
 *
 * The handler itself short-circuits in creative mode, so enqueuing during
 * creative mode is harmless — but to keep things tidy we skip enqueue too.
 */
async function logMonitorLoop() {
  // Small startup delay so the worker has time to connect MCP servers etc.
  await new Promise(resolve => setTimeout(resolve, 30_000));

  while (running) {
    try {
      const mode = await platformMode.getPlatformMode();
      if (mode.mode !== 'creative') {
        if (await enqueueIfAbsent('log-monitor')) {
          logger.logVerbose('Enqueued log-monitor job');
        }
      }
    } catch (err) {
      logger.logMinimal('Log monitor scheduling error:', (err as Error).message);
    }
    await new Promise(resolve => setTimeout(resolve, LOG_MONITOR_INTERVAL_MS));
  }
}

async function main() {
  const mode = getWorkerMode();
  logger.log(`Nexus worker starting | mode: ${mode}`);

  // Register job handlers (both modes need these)
  registerAllHandlers();
  const handlerCount = getRegisteredHandlers().length;
  logger.log(`Registered ${handlerCount} job handlers`);

  if (mode === 'primary') {
    // Primary: full stack — agents, events, MCP, tool server, all job lanes
    await startEventListener();

    try {
      await connectAllServers();
    } catch (err) {
      logger.logMinimal('MCP server connection errors (non-fatal):', (err as Error).message);
    }

    await startToolServer();

    logger.log('Agent loop: polling every', AGENT_POLL_INTERVAL_MS / 1000, 's');
    logger.log('Job loop: polling every', JOB_POLL_INTERVAL_MS / 1000, 's');
    logger.log('Stale reaper: every', STALE_REAPER_INTERVAL_MS / 1000, 's');

    await Promise.all([agentLoop(), priorityJobLoop(), jobLoop(), staleReaperLoop(), recurringTaskLoop(), logMonitorLoop(), startAutoscaler()]);
  } else if (mode === 'scaler') {
    // Scaler: only autoscaler + stale reaper — spawns ephemeral bulk workers on demand
    // Used on Secondary-Server and other satellite machines
    logger.log('Scaler mode: autoscaler + stale reaper only');
    await Promise.all([startAutoscaler(), staleReaperLoop()]);
  } else {
    // Bulk: only long-running LLM jobs — no agents, no events, no tool server
    logger.log('Bulk job loop: polling every', JOB_POLL_INTERVAL_MS / 1000, 's');
    logger.log('Processing only:', 'sentiment-backfill, embed-backfill, knowledge-backfill, memory-summarize, photo-describe, aurora-*, etc.');

    await Promise.all([jobLoop(), staleReaperLoop()]);
  }
}

let shuttingDown = false;
async function gracefulShutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.log(`Received ${signal}, shutting down`);
  running = false;

  // Force exit after 15s no matter what — prevents systemd SIGKILL escalation
  // when cleanup is blocked by an in-flight long job. Any jobs still claimed
  // get requeued by the staleReaperLoop on next worker start.
  const forceExit = setTimeout(() => {
    logger.logMinimal('Graceful shutdown deadline reached, forcing exit');
    process.exit(0);
  }, 15_000);
  forceExit.unref();

  try {
    stopAutoscaler();
    await disconnectAllServers();
    await stopEventListener();
    await shutdown();
  } catch (err) {
    logger.logMinimal('Shutdown cleanup error (ignoring):', (err as Error).message);
  }
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

main().catch(err => {
  logger.logMinimal('Worker crashed:', err);
  process.exit(1);
});
