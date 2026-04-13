/**
 * Ephemeral bulk job runner.
 *
 * Lightweight single-purpose process that claims and processes bulk jobs
 * (embed-backfill, sentiment-backfill, knowledge-backfill, etc.) then
 * exits when the queue is empty. Spawned by the autoscaler when backlogs
 * build up, killed when they drain.
 *
 * Usage: node dist/packages/worker/src/bulk-runner.js [--max-idle-cycles=5]
 *
 * The process will:
 * 1. Register all job handlers
 * 2. Poll for bulk jobs every 5 seconds
 * 3. Process whatever it claims via FOR UPDATE SKIP LOCKED
 * 4. Exit after N consecutive empty polls (default 5 = 25 seconds idle)
 */

import { createLogger, shutdown } from '@nexus/core';
import { pollAndProcessJobs, registerJobHandler, BULK_JOB_TYPES, getRegisteredHandlers } from './job-worker.js';
import { registerAllHandlers } from './handlers/index.js';

const logger = createLogger('bulk-runner');

const POLL_INTERVAL_MS = 5_000;
const MAX_IDLE_CYCLES = parseInt(process.argv.find(a => a.startsWith('--max-idle-cycles='))?.split('=')[1] ?? '5', 10);
let shuttingDown = false;

async function run() {
  const pid = process.pid;
  logger.log(`Ephemeral bulk runner starting (pid=${pid}, exit after ${MAX_IDLE_CYCLES} idle cycles)`);

  // Force bulk mode
  process.env.WORKER_MODE = 'bulk';

  registerAllHandlers();
  logger.log(`Registered ${getRegisteredHandlers().length} handlers, processing: ${BULK_JOB_TYPES.length} bulk types`);

  let idleCycles = 0;
  let totalProcessed = 0;

  while (!shuttingDown) {
    try {
      const processed = await pollAndProcessJobs(3);
      totalProcessed += processed;

      if (processed > 0) {
        idleCycles = 0;
        logger.logVerbose(`Processed ${processed} job(s) (total: ${totalProcessed})`);
      } else {
        idleCycles++;
        if (idleCycles >= MAX_IDLE_CYCLES) {
          logger.log(`Queue empty for ${idleCycles} cycles, exiting (total processed: ${totalProcessed})`);
          break;
        }
      }
    } catch (err) {
      logger.logMinimal('Poll error:', (err as Error).message);
      idleCycles++;
    }

    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
  }

  await shutdown();
  process.exit(0);
}

// Graceful shutdown on SIGTERM (sent by autoscaler during scale-down)
// Set flag to stop claiming new jobs; current job finishes naturally via the loop
process.on('SIGTERM', () => {
  logger.log('Received SIGTERM — will exit after current job finishes');
  shuttingDown = true;
});

run().catch(err => {
  logger.logMinimal('Bulk runner fatal error:', err.message);
  process.exit(1);
});
