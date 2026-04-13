/**
 * Pattern Maintenance handler.
 *
 * Runs daily to decay pattern confidence, promote high-confidence
 * patterns to core_memory, and deactivate exhausted patterns.
 */
import type { TempoJob } from '../job-worker.js';
import { createLogger } from '@nexus/core';
import { runPatternMaintenance } from '../lib/proactive/pattern-maintenance.js';

const logger = createLogger('pattern-maintenance');

export async function handlePatternMaintenance(job: TempoJob): Promise<Record<string, unknown>> {
  logger.log(`Starting pattern maintenance job ${job.id}`);
  return await runPatternMaintenance();
}
