import { createLogger } from '@nexus/core';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/echo');

export async function handleEcho(job: TempoJob): Promise<Record<string, unknown>> {
  logger.log(`Echo job ${job.id.slice(0, 8)}:`, JSON.stringify(job.payload).slice(0, 200));
  return { echo: job.payload, processed_at: new Date().toISOString() };
}
