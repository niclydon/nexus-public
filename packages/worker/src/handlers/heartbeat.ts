import { createLogger } from '@nexus/core';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/heartbeat');

export async function handleHeartbeat(_job: TempoJob): Promise<Record<string, unknown>> {
  const now = new Date().toISOString();
  logger.log(`Nexus heartbeat at ${now}`);
  return { status: 'alive', worker: 'nexus', timestamp: now };
}
