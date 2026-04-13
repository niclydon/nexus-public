/**
 * Inserts a command into appletv_queue for the Mac relay to execute via pyatv.
 */
import { query, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/appletv-control');

export async function handleAppleTvControl(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as {
    device_name?: string;
    command?: string;
    arg?: string;
    expects_result?: boolean;
  };

  if (!payload.device_name || !payload.command) {
    return { error: 'Missing device_name or command' };
  }

  logger.log(`Queueing "${payload.command}" for "${payload.device_name}"`);

  const { rows } = await query<{ id: string }>(
    `INSERT INTO appletv_queue (device_name, command, args, expects_result)
     VALUES ($1, $2, $3, $4)
     RETURNING id`,
    [
      payload.device_name,
      payload.command,
      payload.arg ? JSON.stringify({ value: payload.arg }) : null,
      payload.expects_result ?? false,
    ],
  );

  const queueId = rows[0].id;

  await logEvent({
    action: `Apple TV command queued: ${payload.command} on ${payload.device_name}`,
    component: 'appletv',
    category: 'interaction',
    metadata: { queue_id: queueId, device: payload.device_name, command: payload.command },
  });

  logger.log(`Queued command ${queueId}`);
  return { queued: true, queue_id: queueId };
}
