/**
 * Inserts a message into homepod_queue for the Mac relay to announce.
 */
import { query, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/speak-homepod');

export async function handleSpeakHomepod(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { message?: string; target?: string };

  if (!payload.message) {
    return { error: 'Missing message' };
  }

  const { rows } = await query<{ id: string }>(
    `INSERT INTO homepod_queue (message, target) VALUES ($1, $2) RETURNING id`,
    [payload.message, payload.target ?? null],
  );

  const queueId = rows[0].id;
  logger.log(`Queued HomePod announce: ${queueId}`);

  await logEvent({
    action: `HomePod announce queued${payload.target ? ` to ${payload.target}` : ''}: "${payload.message.slice(0, 80)}"`,
    component: 'homepod',
    category: 'interaction',
    metadata: { queue_id: queueId, target: payload.target ?? 'all' },
  });

  return { queued: true, queue_id: queueId };
}
