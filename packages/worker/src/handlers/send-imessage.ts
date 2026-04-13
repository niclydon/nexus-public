/**
 * Inserts a message into imessage_queue for the Mac relay to send via Messages.app.
 */
import { query, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/send-imessage');

export async function handleSendIMessage(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { recipient?: string; message?: string };

  if (!payload.recipient || !payload.message) {
    return { error: 'Missing recipient or message' };
  }

  const recipientPattern = /^(\+?[0-9]{7,15}|[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$/;
  if (!recipientPattern.test(payload.recipient)) {
    return { error: `Invalid recipient format: ${payload.recipient.slice(0, 20)}` };
  }

  const { rows } = await query<{ id: string }>(
    `INSERT INTO imessage_queue (recipient, message) VALUES ($1, $2) RETURNING id`,
    [payload.recipient, payload.message],
  );

  const queueId = rows[0].id;

  await logEvent({
    action: `iMessage queued to ${payload.recipient}: "${payload.message.slice(0, 50)}"`,
    component: 'imessage',
    category: 'interaction',
    metadata: { queue_id: queueId, recipient: payload.recipient },
  });

  logger.log(`Queued iMessage ${queueId} to ${payload.recipient}`);
  return { queued: true, queue_id: queueId };
}
