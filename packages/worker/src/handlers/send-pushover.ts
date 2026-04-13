/**
 * Send a Pushover notification.
 */
import { createLogger } from '@nexus/core';
import { sendPushover } from '../lib/pushover.js';
import { logEvent } from '../lib/event-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/send-pushover');

export async function handleSendPushover(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as {
    title?: string;
    message?: string;
    priority?: number;
    sound?: string;
    url?: string;
  };

  if (!payload.title || !payload.message) {
    throw new Error('Missing required fields: title, message');
  }

  const result = await sendPushover({
    title: payload.title,
    message: payload.message,
    priority: (payload.priority ?? 0) as -2 | -1 | 0 | 1 | 2,
    sound: payload.sound,
    url: payload.url,
  });

  logger.log(`Pushover sent: "${payload.title}"`);

  await logEvent({
    action: `Pushover: "${payload.title}"`,
    component: 'pushover',
    category: 'interaction',
    metadata: { request_id: result.request, priority: payload.priority },
  });

  return { sent: true, request_id: result.request };
}
