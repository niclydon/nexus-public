import { createLogger } from '@nexus/core';
import { sendPushNotification } from '../lib/apns.js';
import { logEvent } from '../lib/event-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/push-notification');

export async function handlePushNotification(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as {
    device_token?: string;
    title?: string;
    body?: string;
    priority?: 'default' | 'time-sensitive';
    category?: 'reply' | 'general' | 'feedback';
    conversation_id?: string;
    thread_id?: string;
    data?: Record<string, unknown>;
    is_sandbox?: boolean;
  };

  if (!payload.device_token || !payload.title || !payload.body) {
    throw new Error('Missing required fields: device_token, title, body');
  }

  const { messageId, statusCode } = await sendPushNotification({
    deviceToken: payload.device_token,
    title: payload.title,
    body: payload.body,
    priority: payload.priority,
    category: payload.category,
    conversationId: payload.conversation_id,
    threadId: payload.thread_id,
    data: payload.data,
    isSandbox: payload.is_sandbox,
  });

  logger.log(`Push sent (${statusCode}): apns-id=${messageId}`);

  await logEvent({
    action: `Sent push notification: "${payload.title}"`,
    component: 'push-notification',
    category: 'interaction',
    metadata: { messageId, statusCode, title: payload.title, sandbox: payload.is_sandbox ?? false },
  });

  return { messageId, statusCode, title: payload.title };
}
