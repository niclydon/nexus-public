import { createLogger } from '@nexus/core';
import { sendSms } from '../lib/twilio.js';
import { logEvent } from '../lib/event-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/send-reminder');

export async function handleSendReminder(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { channel?: string; recipient?: string; message?: string };

  if (!payload.channel || !payload.recipient || !payload.message) {
    throw new Error('Missing required fields: channel, recipient, message');
  }

  if (payload.channel === 'sms') {
    const { sid, status } = await sendSms(payload.recipient, payload.message);
    logger.log(`SMS sent to ${payload.recipient}: sid=${sid}`);

    await logEvent({
      action: `Sent reminder SMS to ${payload.recipient}: "${payload.message.slice(0, 60)}"`,
      component: 'twilio',
      category: 'interaction',
      metadata: { sid, recipient: payload.recipient },
    });

    return { channel: 'sms', sid, status, recipient: payload.recipient };
  }

  throw new Error(`Unsupported reminder channel: ${payload.channel}`);
}
