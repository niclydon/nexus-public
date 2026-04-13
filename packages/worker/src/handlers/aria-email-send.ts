/**
 * Sends email from aria@example.io via Resend.
 */
import { Resend } from 'resend';
import { createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/aria-email-send');

const ARIA_FROM = process.env.ARIA_EMAIL_ADDRESS || 'aria@example.io';
let resendClient: Resend | null = null;

function getResend(): Resend {
  if (!resendClient) {
    const key = process.env.RESEND_API_KEY;
    if (!key) throw new Error('RESEND_API_KEY not set');
    resendClient = new Resend(key);
  }
  return resendClient;
}

export async function handleAriaEmailSend(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { to?: string | string[]; subject?: string; body?: string };

  if (!payload.to || !payload.subject || !payload.body) {
    throw new Error('Missing required fields: to, subject, body');
  }

  const recipients = Array.isArray(payload.to) ? payload.to : [payload.to];
  logger.log(`Sending to ${recipients.join(', ')}: "${payload.subject}"`);

  const { data, error } = await getResend().emails.send({
    from: `ARIA <${ARIA_FROM}>`,
    to: recipients,
    subject: payload.subject,
    text: payload.body,
  });

  if (error) throw new Error(`Resend error: ${error.message}`);

  const messageId = data?.id ?? '';
  logger.log(`Email sent: ${messageId}`);

  await logEvent({
    action: `Sent email as ARIA to ${recipients.join(', ')}: "${payload.subject}"`,
    component: 'aria-email',
    category: 'background',
    metadata: { messageId, recipients, subject: payload.subject },
  });

  return { messageId, recipients, subject: payload.subject };
}
