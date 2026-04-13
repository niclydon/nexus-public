import twilio from 'twilio';
import { getPool, createLogger } from '@nexus/core';

const logger = createLogger('twilio');

let client: twilio.Twilio | null = null;

function getTwilioClient(): twilio.Twilio {
  if (!client) {
    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    if (!accountSid || !authToken) {
      throw new Error('TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN must be set');
    }
    client = twilio(accountSid, authToken);
  }
  return client;
}

function getFromNumber(): string {
  const num = process.env.TWILIO_PHONE_NUMBER;
  if (!num) throw new Error('TWILIO_PHONE_NUMBER is not set');
  return num;
}

export async function sendSms(to: string, body: string): Promise<{ sid: string; status: string }> {
  const msg = await getTwilioClient().messages.create({
    to,
    from: getFromNumber(),
    body,
  });

  const pool = getPool();
  await pool.query(
    `INSERT INTO twilio_messages (sid, direction, from_number, to_number, body, status)
     VALUES ($1, 'outbound', $2, $3, $4, $5)
     ON CONFLICT (sid) DO NOTHING`,
    [msg.sid, getFromNumber(), to, body, msg.status],
  );

  logger.log(`SMS sent to ${to}: sid=${msg.sid}`);
  return { sid: msg.sid, status: msg.status };
}
