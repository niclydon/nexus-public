/**
 * Direct APNs push notification sender via HTTP/2.
 * Token-based auth with .p8 key.
 */
import * as fs from 'node:fs';
import * as http2 from 'node:http2';
import jwt from 'jsonwebtoken';
import { createLogger } from '@nexus/core';

const logger = createLogger('apns');

const APNS_HOST_PRODUCTION = 'api.push.apple.com';
const APNS_HOST_SANDBOX = 'api.sandbox.push.apple.com';

const KEY_ID = process.env.APNS_KEY_ID || '';
const TEAM_ID = process.env.APNS_TEAM_ID || '';
const BUNDLE_ID = process.env.APNS_BUNDLE_ID || 'com.example.app';

let cachedKey: string | null = null;
let cachedToken: string | null = null;
let tokenExpiry = 0;

function getSigningKey(): string {
  if (cachedKey) return cachedKey;

  const inlineKey = process.env.APNS_KEY_CONTENT;
  if (inlineKey) {
    cachedKey = inlineKey.startsWith('-----')
      ? inlineKey
      : Buffer.from(inlineKey, 'base64').toString('utf8');
    return cachedKey;
  }

  const keyPath = process.env.APNS_KEY_PATH;
  if (!keyPath) throw new Error('Neither APNS_KEY_CONTENT nor APNS_KEY_PATH is set');
  if (!fs.existsSync(keyPath)) throw new Error(`APNs key not found at: ${keyPath}`);

  cachedKey = fs.readFileSync(keyPath, 'utf8');
  return cachedKey;
}

function getAuthToken(): string {
  const now = Math.floor(Date.now() / 1000);
  if (cachedToken && now < tokenExpiry) return cachedToken;

  const key = getSigningKey();
  cachedToken = jwt.sign({}, key, {
    algorithm: 'ES256',
    keyid: KEY_ID,
    issuer: TEAM_ID,
    expiresIn: '1h',
    header: { alg: 'ES256', kid: KEY_ID },
  });

  tokenExpiry = now + 3000;
  return cachedToken;
}

export interface PushParams {
  deviceToken: string;
  title: string;
  body: string;
  priority?: 'default' | 'time-sensitive';
  category?: 'reply' | 'general' | 'feedback';
  conversationId?: string;
  threadId?: string;
  data?: Record<string, unknown>;
  isSandbox?: boolean;
}

export interface PushResult {
  messageId: string | undefined;
  statusCode: number;
}

export async function sendPushNotification(params: PushParams): Promise<PushResult> {
  const isSandbox = params.isSandbox ?? false;
  const host = isSandbox ? APNS_HOST_SANDBOX : APNS_HOST_PRODUCTION;
  const token = getAuthToken();

  const interruptionLevel = params.priority === 'time-sensitive' ? 'time-sensitive' : 'active';

  const apnsPayload = {
    aps: {
      alert: { title: params.title, body: params.body },
      sound: 'default',
      'mutable-content': 1,
      'interruption-level': interruptionLevel,
      ...(params.category && {
        category: params.category === 'reply' ? 'ARIA_REPLY' : params.category === 'feedback' ? 'ARIA_FEEDBACK' : 'ARIA_GENERAL',
      }),
      ...(params.threadId && { 'thread-id': params.threadId }),
    },
    ...(params.conversationId && { conversation_id: params.conversationId }),
    ...(params.data && { ...params.data }),
  };

  const payloadString = JSON.stringify(apnsPayload);

  return new Promise<PushResult>((resolve, reject) => {
    const client = http2.connect(`https://${host}`);
    client.on('error', (err) => { client.close(); reject(new Error(`APNs connection error: ${err.message}`)); });

    const req = client.request({
      ':method': 'POST',
      ':path': `/3/device/${params.deviceToken}`,
      'authorization': `bearer ${token}`,
      'apns-topic': BUNDLE_ID,
      'apns-push-type': 'alert',
      'apns-priority': params.priority === 'time-sensitive' ? '10' : '5',
      'apns-expiration': '0',
      'content-type': 'application/json',
    });

    let responseData = '';
    let statusCode = 0;
    let apnsId: string | undefined;

    req.on('response', (headers) => {
      statusCode = headers[':status'] as number;
      apnsId = headers['apns-id'] as string | undefined;
    });
    req.on('data', (chunk) => { responseData += chunk; });
    req.on('end', () => {
      client.close();
      if (statusCode === 200) {
        logger.log(`Push sent (${isSandbox ? 'sandbox' : 'prod'}): apns-id=${apnsId}`);
        resolve({ messageId: apnsId, statusCode });
      } else {
        let reason = responseData;
        try { reason = JSON.parse(responseData).reason || responseData; } catch {}
        reject(new Error(`APNs error ${statusCode}: ${reason}`));
      }
    });
    req.on('error', (err) => { client.close(); reject(new Error(`APNs request error: ${err.message}`)); });
    req.write(payloadString);
    req.end();
  });
}
