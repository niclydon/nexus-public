/**
 * Pushover notification delivery.
 * Simple HTTP POST — no SDK needed.
 */
import { createLogger } from '@nexus/core';

const logger = createLogger('pushover');

const PUSHOVER_API = 'https://api.pushover.net/1/messages.json';

export interface PushoverParams {
  title: string;
  message: string;
  priority?: -2 | -1 | 0 | 1 | 2;
  sound?: string;
  url?: string;
  url_title?: string;
}

export async function sendPushover(params: PushoverParams): Promise<{ status: number; request: string }> {
  const token = process.env.PUSHOVER_APP_TOKEN;
  const user = process.env.PUSHOVER_USER_KEY;

  if (!token || !user) {
    throw new Error('PUSHOVER_APP_TOKEN and PUSHOVER_USER_KEY must be set');
  }

  const body = new URLSearchParams({
    token,
    user,
    title: params.title,
    message: params.message,
    priority: String(params.priority ?? 0),
    ...(params.sound ? { sound: params.sound } : {}),
    ...(params.url ? { url: params.url } : {}),
    ...(params.url_title ? { url_title: params.url_title } : {}),
  });

  // Priority 2 (emergency) requires retry and expire params
  if (params.priority === 2) {
    body.set('retry', '60');
    body.set('expire', '3600');
  }

  const response = await fetch(PUSHOVER_API, {
    method: 'POST',
    body,
    signal: AbortSignal.timeout(10_000),
  });

  const data = await response.json() as { status: number; request: string; errors?: string[] };

  if (data.status !== 1) {
    throw new Error(`Pushover error: ${data.errors?.join(', ') ?? 'unknown'}`);
  }

  logger.log(`Pushover sent: "${params.title}" (priority ${params.priority ?? 0})`);
  return { status: data.status, request: data.request };
}
