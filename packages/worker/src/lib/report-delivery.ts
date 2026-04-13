/**
 * Unified report delivery — routes reports to configured delivery methods.
 */
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from './event-log.js';

const logger = createLogger('report-delivery');

interface DeliveryMethod {
  id: string;
  channel: string;
  label: string;
  config: Record<string, string>;
  is_enabled: boolean;
  push_preferences?: Record<string, unknown>;
}

interface ReportSubscription {
  id: string;
  report_type: string;
  is_enabled: boolean;
  delivery_method_ids: string[];
}

export async function deliverReport(params: {
  reportType: string;
  title: string;
  body: string;
  category?: string;
  metadata?: Record<string, unknown>;
}): Promise<string[]> {
  const pool = getPool();
  const { reportType, title, body, category, metadata } = params;

  const { rows: subRows } = await pool.query<ReportSubscription>(
    `SELECT id, report_type, is_enabled, delivery_method_ids
     FROM report_subscriptions WHERE report_type = $1`,
    [reportType],
  );

  const subscription = subRows[0];
  if (!subscription) {
    logger.logMinimal(`No subscription found for report type: ${reportType}`);
    return [];
  }
  if (!subscription.is_enabled) {
    logger.log(`Report type '${reportType}' is disabled, skipping`);
    return [];
  }

  let methodIds = subscription.delivery_method_ids ?? [];
  if (methodIds.length === 0) {
    const { rows: reportMethod } = await pool.query<{ id: string }>(
      `SELECT id FROM notification_delivery_methods WHERE channel = 'report' AND is_enabled = true LIMIT 1`,
    );
    if (reportMethod.length > 0) {
      methodIds = [reportMethod[0].id];
    } else {
      await insertReport(title, body, category ?? 'general', reportType, metadata);
      return ['report:fallback'];
    }
  }

  const placeholders = methodIds.map((_, i) => `$${i + 1}`).join(', ');
  const { rows: methods } = await pool.query<DeliveryMethod>(
    `SELECT id, channel, label, config, is_enabled, push_preferences
     FROM notification_delivery_methods WHERE id IN (${placeholders})`,
    methodIds,
  );

  const deliveredVia: string[] = [];

  try {
    await insertReport(title, body, category ?? 'general', reportType, metadata);
    deliveredVia.push('inbox');
  } catch (err) {
    logger.logMinimal('Failed to create inbox item:', (err as Error).message);
  }

  for (const method of methods) {
    if (!method.is_enabled) continue;

    try {
      switch (method.channel) {
        case 'email':
          await deliverEmail(title, body, method.config);
          deliveredVia.push(`email:${method.label}`);
          break;
        case 'push': {
          const pushPrefs = method.push_preferences as { reports?: boolean } | undefined;
          if (pushPrefs && pushPrefs.reports === false) {
            logger.log(`Skipping push for ${method.label} (push_preferences.reports=false)`);
            break;
          }
          await deliverPush(title, body, reportType, method.config);
          deliveredVia.push(`push:${method.label}`);
          break;
        }
        case 'homepod': {
          const target = method.config?.target || null;
          await pool.query(
            `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
             VALUES ('speak-homepod', $1, 'pending', 5, 2)`,
            [JSON.stringify({ message: title, target })],
          );
          deliveredVia.push(`homepod:${method.label}`);
          break;
        }
        case 'report':
          await insertReport(title, body, category ?? 'general', reportType, metadata);
          deliveredVia.push(`report:${method.label}`);
          break;
        default:
          logger.logMinimal(`Unknown channel: ${method.channel}`);
      }
    } catch (err) {
      logger.logMinimal(`Failed to deliver via ${method.channel}:${method.label}:`, (err as Error).message);
    }
  }

  await pool.query(
    `UPDATE report_subscriptions SET last_generated_at = NOW() WHERE report_type = $1`,
    [reportType],
  );

  await logEvent({
    action: `Report delivered: "${title}" via ${deliveredVia.join(', ') || 'none'}`,
    component: 'reports',
    category: 'interaction',
    metadata: { report_type: reportType, methods: deliveredVia },
  });

  return deliveredVia;
}

async function deliverEmail(title: string, body: string, config: Record<string, string>): Promise<void> {
  const pool = getPool();
  const address = config.address;
  if (!address || !body?.trim()) return;

  await pool.query(
    `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
     VALUES ('aria-email-send', $1, 1, 2)`,
    [JSON.stringify({ to: address, subject: `ARIA: ${title}`, body })],
  );
}

async function deliverPush(title: string, body: string, reportType: string, config: Record<string, string>): Promise<void> {
  const pool = getPool();
  const deviceToken = config.device_token;
  if (!deviceToken) return;

  const summary = body.length > 200 ? body.slice(0, 197) + '...' : body;

  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO aria_inbox (title, body, category, source, metadata)
     VALUES ($1, $2, 'general', $3, $4) RETURNING id`,
    [title, body, reportType, JSON.stringify({ report_type: reportType })],
  );
  const inboxId = rows[0]?.id;

  await pool.query(
    `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
     VALUES ('push-notification', $1, 1, 2)`,
    [JSON.stringify({
      device_token: deviceToken,
      title,
      body: summary,
      priority: 'default',
      category: 'general',
      is_sandbox: config.is_sandbox === 'true',
      data: { report_type: reportType, inbox_id: inboxId },
    })],
  );
}

async function insertReport(
  title: string, body: string, category: string, reportType: string,
  metadata?: Record<string, unknown>,
): Promise<string> {
  const pool = getPool();
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO aria_inbox (title, body, category, source, source_id, metadata)
     VALUES ($1, $2, $3, 'report', $4, $5) RETURNING id`,
    [title, body, category, reportType, JSON.stringify({ ...metadata, report_type: reportType })],
  );
  return rows[0].id;
}
