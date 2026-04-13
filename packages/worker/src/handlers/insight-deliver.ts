/**
 * Insight Deliver handler.
 *
 * Delivers a proactive insight via the user's configured notification
 * delivery methods (email, push, homepod). Looks up each method's channel
 * and config, then dispatches accordingly. Each method's min_urgency
 * threshold is checked before delivery.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';

const logger = createLogger('insight-deliver');

interface InsightRow {
  id: string;
  title: string;
  body: string;
  category: string;
  urgency: string;
  confidence: number;
}

interface MethodRow {
  id: string;
  channel: string;
  label: string;
  config: Record<string, unknown>;
  is_enabled: boolean;
  min_urgency?: string;
  push_preferences?: {
    notifications?: boolean;
    reports?: boolean;
    urgency_filter?: string;
  };
}

/** Numeric rank for urgency levels — higher = more urgent */
const urgencyRank: Record<string, number> = { low: 0, standard: 1, normal: 1, urgent: 2, critical: 3 };

/** Returns true if insightUrgency meets or exceeds the method's min_urgency threshold */
function meetsMinUrgency(insightUrgency: string, minUrgency: string | undefined): boolean {
  const insightRank = urgencyRank[insightUrgency] ?? 1;
  const minRank = urgencyRank[minUrgency ?? 'normal'] ?? 1;
  return insightRank >= minRank;
}

/**
 * Send insight via email using SES (via aria-email-send job).
 */
async function deliverEmail(insight: InsightRow, config: Record<string, unknown>): Promise<void> {
  const pool = getPool();
  const address = config.address as string | undefined;
  if (!address) {
    logger.logMinimal('Email method has no address configured');
    return;
  }

  const subject = insight.urgency === 'urgent'
    ? `[Urgent] ARIA: ${insight.title}`
    : `ARIA: ${insight.title}`;

  const body = `${insight.body}\n\n---\nCategory: ${insight.category}\nConfidence: ${Math.round(insight.confidence * 100)}%\n\nReply to this email with feedback (e.g., "useful" or "not useful") to help ARIA learn.`;

  await pool.query(
    `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
     VALUES ($1, $2, $3, $4)`,
    [
      'aria-email-send',
      JSON.stringify({ to: address, subject, body }),
      insight.urgency === 'urgent' ? 3 : 1,
      2,
    ]
  );
}

/**
 * Deliver insight to the inbox.
 */
async function deliverToInbox(insight: InsightRow, conversationId?: string): Promise<string> {
  const pool = getPool();
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO aria_inbox (title, body, category, urgency, source, source_id, insight_id, conversation_id, metadata)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     RETURNING id`,
    [
      insight.title,
      insight.body,
      'proactive_insight',
      insight.urgency,
      'proactive_intelligence',
      insight.id,
      insight.id,
      conversationId ?? null,
      JSON.stringify({
        confidence: insight.confidence,
        insight_category: insight.category,
      }),
    ]
  );
  return rows[0].id;
}

/**
 * Create a conversation where ARIA proactively starts talking about an insight.
 * Returns the conversation ID for deep-linking from push notifications.
 */
async function createInsightConversation(insight: InsightRow): Promise<string> {
  const pool = getPool();

  // Create conversation
  const { rows: convRows } = await pool.query<{ id: string }>(
    `INSERT INTO conversations (title) VALUES ($1) RETURNING id`,
    [insight.title]
  );
  const conversationId = convRows[0].id;

  // Build ARIA's opening message -- she's proactively reaching out
  const categoryLabel = insight.category.replace(/_/g, ' ');
  let message = insight.body;

  // Add context about why she's reaching out
  if (insight.urgency === 'urgent') {
    message = `Hey -- I wanted to flag something for you.\n\n${insight.body}\n\nLet me know if you want me to do anything about this.`;
  } else {
    message = `I noticed something I thought you should know about.\n\n${insight.body}\n\nWant me to look into this further?`;
  }

  // Insert ARIA's message as the first (and only) message
  await pool.query(
    `INSERT INTO messages (conversation_id, role, content) VALUES ($1, 'assistant', $2)`,
    [conversationId, message]
  );

  logger.log(`Created conversation ${conversationId} for insight "${insight.title}"`);
  return conversationId;
}

/**
 * Send insight via push notification.
 * For urgent insights, creates a conversation and deep-links into it.
 */
async function deliverPush(insight: InsightRow, config: Record<string, unknown>, inboxId?: string): Promise<void> {
  const pool = getPool();
  const deviceToken = config.device_token as string | undefined;
  if (!deviceToken) {
    logger.logMinimal('Push method has no device_token configured');
    return;
  }

  const isSandbox = config.is_sandbox === true || config.is_sandbox === 'true';

  // For urgent insights, create a conversation so the push deep-links into a chat
  let conversationId: string | undefined;
  if (insight.urgency === 'urgent') {
    try {
      conversationId = await createInsightConversation(insight);
      // Update the inbox item with the conversation_id
      if (inboxId) {
        await pool.query(
          `UPDATE aria_inbox SET conversation_id = $1 WHERE id = $2`,
          [conversationId, inboxId]
        );
      }
    } catch (err) {
      logger.logMinimal('Failed to create insight conversation:', err instanceof Error ? err.message : err);
    }
  }

  // Truncate body for push notification display
  const pushBody = insight.body.length > 200 ? insight.body.slice(0, 197) + '...' : insight.body;

  await pool.query(
    `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
     VALUES ($1, $2, $3, $4)`,
    [
      'push-notification',
      JSON.stringify({
        device_token: deviceToken,
        title: insight.urgency === 'urgent' ? `ARIA: ${insight.title}` : insight.title,
        body: pushBody,
        priority: insight.urgency === 'urgent' ? 'time-sensitive' : 'default',
        category: 'feedback',
        is_sandbox: isSandbox,
        conversation_id: conversationId,
        data: {
          insight_id: insight.id,
          insight_category: insight.category,
          inbox_id: inboxId,
          urgency: insight.urgency,
          ...(conversationId && { conversation_id: conversationId }),
        },
      }),
      insight.urgency === 'urgent' ? 3 : 1,
      2,
    ]
  );
}

export async function handleInsightDeliver(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { insight_id: string; delivery_method_ids: string[] };
  logger.log(`Delivering insight ${payload.insight_id} via ${payload.delivery_method_ids.length} method(s)`);

  const pool = getPool();

  // Load the insight
  const { rows: insightRows } = await pool.query<InsightRow>(
    `SELECT id, title, body, category, urgency, confidence
     FROM proactive_insights WHERE id = $1`,
    [payload.insight_id]
  );

  if (insightRows.length === 0) {
    logger.logMinimal(`Insight ${payload.insight_id} not found`);
    return { error: 'Insight not found' };
  }

  const insight = insightRows[0];

  // Load delivery methods
  const placeholders = payload.delivery_method_ids.map((_, i) => `$${i + 1}`).join(', ');
  const { rows: methods } = await pool.query<MethodRow>(
    `SELECT id, channel, label, config, is_enabled, min_urgency, push_preferences
     FROM notification_delivery_methods WHERE id IN (${placeholders})`,
    payload.delivery_method_ids
  );

  // Always create an inbox item for every delivered insight
  let inboxId: string | undefined;
  try {
    inboxId = await deliverToInbox(insight);
  } catch (err) {
    logger.logMinimal('Failed to create inbox item:', err instanceof Error ? err.message : err);
  }

  const deliveredVia: string[] = [];
  if (inboxId) deliveredVia.push('inbox');

  for (const method of methods) {
    if (!method.is_enabled) {
      logger.log(`Skipping disabled method: ${method.label}`);
      continue;
    }

    try {
      switch (method.channel) {
        case 'email':
          if (!meetsMinUrgency(insight.urgency, method.min_urgency)) {
            logger.logVerbose(`Skipping email for ${method.label} — insight urgency ${insight.urgency} below min ${method.min_urgency}`);
            break;
          }
          await deliverEmail(insight, method.config);
          deliveredVia.push(`email:${method.label}`);
          break;
        case 'push': {
          if (!meetsMinUrgency(insight.urgency, method.min_urgency)) {
            logger.logVerbose(`Skipping push for ${method.label} — insight urgency ${insight.urgency} below min ${method.min_urgency}`);
            break;
          }
          const pushPrefs = method.push_preferences;
          // Check if notifications are disabled for this device
          if (pushPrefs && pushPrefs.notifications === false) {
            logger.log(`Skipping push for ${method.label} (push_preferences.notifications=false)`);
            break;
          }
          // Check urgency filter — skip if device only wants urgent and this isn't
          if (pushPrefs?.urgency_filter === 'urgent' && insight.urgency !== 'urgent') {
            logger.log(`Skipping push for ${method.label} (urgency_filter=urgent, insight=${insight.urgency})`);
            break;
          }
          await deliverPush(insight, method.config, inboxId);
          deliveredVia.push(`push:${method.label}`);
          break;
        }
        case 'homepod': {
          if (!meetsMinUrgency(insight.urgency, method.min_urgency)) {
            logger.logVerbose(`Skipping HomePod — insight urgency ${insight.urgency} below min ${method.min_urgency}`);
            break;
          }
          // Queue HomePod announcement via speak-homepod job
          const target = (method.config as Record<string, string>)?.target || null;
          await pool.query(
            `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
             VALUES ('speak-homepod', $1, 'pending', 8, 2)`,
            [JSON.stringify({ message: insight.title + '. ' + insight.body, target })]
          );
          deliveredVia.push('homepod');
          logger.log(`Queued HomePod announcement for insight ${insight.id}`);
          break;
        }
        case 'report':
          // No longer needed — inbox item already created above
          deliveredVia.push(`report:${method.label}`);
          break;
        case 'pushover': {
          if (!meetsMinUrgency(insight.urgency, method.min_urgency)) {
            logger.logVerbose(`Skipping Pushover — insight urgency ${insight.urgency} below min ${method.min_urgency}`);
            break;
          }
          // Queue Pushover notification via send-pushover job
          await pool.query(
            `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
             VALUES ('send-pushover', $1, $2, 2)`,
            [JSON.stringify({
              title: insight.urgency === 'urgent' ? `⚡ ${insight.title}` : insight.title,
              message: insight.body.length > 500 ? insight.body.slice(0, 497) + '...' : insight.body,
              priority: insight.urgency === 'urgent' ? 1 : 0,
            }),
            insight.urgency === 'urgent' ? 3 : 1]
          );
          deliveredVia.push('pushover');
          logger.log(`Queued Pushover notification for insight ${insight.id}`);
          break;
        }
        default:
          logger.logMinimal(`Unknown channel: ${method.channel}`);
      }
    } catch (err) {
      logger.logMinimal(`Failed to deliver via ${method.channel}:${method.label}:`, err instanceof Error ? err.message : err);
    }
  }

  // Update insight status
  if (deliveredVia.length > 0) {
    await pool.query(
      `UPDATE proactive_insights
       SET status = 'delivered', delivered_at = NOW(), delivery_methods = $1
       WHERE id = $2`,
      [deliveredVia, payload.insight_id]
    );
  }

  logEvent({
    action: `Insight delivered: "${insight.title}" via ${deliveredVia.join(', ')}`,
    component: 'proactive',
    category: 'interaction',
    metadata: { insight_id: insight.id, methods: deliveredVia },
  });

  logger.log(`Delivered "${insight.title}" via ${deliveredVia.join(', ')}`);

  return {
    insight_id: insight.id,
    delivered_via: deliveredVia,
    title: insight.title,
  };
}
