/**
 * Digest Compile handler.
 *
 * Batches pending standard insights into a formatted digest email,
 * then delivers via the user's configured standard delivery methods.
 *
 * Scheduled twice daily (morning + evening) via ensureRecurringJob.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';
import { formatDateET, formatDateTimeET } from '../lib/timezone.js';

const logger = createLogger('digest-compile');

interface InsightRow {
  id: string;
  title: string;
  body: string;
  category: string;
  urgency: string;
  confidence: number;
  created_at: Date;
}

export async function handleDigestCompile(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { period: string };
  logger.log(`Compiling ${payload.period} digest...`);

  const pool = getPool();

  // Check if digest-compile subscription is enabled
  const { rows: subRows } = await pool.query(
    `SELECT is_enabled FROM report_subscriptions WHERE report_type = 'digest-compile' LIMIT 1`
  );
  if (subRows.length > 0 && !subRows[0].is_enabled) {
    logger.log('digest-compile subscription is disabled, skipping');
    return { skipped: true, reason: 'subscription_disabled' };
  }

  // Fetch pending standard insights not yet delivered
  const { rows: insights } = await pool.query<InsightRow>(
    `SELECT id, title, body, category, urgency, confidence, created_at
     FROM proactive_insights
     WHERE status = 'pending' AND urgency = 'standard'
     ORDER BY confidence DESC, created_at ASC
     LIMIT 20`
  );

  if (insights.length === 0) {
    logger.log('No pending standard insights to compile');
    return { compiled: 0, period: payload.period };
  }

  // Use LLM router to compose a cohesive digest from the individual insights
  const insightsList = insights.map((i, idx) =>
    `${idx + 1}. [${i.category}] ${i.title}\n   ${i.body} (confidence: ${Math.round(i.confidence * 100)}%)`
  ).join('\n\n');

  const now = new Date();
  const greeting = payload.period === 'morning'
    ? `Good morning! Here's what ARIA noticed overnight.`
    : `Good evening! Here's what ARIA picked up today.`;

  const result = await routeRequest({
    handler: 'digest-compile',
    taskTier: 'generation',
    systemPrompt: `You are ARIA, composing a ${payload.period} digest of proactive insights for your owner. Format as a clean, scannable email. Start with a brief greeting, then organize the insights by category. Be concise but informative. Plain text, no HTML.`,
    userMessage: `${greeting}\n\nTime: ${formatDateTimeET(now, { weekday: 'long', month: 'long', day: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true })}\n\nInsights to include:\n${insightsList}\n\nCompose the digest:`,
    maxTokens: 2048,
    useBatch: true,
  });
  logger.log(`Generated via ${result.model} (${result.provider}, ${result.estimatedCostCents}¢)`);

  const digestBody = result.text;

  // Load delivery preferences for standard insights
  const { rows: settingsRows } = await pool.query(
    `SELECT value FROM proactive_settings WHERE key = 'standard_delivery_methods'`
  );
  const deliveryMethodIds = (settingsRows[0]?.value as string[]) ?? [];

  if (deliveryMethodIds.length === 0) {
    logger.log('No standard delivery methods configured, skipping delivery');
    return { compiled: insights.length, delivered: false, period: payload.period };
  }

  // Create a synthetic insight for the digest itself
  const { rows: digestRows } = await pool.query<{ id: string }>(
    `INSERT INTO proactive_insights
     (title, body, category, urgency, confidence, risk_level_required, status)
     VALUES ($1, $2, 'briefing', 'standard', 1.0, 'low', 'pending')
     RETURNING id`,
    [
      `${payload.period === 'morning' ? 'Morning' : 'Evening'} Digest — ${formatDateET(now, { month: 'short', day: 'numeric' })}`,
      digestBody,
    ]
  );

  const digestInsightId = digestRows[0].id;

  // Enqueue delivery of the compiled digest
  await pool.query(
    `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
     VALUES ($1, $2, $3, $4)`,
    [
      'insight-deliver',
      JSON.stringify({ insight_id: digestInsightId, delivery_method_ids: deliveryMethodIds }),
      1,
      2,
    ]
  );

  // Mark the individual insights as delivered (they're part of the digest now)
  const insightIds = insights.map(i => i.id);
  await pool.query(
    `UPDATE proactive_insights
     SET status = 'delivered', delivered_at = NOW(), delivery_methods = ARRAY['digest']
     WHERE id = ANY($1)`,
    [insightIds]
  );

  logEvent({
    action: `Compiled ${payload.period} digest with ${insights.length} insight(s)`,
    component: 'proactive',
    category: 'background',
    metadata: { period: payload.period, insight_count: insights.length, digest_length: digestBody.length },
  });

  logger.log(`Compiled ${insights.length} insights into ${payload.period} digest (${digestBody.length} chars)`);

  return {
    compiled: insights.length,
    delivered: true,
    period: payload.period,
    digest_insight_id: digestInsightId,
    digest_length: digestBody.length,
  };
}
