/**
 * Digest email handler.
 *
 * Generates a weekly recap of conversations, decisions, and action items,
 * then sends it as an email via Gmail.
 *
 * Requires: ANTHROPIC_API_KEY, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { deliverReport } from '../lib/report-delivery.js';
import { routeRequest } from '../lib/llm/index.js';
import { formatDateET } from '../lib/timezone.js';

const logger = createLogger('digest-email');

/**
 * Fetch conversations and messages from the past N days.
 */
async function fetchRecentActivity(periodDays: number): Promise<string> {
  const pool = getPool();
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - periodDays);

  // Single query using LATERAL join to fetch conversations + their messages
  // Replaces N+1 pattern (1 query for conversations + 1 per conversation)
  const { rows } = await pool.query<{
    id: string;
    title: string | null;
    created_at: string;
    msg_count: string;
    role: string;
    content: string;
    msg_created: string;
  }>(
    `SELECT c.id, c.title, c.created_at,
            (SELECT COUNT(*) FROM messages m2 WHERE m2.conversation_id = c.id)::text as msg_count,
            m.role, LEFT(m.content, 300) as content, m.created_at as msg_created
     FROM conversations c
     CROSS JOIN LATERAL (
       (SELECT role, content, created_at FROM messages WHERE conversation_id = c.id AND role = 'user' ORDER BY created_at ASC LIMIT 3)
       UNION ALL
       (SELECT role, content, created_at FROM messages WHERE conversation_id = c.id AND role = 'assistant' ORDER BY created_at DESC LIMIT 1)
     ) m
     WHERE c.created_at > $1
       AND c.hidden_at IS NULL
     ORDER BY c.created_at DESC, m.created_at ASC
     LIMIT 120`,
    [cutoff.toISOString()]
  );

  if (rows.length === 0) return 'No conversations in this period.';

  // Group rows by conversation ID to reconstruct the conversation -> messages structure
  const conversationMap = new Map<string, {
    title: string | null;
    created_at: string;
    msg_count: string;
    messages: Array<{ role: string; content: string }>;
  }>();

  for (const row of rows) {
    let convo = conversationMap.get(row.id);
    if (!convo) {
      convo = { title: row.title, created_at: row.created_at, msg_count: row.msg_count, messages: [] };
      conversationMap.set(row.id, convo);
    }
    convo.messages.push({ role: row.role, content: row.content });
  }

  const sections: string[] = [];
  for (const [, convo] of conversationMap) {
    const dateStr = formatDateET(new Date(convo.created_at), { month: 'short', day: 'numeric' });
    const msgSummaries = convo.messages.map(m => {
      const truncated = m.content;
      return `  [${m.role}]: ${truncated}${truncated.length >= 300 ? '...' : ''}`;
    });
    sections.push(`Conversation: "${convo.title || '(untitled)'}" (${dateStr}, ${convo.msg_count} messages)\n${msgSummaries.join('\n')}`);
  }

  return sections.join('\n\n---\n\n');
}

/**
 * Fetch memory changes from the period.
 */
async function fetchMemoryChanges(periodDays: number): Promise<string> {
  const pool = getPool();
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - periodDays);

  const { rows } = await pool.query(
    `SELECT category, key, value, created_at FROM core_memory
     WHERE created_at > $1
       AND superseded_by IS NULL
     ORDER BY created_at DESC
     LIMIT 20`,
    [cutoff.toISOString()]
  );

  if (rows.length === 0) return 'No new memories saved.';

  const lines = rows.map(r => {
    const date = formatDateET(new Date(r.created_at), { month: 'short', day: 'numeric' });
    return `  - [${r.category}] ${r.key}: ${r.value} (${date})`;
  });

  return `New memories:\n${lines.join('\n')}`;
}

/**
 * Fetch actions taken during the period.
 */
async function fetchRecentActions(periodDays: number): Promise<string> {
  const pool = getPool();
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - periodDays);

  const { rows } = await pool.query(
    `SELECT action_type, description, created_at FROM actions_log
     WHERE created_at > $1
     ORDER BY created_at DESC
     LIMIT 30`,
    [cutoff.toISOString()]
  );

  if (rows.length === 0) return 'No logged actions.';

  const lines = rows.map(r => {
    const date = formatDateET(new Date(r.created_at), { month: 'short', day: 'numeric' });
    return `  - ${date}: [${r.action_type}] ${r.description}`;
  });

  return `Actions taken:\n${lines.join('\n')}`;
}

export async function handleDigestEmail(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { period_days?: number; recipient_email: string };

  // Check if subscription is enabled
  const pool = getPool();
  const { rows: subRows } = await pool.query(
    `SELECT is_enabled FROM report_subscriptions WHERE report_type = $1 LIMIT 1`,
    ['digest-email']
  );
  if (subRows.length > 0 && !subRows[0].is_enabled) {
    logger.log('Subscription disabled, skipping');
    return { skipped: true, reason: 'subscription_disabled' };
  }

  const periodDays = payload.period_days ?? 7;
  logger.log(`Generating ${periodDays}-day digest for ${payload.recipient_email}`);

  // Gather all data in parallel
  const [activity, memoryChanges, actions] = await Promise.all([
    fetchRecentActivity(periodDays),
    fetchMemoryChanges(periodDays),
    fetchRecentActions(periodDays),
  ]);

  const now = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - periodDays);
  const dateRange = `${formatDateET(startDate, { month: 'long', day: 'numeric' })} - ${formatDateET(now, { month: 'long', day: 'numeric', year: 'numeric' })}`;

  const result = await routeRequest({
    handler: 'digest-email',
    taskTier: 'generation',
    systemPrompt: `You are ARIA, a personal AI assistant. Compose a weekly digest email for your owner. Structure it as:

1. **Highlights** — 3-5 bullet points of the most important things that happened
2. **Decisions Made** — Any decisions or conclusions reached in conversations
3. **Action Items** — Outstanding tasks or follow-ups mentioned but possibly not completed
4. **Memory Updates** — Notable new things learned/remembered
5. **Patterns & Observations** — Any patterns you notice (e.g., recurring topics, shifts in focus)

Be direct and useful. Use plain text formatting (no HTML). Keep it thorough but scannable.`,
    userMessage: `Generate my digest for ${dateRange}.\n\nCONVERSATIONS:\n${activity}\n\n${memoryChanges}\n\n${actions}`,
    maxTokens: 4096,
    useBatch: true,
  });

  const digestBody = result.text;
  logger.log(`Generated via ${result.model} (${result.provider}, ${result.estimatedCostCents}¢)`);

  const subject = `Weekly Digest: ${dateRange}`;

  // Deliver via configured methods
  const deliveredVia = await deliverReport({
    reportType: 'digest-email',
    title: subject,
    body: digestBody,
    category: 'general',
    metadata: { periodDays, dateRange },
  });

  logger.log(`Delivered via: ${deliveredVia.join(', ') || 'none'}`);

  // Log to actions_log
  await pool.query(
    `INSERT INTO actions_log (action_type, description, metadata)
     VALUES ($1, $2, $3)`,
    ['digest_email', `Weekly digest delivered via ${deliveredVia.join(', ')} (${dateRange})`, JSON.stringify({ deliveredVia, periodDays })]
  );

  return {
    delivered_via: deliveredVia,
    period_days: periodDays,
    digest_length: digestBody.length,
  };
}
