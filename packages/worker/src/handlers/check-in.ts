/**
 * Check-in handler.
 *
 * Sends a periodic "how are you doing?" text. If no custom message is provided,
 * uses the LLM router to generate a contextual check-in based on recent activity
 * and core memory.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { deliverReport } from '../lib/report-delivery.js';
import { routeRequest } from '../lib/llm/index.js';
import { hourET, formatDateTimeET } from '../lib/timezone.js';

const logger = createLogger('check-in');

/**
 * Generate a contextual check-in message using recent activity and memory.
 */
async function generateCheckInMessage(): Promise<string> {
  const pool = getPool();

  const { rows: recentConvos } = await pool.query(
    `SELECT title FROM conversations
     WHERE hidden_at IS NULL
     ORDER BY updated_at DESC
     LIMIT 5`
  );

  const { rows: goals } = await pool.query(
    `SELECT key, value FROM core_memory
     WHERE superseded_by IS NULL
       AND category IN ('goals', 'recurring', 'context')
     ORDER BY created_at DESC
     LIMIT 8`
  );

  // Fetch recent knowledge graph facts for context
  const { rows: profileFacts } = await pool.query(
    `SELECT domain, key, value FROM knowledge_facts
     WHERE superseded_by IS NULL AND confidence >= 0.5
     ORDER BY last_confirmed_at DESC NULLS LAST
     LIMIT 10`
  );

  const recentTopics = recentConvos.map(r => r.title).filter(Boolean).join(', ');
  const goalContext = goals.map(r => `${r.key}: ${r.value}`).join('\n');
  const profileContext = profileFacts.map((r: { domain: string; key: string; value: string }) => `[${r.domain}] ${r.key}: ${r.value}`).join('\n');

  const now = new Date();
  const hour = hourET(now);
  const timeContext = hour < 12 ? 'morning' : hour < 17 ? 'afternoon' : 'evening';

  const result = await routeRequest({
    handler: 'check-in',
    taskTier: 'generation',
    systemPrompt: `You are ARIA, a caring personal AI assistant. Generate a brief, warm check-in text message (2-3 sentences max). Be genuine, not performative. Reference something specific when possible. No emojis. Keep it short.`,
    userMessage: `It's ${timeContext} (${formatDateTimeET(now, { weekday: 'long', hour: 'numeric', minute: '2-digit', hour12: true })}). Generate a check-in.\n\nRecent topics: ${recentTopics || 'none'}\n\nGoals/context:\n${goalContext || 'none'}\n\nPersonal knowledge:\n${profileContext || 'none'}`,
    maxTokens: 600,
  });

  logger.log(`Generated via ${result.model} (${result.provider}, ${result.estimatedCostCents}¢)`);
  return result.text;
}

export async function handleCheckIn(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { recipient: string; message?: string };
  logger.log(`Processing check-in for ${payload.recipient}`);

  const message = payload.message || await generateCheckInMessage();

  const deliveredVia = await deliverReport({
    reportType: 'check-in',
    title: 'Check-In',
    body: message,
    category: 'general',
  });

  logger.log(`Delivered via: ${deliveredVia.join(', ') || 'none'}`);

  const pool = getPool();
  await pool.query(
    `INSERT INTO actions_log (action_type, description, metadata)
     VALUES ($1, $2, $3)`,
    ['check_in', `Check-in delivered via ${deliveredVia.join(', ')}`, JSON.stringify({ deliveredVia, generated: !payload.message })]
  );

  return { delivered_via: deliveredVia, generated: !payload.message };
}
