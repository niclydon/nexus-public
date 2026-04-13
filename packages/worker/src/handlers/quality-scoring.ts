/**
 * Conversation quality scoring handler.
 *
 * Runs daily, analyzes recent conversations for quality signals,
 * scores them 1-5, and feeds low-quality patterns into self-improvement.
 *
 * Uses the classification LLM tier (cheap/free) for scoring.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';

const logger = createLogger('quality-scoring');

interface ConversationRow {
  id: string;
  title: string | null;
  created_at: string;
}

interface MessageRow {
  id: string;
  role: string;
  content: string;
  created_at: string;
}

interface FeedbackRow {
  rating: number;
}

/**
 * Count consecutive user messages (possible rephrasing / frustration).
 */
function countRephrasedQuestions(messages: MessageRow[]): number {
  let count = 0;
  let consecutiveUser = 0;
  for (const msg of messages) {
    if (msg.role === 'user') {
      consecutiveUser++;
      if (consecutiveUser >= 2) count++;
    } else {
      consecutiveUser = 0;
    }
  }
  return count;
}

/**
 * Count messages containing error-like patterns.
 */
function countToolErrors(messages: MessageRow[]): number {
  const errorPatterns = /error|failed|exception|couldn't|unable to|sorry.*can't/i;
  return messages.filter(
    (m) => m.role === 'assistant' && errorPatterns.test(m.content)
  ).length;
}

/**
 * Count user messages sent within 10 seconds of each other (rapid-fire / impatience).
 */
function countRapidFire(messages: MessageRow[]): number {
  let count = 0;
  let lastUserTime: number | null = null;
  for (const msg of messages) {
    if (msg.role === 'user') {
      const t = new Date(msg.created_at).getTime();
      if (lastUserTime !== null && t - lastUserTime < 10_000) {
        count++;
      }
      lastUserTime = t;
    }
  }
  return count;
}

export async function handleQualityScoring(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  logger.log(`Starting quality scoring job ${job.id}`);

  // 1. Fetch conversations from the last 24 hours with at least 4 messages
  const { rows: conversations } = await pool.query<ConversationRow>(`
    SELECT DISTINCT c.id, c.title, c.created_at::text
    FROM conversations c
    JOIN messages m ON m.conversation_id = c.id
    WHERE c.created_at >= NOW() - INTERVAL '24 hours'
    GROUP BY c.id, c.title, c.created_at
    HAVING COUNT(*) >= 4
  `);

  if (conversations.length === 0) {
    logger.log('No qualifying conversations found. Skipping.');
    logEvent({
      action: 'Quality scoring skipped — no qualifying conversations',
      component: 'quality-scoring',
      category: 'self_maintenance',
      metadata: { job_id: job.id, reason: 'no_conversations' },
    });
    return { skipped: true, reason: 'no_conversations' };
  }

  // Limit to 10 per run to control costs
  const batch = conversations.slice(0, 10);
  logger.log(`Scoring ${batch.length} conversation(s)`);

  let scored = 0;
  let lowQuality = 0;

  for (const convo of batch) {
    try {
      // Check if already scored
      const { rowCount: alreadyScored } = await pool.query(
        `SELECT 1 FROM conversation_quality WHERE conversation_id = $1`,
        [convo.id]
      );
      if (alreadyScored && alreadyScored > 0) continue;

      // Fetch messages (limit 30)
      const { rows: messages } = await pool.query<MessageRow>(
        `SELECT id, role, content, created_at::text FROM messages
         WHERE conversation_id = $1
         ORDER BY created_at ASC LIMIT 30`,
        [convo.id]
      );

      // Fetch explicit feedback
      const { rows: feedback } = await pool.query<FeedbackRow>(
        `SELECT rating FROM message_feedback WHERE conversation_id = $1`,
        [convo.id]
      );

      // Compute implicit signals
      const rephrasedQuestions = countRephrasedQuestions(messages);
      const toolErrors = countToolErrors(messages);
      const rapidFire = countRapidFire(messages);
      const conversationLength = messages.length;
      const explicitFeedbackAvg =
        feedback.length > 0
          ? (feedback.reduce((sum, f) => sum + f.rating, 0) / feedback.length).toFixed(2)
          : 'none';

      // Build message sample: first 3 and last 3, truncated
      const first3 = messages.slice(0, 3);
      const last3 = messages.slice(-3);
      const sampleMessages = [...new Set([...first3, ...last3])];
      const messageSample = sampleMessages
        .map((m) => `${m.role}: ${m.content.slice(0, 100)}`)
        .join('\n');

      // Send to LLM for scoring (classification tier = cheap)
      const llmResult = await routeRequest({
        handler: 'quality-scoring',
        taskTier: 'classification',
        systemPrompt:
          'You are a conversation quality analyst. Score the conversation quality 1-5 based on the provided signals. Respond with JSON only: {"score": N, "reason": "brief explanation"}',
        userMessage: `Score this conversation quality 1-5 based on these signals:
- Rephrased questions: ${rephrasedQuestions} (higher = more frustration)
- Tool errors: ${toolErrors}
- Rapid-fire messages: ${rapidFire} (possible impatience)
- Conversation length: ${conversationLength} messages
- Explicit feedback: ${explicitFeedbackAvg}
- Message sample:
${messageSample}

Respond with JSON: {"score": N, "reason": "brief explanation"}`,
        maxTokens: 1200,
        useBatch: false,
      });

      // Parse response
      let score: number;
      let reason: string;
      try {
        const jsonMatch = llmResult.text.match(/\{[\s\S]*\}/);
        const parsed = JSON.parse(jsonMatch?.[0] ?? '{}');
        score = Math.max(1, Math.min(5, Math.round(parsed.score ?? 3)));
        reason = parsed.reason ?? 'No reason provided';
      } catch {
        logger.logMinimal(`Failed to parse LLM response for ${convo.id}, defaulting to 3`);
        score = 3;
        reason = 'Unable to parse LLM response';
      }

      const signals = {
        rephrased_questions: rephrasedQuestions,
        tool_errors: toolErrors,
        rapid_fire: rapidFire,
        conversation_length: conversationLength,
        explicit_feedback_avg: explicitFeedbackAvg,
        llm_reason: reason,
      };

      // Insert into conversation_quality
      await pool.query(
        `INSERT INTO conversation_quality (conversation_id, score, signals)
         VALUES ($1, $2, $3)`,
        [convo.id, score, JSON.stringify(signals)]
      );

      scored++;

      // If score <= 2, feed into self-improvement pipeline
      if (score <= 2) {
        lowQuality++;
        const title = `Low quality conversation: ${convo.title ?? convo.id}`;
        await pool.query(
          `INSERT INTO aria_self_improvement (category, status, title, description, source, confidence)
           VALUES ('ux_improvement', 'idea', $1, $2, 'quality_scoring', 0.7)`,
          [title, reason]
        );
        logger.log(`Low quality (${score}/5) for conversation ${convo.id}: ${reason}`);
      }
    } catch (err) {
      logger.logMinimal(`Error scoring conversation ${convo.id}:`, err);
    }
  }

  logEvent({
    action: `Quality scoring completed: ${scored} scored, ${lowQuality} low-quality`,
    component: 'quality-scoring',
    category: 'self_maintenance',
    metadata: { job_id: job.id, scored, low_quality: lowQuality, total_candidates: batch.length },
  });

  logger.log(`Done. Scored ${scored} conversations, ${lowQuality} flagged as low quality.`);

  return { scored, low_quality: lowQuality, total_candidates: batch.length };
}
