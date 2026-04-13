/**
 * Self-improve research handler.
 *
 * Periodically researches AI assistant trends and ARIA's competitive landscape,
 * then generates improvement ideas stored in aria_self_improvement.
 *
 * Requires: BRAVE_SEARCH_API_KEY (optional), LLM provider
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { researchTopic } from '../lib/web-research.js';
import { routeRequest } from '../lib/llm/index.js';

const logger = createLogger('self-improve-research');

/** Rotating research topics — cycled by day of month */
const RESEARCH_TOPICS = [
  'AI personal assistant features trends 2026',
  'proactive AI assistant capabilities',
  'AI assistant privacy data protection trends',
  'best personal AI integrations consumers want',
  'AI wearable companion assistant trends',
];

interface ImprovementIdea {
  title: string;
  description: string;
  category: string;
  priority: number;
  confidence: number;
  estimated_effort: string;
}

/**
 * Fetch ARIA's current stats for context.
 */
async function fetchAriaStats(): Promise<string> {
  const pool = getPool();
  const lines: string[] = [];

  try {
    const { rows: memRows } = await pool.query(
      `SELECT COUNT(*) as count FROM core_memory WHERE superseded_by IS NULL`
    );
    lines.push(`Active memories: ${memRows[0].count}`);
  } catch {
    lines.push('Active memories: unavailable');
  }

  try {
    const { rows: insightRows } = await pool.query(
      `SELECT COUNT(*) as count FROM proactive_insights WHERE created_at > NOW() - INTERVAL '30 days'`
    );
    lines.push(`Proactive insights (30d): ${insightRows[0].count}`);
  } catch {
    lines.push('Proactive insights: unavailable');
  }

  try {
    const { rows: convRows } = await pool.query(
      `SELECT COUNT(*) as count FROM conversations`
    );
    lines.push(`Total conversations: ${convRows[0].count}`);
  } catch {
    lines.push('Total conversations: unavailable');
  }

  try {
    const { rows: siRows } = await pool.query(
      `SELECT status, COUNT(*) as count FROM aria_self_improvement GROUP BY status`
    );
    if (siRows.length > 0) {
      const statusLine = siRows.map((r: { status: string; count: string }) => `${r.status}: ${r.count}`).join(', ');
      lines.push(`Self-improvement entries: ${statusLine}`);
    } else {
      lines.push('Self-improvement entries: none');
    }
  } catch {
    lines.push('Self-improvement entries: unavailable');
  }

  return lines.join('\n');
}

export async function handleSelfImproveResearch(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = (job.payload ?? {}) as { focus?: string };
  logger.log(`Starting research job ${job.id}`);

  // Pick research focus — use payload override or rotate by day of month
  const dayOfMonth = new Date().getDate();
  const topicIndex = dayOfMonth % RESEARCH_TOPICS.length;
  const query = payload.focus ?? RESEARCH_TOPICS[topicIndex];
  logger.log(`Research focus: "${query}"`);

  // Research the topic
  const research = await researchTopic({
    query,
    maxResults: 5,
    handler: 'self-improve-research',
  });

  // Fetch ARIA's current stats
  const ariaStats = await fetchAriaStats();

  // Ask LLM to generate improvement ideas
  const result = await routeRequest({
    handler: 'self-improve-research',
    taskTier: 'generation',
    preferredModel: 'gpt-4o-mini',
    systemPrompt:
      'You are analyzing market research and a personal AI assistant\'s current capabilities to suggest specific, small improvements. ' +
      'Return ONLY valid JSON (no markdown fences): an array of 1-3 objects, each with: ' +
      '{ "title": "string", "description": "string", "category": "string (one of: bug_fix, optimization, feature_idea, code_quality, database_tune, memory_insight, research_finding, proactive_tune, integration_idea, ux_improvement)", ' +
      '"priority": number (1-10), "confidence": number (0-1), "estimated_effort": "string (trivial|small|medium|large)" }. ' +
      'Focus on actionable, specific improvements — not vague suggestions. If no research data is available, use your general knowledge about AI personal assistant best practices.',
    userMessage: research.sources.length > 0
      ? `Research query: "${query}"

Research findings:
${research.summary}

Key findings:
${research.keyFindings.map((f: string, i: number) => `${i + 1}. ${f}`).join('\n')}

Sources: ${research.sources.join(', ')}

ARIA's current stats:
${ariaStats}

Based on this market research and ARIA's current capabilities, suggest 1-3 specific, small improvements ARIA could make to herself.`
      : `Topic: "${query}"

No web search results were available, so use your general knowledge about AI personal assistant trends and best practices for 2025-2026.

ARIA's current stats:
${ariaStats}

ARIA is a personal AI assistant with 119 tools, 43 database tables, 27 background jobs, a 4-gate proactive intelligence pipeline monitoring 17 data sources, Apple Music integration, a private journal system, and a self-improvement system. She runs on Next.js + PostgreSQL + ECS Fargate.

Based on your knowledge of AI assistant best practices and ARIA's current capabilities, suggest 1-3 specific, small improvements.`,
    maxTokens: 1500,
    useBatch: true,
  });

  logger.log(`LLM response via ${result.model} (${result.provider}, ${result.estimatedCostCents}¢)`);

  // Parse ideas from LLM response (resilient: handles text around JSON)
  let ideas: ImprovementIdea[] = [];
  try {
    const cleaned = result.text.replace(/```json\s*/g, '').replace(/```\s*/g, '').trim();
    const parsed = JSON.parse(cleaned);
    ideas = Array.isArray(parsed) ? parsed : [];
  } catch {
    logger.logVerbose('Fence-strip parse failed, trying array extraction');
    const firstBracket = result.text.indexOf('[');
    const lastBracket = result.text.lastIndexOf(']');
    if (firstBracket !== -1 && lastBracket > firstBracket) {
      try {
        const extracted = JSON.parse(result.text.slice(firstBracket, lastBracket + 1));
        ideas = Array.isArray(extracted) ? extracted : [];
      } catch {
        logger.logMinimal('Failed to parse improvement ideas after extraction:', result.text.slice(0, 500));
      }
    } else {
      logger.logMinimal('No JSON array found in improvement response:', result.text.slice(0, 500));
    }
  }

  // Insert each idea into aria_self_improvement
  const pool = getPool();
  const insertedIds: string[] = [];

  const VALID_CATEGORIES = ['bug_fix', 'optimization', 'feature_idea', 'code_quality', 'database_tune', 'memory_insight', 'research_finding', 'proactive_tune', 'integration_idea', 'ux_improvement'];

  for (const idea of ideas) {
    try {
      const category = VALID_CATEGORIES.includes(idea.category) ? idea.category : 'feature_idea';
      const { rows } = await pool.query(
        `INSERT INTO aria_self_improvement
         (category, status, title, description, priority, confidence, estimated_effort, source, research_notes)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         RETURNING id`,
        [
          category,
          'idea',
          idea.title,
          idea.description,
          idea.priority ?? 5,
          idea.confidence ?? 0.5,
          idea.estimated_effort || 'small',
          'research',
          JSON.stringify({
            query,
            summary: research.summary,
            keyFindings: research.keyFindings,
            sources: research.sources,
          }),
        ]
      );
      insertedIds.push(rows[0].id);
      logger.log(`Inserted idea: "${idea.title}" (${rows[0].id})`);
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal(`Failed to insert idea "${idea.title}": ${msg}`);
    }
  }

  // Log to event_log
  logEvent({
    action: `Self-improve research: generated ${insertedIds.length} idea(s) from "${query}"`,
    component: 'self-improvement',
    category: 'self_maintenance',
    metadata: {
      query,
      ideas_count: insertedIds.length,
      idea_ids: insertedIds,
      sources_count: research.sources.length,
    },
  });

  return {
    query,
    ideas_generated: insertedIds.length,
    idea_ids: insertedIds,
    sources: research.sources.length,
  };
}
