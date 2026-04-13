/**
 * Anticipation Engine — Gate 3 of the Proactive Intelligence pipeline.
 *
 * Uses Claude Sonnet to perform deep analysis on significant context changes,
 * generate proactive insights, detect patterns, and determine appropriate
 * actions based on the current risk level setting.
 *
 * This is the expensive gate — only invoked when Gate 2 (Gemini) determines
 * something meaningful warrants full analysis.
 */
import { routeRequest } from '../llm/index.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../event-log.js';
import { formatDateTimeET } from '../timezone.js';

const logger = createLogger('proactive-anticipation');


interface SnapshotRow {
  id: string;
  source: string;
  change_summary: string;
  change_data: Record<string, unknown>;
  significance: string;
}

interface GeneratedInsight {
  title: string;
  body: string;
  reasoning: string;
  category: string;
  urgency: 'standard' | 'urgent';
  confidence: number;
  risk_level_required: 'low' | 'medium' | 'high';
  action_type?: string;
  action_data?: Record<string, unknown>;
  expires_at?: string;
}

interface PatternObservation {
  pattern_type: 'temporal' | 'preparatory' | 'sequential' | 'contextual' | 'absence' | 'correlation';
  description: string;
  evidence: string;
  confidence: number;
}

interface AnalysisResult {
  insights: GeneratedInsight[];
  patterns: PatternObservation[];
  summary: string;
}

export interface EngineOutput {
  insight_ids: string[];
  pattern_ids: string[];
  analysis_summary: string;
}

/**
 * Load current proactive settings from the database.
 */
interface EngineSettings {
  risk_level: string;
  confidence_threshold: number;
  category_thresholds: Record<string, number>;
}

async function loadSettings(): Promise<EngineSettings> {
  const pool = getPool();
  try {
    const { rows } = await pool.query(
      `SELECT key, value FROM proactive_settings WHERE key IN ('risk_level', 'confidence_threshold', 'confidence_thresholds')`
    );
    let risk_level = 'medium';
    let confidence_threshold = 0.7;
    let category_thresholds: Record<string, number> = {};
    for (const row of rows) {
      if (row.key === 'risk_level') risk_level = row.value as string;
      if (row.key === 'confidence_threshold') confidence_threshold = row.value as number;
      if (row.key === 'confidence_thresholds') {
        category_thresholds = (typeof row.value === 'string' ? JSON.parse(row.value) : row.value) as Record<string, number>;
      }
    }
    return { risk_level, confidence_threshold, category_thresholds };
  } catch {
    return { risk_level: 'medium', confidence_threshold: 0.7, category_thresholds: {} };
  }
}

/**
 * Load recent core memory for context.
 */
async function loadMemoryContext(): Promise<string> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT category, key, value FROM core_memory
     WHERE superseded_by IS NULL
     ORDER BY category, key
     LIMIT 50`
  );
  if (rows.length === 0) return 'No stored memories.';
  let current = '';
  const lines: string[] = [];
  for (const r of rows) {
    if (r.category !== current) {
      current = r.category;
      lines.push(`\n[${current.toUpperCase()}]`);
    }
    lines.push(`  ${r.key}: ${r.value}`);
  }
  return lines.join('\n');
}

/**
 * Load recently generated insights to avoid duplicates.
 */
async function loadRecentInsights(): Promise<string[]> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT title FROM proactive_insights
     WHERE created_at > NOW() - INTERVAL '24 hours'
     ORDER BY created_at DESC LIMIT 20`
  );
  return rows.map(r => r.title);
}

/**
 * Load cross-source behavioral context — mood, correlations, transitions, decaying contacts.
 * Gives Gate 3 the bigger picture beyond the individual snapshot.
 */
async function loadBehavioralContext(pool: ReturnType<typeof getPool>): Promise<string> {
  const sections: string[] = [];

  try {
    // Mood proxy (last 7 days)
    const { rows: mood } = await pool.query(
      `SELECT date::text, mood_score FROM aurora_mood_proxy ORDER BY date DESC LIMIT 7`
    );
    if (mood.length > 0) {
      const avg = mood.reduce((s: number, m: { mood_score: number }) => s + m.mood_score, 0) / mood.length;
      const latest = mood[0];
      sections.push(`[MOOD] Latest: ${(latest.mood_score * 100).toFixed(0)}/100 (${latest.date}), 7-day avg: ${(avg * 100).toFixed(0)}/100`);
    }
  } catch { /* table may not exist yet */ }

  try {
    // Decaying relationships
    const { rows: decay } = await pool.query(
      `SELECT r.contact_name, r.relationship_tier, r.frequency_trend
       FROM aurora_relationships r
       JOIN aurora_social_identities si ON si.id = r.identity_id
       WHERE r.period_start = (SELECT MAX(period_start) FROM aurora_relationships)
         AND si.is_person = TRUE
         AND r.relationship_tier IN ('inner_circle', 'close')
         AND r.frequency_trend IN ('decreasing', 'silent')
       LIMIT 5`
    );
    if (decay.length > 0) {
      sections.push(`[RELATIONSHIPS] Decaying: ${decay.map((d: { contact_name: string; frequency_trend: string }) => `${d.contact_name} (${d.frequency_trend})`).join(', ')}`);
    }
  } catch { /* */ }

  try {
    // Active life chapter
    const { rows: chapters } = await pool.query(
      `SELECT title, description FROM aurora_life_chapters WHERE end_date IS NULL ORDER BY start_date DESC LIMIT 1`
    );
    if (chapters.length > 0) {
      sections.push(`[LIFE CHAPTER] ${chapters[0].title}: ${(chapters[0].description as string).slice(0, 200)}`);
    }
  } catch { /* */ }

  try {
    // Top correlations
    const { rows: corrs } = await pool.query(
      `SELECT stream_a, stream_b, coefficient FROM aurora_correlations
       WHERE ABS(coefficient) > 0.6 ORDER BY ABS(coefficient) DESC LIMIT 3`
    );
    if (corrs.length > 0) {
      sections.push(`[CORRELATIONS] ${corrs.map((c: { stream_a: string; stream_b: string; coefficient: number }) => `${c.stream_a}↔${c.stream_b} (r=${c.coefficient.toFixed(2)})`).join(', ')}`);
    }
  } catch { /* */ }

  return sections.length > 0 ? sections.join('\n') : 'No behavioral context available.';
}

const SYSTEM_PROMPT = `You are ARIA's Anticipation Engine — the proactive intelligence core of a personal AI assistant. Your job is to analyze detected changes in the user's world and generate actionable insights.

You will receive:
1. Recent changes detected across the user's data sources
2. The user's stored memories (preferences, goals, routines, relationships)
3. Recently generated insights (to avoid duplicates)
4. The current risk level setting

Your outputs:
1. **Insights**: Proactive observations, suggestions, or alerts the user should know about
2. **Patterns**: Behavioral patterns you've detected that could inform future predictions

INSIGHT GUIDELINES:

QUALITY BAR: Only generate an insight if it would be WORTH A PUSH NOTIFICATION to the owner's phone. If you wouldn't tap him on the shoulder to tell him this, don't generate it.

NEVER generate insights about:
- System/infrastructure status (sync jobs, build deploys, database errors, agent escalations)
- Vague or unclear signals ("Brief Check-in from Contact", "Unclear Context")
- The user's own outgoing emails or messages
- Anything from Chancery, agent escalations, or Nexus platform internals
- Generic app update notifications
- Things already mentioned in recent insights (check the list provided)

DO generate insights about:
- Personal emails/messages from real people that need a response
- Financial matters (bills, pre-approvals, account changes)
- Calendar events needing preparation
- Health anomalies
- Travel changes or upcoming trips
- Social signals (friend reaching out, birthday coming up, relationship drift)
- Time-sensitive deadlines

Rules:
- Each insight MUST include a "reasoning" field explaining WHY this matters
- Confidence 0-1: only generate if confidence >= 0.6
- Urgency: "urgent" ONLY for things needing action in the next 2 hours. Everything else is "standard"
- risk_level_required: "low" (informing), "medium" (prep needed), "high" (action needed)
- Category: calendar_prep, follow_up, health_alert, commitment_reminder, social, financial, travel, general
- If the recent insights list already has something similar, DO NOT duplicate it. Return empty insights array instead.
- It is BETTER to return zero insights than to return noise. Quality over quantity.

PATTERN GUIDELINES:
- Only report patterns you have evidence for
- Confidence should reflect how many times you've observed this

Respond with a JSON object (no markdown fences):
{
  "insights": [...],
  "patterns": [...],
  "summary": "One-sentence summary of this analysis pass"
}`;

/**
 * Run the full anticipation engine on significant snapshots.
 */
export async function analyzeAndGenerate(
  snapshotIds: string[],
  riskLevel: string
): Promise<EngineOutput> {
  logger.logVerbose(`analyzeAndGenerate() entry, ${snapshotIds.length} snapshot(s), riskLevel=${riskLevel}`);
  if (snapshotIds.length === 0) {
    return { insight_ids: [], pattern_ids: [], analysis_summary: 'No snapshots to analyze' };
  }

  const pool = getPool();
  const settings = await loadSettings();

  // Load snapshots
  const placeholders = snapshotIds.map((_, i) => `$${i + 1}`).join(', ');
  const { rows: snapshots } = await pool.query<SnapshotRow>(
    `SELECT id, source, change_summary, change_data, significance
     FROM context_snapshots WHERE id IN (${placeholders})`,
    snapshotIds
  );

  if (snapshots.length === 0) {
    return { insight_ids: [], pattern_ids: [], analysis_summary: 'Snapshots not found' };
  }

  // Load context in parallel — enriched with cross-source intelligence
  const [memoryContext, recentInsightTitles, behavioralContext] = await Promise.all([
    loadMemoryContext(),
    loadRecentInsights(),
    loadBehavioralContext(pool),
  ]);

  // Build prompt
  const now = new Date();
  const changesBlock = snapshots.map(s =>
    `[${s.source}] (${s.significance})\n${s.change_summary}\nData: ${JSON.stringify(s.change_data).slice(0, 500)}`
  ).join('\n\n');

  const userMessage = `Current time: ${now.toISOString()} (${formatDateTimeET(now, {
    weekday: 'long', month: 'long', day: 'numeric',
    hour: 'numeric', minute: '2-digit', hour12: true
  })})

Risk level: ${riskLevel}
Confidence threshold: ${settings.confidence_threshold}

DETECTED CHANGES:
${changesBlock}

USER'S MEMORY:
${memoryContext}

BEHAVIORAL CONTEXT:
${behavioralContext}

RECENT INSIGHTS (avoid duplicates):
${recentInsightTitles.length > 0 ? recentInsightTitles.map(t => `- ${t}`).join('\n') : 'None'}

Analyze these changes in the context of the user's current behavioral state. Generate insights that connect the detected changes to the bigger picture — mood trends, relationship patterns, life transitions. Be specific and actionable:`;

  const stopLlm = logger.time('llm-analysis');
  const llmResult = await routeRequest({
    handler: 'anticipation-analyze',
    taskTier: 'reasoning',
    systemPrompt: SYSTEM_PROMPT,
    userMessage,
    maxTokens: 6144,
  });
  stopLlm();
  logger.log(`Analyzed via ${llmResult.model} (${llmResult.provider}, ${llmResult.estimatedCostCents}¢)`);

  const text = llmResult.text;
  logger.logDebug('LLM raw response:', text.slice(0, 500));

  let analysis: AnalysisResult;
  try {
    const cleaned = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    analysis = JSON.parse(cleaned);
  } catch {
    logger.logVerbose('Fence-strip parse failed, trying JSON extraction');
    const firstBrace = text.indexOf('{');
    const lastBrace = text.lastIndexOf('}');
    if (firstBrace !== -1 && lastBrace > firstBrace) {
      try {
        analysis = JSON.parse(text.slice(firstBrace, lastBrace + 1));
      } catch {
        logger.logMinimal('Failed to parse anticipation response after extraction, skipping cycle:', text.slice(0, 500));
        analysis = { insights: [], patterns: [], summary: 'Parse failure — skipped cycle' };
      }
    } else {
      logger.logMinimal('No JSON object found in anticipation response, skipping cycle:', text.slice(0, 500));
      analysis = { insights: [], patterns: [], summary: 'Parse failure — skipped cycle' };
    }
  }

  // Save insights
  const insightIds: string[] = [];
  logger.log(`Analysis returned ${(analysis.insights ?? []).length} insights, ${(analysis.patterns ?? []).length} patterns`);
  if ((analysis.insights ?? []).length === 0) {
    logger.log('LLM returned empty insights array. Summary:', analysis.summary);
  }
  for (const insight of (analysis.insights ?? [])) {
    logger.log(`Processing insight: "${insight.title}" confidence=${insight.confidence} risk=${insight.risk_level_required}`);
    // Filter by confidence threshold (per-category if available, else global)
    const categoryThreshold = settings.category_thresholds[insight.category] ?? settings.confidence_threshold;
    if (insight.confidence < categoryThreshold) {
      logger.logVerbose(`Skipping low-confidence insight: ${insight.title} (${insight.confidence} < ${categoryThreshold} for ${insight.category})`);
      continue;
    }

    // Risk level enforcement
    const riskOrder = { low: 0, medium: 1, high: 2 };
    const requiredLevel = riskOrder[insight.risk_level_required as keyof typeof riskOrder] ?? 0;
    const currentLevel = riskOrder[riskLevel as keyof typeof riskOrder] ?? 1;
    if (requiredLevel > currentLevel) {
      logger.logVerbose(`Skipping insight requiring ${insight.risk_level_required} (current: ${riskLevel}): ${insight.title}`);
      continue;
    }

    try {
      // Build evidence payload from source snapshots
      const evidencePayload = {
        snapshots: snapshots.map(s => ({
          source: s.source,
          summary: s.change_summary,
        })),
        triggered_at: new Date().toISOString(),
      };

      const { rows } = await pool.query<{ id: string }>(
        `INSERT INTO proactive_insights
         (title, body, reasoning, evidence, category, urgency, confidence, risk_level_required,
          action_type, action_data, source_snapshots, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         RETURNING id`,
        [
          insight.title,
          insight.body,
          insight.reasoning ?? null,
          JSON.stringify(evidencePayload),
          insight.category,
          insight.urgency,
          insight.confidence,
          insight.risk_level_required,
          insight.action_type ?? null,
          insight.action_data ? JSON.stringify(insight.action_data) : null,
          JSON.stringify(snapshotIds),
          insight.expires_at ?? null,
        ]
      );
      insightIds.push(rows[0].id);
    } catch (err) {
      logger.logMinimal(`Failed to save insight "${insight.title}":`, err instanceof Error ? err.message : err);
    }
  }

  // Save patterns
  const patternIds: string[] = [];
  for (const pattern of (analysis.patterns ?? [])) {
    try {
      // Check if similar pattern already exists
      const { rows: existing } = await pool.query(
        `SELECT id, confidence, observation_count FROM anticipation_patterns
         WHERE description = $1 AND is_active = true`,
        [pattern.description]
      );

      if (existing.length > 0) {
        // Update existing pattern
        const current = existing[0];
        const newConfidence = Math.min(1, current.confidence + 0.1);
        await pool.query(
          `UPDATE anticipation_patterns
           SET confidence = $1, observation_count = observation_count + 1,
               last_observed_at = NOW(), evidence = evidence || $2::jsonb
           WHERE id = $3`,
          [newConfidence, JSON.stringify([pattern.evidence]), current.id]
        );
        patternIds.push(current.id);
      } else {
        // Create new pattern
        const { rows } = await pool.query<{ id: string }>(
          `INSERT INTO anticipation_patterns
           (pattern_type, description, evidence, confidence)
           VALUES ($1, $2, $3, $4)
           RETURNING id`,
          [
            pattern.pattern_type,
            pattern.description,
            JSON.stringify([pattern.evidence]),
            pattern.confidence,
          ]
        );
        patternIds.push(rows[0].id);
      }
    } catch (err) {
      logger.logMinimal(`Failed to save pattern:`, err instanceof Error ? err.message : err);
    }
  }

  // Mark snapshots as analyzed
  await pool.query(
    `UPDATE context_snapshots SET analyzed = true WHERE id = ANY($1)`,
    [snapshotIds]
  );

  const summary = analysis.summary || `Generated ${insightIds.length} insight(s), detected ${patternIds.length} pattern(s)`;

  logEvent({
    action: `Anticipation engine: ${summary}`,
    component: 'proactive',
    category: 'background',
    metadata: {
      snapshots_analyzed: snapshotIds.length,
      insights_generated: insightIds.length,
      patterns_detected: patternIds.length,
      risk_level: riskLevel,
    },
  });

  logger.log(summary);

  return { insight_ids: insightIds, pattern_ids: patternIds, analysis_summary: summary };
}
