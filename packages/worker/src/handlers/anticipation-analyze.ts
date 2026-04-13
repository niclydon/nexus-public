/**
 * Anticipation Analyze handler — Gate 3.
 *
 * Uses Claude Sonnet to perform deep analysis on significant changes,
 * generate proactive insights, and detect behavioral patterns.
 *
 * After generating insights, enqueues delivery jobs for any that
 * match the user's notification preferences.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { analyzeAndGenerate } from '../lib/proactive/anticipation-engine.js';
import { ingestFacts, type FactInput } from '../lib/knowledge.js';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('anticipation-analyze');

export async function handleAnticipationAnalyze(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { snapshot_ids: string[]; risk_level: string };
  logger.log(`Analyzing ${payload.snapshot_ids.length} snapshot(s) at risk level: ${payload.risk_level}`);
  await jobLog(job.id, `Analyzing ${payload.snapshot_ids.length} snapshot(s) at risk level: ${payload.risk_level}`);

  const result = await analyzeAndGenerate(payload.snapshot_ids, payload.risk_level);
  await jobLog(job.id, `Generated ${result.insight_ids.length} insight(s), detected ${result.pattern_ids.length} pattern(s)`);

  // Enqueue delivery for generated insights
  if (result.insight_ids.length > 0) {
    const pool = getPool();

    // Load delivery preferences
    const { rows: settingsRows } = await pool.query(
      `SELECT key, value FROM proactive_settings
       WHERE key IN ('urgent_delivery_methods', 'standard_delivery_methods')`
    );

    const deliveryPrefs: Record<string, string[]> = {
      urgent: [],
      standard: [],
    };
    for (const row of settingsRows) {
      if (row.key === 'urgent_delivery_methods') deliveryPrefs.urgent = row.value as string[];
      if (row.key === 'standard_delivery_methods') deliveryPrefs.standard = row.value as string[];
    }

    // Load the generated insights to get their urgency
    const placeholders = result.insight_ids.map((_: unknown, i: number) => `$${i + 1}`).join(', ');
    const { rows: insights } = await pool.query(
      `SELECT id, urgency FROM proactive_insights WHERE id IN (${placeholders})`,
      result.insight_ids
    );

    for (const insight of insights) {
      const methods = insight.urgency === 'urgent'
        ? deliveryPrefs.urgent
        : deliveryPrefs.standard;

      if (methods.length > 0) {
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
           VALUES ($1, $2, $3, $4)`,
          [
            'insight-deliver',
            JSON.stringify({
              insight_id: insight.id,
              delivery_method_ids: methods,
            }),
            insight.urgency === 'urgent' ? 3 : 1,
            2,
          ]
        );
      }
    }

    if (insights.length > 0) {
      logger.log(`Enqueued delivery for ${insights.length} insight(s)`);
      await jobLog(job.id, `Enqueued delivery for ${insights.length} insight(s)`);
    }
  }

  // Write detected patterns to Personal Knowledge Graph
  if (result.pattern_ids.length > 0 || result.insight_ids.length > 0) {
    try {
      const pool = getPool();
      const pkgFacts: FactInput[] = [];

      // Load detected patterns and map to PKG facts
      if (result.pattern_ids.length > 0) {
        const patternPlaceholders = result.pattern_ids.map((_: unknown, i: number) => `$${i + 1}`).join(', ');
        const { rows: patterns } = await pool.query<{
          id: string;
          pattern_type: string;
          description: string;
        }>(
          `SELECT id, pattern_type, description FROM anticipation_patterns WHERE id IN (${patternPlaceholders})`,
          result.pattern_ids
        );

        for (const pattern of patterns) {
          // Determine domain/category based on pattern type
          const isCommunication = pattern.pattern_type === 'contextual' &&
            /\b(message|email|call|contact|reply|respond|text)\b/i.test(pattern.description);

          const isSchedule = pattern.pattern_type === 'temporal' ||
            /\b(morning|evening|daily|weekly|routine|schedule|wake|sleep|time)\b/i.test(pattern.description);

          if (isCommunication) {
            pkgFacts.push({
              domain: 'communication',
              category: 'patterns',
              key: `pie_pattern_${pattern.id}`,
              value: pattern.description,
              confidence: 0.6,
              source: 'proactive_intelligence',
            });
          } else if (isSchedule) {
            pkgFacts.push({
              domain: 'lifestyle',
              category: 'schedule',
              key: `pie_pattern_${pattern.id}`,
              value: pattern.description,
              confidence: 0.6,
              source: 'proactive_intelligence',
            });
          } else {
            pkgFacts.push({
              domain: 'lifestyle',
              category: 'patterns',
              key: `pie_pattern_${pattern.id}`,
              value: pattern.description,
              confidence: 0.6,
              source: 'proactive_intelligence',
            });
          }
        }
      }

      // Extract behavioral facts from actionable insights
      if (result.insight_ids.length > 0) {
        const insightPlaceholders = result.insight_ids.map((_: unknown, i: number) => `$${i + 1}`).join(', ');
        const { rows: actionableInsights } = await pool.query<{
          id: string;
          title: string;
          body: string;
          category: string;
        }>(
          `SELECT id, title, body, category FROM proactive_insights
           WHERE id IN (${insightPlaceholders})
             AND category IN ('pattern_observation', 'health_alert', 'social')`,
          result.insight_ids
        );

        for (const insight of actionableInsights) {
          if (insight.category === 'social') {
            pkgFacts.push({
              domain: 'communication',
              category: 'patterns',
              key: `pie_insight_${insight.id}`,
              value: `${insight.title}: ${insight.body}`.slice(0, 500),
              confidence: 0.6,
              source: 'proactive_intelligence',
            });
          } else {
            // pattern_observation, health_alert -> lifestyle patterns
            pkgFacts.push({
              domain: 'lifestyle',
              category: 'patterns',
              key: `pie_insight_${insight.id}`,
              value: `${insight.title}: ${insight.body}`.slice(0, 500),
              confidence: 0.6,
              source: 'proactive_intelligence',
            });
          }
        }
      }

      if (pkgFacts.length > 0) {
        const { written } = await ingestFacts(pkgFacts);
        logger.log(`Wrote ${written}/${pkgFacts.length} fact(s) to knowledge graph`);
        await jobLog(job.id, `Wrote ${written}/${pkgFacts.length} fact(s) to knowledge graph`);
      }
    } catch (err) {
      logger.logMinimal('PKG write failed:', err instanceof Error ? err.message : err);
    }
  }

  return {
    insights_generated: result.insight_ids.length,
    patterns_detected: result.pattern_ids.length,
    summary: result.analysis_summary,
  };
}
