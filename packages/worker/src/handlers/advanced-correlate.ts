/**
 * Advanced Correlation Handler
 *
 * Runs weekly. Three analysis types:
 * 1. Lag Correlation — Pearson at lags -3 to +3 days for each metric pair
 * 2. Event-Triggered Windows — behavioral impact around life events
 * 3. Predictive Signals — human-readable rules from correlations + event impacts
 *
 * Reads from aurora_cross_metrics (populated by cross-metrics-compute).
 * Writes to aurora_lag_correlations, aurora_event_impacts, aurora_predictive_signals.
 * Uses LLM (via routeRequest) for human_summary generation.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { routeRequest } from '../lib/llm/index.js';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('advanced-correlate');

const MIN_PAIRS = 14;
const LAG_RANGE = 3; // -3 to +3 days
const SIGNIFICANCE_THRESHOLD = 0.35;

interface MetricPoint {
  date: string;
  value: number;
}

interface EventDefinition {
  name: string;
  detectSql: string;
  description: string;
}

// ── Event Definitions ─────────────────────────────────────────────

const EVENT_DEFINITIONS: EventDefinition[] = [
  {
    name: 'low_mood_streak',
    detectSql: `
      WITH streaks AS (
        SELECT date, mood_score,
          date - ROW_NUMBER() OVER (ORDER BY date)::int * INTERVAL '1 day' as grp
        FROM aurora_mood_proxy WHERE mood_score < 0.25
      )
      SELECT MIN(date)::date as event_date FROM streaks
      GROUP BY grp HAVING COUNT(*) >= 3
    `,
    description: 'Sustained low mood (mood_score < 0.25 for 3+ consecutive days)',
  },
  {
    name: 'travel_start',
    detectSql: `
      SELECT DISTINCT date(timestamp)::date as event_date
      FROM aurora_unified_location
      WHERE source = 'looki' OR source = 'device'
      GROUP BY date(timestamp)
      HAVING COUNT(DISTINCT ROUND(latitude::numeric, 0) || ',' || ROUND(longitude::numeric, 0)) > 3
    `,
    description: 'High location diversity day (likely travel)',
  },
  {
    name: 'binge_event',
    detectSql: `
      SELECT watched_date as event_date
      FROM netflix_viewing_history
      GROUP BY watched_date, show_name
      HAVING COUNT(*) >= 4
    `,
    description: '4+ episodes of the same show in one day',
  },
  {
    name: 'social_spike',
    detectSql: `
      WITH daily AS (
        SELECT date(timestamp) as d, COUNT(*) as cnt
        FROM aurora_unified_communication GROUP BY d
      ),
      stats AS (
        SELECT AVG(cnt) as avg_cnt, STDDEV(cnt) as std_cnt FROM daily
      )
      SELECT d as event_date FROM daily, stats
      WHERE cnt > avg_cnt + 2 * std_cnt AND std_cnt > 0
    `,
    description: 'Communication volume >2 standard deviations above mean',
  },
  {
    name: 'high_activity',
    detectSql: `
      WITH daily AS (
        SELECT DATE(date) as d, SUM((value->>'value')::numeric)::int as steps
        FROM health_data WHERE data_type = 'steps'
        GROUP BY DATE(date)
      ),
      stats AS (
        SELECT AVG(steps) as avg_s, STDDEV(steps) as std_s FROM daily
      )
      SELECT d as event_date FROM daily, stats
      WHERE steps > avg_s + 1.5 * std_s AND std_s > 0
    `,
    description: 'Step count >1.5 standard deviations above mean',
  },
];

// ── Handler ─────────────────────────────────────────────────────

export async function handleAdvancedCorrelate(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { lookback_days?: number } | null;
  const lookback = payload?.lookback_days ?? 180;

  logger.log(`Starting advanced correlation analysis (${lookback}-day window)`);
  await jobLog(job.id, `Advanced correlation: ${lookback}-day window`);

  // Load all metrics from aurora_cross_metrics
  const since = new Date(Date.now() - lookback * 86400000).toISOString().split('T')[0];
  const { rows: rawMetrics } = await pool.query<{ date: string; metric_name: string; value: number }>(`
    SELECT date::text, metric_name, value FROM aurora_cross_metrics
    WHERE date >= $1::date ORDER BY date
  `, [since]);

  if (rawMetrics.length === 0) {
    logger.log('No cross-metrics data found — run cross-metrics-compute first');
    return { status: 'no_data' };
  }

  // Index by metric name
  const metricsByName: Record<string, MetricPoint[]> = {};
  for (const row of rawMetrics) {
    if (!metricsByName[row.metric_name]) metricsByName[row.metric_name] = [];
    metricsByName[row.metric_name].push({ date: row.date, value: row.value });
  }

  const metricNames = Object.keys(metricsByName);
  logger.log(`Loaded ${rawMetrics.length} data points across ${metricNames.length} metrics`);

  // ── Phase 1: Lag Correlations ─────────────────────────────────

  logger.log('Phase 1: Computing lag correlations');
  await jobLog(job.id, 'Phase 1: Lag correlations');

  let lagCorrelationsStored = 0;

  for (let i = 0; i < metricNames.length; i++) {
    for (let j = i + 1; j < metricNames.length; j++) {
      const nameA = metricNames[i];
      const nameB = metricNames[j];

      const result = computeLagCorrelation(
        metricsByName[nameA],
        metricsByName[nameB],
      );

      if (!result || Math.abs(result.coefficient) < SIGNIFICANCE_THRESHOLD) continue;
      if (result.sampleSize < MIN_PAIRS) continue;

      await pool.query(`
        INSERT INTO aurora_lag_correlations (metric_a, metric_b, optimal_lag_days, coefficient, sample_size, calculated_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (metric_a, metric_b) DO UPDATE SET
          optimal_lag_days = EXCLUDED.optimal_lag_days,
          coefficient = EXCLUDED.coefficient,
          sample_size = EXCLUDED.sample_size,
          calculated_at = NOW()
      `, [nameA, nameB, result.lag, Math.round(result.coefficient * 1000) / 1000, result.sampleSize]);

      lagCorrelationsStored++;
    }
  }

  logger.log(`Lag correlations: ${lagCorrelationsStored} significant pairs stored`);

  // ── Phase 2: Event-Triggered Windows ──────────────────────────

  logger.log('Phase 2: Computing event-triggered impacts');
  await jobLog(job.id, 'Phase 2: Event impact windows');

  let eventImpactsStored = 0;

  // Build date-indexed metric lookup for window computation
  const metricByDate: Record<string, Record<string, number>> = {};
  for (const row of rawMetrics) {
    if (!metricByDate[row.date]) metricByDate[row.date] = {};
    metricByDate[row.date][row.metric_name] = row.value;
  }

  for (const eventDef of EVENT_DEFINITIONS) {
    let eventDates: string[];
    try {
      const { rows } = await pool.query<{ event_date: string }>(eventDef.detectSql);
      eventDates = rows.map(r => r.event_date?.toString().split('T')[0]).filter(Boolean);
    } catch (err) {
      logger.logVerbose(`Event "${eventDef.name}" detection failed:`, (err as Error).message);
      continue;
    }

    if (eventDates.length === 0) {
      logger.logVerbose(`No events detected for "${eventDef.name}"`);
      continue;
    }

    logger.logVerbose(`Event "${eventDef.name}": ${eventDates.length} occurrences`);

    // For each metric, compute before/during/after averages
    for (const metricName of metricNames) {
      const beforeValues: number[] = [];
      const duringValues: number[] = [];
      const afterValues: number[] = [];

      for (const eventDate of eventDates) {
        const eventMs = new Date(eventDate).getTime();

        // Before window: -7 to -1 days
        for (let d = -7; d <= -1; d++) {
          const dayStr = new Date(eventMs + d * 86400000).toISOString().split('T')[0];
          const val = metricByDate[dayStr]?.[metricName];
          if (val != null) beforeValues.push(val);
        }

        // During: day 0
        const val = metricByDate[eventDate]?.[metricName];
        if (val != null) duringValues.push(val);

        // After window: +1 to +7 days
        for (let d = 1; d <= 7; d++) {
          const dayStr = new Date(eventMs + d * 86400000).toISOString().split('T')[0];
          const val2 = metricByDate[dayStr]?.[metricName];
          if (val2 != null) afterValues.push(val2);
        }
      }

      // Need at least some data in each window
      if (beforeValues.length < 3 || duringValues.length < 1 || afterValues.length < 3) continue;

      const beforeAvg = avg(beforeValues);
      const duringAvg = avg(duringValues);
      const afterAvg = avg(afterValues);
      const impactPct = beforeAvg !== 0 ? ((duringAvg - beforeAvg) / Math.abs(beforeAvg)) * 100 : 0;

      // Only store if there's a meaningful impact (>10% change)
      if (Math.abs(impactPct) < 10) continue;

      const confidence = Math.min(0.95, 0.3 + (eventDates.length * 0.05) + (beforeValues.length * 0.01));

      await pool.query(`
        INSERT INTO aurora_event_impacts (event_type, affected_metric, before_avg, during_avg, after_avg, impact_pct, event_count, confidence, calculated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (event_type, affected_metric) DO UPDATE SET
          before_avg = EXCLUDED.before_avg,
          during_avg = EXCLUDED.during_avg,
          after_avg = EXCLUDED.after_avg,
          impact_pct = EXCLUDED.impact_pct,
          event_count = EXCLUDED.event_count,
          confidence = EXCLUDED.confidence,
          calculated_at = NOW()
      `, [
        eventDef.name, metricName,
        round2(beforeAvg), round2(duringAvg), round2(afterAvg),
        round2(impactPct), eventDates.length, round2(confidence),
      ]);

      eventImpactsStored++;
    }
  }

  logger.log(`Event impacts: ${eventImpactsStored} stored`);

  // ── Phase 3: Generate Summaries + Predictive Signals ──────────

  logger.log('Phase 3: Generating summaries and predictive signals');
  await jobLog(job.id, 'Phase 3: LLM summaries + predictive signals');

  // Generate summaries for lag correlations that don't have one
  const { rows: unsummarized } = await pool.query<{
    id: number; metric_a: string; metric_b: string; optimal_lag_days: number; coefficient: number; sample_size: number;
  }>(`
    SELECT id, metric_a, metric_b, optimal_lag_days, coefficient, sample_size
    FROM aurora_lag_correlations WHERE summary IS NULL
    ORDER BY ABS(coefficient) DESC LIMIT 20
  `);

  let summariesGenerated = 0;

  if (unsummarized.length > 0) {
    const correlationData = unsummarized.map(r =>
      `- ${r.metric_a} → ${r.metric_b}: r=${r.coefficient.toFixed(2)}, lag=${r.optimal_lag_days}d, n=${r.sample_size}`
    ).join('\n');

    try {
      const result = await routeRequest({
        handler: 'advanced-correlate',
        taskTier: 'generation',
        systemPrompt: `You are a behavioral analyst. Given correlation data between daily life metrics, write a brief human-readable summary for each one. Return a JSON array of objects with "id" (number) and "summary" (string, 1 sentence, plain language). No markdown fences.`,
        userMessage: `Generate summaries for these lag correlations:\n${correlationData}\n\nIDs: ${unsummarized.map(r => r.id).join(', ')}`,
        maxTokens: 2000,
      });

      const cleaned = result.text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
      const summaries = JSON.parse(cleaned) as Array<{ id: number; summary: string }>;

      for (const s of summaries) {
        if (s.id && s.summary) {
          await pool.query(`UPDATE aurora_lag_correlations SET summary = $1 WHERE id = $2`, [s.summary, s.id]);
          summariesGenerated++;
        }
      }

      logger.log(`Generated ${summariesGenerated} lag correlation summaries`);
    } catch (err) {
      logger.logMinimal('LLM summary generation for lag correlations failed:', (err as Error).message);
    }
  }

  // Generate summaries for event impacts that don't have one
  const { rows: unsummarizedEvents } = await pool.query<{
    id: number; event_type: string; affected_metric: string; impact_pct: number; event_count: number; confidence: number;
  }>(`
    SELECT id, event_type, affected_metric, impact_pct, event_count, confidence
    FROM aurora_event_impacts WHERE summary IS NULL
    ORDER BY ABS(impact_pct) DESC LIMIT 20
  `);

  let eventSummariesGenerated = 0;

  if (unsummarizedEvents.length > 0) {
    const eventData = unsummarizedEvents.map(r =>
      `- ${r.event_type} → ${r.affected_metric}: ${r.impact_pct > 0 ? '+' : ''}${r.impact_pct.toFixed(1)}% impact, n=${r.event_count}, conf=${r.confidence.toFixed(2)}`
    ).join('\n');

    try {
      const result = await routeRequest({
        handler: 'advanced-correlate',
        taskTier: 'generation',
        systemPrompt: `You are a behavioral analyst. Given event impact data, write a brief human-readable summary for each. Return a JSON array of objects with "id" (number) and "summary" (string, 1 sentence, plain language). No markdown fences.`,
        userMessage: `Generate summaries for these event impacts:\n${eventData}\n\nIDs: ${unsummarizedEvents.map(r => r.id).join(', ')}`,
        maxTokens: 2000,
      });

      const cleaned = result.text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
      const summaries = JSON.parse(cleaned) as Array<{ id: number; summary: string }>;

      for (const s of summaries) {
        if (s.id && s.summary) {
          await pool.query(`UPDATE aurora_event_impacts SET summary = $1 WHERE id = $2`, [s.summary, s.id]);
          eventSummariesGenerated++;
        }
      }

      logger.log(`Generated ${eventSummariesGenerated} event impact summaries`);
    } catch (err) {
      logger.logMinimal('LLM summary generation for event impacts failed:', (err as Error).message);
    }
  }

  // Generate predictive signals from strong correlations + high-impact events
  const { rows: strongCorrelations } = await pool.query<{
    metric_a: string; metric_b: string; optimal_lag_days: number; coefficient: number; summary: string;
  }>(`
    SELECT metric_a, metric_b, optimal_lag_days, coefficient, summary
    FROM aurora_lag_correlations
    WHERE ABS(coefficient) > 0.5 AND optimal_lag_days != 0 AND summary IS NOT NULL
    ORDER BY ABS(coefficient) DESC LIMIT 10
  `);

  const { rows: strongImpacts } = await pool.query<{
    event_type: string; affected_metric: string; impact_pct: number; confidence: number; summary: string;
  }>(`
    SELECT event_type, affected_metric, impact_pct, confidence, summary
    FROM aurora_event_impacts
    WHERE ABS(impact_pct) > 20 AND confidence > 0.5 AND summary IS NOT NULL
    ORDER BY ABS(impact_pct) DESC LIMIT 10
  `);

  let signalsGenerated = 0;

  if (strongCorrelations.length > 0 || strongImpacts.length > 0) {
    const signalData = [
      ...strongCorrelations.map(c => `LAG: ${c.summary} (r=${c.coefficient.toFixed(2)}, lag=${c.optimal_lag_days}d)`),
      ...strongImpacts.map(e => `EVENT: ${e.summary} (impact=${e.impact_pct.toFixed(1)}%, conf=${e.confidence.toFixed(2)})`),
    ].join('\n');

    try {
      const result = await routeRequest({
        handler: 'advanced-correlate',
        taskTier: 'generation',
        systemPrompt: `You are a behavioral analyst. Given correlation and event impact findings, generate predictive rules — actionable "if X then Y" statements. Return a JSON array of objects:
- trigger_condition: { type: "lag"|"event", metric: string, threshold?: number, description: string }
- predicted_outcome: { metric: string, direction: "increase"|"decrease", magnitude: string }
- human_summary: string (1 sentence, plain language, actionable)
- accuracy: number (0.0-1.0 estimate)

Return ONLY valid JSON, no markdown fences. Generate 3-8 rules from the strongest signals.`,
        userMessage: `Generate predictive signals from these findings:\n${signalData}`,
        maxTokens: 3000,
      });

      const cleaned = result.text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
      const signals = JSON.parse(cleaned) as Array<{
        trigger_condition: Record<string, unknown>;
        predicted_outcome: Record<string, unknown>;
        human_summary: string;
        accuracy: number;
      }>;

      for (const signal of signals) {
        if (!signal.trigger_condition || !signal.predicted_outcome || !signal.human_summary) continue;

        await pool.query(`
          INSERT INTO aurora_predictive_signals (trigger_condition, predicted_outcome, accuracy, sample_size, human_summary, active)
          VALUES ($1, $2, $3, $4, $5, TRUE)
        `, [
          JSON.stringify(signal.trigger_condition),
          JSON.stringify(signal.predicted_outcome),
          signal.accuracy ?? 0.5,
          rawMetrics.length,
          signal.human_summary,
        ]);

        signalsGenerated++;
      }

      logger.log(`Generated ${signalsGenerated} predictive signals`);
    } catch (err) {
      logger.logMinimal('LLM predictive signal generation failed:', (err as Error).message);
    }
  }

  const summary = {
    lag_correlations_stored: lagCorrelationsStored,
    event_impacts_stored: eventImpactsStored,
    summaries_generated: summariesGenerated + eventSummariesGenerated,
    predictive_signals_generated: signalsGenerated,
    metrics_analyzed: metricNames.length,
    lookback_days: lookback,
  };

  logger.log(`Done: ${JSON.stringify(summary)}`);
  await jobLog(job.id, `Advanced correlate complete: ${lagCorrelationsStored} lag corr, ${eventImpactsStored} event impacts, ${signalsGenerated} signals`);

  return summary;
}

// ── Lag Correlation Computation ──────────────────────────────────

function computeLagCorrelation(
  seriesA: MetricPoint[],
  seriesB: MetricPoint[],
): { lag: number; coefficient: number; sampleSize: number } | null {
  // Index both series by date
  const mapA = new Map(seriesA.map(p => [p.date, p.value]));
  const mapB = new Map(seriesB.map(p => [p.date, p.value]));

  let bestLag = 0;
  let bestR = 0;
  let bestN = 0;

  for (let lag = -LAG_RANGE; lag <= LAG_RANGE; lag++) {
    const pairs: { a: number; b: number }[] = [];

    for (const [dateStr, valA] of mapA) {
      const dateMs = new Date(dateStr).getTime();
      const laggedDate = new Date(dateMs + lag * 86400000).toISOString().split('T')[0];
      const valB = mapB.get(laggedDate);

      if (valB != null && !isNaN(valA) && !isNaN(valB)) {
        pairs.push({ a: valA, b: valB });
      }
    }

    if (pairs.length < MIN_PAIRS) continue;

    const r = pearsonCorrelation(pairs);
    if (Math.abs(r) > Math.abs(bestR)) {
      bestR = r;
      bestLag = lag;
      bestN = pairs.length;
    }
  }

  if (bestN < MIN_PAIRS) return null;
  return { lag: bestLag, coefficient: bestR, sampleSize: bestN };
}

// ── Pearson Correlation ──────────────────────────────────────────

function pearsonCorrelation(pairs: { a: number; b: number }[]): number {
  const n = pairs.length;
  if (n < 2) return 0;

  let sumA = 0, sumB = 0, sumAB = 0, sumA2 = 0, sumB2 = 0;
  for (const { a, b } of pairs) {
    sumA += a; sumB += b;
    sumAB += a * b; sumA2 += a * a; sumB2 += b * b;
  }

  const denom = Math.sqrt((n * sumA2 - sumA * sumA) * (n * sumB2 - sumB * sumB));
  if (denom === 0) return 0;

  return (n * sumAB - sumA * sumB) / denom;
}

// ── Helpers ──────────────────────────────────────────────────────

function avg(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((s, v) => s + v, 0) / values.length;
}

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}
