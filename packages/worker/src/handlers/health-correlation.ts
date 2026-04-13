/**
 * Health correlation engine.
 * Runs monthly (or on-demand with force:true), computing Pearson correlations
 * between health metrics and behavioral signals.
 * Stores significant correlations and generates human-readable insights.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';
import { ingestFacts, type FactInput } from '../lib/knowledge.js';

const logger = createLogger('health-correlation');

// ── Statistical helpers ──────────────────────────────────────────

function pearsonCorrelation(xs: number[], ys: number[]): { r: number; n: number } {
  const n = xs.length;
  if (n < 30) return { r: 0, n };

  const sumX = xs.reduce((a, b) => a + b, 0);
  const sumY = ys.reduce((a, b) => a + b, 0);
  const sumXY = xs.reduce((a, x, i) => a + x * ys[i], 0);
  const sumX2 = xs.reduce((a, x) => a + x * x, 0);
  const sumY2 = ys.reduce((a, y) => a + y * y, 0);

  const numerator = n * sumXY - sumX * sumY;
  const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

  if (denominator === 0) return { r: 0, n };
  return { r: numerator / denominator, n };
}

function normalCDF(x: number): number {
  const a1 = 0.254829592, a2 = -0.284496736, a3 = 1.421413741;
  const a4 = -1.453152027, a5 = 1.061405429, p = 0.3275911;
  const sign = x < 0 ? -1 : 1;
  x = Math.abs(x) / Math.sqrt(2);
  const t = 1.0 / (1.0 + p * x);
  const y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-x * x);
  return 0.5 * (1.0 + sign * y);
}

function pValue(r: number, n: number): number {
  if (n < 3) return 1;
  const denom = 1 - r * r;
  if (denom <= 0) return 0;
  const t = r * Math.sqrt((n - 2) / denom);
  return 2 * (1 - normalCDF(Math.abs(t)));
}

// ── Value extraction ─────────────────────────────────────────────

/**
 * Extract a single numeric value from the JSONB value column.
 * Handles various shapes that HealthKit data may arrive in.
 */
function extractNumericValue(dataType: string, value: unknown): number | null {
  if (typeof value === 'number') return value;
  if (value == null || typeof value !== 'object') return null;

  const v = value as Record<string, unknown>;

  switch (dataType) {
    case 'steps': {
      if (typeof v.count === 'number') return v.count;
      if (typeof v.steps === 'number') return v.steps;
      if (typeof v.value === 'number') return v.value;
      return null;
    }
    case 'heart_rate': {
      if (typeof v.average === 'number') return v.average;
      if (typeof v.avg === 'number') return v.avg;
      if (typeof v.value === 'number') return v.value;
      return null;
    }
    case 'sleep': {
      if (typeof v.duration_hours === 'number') return v.duration_hours;
      if (typeof v.total_minutes === 'number') return v.total_minutes / 60;
      if (typeof v.hours === 'number') return v.hours;
      if (typeof v.duration === 'number') return v.duration;
      if (typeof v.value === 'number') return v.value;
      return null;
    }
    case 'active_energy': {
      if (typeof v.calories === 'number') return v.calories;
      if (typeof v.kcal === 'number') return v.kcal;
      if (typeof v.value === 'number') return v.value;
      return null;
    }
    case 'workout': {
      if (typeof v.duration_minutes === 'number') return v.duration_minutes;
      if (typeof v.duration === 'number') return v.duration;
      if (typeof v.count === 'number') return v.count;
      if (typeof v.value === 'number') return v.value;
      return null;
    }
    default:
      return typeof v.value === 'number' ? v.value : null;
  }
}

// ── Main handler ─────────────────────────────────────────────────

export async function handleHealthCorrelation(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const force = !!job.payload?.force;
  logger.log(`Starting health correlation engine (job ${job.id}, force=${force})`);

  // 1. Fetch health data for the last 90 days
  const { rows: healthRows } = await pool.query<{
    date: string;
    data_type: string;
    value: unknown;
  }>(
    `SELECT date::text, data_type, value FROM health_data
     WHERE date >= CURRENT_DATE - INTERVAL '90 days'
     ORDER BY date ASC`
  );

  if (healthRows.length === 0) {
    logger.log('No health data found in the last 90 days. Skipping.');
    logEvent({
      action: 'Health correlation skipped — no health data',
      component: 'health-correlation',
      category: 'self_maintenance',
      metadata: { job_id: job.id, reason: 'no_data' },
    });
    return { skipped: true, reason: 'no_data' };
  }

  // 2. Build time series: Map<data_type, Map<date_string, number>>
  const timeSeries = new Map<string, Map<string, number>>();

  for (const row of healthRows) {
    const numericValue = extractNumericValue(row.data_type, row.value);
    if (numericValue === null) continue;

    if (!timeSeries.has(row.data_type)) {
      timeSeries.set(row.data_type, new Map());
    }
    const dateMap = timeSeries.get(row.data_type)!;
    // If multiple entries per day, keep the latest (last in sorted order)
    dateMap.set(row.date, numericValue);
  }

  const metricNames = Array.from(timeSeries.keys());
  logger.log(`Found ${metricNames.length} metric types: ${metricNames.join(', ')}`);

  if (metricNames.length < 2) {
    logger.log('Need at least 2 metric types to compute correlations. Skipping.');
    logEvent({
      action: 'Health correlation skipped — insufficient metric types',
      component: 'health-correlation',
      category: 'self_maintenance',
      metadata: { job_id: job.id, reason: 'insufficient_metrics', metric_count: metricNames.length },
    });
    return { skipped: true, reason: 'insufficient_metrics', metric_count: metricNames.length };
  }

  // 3. Compute correlations for all pairs
  interface CorrelationResult {
    metricA: string;
    metricB: string;
    r: number;
    n: number;
    p: number;
  }

  const significantCorrelations: CorrelationResult[] = [];
  let pairsChecked = 0;

  // Determine period boundaries
  const allDates = healthRows.map((r) => r.date).sort();
  const periodStart = allDates[0];
  const periodEnd = allDates[allDates.length - 1];

  for (let i = 0; i < metricNames.length; i++) {
    for (let j = i + 1; j < metricNames.length; j++) {
      const metricA = metricNames[i];
      const metricB = metricNames[j];
      const seriesA = timeSeries.get(metricA)!;
      const seriesB = timeSeries.get(metricB)!;

      // Find overlapping dates
      const overlappingDates: string[] = [];
      for (const date of seriesA.keys()) {
        if (seriesB.has(date)) {
          overlappingDates.push(date);
        }
      }

      if (overlappingDates.length < 30) continue;

      overlappingDates.sort();
      const xs = overlappingDates.map((d) => seriesA.get(d)!);
      const ys = overlappingDates.map((d) => seriesB.get(d)!);

      const { r, n } = pearsonCorrelation(xs, ys);
      const p = pValue(r, n);
      pairsChecked++;

      // Filter: |r| > 0.3, p < 0.05, n >= 30
      if (Math.abs(r) > 0.3 && p < 0.05 && n >= 30) {
        // Ensure consistent ordering (alphabetical) for the unique index
        const [orderedA, orderedB] = metricA < metricB ? [metricA, metricB] : [metricB, metricA];
        significantCorrelations.push({
          metricA: orderedA,
          metricB: orderedB,
          r: metricA < metricB ? r : r, // r is symmetric
          n,
          p,
        });
      }
    }
  }

  logger.log(`Checked ${pairsChecked} pairs, found ${significantCorrelations.length} significant correlations`);

  if (significantCorrelations.length === 0) {
    logEvent({
      action: 'Health correlation completed — no significant correlations found',
      component: 'health-correlation',
      category: 'self_maintenance',
      metadata: { job_id: job.id, pairs_checked: pairsChecked },
    });
    return { pairs_checked: pairsChecked, significant: 0 };
  }

  // 4. Sort by significance (|r| descending) and take top 5 for insight generation
  significantCorrelations.sort((a, b) => Math.abs(b.r) - Math.abs(a.r));
  const topCorrelations = significantCorrelations.slice(0, 5);

  // 5. Generate insights for top correlations using LLM (classification tier = cheap)
  const insightPrompt = topCorrelations.map((c, i) =>
    `${i + 1}. Metric A: ${c.metricA}, Metric B: ${c.metricB}, Correlation: ${c.r.toFixed(3)} (${c.r > 0 ? 'positive' : 'negative'}), Data points: ${c.n} days, Period: ${periodStart} to ${periodEnd}`
  ).join('\n');

  let insights: string[] = [];
  try {
    const llmResult = await routeRequest({
      handler: 'health-correlation',
      taskTier: 'classification',
      systemPrompt: 'You generate concise health insights from correlation data. For each correlation, write a single plain-language sentence the user can act on. Number each insight to match the input. Do not use markdown.',
      userMessage: `Given these health data correlations:\n${insightPrompt}\n\nWrite one concise sentence per correlation in plain language. Example: "On days you walk more than 8,000 steps, you tend to sleep about 40 minutes longer."`,
      maxTokens: 800,
      useBatch: false,
    });

    // Parse numbered insights
    insights = llmResult.text.trim().split('\n')
      .map((line) => line.replace(/^\d+\.\s*/, '').trim())
      .filter((line) => line.length > 0);

    logger.log(`Generated ${insights.length} insights via ${llmResult.model}`);
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal(`LLM insight generation failed: ${msg}`);
    // Continue without insights — still store the correlation data
  }

  // 6. Store results via UPSERT
  let stored = 0;
  for (let i = 0; i < significantCorrelations.length; i++) {
    const c = significantCorrelations[i];
    const insight = i < insights.length ? insights[i] : null;

    try {
      await pool.query(
        `INSERT INTO health_correlations (metric_a, metric_b, correlation_coefficient, data_points, p_value, insight, period_start, period_end, calculated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
         ON CONFLICT (metric_a, metric_b)
         DO UPDATE SET correlation_coefficient = $3, data_points = $4, p_value = $5, insight = $6, period_start = $7, period_end = $8, calculated_at = NOW()`,
        [c.metricA, c.metricB, c.r.toFixed(4), c.n, c.p, insight, periodStart, periodEnd]
      );
      stored++;
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal(`Failed to store ${c.metricA} \u2194 ${c.metricB}: ${msg}`);
    }
  }

  // 7. Feed strong correlations (|r| > 0.5) into self-improvement
  let selfImprovementLogged = 0;
  for (let i = 0; i < significantCorrelations.length; i++) {
    const c = significantCorrelations[i];
    if (Math.abs(c.r) <= 0.5) continue;

    const insightText = i < insights.length ? insights[i] : `${c.metricA} and ${c.metricB} have a ${c.r > 0 ? 'positive' : 'negative'} correlation of ${c.r.toFixed(3)}`;
    const confidence = Math.min(Math.abs(c.r), 0.99);

    try {
      // Check if a similar finding already exists to avoid duplicates
      const { rows: existing } = await pool.query(
        `SELECT id FROM aria_self_improvement
         WHERE source = 'health_correlation' AND title LIKE $1 AND status NOT IN ('rejected', 'implemented')
         LIMIT 1`,
        [`%${c.metricA}%${c.metricB}%`]
      );

      if (existing.length === 0) {
        await pool.query(
          `INSERT INTO aria_self_improvement (category, status, title, description, source, confidence)
           VALUES ('research_finding', 'idea', $1, $2, 'health_correlation', $3)`,
          [
            `Health correlation: ${c.metricA} \u2194 ${c.metricB}`,
            insightText,
            confidence.toFixed(2),
          ]
        );
        selfImprovementLogged++;
      }
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal(`Failed to log self-improvement: ${msg}`);
    }
  }

  // 7b. Write significant health correlations to PKG
  const pkgFacts: FactInput[] = [];
  for (let i = 0; i < significantCorrelations.length; i++) {
    const c = significantCorrelations[i];
    if (Math.abs(c.r) < 0.3) continue;
    const insightText = i < insights.length ? insights[i] : `${c.metricA} and ${c.metricB} have a ${c.r > 0 ? 'positive' : 'negative'} correlation (r=${c.r.toFixed(3)})`;
    pkgFacts.push({
      domain: 'health',
      category: 'correlations',
      key: `${c.metricA}_${c.metricB}`.toLowerCase().replace(/\s+/g, '_'),
      value: insightText,
      confidence: Math.min(Math.abs(c.r), 0.99),
      source: 'health_correlation',
    });
  }
  const { written: pkgWritten } = await ingestFacts(pkgFacts);
  if (pkgWritten > 0) {
    logger.log(`Wrote ${pkgWritten} facts to knowledge graph`);
  }

  // 8. Update watermark
  try {
    await pool.query(
      `UPDATE context_watermarks SET last_change_detected_at = NOW(), updated_at = NOW() WHERE source = 'health_correlations'`
    );
  } catch {
    // Watermark may not exist yet — safe to ignore
  }

  logEvent({
    action: `Health correlation engine completed: ${stored} correlations stored, ${selfImprovementLogged} self-improvement items`,
    component: 'health-correlation',
    category: 'self_maintenance',
    metadata: {
      job_id: job.id,
      pairs_checked: pairsChecked,
      significant: significantCorrelations.length,
      stored,
      self_improvement_logged: selfImprovementLogged,
      top_correlation: significantCorrelations[0]
        ? `${significantCorrelations[0].metricA} \u2194 ${significantCorrelations[0].metricB} (r=${significantCorrelations[0].r.toFixed(3)})`
        : null,
    },
  });

  logger.log(`Done: ${stored} stored, ${selfImprovementLogged} self-improvement items`);

  return {
    pairs_checked: pairsChecked,
    significant: significantCorrelations.length,
    stored,
    self_improvement_logged: selfImprovementLogged,
    period: { start: periodStart, end: periodEnd },
    top_correlations: significantCorrelations.slice(0, 3).map((c) => ({
      metrics: `${c.metricA} \u2194 ${c.metricB}`,
      r: parseFloat(c.r.toFixed(4)),
      n: c.n,
      p: parseFloat(c.p.toFixed(6)),
    })),
  };
}
