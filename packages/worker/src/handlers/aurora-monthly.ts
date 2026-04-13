/**
 * AURORA monthly deep analysis handler.
 *
 * Runs on the 1st of each month at 3 AM ET. Three phases:
 *   1. 1-year trend vectors (linear regression over 365 days)
 *   2. Year-over-year comparison (same month across all available years)
 *   3. Behavioral DNA synthesis (LLM — stub, blocked on Dev-Server/Forge)
 *   4. Prediction accuracy tracking (stub)
 */
import type { TempoJob } from '../job-worker.js';
import type { Pool } from 'pg';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import { routeRequest } from '../lib/llm/index.js';

const logger = createLogger('aurora-monthly');

// ── Math helpers (shared with aurora-weekly) ─────────────────

function linearRegression(points: { x: number; y: number }[]): {
  slope: number; intercept: number; r2: number; n: number;
} {
  const n = points.length;
  if (n < 2) return { slope: 0, intercept: 0, r2: 0, n };

  let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
  for (const p of points) {
    sumX += p.x; sumY += p.y;
    sumXY += p.x * p.y; sumX2 += p.x * p.x;
  }

  const denom = n * sumX2 - sumX * sumX;
  if (denom === 0) return { slope: 0, intercept: sumY / n, r2: 0, n };

  const slope = (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;

  const meanY = sumY / n;
  let ssTot = 0, ssRes = 0;
  for (const p of points) {
    ssTot += (p.y - meanY) ** 2;
    ssRes += (p.y - (slope * p.x + intercept)) ** 2;
  }
  const r2 = ssTot > 0 ? 1 - ssRes / ssTot : 0;

  return { slope, intercept, r2, n };
}

function classifyDirection(slope: number, r2: number): 'improving' | 'declining' | 'stable' {
  if (r2 < 0.1) return 'stable';
  return slope > 0 ? 'improving' : 'declining';
}

function classifyVelocity(changePctPerDay: number): 'rapid' | 'moderate' | 'slow' {
  const abs = Math.abs(changePctPerDay);
  if (abs > 2) return 'rapid';
  if (abs > 0.5) return 'moderate';
  return 'slow';
}

// ── Phase 1: 1-Year Trend Vectors ────────────────────────────

interface TrendResult { written: number; skipped: number }

async function compute1YearTrends(pool: Pool, asOfDate: string, jobId: string): Promise<TrendResult> {
  let written = 0;
  let skipped = 0;
  const stop = logger.time('1y-trends');

  const startDate = new Date(new Date(asOfDate).getTime() - 365 * 86400000).toISOString().split('T')[0];

  const { rows } = await pool.query(
    `SELECT date, dimension, metrics FROM aurora_signatures
     WHERE date >= $1 AND date <= $2 ORDER BY date`,
    [startDate, asOfDate]
  );

  // Group by dimension → metric → time series
  const dimMetrics = new Map<string, Map<string, { x: number; y: number }[]>>();
  for (const row of rows) {
    const d = row.date instanceof Date ? row.date.toISOString().split('T')[0] : String(row.date);
    const dayIndex = Math.floor((new Date(d).getTime() - new Date(startDate).getTime()) / 86400000);
    const m = typeof row.metrics === 'string' ? JSON.parse(row.metrics) : row.metrics;

    if (!dimMetrics.has(row.dimension)) dimMetrics.set(row.dimension, new Map());
    const metricMap = dimMetrics.get(row.dimension)!;

    for (const [key, val] of Object.entries(m)) {
      if (typeof val !== 'number') continue;
      if (!metricMap.has(key)) metricMap.set(key, []);
      metricMap.get(key)!.push({ x: dayIndex, y: val });
    }
  }

  for (const [dimension, metrics] of dimMetrics) {
    for (const [metricKey, points] of metrics) {
      if (points.length < 60) { skipped++; continue; } // Need 60+ days for 1y trend

      const reg = linearRegression(points);
      if (reg.r2 < 0.03) { skipped++; continue; } // Lower threshold for 1y (noisier)

      const firstVal = reg.intercept;
      const lastVal = reg.slope * 364 + reg.intercept;
      const changePct = firstVal !== 0 ? ((lastVal - firstVal) / Math.abs(firstVal)) * 100 : 0;
      const direction = classifyDirection(reg.slope, reg.r2);
      const velocity = classifyVelocity(changePct / 365);
      const metricFullName = `${dimension}/${metricKey}`;

      const narrative = `${metricFullName} ${direction} over 1y: ${Math.round(changePct)}% change (R²=${Math.round(reg.r2 * 100) / 100})`;

      await pool.query(
        `INSERT INTO aurora_trends (metric, period, direction, velocity, change_pct, current_value, baseline_value, significance, sigma, narrative, calculated_at)
         VALUES ($1, '1y', $2, $3, $4, $5, $6, $7, $8, $9, NOW())
         ON CONFLICT (metric, period) DO UPDATE SET
           direction = EXCLUDED.direction, velocity = EXCLUDED.velocity,
           change_pct = EXCLUDED.change_pct, current_value = EXCLUDED.current_value,
           baseline_value = EXCLUDED.baseline_value, significance = EXCLUDED.significance,
           sigma = EXCLUDED.sigma, narrative = EXCLUDED.narrative, calculated_at = NOW()`,
        [metricFullName, direction, velocity,
         Math.round(changePct * 10) / 10, Math.round(lastVal * 100) / 100,
         Math.round(firstVal * 100) / 100, Math.round(reg.r2 * 1000) / 1000,
         Math.round((reg.slope / (firstVal !== 0 ? Math.abs(firstVal) : 1)) * 100) / 100,
         narrative]
      );
      written++;
    }
  }

  stop();
  logger.log(`1-year trends: ${written} written, ${skipped} skipped`);
  await jobLog(jobId, `1-year trends: ${written} written`);
  return { written, skipped };
}

// ── Phase 2: Year-over-Year Comparison ───────────────────────

interface YoYMetric {
  metric: string;
  years: Array<{ year: number; avg: number; samples: number }>;
  yoyChangePct: number | null; // vs same month last year
  allTimeChangePct: number | null; // first year vs latest year
}

interface YoYResult { metrics: number; yearsSpanned: number }

async function computeYearOverYear(pool: Pool, month: string, jobId: string): Promise<YoYResult> {
  const stop = logger.time('yoy-comparison');
  const [yearStr, monthStr] = month.split('-');
  const targetMonth = parseInt(monthStr);

  // Load all signatures for this calendar month across all years
  const { rows } = await pool.query(
    `SELECT date, dimension, metrics FROM aurora_signatures
     WHERE EXTRACT(MONTH FROM date) = $1
     ORDER BY date`,
    [targetMonth]
  );

  if (rows.length === 0) {
    logger.logVerbose('YoY: no data for month', targetMonth);
    stop();
    return { metrics: 0, yearsSpanned: 0 };
  }

  // Group by dimension/metric → year → values
  const metricYears = new Map<string, Map<number, number[]>>();

  for (const row of rows) {
    const d = row.date instanceof Date ? row.date : new Date(row.date);
    const year = d.getFullYear();
    const m = typeof row.metrics === 'string' ? JSON.parse(row.metrics) : row.metrics;

    for (const [key, val] of Object.entries(m)) {
      if (typeof val !== 'number') continue;
      const fullKey = `${row.dimension}/${key}`;
      if (!metricYears.has(fullKey)) metricYears.set(fullKey, new Map());
      const yearMap = metricYears.get(fullKey)!;
      if (!yearMap.has(year)) yearMap.set(year, []);
      yearMap.get(year)!.push(val);
    }
  }

  const yoyMetrics: YoYMetric[] = [];
  const currentYear = parseInt(yearStr);

  for (const [metric, yearMap] of metricYears) {
    if (yearMap.size < 2) continue; // Need at least 2 years

    const years = [...yearMap.entries()]
      .map(([year, vals]) => ({
        year,
        avg: Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 100) / 100,
        samples: vals.length,
      }))
      .sort((a, b) => a.year - b.year);

    const currentYearData = years.find(y => y.year === currentYear);
    const lastYearData = years.find(y => y.year === currentYear - 1);
    const firstYear = years[0];
    const latestYear = years[years.length - 1];

    const yoyChangePct = (currentYearData && lastYearData && lastYearData.avg !== 0)
      ? Math.round(((currentYearData.avg - lastYearData.avg) / Math.abs(lastYearData.avg)) * 10000) / 100
      : null;

    const allTimeChangePct = (firstYear.avg !== 0)
      ? Math.round(((latestYear.avg - firstYear.avg) / Math.abs(firstYear.avg)) * 10000) / 100
      : null;

    yoyMetrics.push({ metric, years, yoyChangePct, allTimeChangePct });
  }

  // Store YoY results in aurora_trends with period '1y' and a special narrative
  for (const yoy of yoyMetrics) {
    if (yoy.yoyChangePct === null) continue;

    const direction = yoy.yoyChangePct > 5 ? 'improving' : yoy.yoyChangePct < -5 ? 'declining' : 'stable';
    const velocity = Math.abs(yoy.yoyChangePct) > 50 ? 'rapid' : Math.abs(yoy.yoyChangePct) > 15 ? 'moderate' : 'slow';
    const currentYearData = yoy.years.find(y => y.year === currentYear);
    const lastYearData = yoy.years.find(y => y.year === currentYear - 1);

    const yearsRange = `${yoy.years[0].year}-${yoy.years[yoy.years.length - 1].year}`;
    const narrative = `${yoy.metric} YoY (month ${month}): ${yoy.yoyChangePct > 0 ? '+' : ''}${yoy.yoyChangePct}% vs last year. ${yoy.years.length} years of data (${yearsRange}).${yoy.allTimeChangePct !== null ? ` All-time: ${yoy.allTimeChangePct > 0 ? '+' : ''}${yoy.allTimeChangePct}%` : ''}`;

    // Store as a separate metric with _yoy suffix to avoid conflicting with weekly trends
    await pool.query(
      `INSERT INTO aurora_trends (metric, period, direction, velocity, change_pct, current_value, baseline_value, significance, narrative, calculated_at)
       VALUES ($1, '1y', $2, $3, $4, $5, $6, $7, $8, NOW())
       ON CONFLICT (metric, period) DO UPDATE SET
         direction = EXCLUDED.direction, velocity = EXCLUDED.velocity,
         change_pct = EXCLUDED.change_pct, current_value = EXCLUDED.current_value,
         baseline_value = EXCLUDED.baseline_value, significance = EXCLUDED.significance,
         narrative = EXCLUDED.narrative, calculated_at = NOW()`,
      [`${yoy.metric}_yoy`, direction, velocity,
       yoy.yoyChangePct,
       currentYearData?.avg ?? 0, lastYearData?.avg ?? 0,
       Math.min(1, yoy.years.length / 10), // More years = higher significance
       narrative]
    );
  }

  const yearsSpanned = new Set(rows.map(r => {
    const d = r.date instanceof Date ? r.date : new Date(r.date);
    return d.getFullYear();
  })).size;

  stop();
  logger.log(`YoY comparison: ${yoyMetrics.length} metrics across ${yearsSpanned} years for month ${targetMonth}`);
  await jobLog(jobId, `YoY: ${yoyMetrics.length} metrics, ${yearsSpanned} years`);
  return { metrics: yoyMetrics.length, yearsSpanned };
}

// ── Phase 3: Schedule Predictions (day-of-week patterns) ─────

const PREDICTION_METRICS = [
  { dimension: 'health', metric: 'steps' },
  { dimension: 'health', metric: 'sleep_hours' },
  { dimension: 'health', metric: 'active_energy_cal' },
  { dimension: 'communication', metric: 'total_messages' },
  { dimension: 'schedule', metric: 'events' },
];

interface PredictionResult { generated: number; evaluated: number }

async function generateDayOfWeekPredictions(pool: Pool, asOfDate: string, jobId: string): Promise<PredictionResult> {
  const stop = logger.time('predictions');
  let generated = 0;

  // For each key metric, compute day-of-week averages from last 90 days
  for (const pm of PREDICTION_METRICS) {
    const { rows } = await pool.query(`
      SELECT EXTRACT(DOW FROM date) as dow, AVG((metrics->>$2)::real) as avg_val, COUNT(*) as n
      FROM aurora_signatures
      WHERE dimension = $1 AND date > ($3::date - INTERVAL '90 days') AND date <= $3
        AND metrics ? $2
      GROUP BY dow HAVING COUNT(*) >= 4
    `, [pm.dimension, pm.metric, asOfDate]);

    if (rows.length < 5) continue; // Need at least 5 days of week covered

    const dowAvg = new Map(rows.map(r => [parseInt(r.dow), parseFloat(r.avg_val)]));

    // Generate predictions for the next 30 days
    const startDate = new Date(asOfDate);
    for (let i = 1; i <= 30; i++) {
      const target = new Date(startDate.getTime() + i * 86400000);
      const dow = target.getUTCDay();
      const predicted = dowAvg.get(dow);
      if (predicted === undefined) continue;

      await pool.query(`
        INSERT INTO aurora_predictions (metric, prediction_type, predicted_value, predicted_for)
        VALUES ($1, 'day_of_week', $2, $3)
        ON CONFLICT DO NOTHING
      `, [`${pm.dimension}/${pm.metric}`, Math.round(predicted * 100) / 100, target.toISOString().split('T')[0]]);
      generated++;
    }
  }

  // Evaluate past predictions that now have actual data
  const evalResult = await pool.query(`
    UPDATE aurora_predictions p
    SET actual_value = (
      SELECT (s.metrics->>SPLIT_PART(p.metric, '/', 2))::real
      FROM aurora_signatures s
      WHERE s.dimension = SPLIT_PART(p.metric, '/', 1) AND s.date = p.predicted_for
      LIMIT 1
    ),
    evaluated_at = NOW()
    WHERE p.actual_value IS NULL
      AND p.predicted_for < $1
      AND EXISTS (
        SELECT 1 FROM aurora_signatures s
        WHERE s.dimension = SPLIT_PART(p.metric, '/', 1) AND s.date = p.predicted_for
      )
  `, [asOfDate]);

  // Compute error percentages
  await pool.query(`
    UPDATE aurora_predictions
    SET error_pct = CASE WHEN predicted_value != 0
      THEN ABS(actual_value - predicted_value) / ABS(predicted_value) * 100
      ELSE NULL END
    WHERE evaluated_at IS NOT NULL AND error_pct IS NULL AND actual_value IS NOT NULL
  `);

  const evaluated = evalResult.rowCount ?? 0;

  stop();
  logger.log(`Predictions: ${generated} generated, ${evaluated} evaluated`);
  await jobLog(jobId, `Predictions: ${generated} new, ${evaluated} evaluated`);
  return { generated, evaluated };
}

// ── Phase 4: Behavioral DNA Synthesis ────────────────────────

interface DNATrait {
  trait: string;
  value: Record<string, unknown>;
  confidence: number;
  evidenceCount: number;
}

interface DNAResult { traitsWritten: number; traitsSkipped: number }

async function synthesizeBehavioralDNA(pool: Pool, asOfDate: string, jobId: string): Promise<DNAResult> {
  const stop = logger.time('behavioral-dna');
  let traitsWritten = 0;
  let traitsSkipped = 0;

  const startDate90d = new Date(new Date(asOfDate).getTime() - 90 * 86400000).toISOString().split('T')[0];

  // ── Gather source data ──────────────────────────────────────

  // 1. Last 90 days of signatures grouped by dimension
  const { rows: sigRows } = await pool.query(
    `SELECT dimension,
            COUNT(*) as day_count,
            jsonb_agg(jsonb_build_object('date', date, 'metrics', metrics) ORDER BY date) as entries
     FROM aurora_signatures
     WHERE date >= $1 AND date <= $2
     GROUP BY dimension
     ORDER BY dimension`,
    [startDate90d, asOfDate]
  );
  logger.logVerbose('DNA: loaded signatures', { dimensions: sigRows.length, range: `${startDate90d} to ${asOfDate}` });

  if (sigRows.length === 0) {
    logger.log('DNA: no signature data available, skipping');
    stop();
    return { traitsWritten: 0, traitsSkipped: 0 };
  }

  // 2. Top validated trends
  const { rows: trendRows } = await pool.query(
    `SELECT metric, period, direction, velocity, change_pct, narrative
     FROM aurora_trends
     WHERE significance >= 0.1
     ORDER BY significance DESC, ABS(change_pct) DESC
     LIMIT 40`
  );
  logger.logVerbose('DNA: loaded trends', trendRows.length);

  // 3. Top confirmed correlations
  const { rows: corrRows } = await pool.query(
    `SELECT stream_a, stream_b, direction, strength, coefficient, summary
     FROM aurora_correlations
     WHERE confidence >= 0.4 AND review_status != 'rejected'
     ORDER BY confidence DESC
     LIMIT 20`
  );
  logger.logVerbose('DNA: loaded correlations', corrRows.length);

  // 4. Notable anomalies (high/critical, last 90 days)
  const { rows: anomRows } = await pool.query(
    `SELECT category, severity, metric, description, sigma, detected_at
     FROM aurora_anomalies
     WHERE detected_at >= ($1::date - INTERVAL '90 days')
       AND severity IN ('high', 'critical')
     ORDER BY sigma DESC NULLS LAST
     LIMIT 20`,
    [asOfDate]
  );
  logger.logVerbose('DNA: loaded anomalies', anomRows.length);

  // ── Build dimension summaries for the prompt ────────────────

  const dimensionSummaries: string[] = [];
  for (const dim of sigRows) {
    const entries = typeof dim.entries === 'string' ? JSON.parse(dim.entries) : dim.entries;
    // Compute per-metric averages across the 90-day window
    const metricSums = new Map<string, { sum: number; count: number }>();
    for (const entry of entries) {
      const metrics = typeof entry.metrics === 'string' ? JSON.parse(entry.metrics) : entry.metrics;
      for (const [key, val] of Object.entries(metrics)) {
        if (typeof val !== 'number') continue;
        const existing = metricSums.get(key) || { sum: 0, count: 0 };
        existing.sum += val;
        existing.count++;
        metricSums.set(key, existing);
      }
    }

    // Sort by sample count (most data first) and take top 8 metrics per dimension to keep prompt compact
    const sorted = [...metricSums.entries()].sort((a, b) => b[1].count - a[1].count);
    const metricAvgs: string[] = [];
    for (const [key, { sum, count }] of sorted.slice(0, 8)) {
      metricAvgs.push(`  ${key}: avg=${Math.round((sum / count) * 100) / 100} (${count}d)`);
    }

    dimensionSummaries.push(
      `## ${dim.dimension} (${dim.day_count} days)\n${metricAvgs.join('\n')}`
    );
  }

  const trendSummary = trendRows.length > 0
    ? trendRows.map(t => `- ${t.metric} (${t.period}): ${t.direction} ${t.velocity}, ${t.change_pct > 0 ? '+' : ''}${Math.round(t.change_pct)}%`).join('\n')
    : 'No significant trends available.';

  const corrSummary = corrRows.length > 0
    ? corrRows.map(c => `- ${c.stream_a} \u2194 ${c.stream_b}: ${c.direction} ${c.strength} (r=${c.coefficient}). ${c.summary}`).join('\n')
    : 'No validated correlations available.';

  const anomSummary = anomRows.length > 0
    ? anomRows.map(a => `- [${a.severity}] ${a.category}/${a.metric}: ${a.description} (\u03C3=${a.sigma ?? '?'})`).join('\n')
    : 'No notable anomalies in period.';

  // ── LLM synthesis ───────────────────────────────────────────

  const systemPrompt = `You are AURORA, a behavioral intelligence engine analyzing a person's daily behavioral data over the last 90 days.

Your task is to synthesize a behavioral DNA profile — a set of stable traits that characterize this person's patterns, habits, and tendencies. Each trait should be data-grounded and actionable.

Output valid JSON only (no markdown fencing). Return an array of trait objects with this structure:
[
  {
    "trait": "trait_key_in_snake_case",
    "category": "one of: daily_patterns, communication_style, health_habits, digital_behavior, social_patterns, productivity_rhythms, emotional_patterns",
    "summary": "one-sentence natural language description",
    "value": { "key findings as structured data" },
    "confidence": 0.0 to 1.0,
    "evidence_count": number of data points supporting this trait
  }
]

Rules:
- Identify 8-12 traits across all categories
- Ground every trait in specific metric averages, trends, or correlations
- confidence reflects data quality: >0.8 for traits with 60+ days of data and strong trends; 0.5-0.8 for moderate evidence; <0.5 for emerging patterns
- evidence_count should reflect the number of data points (days, correlations, anomalies) backing the trait
- Be specific: "averages 7,200 steps on weekdays, 4,100 on weekends" not "active person"
- Include negative/declining patterns too — this is an honest profile`;

  const userMessage = `Analyze the following 90-day behavioral data and synthesize a Behavioral DNA profile.

# Behavioral Signatures (90-day averages by dimension)

${dimensionSummaries.join('\n\n')}

# Trends

${trendSummary}

# Validated Correlations

${corrSummary}

# Notable Anomalies (last 90 days)

${anomSummary}

Synthesize these into a comprehensive Behavioral DNA profile. Return ONLY a JSON array.`;

  logger.logDebug('DNA: prompt length', userMessage.length, 'chars');

  const stopLlm = logger.time('dna-llm-call');
  let llmResult;
  try {
    llmResult = await routeRequest({
      handler: 'aurora-monthly',
      taskTier: 'reasoning',
      systemPrompt,
      userMessage,
      maxTokens: 8000,
    });
  } catch (err) {
    logger.logMinimal('DNA: LLM call failed:', (err as Error).message);
    await jobLog(jobId, `DNA synthesis LLM failed: ${(err as Error).message}`, 'error');
    stop();
    return { traitsWritten: 0, traitsSkipped: 0 };
  }
  stopLlm();

  logger.logVerbose('DNA: LLM response', {
    model: llmResult.model,
    provider: llmResult.provider,
    tokens: llmResult.inputTokens + llmResult.outputTokens,
    costCents: llmResult.estimatedCostCents,
    latencyMs: llmResult.latencyMs,
  });

  // ── Parse LLM response ─────────────────────────────────────

  let traits: DNATrait[];
  try {
    // Strip markdown fencing if present
    let text = llmResult.text.trim();
    if (text.startsWith('```')) {
      text = text.replace(/^```(?:json)?\s*/, '').replace(/\s*```$/, '');
    }

    const parsed = JSON.parse(text);
    if (!Array.isArray(parsed)) throw new Error('Expected JSON array');

    traits = parsed.map((t: Record<string, unknown>) => ({
      trait: String(t.trait || ''),
      value: {
        category: t.category || 'uncategorized',
        summary: t.summary || '',
        ...(typeof t.value === 'object' && t.value !== null ? t.value as Record<string, unknown> : {}),
      },
      confidence: typeof t.confidence === 'number' ? Math.min(1, Math.max(0, t.confidence)) : 0.5,
      evidenceCount: typeof t.evidence_count === 'number' ? t.evidence_count : 0,
    }));
  } catch (err) {
    logger.logMinimal('DNA: failed to parse LLM response:', (err as Error).message);
    logger.logDebug('DNA: raw response:', llmResult.text.slice(0, 500));
    await jobLog(jobId, `DNA parse failed: ${(err as Error).message}`, 'error');
    stop();
    return { traitsWritten: 0, traitsSkipped: 0 };
  }

  // ── Write to aurora_behavioral_dna ──────────────────────────

  for (const t of traits) {
    if (!t.trait || t.trait.length < 3) {
      traitsSkipped++;
      continue;
    }

    try {
      // Load existing row to maintain value_history
      const { rows: existing } = await pool.query(
        `SELECT current_value, value_history FROM aurora_behavioral_dna WHERE trait = $1`,
        [t.trait]
      );

      let valueHistory: unknown[] = [];
      if (existing.length > 0) {
        const prev = existing[0];
        valueHistory = Array.isArray(prev.value_history) ? prev.value_history : [];
        // Append previous value to history (keep last 12 months)
        valueHistory.push({
          value: prev.current_value,
          recorded_at: new Date().toISOString(),
        });
        if (valueHistory.length > 12) valueHistory = valueHistory.slice(-12);
      }

      await pool.query(
        `INSERT INTO aurora_behavioral_dna (trait, current_value, confidence, evidence_count, value_history, last_updated)
         VALUES ($1, $2, $3, $4, $5, NOW())
         ON CONFLICT (trait) DO UPDATE SET
           current_value = EXCLUDED.current_value,
           confidence = EXCLUDED.confidence,
           evidence_count = EXCLUDED.evidence_count,
           value_history = EXCLUDED.value_history,
           last_updated = NOW(),
           active = TRUE,
           archived_at = NULL`,
        [t.trait, JSON.stringify(t.value), t.confidence, t.evidenceCount, JSON.stringify(valueHistory)]
      );
      traitsWritten++;
    } catch (err) {
      logger.logMinimal(`DNA: failed to write trait "${t.trait}":`, (err as Error).message);
      traitsSkipped++;
    }
  }

  // Archive traits not seen in this run (they may no longer be relevant)
  const traitNames = traits.filter(t => t.trait && t.trait.length >= 3).map(t => t.trait);
  if (traitNames.length > 0) {
    await pool.query(
      `UPDATE aurora_behavioral_dna
       SET active = FALSE, archived_at = NOW()
       WHERE trait != ALL($1) AND active = TRUE AND last_updated < NOW() - INTERVAL '60 days'`,
      [traitNames]
    );
  }

  stop();
  logger.log(`DNA synthesis: ${traitsWritten} traits written, ${traitsSkipped} skipped (${llmResult.model}, ${llmResult.latencyMs}ms)`);
  await jobLog(jobId, `DNA: ${traitsWritten} traits via ${llmResult.model} (${llmResult.estimatedCostCents.toFixed(2)}\u00A2)`);
  return { traitsWritten, traitsSkipped };
}

// ── Main handler ─────────────────────────────────────────────

export async function handleAuroraMonthly(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload || {};
  const jobStart = Date.now();

  const month = (payload.month as string) || (() => {
    const d = new Date();
    d.setMonth(d.getMonth() - 1);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  })();

  const [yearStr, monthStr] = month.split('-');
  const asOfDate = `${yearStr}-${monthStr}-${new Date(parseInt(yearStr), parseInt(monthStr), 0).getDate()}`;

  logger.logVerbose('Entering handleAuroraMonthly', { month });
  logger.log(`Monthly deep analysis for ${month}`);
  await jobLog(job.id, `Processing month: ${month}`);

  // Phase 1: 1-year trend vectors
  let trendResult = { written: 0, skipped: 0 };
  try {
    trendResult = await compute1YearTrends(pool, asOfDate, job.id);
  } catch (err) {
    logger.logMinimal(`1-year trends failed: ${(err as Error).message}`);
  }

  // Phase 2: Year-over-year comparison
  let yoyResult = { metrics: 0, yearsSpanned: 0 };
  try {
    yoyResult = await computeYearOverYear(pool, month, job.id);
  } catch (err) {
    logger.logMinimal(`YoY comparison failed: ${(err as Error).message}`);
  }

  // Phase 3: Day-of-week predictions + accuracy evaluation
  let predResult = { generated: 0, evaluated: 0 };
  try {
    predResult = await generateDayOfWeekPredictions(pool, asOfDate, job.id);
  } catch (err) {
    logger.logMinimal(`Predictions failed: ${(err as Error).message}`);
  }

  // Phase 4: Behavioral DNA synthesis (LLM via router)
  let dnaResult = { traitsWritten: 0, traitsSkipped: 0 };
  try {
    dnaResult = await synthesizeBehavioralDNA(pool, asOfDate, job.id);
  } catch (err) {
    logger.logMinimal(`Behavioral DNA synthesis failed: ${(err as Error).message}`);
  }

  const elapsedMs = Date.now() - jobStart;
  logger.log(`Monthly complete: ${trendResult.written} 1y-trends, ${yoyResult.metrics} YoY, ${dnaResult.traitsWritten} DNA traits (${elapsedMs}ms)`);
  await jobLog(job.id, `Complete: ${trendResult.written} trends, ${yoyResult.metrics} YoY, ${dnaResult.traitsWritten} DNA traits, ${elapsedMs}ms`);

  return {
    month,
    yearly_trends_written: trendResult.written,
    yearly_trends_skipped: trendResult.skipped,
    yoy_metrics: yoyResult.metrics,
    yoy_years: yoyResult.yearsSpanned,
    predictions_generated: predResult.generated,
    predictions_evaluated: predResult.evaluated,
    dna_traits_written: dnaResult.traitsWritten,
    dna_traits_skipped: dnaResult.traitsSkipped,
    elapsed_ms: elapsedMs,
  };
}
