/**
 * Cross-Source Correlation Handler
 *
 * Computes daily feature vectors across all data sources, then runs
 * correlation analysis to find relationships between different life
 * dimensions (communication, health, schedule, location, mood).
 *
 * Runs weekly after aurora-weekly. Stores findings in aurora_correlations
 * and bridges significant discoveries to the Knowledge Graph.
 *
 * Examples of insights it can surface:
 * - "Weeks with 15+ meetings correlate with 30% fewer personal messages"
 * - "Step count drops by 40% on days with 3+ hours of video calls"
 * - "Email response time increases 2x on days with poor sleep"
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('cross-correlate');

const LOOKBACK_DAYS = 90;
const MIN_DATA_POINTS = 14; // need at least 2 weeks of paired data
const SIGNIFICANCE_THRESHOLD = 0.4; // |r| > 0.4 to store

interface DailyFeature {
  date: string;
  [metric: string]: number | string;
}

export async function handleCrossCorrelate(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { lookback_days?: number };
  const lookback = payload.lookback_days ?? LOOKBACK_DAYS;

  logger.log(`Starting cross-source correlation (${lookback}-day window)`);
  await jobLog(job.id, `Computing daily feature vectors (${lookback} days)`);

  // Build daily feature vectors from all data sources
  const features = await buildDailyFeatures(pool, lookback);
  const dates = Object.keys(features).sort();

  if (dates.length < MIN_DATA_POINTS) {
    logger.log(`Only ${dates.length} days with data — need ${MIN_DATA_POINTS}. Skipping.`);
    return { status: 'insufficient_data', days: dates.length };
  }

  logger.log(`Built feature vectors for ${dates.length} days`);

  // Get all metric names
  const allMetrics = new Set<string>();
  for (const day of dates) {
    for (const key of Object.keys(features[day])) {
      if (key !== 'date' && typeof features[day][key] === 'number') {
        allMetrics.add(key);
      }
    }
  }

  const metrics = Array.from(allMetrics);
  logger.log(`Metrics: ${metrics.join(', ')}`);

  // Compute pairwise correlations
  let stored = 0;
  let significant = 0;
  const period = `${lookback}d`;

  for (let i = 0; i < metrics.length; i++) {
    for (let j = i + 1; j < metrics.length; j++) {
      const metricA = metrics[i];
      const metricB = metrics[j];

      // Build paired data points (both must have values for the day)
      const pairs: { a: number; b: number }[] = [];
      for (const date of dates) {
        const a = features[date][metricA];
        const b = features[date][metricB];
        if (typeof a === 'number' && typeof b === 'number' && !isNaN(a) && !isNaN(b)) {
          pairs.push({ a, b });
        }
      }

      if (pairs.length < MIN_DATA_POINTS) continue;

      const r = pearsonCorrelation(pairs);
      if (Math.abs(r) < SIGNIFICANCE_THRESHOLD) continue;

      significant++;

      // Upsert into aurora_correlations
      const direction = r > 0 ? 'positive' : 'negative';
      const absR = Math.abs(r);
      const strength = absR > 0.7 ? 'strong' : absR > 0.5 ? 'moderate' : 'weak';

      await pool.query(`
        INSERT INTO aurora_correlations (stream_a, stream_b, direction, strength, coefficient, p_value, sample_size, confidence, summary, last_validated)
        VALUES ($1, $2, $3, $4, $5, 0.05, $6, $7, $8, NOW())
        ON CONFLICT (stream_a, stream_b) DO UPDATE SET
          direction = EXCLUDED.direction,
          strength = EXCLUDED.strength,
          coefficient = EXCLUDED.coefficient,
          sample_size = EXCLUDED.sample_size,
          confidence = EXCLUDED.confidence,
          summary = EXCLUDED.summary,
          last_validated = NOW(),
          confirmation_count = aurora_correlations.confirmation_count + 1
      `, [
        metricA, metricB, direction, strength,
        Math.round(r * 1000) / 1000, pairs.length,
        Math.min(0.9, absR),
        `${metricA.replace(/_/g, ' ')} and ${metricB.replace(/_/g, ' ')} are ${direction}ly correlated (r=${r.toFixed(2)}, n=${pairs.length})`,
      ]);
      stored++;
    }
  }

  logger.log(`Correlations: ${stored} significant (|r| > ${SIGNIFICANCE_THRESHOLD}) out of ${metrics.length * (metrics.length - 1) / 2} pairs`);
  await jobLog(job.id, `Computed ${stored} significant correlations from ${metrics.length} metrics across ${dates.length} days`);

  return {
    days_analyzed: dates.length,
    metrics: metrics.length,
    correlations_stored: stored,
    significant,
  };
}

// ── Daily Feature Vector Builder ──────────────────────────────

async function buildDailyFeatures(
  pool: ReturnType<typeof getPool>,
  lookbackDays: number,
): Promise<Record<string, Record<string, number | string>>> {
  const features: Record<string, Record<string, number | string>> = {};

  const since = new Date(Date.now() - lookbackDays * 86400000).toISOString().split('T')[0];

  // Helper to merge a metric into the features map
  const merge = (rows: Array<{ day: string; value: number }>, metricName: string) => {
    for (const row of rows) {
      if (!features[row.day]) features[row.day] = { date: row.day };
      features[row.day][metricName] = row.value;
    }
  };

  // 1. Communication volume (messages sent + received per day)
  const { rows: msgVol } = await pool.query<{ day: string; value: number }>(`
    SELECT date::date::text as day, COUNT(*)::int as value
    FROM aurora_raw_imessage WHERE date >= $1::date
    GROUP BY date::date ORDER BY day
  `, [since]);
  merge(msgVol, 'imessage_volume');

  // 2. Email volume
  const { rows: emailVol } = await pool.query<{ day: string; value: number }>(`
    SELECT date::date::text as day, COUNT(*)::int as value
    FROM aurora_raw_gmail WHERE date >= $1::date
    GROUP BY date::date ORDER BY day
  `, [since]);
  merge(emailVol, 'email_volume');

  // 3. Calendar event count
  const { rows: calVol } = await pool.query<{ day: string; value: number }>(`
    SELECT start_date::date::text as day, COUNT(*)::int as value
    FROM device_calendar_events WHERE start_date >= $1::date
    GROUP BY start_date::date ORDER BY day
  `, [since]);
  merge(calVol, 'calendar_events');

  // 4. Health: steps
  const { rows: steps } = await pool.query<{ day: string; value: number }>(`
    SELECT DATE(date)::text as day, SUM((value->>'value')::numeric)::int as value
    FROM health_data WHERE data_type = 'steps' AND date >= $1::date
    GROUP BY DATE(date) ORDER BY day
  `, [since]);
  merge(steps, 'steps');

  // 5. Health: active energy
  const { rows: energy } = await pool.query<{ day: string; value: number }>(`
    SELECT DATE(date)::text as day, SUM((value->>'value')::numeric)::int as value
    FROM health_data WHERE data_type = 'active_energy' AND date >= $1::date
    GROUP BY DATE(date) ORDER BY day
  `, [since]);
  merge(energy, 'active_energy');

  // 6. Health: heart rate (daily avg)
  const { rows: hr } = await pool.query<{ day: string; value: number }>(`
    SELECT DATE(date)::text as day, ROUND(AVG((value->>'value')::numeric))::int as value
    FROM health_data WHERE data_type = 'heart_rate' AND date >= $1::date
    GROUP BY DATE(date) ORDER BY day
  `, [since]);
  merge(hr, 'avg_heart_rate');

  // 7. Photos taken
  const { rows: photos } = await pool.query<{ day: string; value: number }>(`
    SELECT taken_at::date::text as day, COUNT(*)::int as value
    FROM photo_metadata WHERE taken_at >= $1::date AND taken_at IS NOT NULL
    GROUP BY taken_at::date ORDER BY day
  `, [since]);
  merge(photos, 'photos_taken');

  // 8. Unique contacts communicated with (iMessage)
  const { rows: contacts } = await pool.query<{ day: string; value: number }>(`
    SELECT date::date::text as day, COUNT(DISTINCT handle_id)::int as value
    FROM aurora_raw_imessage WHERE date >= $1::date AND handle_id IS NOT NULL
    GROUP BY date::date ORDER BY day
  `, [since]);
  merge(contacts, 'unique_contacts');

  // 9. Sentiment (daily average from aurora_sentiment)
  const { rows: sentiment } = await pool.query<{ day: string; value: number }>(`
    SELECT analyzed_at::date::text as day, ROUND(AVG(sentiment_score)::numeric, 2)::float as value
    FROM aurora_sentiment WHERE analyzed_at >= $1::date
    GROUP BY analyzed_at::date ORDER BY day
  `, [since]);
  merge(sentiment, 'avg_sentiment');

  // 10. Location entropy (unique locations per day)
  const { rows: locEntropy } = await pool.query<{ day: string; value: number }>(`
    SELECT DATE(timestamp)::text as day,
           COUNT(DISTINCT ROUND(latitude::numeric, 2) || ',' || ROUND(longitude::numeric, 2))::int as value
    FROM device_location WHERE timestamp >= $1::date AND latitude IS NOT NULL
    GROUP BY DATE(timestamp) ORDER BY day
  `, [since]);
  merge(locEntropy, 'location_variety');

  return features;
}

// ── Pearson Correlation ──────────────────────────────────────

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
