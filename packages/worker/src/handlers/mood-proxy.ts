/**
 * Mood/Energy Proxy Handler
 *
 * Computes a daily mood proxy score from passive behavioral signals.
 * Based on digital phenotyping research showing mood correlates with:
 * - Message response time variance (higher variance → lower mood)
 * - Communication velocity (declining volume → lower mood)
 * - Calendar cancellation/density changes
 * - Step count deviation from personal baseline
 * - Location variety (less movement → lower mood)
 *
 * The proxy is NOT a clinical mood assessment — it's a behavioral
 * baseline tracker. When the score drops significantly below the
 * user's rolling average, ARIA can proactively offer support.
 *
 * Score: 0.0 (very low) to 1.0 (very high), calibrated against
 * personal 30-day rolling baseline.
 *
 * Runs daily via aurora-nightly or standalone.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('mood-proxy');

const BASELINE_DAYS = 30;

interface DailySignals {
  date: string;
  msg_volume: number | null;
  msg_variance: number | null;
  email_volume: number | null;
  calendar_events: number | null;
  steps: number | null;
  location_variety: number | null;
  avg_sentiment: number | null;
}

export async function handleMoodProxy(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { days?: number };
  const daysToCompute = payload.days ?? 7; // compute last N days

  logger.log(`Computing mood proxy for last ${daysToCompute} days`);
  await jobLog(job.id, `Computing mood proxy (${daysToCompute} days, ${BASELINE_DAYS}-day baseline)`);

  // Get today's date and compute range
  const endDate = new Date();
  const startDate = new Date(endDate.getTime() - daysToCompute * 86400000);
  const baselineStart = new Date(endDate.getTime() - (BASELINE_DAYS + daysToCompute) * 86400000);

  // Fetch all daily signals in one pass
  const signals = await fetchDailySignals(pool, baselineStart);

  if (signals.length < BASELINE_DAYS / 2) {
    logger.log(`Only ${signals.length} days of data — need ${BASELINE_DAYS / 2} for baseline`);
    return { status: 'insufficient_data', days: signals.length };
  }

  // Compute baselines from the older data (before the target period)
  const startStr = startDate.toISOString().split('T')[0];
  const baselineSignals = signals.filter(s => s.date < startStr);
  const targetSignals = signals.filter(s => s.date >= startStr);

  const baselines = computeBaselines(baselineSignals);

  let computed = 0;
  let alerts = 0;

  for (const day of targetSignals) {
    const score = computeMoodScore(day, baselines);

    // Upsert into aurora_mood_proxy
    await pool.query(`
      INSERT INTO aurora_mood_proxy (date, mood_score, signals, baselines, created_at)
      VALUES ($1::date, $2, $3, $4, NOW())
      ON CONFLICT (date) DO UPDATE SET
        mood_score = EXCLUDED.mood_score,
        signals = EXCLUDED.signals,
        baselines = EXCLUDED.baselines,
        created_at = NOW()
    `, [
      day.date, score,
      JSON.stringify(day),
      JSON.stringify(baselines),
    ]);
    computed++;

    // Alert if score drops below 0.3 (significant deviation)
    if (score < 0.3) {
      // Check if we already alerted recently
      const { rows: recent } = await pool.query(`
        SELECT id FROM agent_inbox
        WHERE to_agent = 'aria' AND from_agent = 'system'
          AND message LIKE '%Mood Proxy%'
          AND created_at > NOW() - INTERVAL '48 hours'
        LIMIT 1
      `);

      if (recent.length === 0) {
        const lowSignals: string[] = [];
        if (day.msg_volume != null && baselines.msg_volume > 0 && day.msg_volume < baselines.msg_volume * 0.5) {
          lowSignals.push(`message volume ${Math.round((1 - day.msg_volume / baselines.msg_volume) * 100)}% below baseline`);
        }
        if (day.steps != null && baselines.steps > 0 && day.steps < baselines.steps * 0.5) {
          lowSignals.push(`steps ${Math.round((1 - day.steps / baselines.steps) * 100)}% below baseline`);
        }
        if (day.location_variety != null && baselines.location_variety > 0 && day.location_variety < baselines.location_variety * 0.5) {
          lowSignals.push(`location variety dropped`);
        }

        await pool.query(
          `INSERT INTO agent_inbox (to_agent, from_agent, message, priority)
           VALUES ('aria', 'system', $1, 1)`,
          [`[Mood Proxy Alert] Score ${score.toFixed(2)} on ${day.date} — significantly below baseline. Signals: ${lowSignals.join(', ') || 'multiple metrics declining'}. Consider checking in with the owner or suggesting an activity.`],
        );
        alerts++;
      }
    }
  }

  logger.log(`Mood proxy: ${computed} days computed, ${alerts} alerts`);
  await jobLog(job.id, `Computed ${computed} mood scores, ${alerts} alerts sent`);

  return { computed, alerts };
}

async function fetchDailySignals(
  pool: ReturnType<typeof getPool>,
  since: Date,
): Promise<DailySignals[]> {
  const sinceStr = since.toISOString().split('T')[0];

  const { rows } = await pool.query<DailySignals>(`
    WITH dates AS (
      SELECT generate_series($1::date, CURRENT_DATE, '1 day')::date as date
    ),
    msg AS (
      SELECT date::date::text as day, COUNT(*)::int as vol, NULL::float as variance
      FROM aurora_raw_imessage WHERE date >= $1::date AND NOT is_from_me
      GROUP BY date::date
    ),
    email AS (
      SELECT date::date::text as day, COUNT(*)::int as vol
      FROM aurora_raw_gmail WHERE date >= $1::date
      GROUP BY date::date
    ),
    cal AS (
      SELECT start_date::date::text as day, COUNT(*)::int as vol
      FROM device_calendar_events WHERE start_date >= $1::date
      GROUP BY start_date::date
    ),
    step AS (
      SELECT date::date::text as day, SUM((value->>'value')::numeric)::int as vol
      FROM health_data WHERE data_type = 'steps' AND date >= $1::date
      GROUP BY date::date
    ),
    loc AS (
      SELECT DATE(timestamp)::text as day,
        COUNT(DISTINCT ROUND(latitude::numeric, 2) || ',' || ROUND(longitude::numeric, 2))::int as vol
      FROM device_location WHERE timestamp >= $1::date AND latitude IS NOT NULL
      GROUP BY DATE(timestamp)
    ),
    sent AS (
      SELECT analyzed_at::date::text as day, AVG(sentiment_score)::float as avg
      FROM aurora_sentiment WHERE analyzed_at >= $1::date
      GROUP BY analyzed_at::date
    )
    SELECT d.date::text,
      m.vol as msg_volume, m.variance as msg_variance,
      e.vol as email_volume, c.vol as calendar_events,
      s.vol as steps, l.vol as location_variety,
      sent.avg as avg_sentiment
    FROM dates d
    LEFT JOIN msg m ON m.day = d.date::text
    LEFT JOIN email e ON e.day = d.date::text
    LEFT JOIN cal c ON c.day = d.date::text
    LEFT JOIN step s ON s.day = d.date::text
    LEFT JOIN loc l ON l.day = d.date::text
    LEFT JOIN sent ON sent.day = d.date::text
    ORDER BY d.date
  `, [sinceStr]);

  return rows;
}

interface Baselines {
  msg_volume: number;
  email_volume: number;
  calendar_events: number;
  steps: number;
  location_variety: number;
  avg_sentiment: number;
}

function computeBaselines(signals: DailySignals[]): Baselines {
  const avg = (values: (number | null)[]) => {
    const valid = values.filter((v): v is number => v != null && !isNaN(v));
    return valid.length > 0 ? valid.reduce((a, b) => a + b, 0) / valid.length : 0;
  };

  return {
    msg_volume: avg(signals.map(s => s.msg_volume)),
    email_volume: avg(signals.map(s => s.email_volume)),
    calendar_events: avg(signals.map(s => s.calendar_events)),
    steps: avg(signals.map(s => s.steps)),
    location_variety: avg(signals.map(s => s.location_variety)),
    avg_sentiment: avg(signals.map(s => s.avg_sentiment)),
  };
}

function computeMoodScore(day: DailySignals, baselines: Baselines): number {
  const scores: number[] = [];

  // Each dimension: ratio of current to baseline, capped at 0-1
  const ratio = (current: number | null, baseline: number): number | null => {
    if (current == null || baseline <= 0) return null;
    return Math.min(1.0, current / baseline);
  };

  const r1 = ratio(day.msg_volume, baselines.msg_volume);
  if (r1 != null) scores.push(r1);

  const r2 = ratio(day.email_volume, baselines.email_volume);
  if (r2 != null) scores.push(r2);

  const r3 = ratio(day.steps, baselines.steps);
  if (r3 != null) scores.push(r3);

  const r4 = ratio(day.location_variety, baselines.location_variety);
  if (r4 != null) scores.push(r4);

  // Sentiment is already 0-1, use directly
  if (day.avg_sentiment != null) scores.push(day.avg_sentiment);

  // Calendar: both too many and too few can indicate issues.
  // Use distance from baseline (within 50% of baseline = 1.0, outside = lower)
  if (day.calendar_events != null && baselines.calendar_events > 0) {
    const calRatio = day.calendar_events / baselines.calendar_events;
    const calScore = calRatio > 1.5 ? Math.max(0, 1 - (calRatio - 1.5)) : // too many
                     calRatio < 0.5 ? calRatio * 2 : // too few
                     1.0; // within normal range
    scores.push(calScore);
  }

  if (scores.length === 0) return 0.5; // no data = neutral

  return Math.round((scores.reduce((a, b) => a + b, 0) / scores.length) * 100) / 100;
}
