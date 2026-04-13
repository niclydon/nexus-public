/**
 * Cross-Metrics Compute Handler
 *
 * Runs nightly. Computes daily metric values from ALL data sources and
 * stores them in aurora_cross_metrics. This normalized table powers
 * the advanced-correlate handler for lag correlation, event impact,
 * and predictive signal analysis.
 *
 * Payload: { backfill?: boolean, since?: string }
 *   backfill=true computes from `since` (default '2005-01-01') to today.
 *   Otherwise computes just today.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('cross-metrics-compute');

interface MetricRow {
  d: string;
  value: number;
}

export async function handleCrossMetricsCompute(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { backfill?: boolean; since?: string } | null;
  const backfill = payload?.backfill ?? false;

  const since = backfill
    ? (payload?.since ?? '2005-01-01')
    : new Date().toISOString().split('T')[0];

  logger.log(`Starting cross-metrics compute (since=${since}, backfill=${backfill})`);
  await jobLog(job.id, `Computing daily metrics since ${since}`);

  // Collect all metric rows, then batch upsert
  const allMetrics: Array<{ date: string; metric_name: string; value: number; source: string }> = [];

  const collect = (rows: MetricRow[], metricName: string, source: string) => {
    for (const row of rows) {
      if (row.d && row.value != null && !isNaN(row.value)) {
        allMetrics.push({ date: row.d, metric_name: metricName, value: row.value, source });
      }
    }
  };

  // ── Last.fm Scrobbles ─────────────────────────────────────────
  logger.logVerbose('Querying lastfm_scrobbles');
  try {
    const { rows: scrobbleCount } = await pool.query<MetricRow>(`
      SELECT date(scrobbled_at)::text as d, COUNT(*)::int as value
      FROM lastfm_scrobbles WHERE date(scrobbled_at) >= $1::date
      GROUP BY d
    `, [since]);
    collect(scrobbleCount, 'scrobble_count', 'lastfm');

    const { rows: uniqueArtists } = await pool.query<MetricRow>(`
      SELECT date(scrobbled_at)::text as d, COUNT(DISTINCT artist_name)::int as value
      FROM lastfm_scrobbles WHERE date(scrobbled_at) >= $1::date
      GROUP BY d
    `, [since]);
    collect(uniqueArtists, 'unique_artists', 'lastfm');
  } catch (err) {
    logger.logVerbose('lastfm_scrobbles not available:', (err as Error).message);
  }

  // ── Netflix Viewing History ───────────────────────────────────
  logger.logVerbose('Querying netflix_viewing_history');
  try {
    const { rows: netflixEpisodes } = await pool.query<MetricRow>(`
      SELECT watched_date::text as d, COUNT(*)::int as value
      FROM netflix_viewing_history WHERE watched_date >= $1::date
      GROUP BY d
    `, [since]);
    collect(netflixEpisodes, 'netflix_episodes', 'netflix');

    const { rows: uniqueShows } = await pool.query<MetricRow>(`
      SELECT watched_date::text as d, COUNT(DISTINCT show_name)::int as value
      FROM netflix_viewing_history WHERE watched_date >= $1::date
      GROUP BY d
    `, [since]);
    collect(uniqueShows, 'unique_shows', 'netflix');
  } catch (err) {
    logger.logVerbose('netflix_viewing_history not available:', (err as Error).message);
  }

  // ── Life Transactions ─────────────────────────────────────────
  logger.logVerbose('Querying life_transactions');
  try {
    const { rows: txnCount } = await pool.query<MetricRow>(`
      SELECT date(transaction_date)::text as d, COUNT(*)::int as value
      FROM life_transactions WHERE date(transaction_date) >= $1::date
      GROUP BY d
    `, [since]);
    collect(txnCount, 'transaction_count', 'transactions');

    const { rows: uberRides } = await pool.query<MetricRow>(`
      SELECT date(transaction_date)::text as d, COUNT(*)::int as value
      FROM life_transactions WHERE date(transaction_date) >= $1::date AND source = 'uber'
      GROUP BY d
    `, [since]);
    collect(uberRides, 'uber_rides', 'transactions');

    const { rows: foodDelivery } = await pool.query<MetricRow>(`
      SELECT date(transaction_date)::text as d, COUNT(*)::int as value
      FROM life_transactions WHERE date(transaction_date) >= $1::date
        AND (source = 'doordash' OR source = 'uber_eats')
      GROUP BY d
    `, [since]);
    collect(foodDelivery, 'food_delivery', 'transactions');
  } catch (err) {
    logger.logVerbose('life_transactions not available:', (err as Error).message);
  }

  // ── Communication Volume ──────────────────────────────────────
  logger.logVerbose('Querying aurora_unified_communication');
  try {
    const { rows: msgVol } = await pool.query<MetricRow>(`
      SELECT date(timestamp)::text as d, COUNT(*)::int as value
      FROM aurora_unified_communication WHERE date(timestamp) >= $1::date
      GROUP BY d
    `, [since]);
    collect(msgVol, 'msg_volume', 'communication');

    const { rows: uniqueContacts } = await pool.query<MetricRow>(`
      SELECT date(timestamp)::text as d, COUNT(DISTINCT identity_id)::int as value
      FROM aurora_unified_communication WHERE date(timestamp) >= $1::date AND identity_id IS NOT NULL
      GROUP BY d
    `, [since]);
    collect(uniqueContacts, 'unique_contacts', 'communication');
  } catch (err) {
    logger.logVerbose('aurora_unified_communication not available:', (err as Error).message);
  }

  // ── Mood Proxy ────────────────────────────────────────────────
  logger.logVerbose('Querying aurora_mood_proxy');
  try {
    const { rows: mood } = await pool.query<MetricRow>(`
      SELECT date::text as d, mood_score::real as value
      FROM aurora_mood_proxy WHERE date >= $1::date
    `, [since]);
    collect(mood, 'mood_score', 'mood');
  } catch (err) {
    logger.logVerbose('aurora_mood_proxy not available:', (err as Error).message);
  }

  // ── Health Data ───────────────────────────────────────────────
  logger.logVerbose('Querying health_data');
  try {
    const { rows: steps } = await pool.query<MetricRow>(`
      SELECT DATE(date)::text as d, SUM((value->>'value')::numeric)::int as value
      FROM health_data WHERE data_type = 'steps' AND date >= $1::date
      GROUP BY DATE(date)
    `, [since]);
    collect(steps, 'steps', 'health');

    const { rows: energy } = await pool.query<MetricRow>(`
      SELECT DATE(date)::text as d, SUM((value->>'value')::numeric)::int as value
      FROM health_data WHERE data_type = 'active_energy' AND date >= $1::date
      GROUP BY DATE(date)
    `, [since]);
    collect(energy, 'active_energy', 'health');

    const { rows: hr } = await pool.query<MetricRow>(`
      SELECT DATE(date)::text as d, ROUND(AVG((value->>'value')::numeric))::int as value
      FROM health_data WHERE data_type = 'heart_rate' AND date >= $1::date
      GROUP BY DATE(date)
    `, [since]);
    collect(hr, 'avg_heart_rate', 'health');
  } catch (err) {
    logger.logVerbose('health_data not available:', (err as Error).message);
  }

  // ── Photos Taken ──────────────────────────────────────────────
  logger.logVerbose('Querying photo_metadata');
  try {
    const { rows: photos } = await pool.query<MetricRow>(`
      SELECT taken_at::date::text as d, COUNT(*)::int as value
      FROM photo_metadata WHERE taken_at >= $1::date AND taken_at IS NOT NULL
      GROUP BY taken_at::date
    `, [since]);
    collect(photos, 'photos_taken', 'photos');
  } catch (err) {
    logger.logVerbose('photo_metadata not available:', (err as Error).message);
  }

  // ── Calendar Events ───────────────────────────────────────────
  logger.logVerbose('Querying device_calendar_events');
  try {
    const { rows: cal } = await pool.query<MetricRow>(`
      SELECT start_date::date::text as d, COUNT(*)::int as value
      FROM device_calendar_events WHERE start_date >= $1::date
      GROUP BY start_date::date
    `, [since]);
    collect(cal, 'calendar_events', 'calendar');
  } catch (err) {
    logger.logVerbose('device_calendar_events not available:', (err as Error).message);
  }

  // ── Sentiment ─────────────────────────────────────────────────
  logger.logVerbose('Querying aurora_sentiment');
  try {
    const { rows: sentiment } = await pool.query<MetricRow>(`
      SELECT analyzed_at::date::text as d, ROUND(AVG(sentiment_score)::numeric, 2)::real as value
      FROM aurora_sentiment WHERE analyzed_at >= $1::date
      GROUP BY analyzed_at::date
    `, [since]);
    collect(sentiment, 'avg_sentiment', 'sentiment');
  } catch (err) {
    logger.logVerbose('aurora_sentiment not available:', (err as Error).message);
  }

  // ── Music Listening (Apple Music) ─────────────────────────────
  logger.logVerbose('Querying music_listening_history');
  try {
    const { rows: musicPlays } = await pool.query<MetricRow>(`
      SELECT date(played_at)::text as d, COUNT(*)::int as value
      FROM music_listening_history WHERE date(played_at) >= $1::date
      GROUP BY d
    `, [since]);
    collect(musicPlays, 'music_plays', 'music');

    const { rows: musicArtists } = await pool.query<MetricRow>(`
      SELECT date(played_at)::text as d, COUNT(DISTINCT artist_name)::int as value
      FROM music_listening_history WHERE date(played_at) >= $1::date
      GROUP BY d
    `, [since]);
    collect(musicArtists, 'music_unique_artists', 'music');
  } catch (err) {
    logger.logVerbose('music_listening_history not available:', (err as Error).message);
  }

  // ── Batch Upsert ──────────────────────────────────────────────
  if (allMetrics.length === 0) {
    logger.log('No metrics computed — all sources empty or unavailable');
    return { status: 'no_data', metrics_stored: 0 };
  }

  logger.log(`Upserting ${allMetrics.length} metric rows`);
  await jobLog(job.id, `Upserting ${allMetrics.length} metric rows`);

  // Batch in chunks of 500 to avoid oversized queries
  const CHUNK_SIZE = 500;
  let stored = 0;

  for (let i = 0; i < allMetrics.length; i += CHUNK_SIZE) {
    const chunk = allMetrics.slice(i, i + CHUNK_SIZE);
    const values: unknown[] = [];
    const placeholders: string[] = [];

    for (let j = 0; j < chunk.length; j++) {
      const offset = j * 4;
      placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4})`);
      values.push(chunk[j].date, chunk[j].metric_name, chunk[j].value, chunk[j].source);
    }

    await pool.query(`
      INSERT INTO aurora_cross_metrics (date, metric_name, value, source)
      VALUES ${placeholders.join(', ')}
      ON CONFLICT (date, metric_name) DO UPDATE SET
        value = EXCLUDED.value,
        source = EXCLUDED.source
    `, values);

    stored += chunk.length;
  }

  const sources = [...new Set(allMetrics.map(m => m.source))];
  const metricNames = [...new Set(allMetrics.map(m => m.metric_name))];

  logger.log(`Done: ${stored} rows across ${metricNames.length} metrics from ${sources.length} sources`);
  await jobLog(job.id, `Stored ${stored} metric rows: ${metricNames.join(', ')}`);

  return {
    metrics_stored: stored,
    metric_names: metricNames,
    sources,
    backfill,
    since,
  };
}
