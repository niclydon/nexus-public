/**
 * AURORA weekly synthesis handler.
 *
 * Runs every Sunday at 3 AM ET. Four phases:
 *   1. Trend vectors (7d/30d/90d linear regression across all numeric metrics)
 *   2. Change point detection (z-score of weekly averages on key metrics)
 *   3. Life chapter detection (group change points into behavioral phases)
 *   4. Chapter enrichment (flights, photo volume, similar chapters)
 */
import type { TempoJob } from '../job-worker.js';
import type { Pool } from 'pg';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('aurora-weekly');

// ── Constants ────────────────────────────────────────────────

const TREND_WINDOWS = [
  { period: '7d' as const, days: 7, minSamples: 4 },
  { period: '30d' as const, days: 30, minSamples: 14 },
  { period: '90d' as const, days: 90, minSamples: 30 },
];

const KEY_METRICS = [
  { dimension: 'health', metric: 'steps' },
  { dimension: 'health', metric: 'sleep_hours' },
  { dimension: 'health', metric: 'avg_heart_rate' },
  { dimension: 'health', metric: 'active_energy_cal' },
  { dimension: 'communication', metric: 'total_messages' },
  { dimension: 'communication', metric: 'unique_contacts' },
  { dimension: 'schedule', metric: 'events' },
  { dimension: 'search_behavior', metric: 'total_searches' },
  { dimension: 'photos', metric: 'photos_taken' },
  { dimension: 'digital_activity', metric: 'app_usage_seconds' },
];

const CHANGE_POINT_THRESHOLD = 2.0;
const CHANGE_POINT_LOOKBACK_WEEKS = 8;
const CHANGE_POINT_CLUSTER_WEEKS = 2;

// ── Math helpers ─────────────────────────────────────────────

function linearRegression(points: { x: number; y: number }[]): {
  slope: number; intercept: number; r2: number; n: number;
} {
  const n = points.length;
  if (n < 2) return { slope: 0, intercept: 0, r2: 0, n };

  let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;
  for (const p of points) {
    sumX += p.x; sumY += p.y;
    sumXY += p.x * p.y; sumX2 += p.x * p.x; sumY2 += p.y * p.y;
  }

  const denom = n * sumX2 - sumX * sumX;
  if (denom === 0) return { slope: 0, intercept: sumY / n, r2: 0, n };

  const slope = (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;

  // R² (coefficient of determination)
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

function isoWeek(dateStr: string): string {
  const d = new Date(dateStr + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNum = Math.ceil((((d.getTime() - yearStart.getTime()) / 86400000) + 1) / 7);
  return `${d.getUTCFullYear()}-W${String(weekNum).padStart(2, '0')}`;
}

// ── Phase 1: Trend Vectors ───────────────────────────────────

interface TrendResult { written: number; skipped: number }

async function computeTrendVectors(pool: Pool, asOfDate: string, jobId: string): Promise<TrendResult> {
  let written = 0;
  let skipped = 0;

  for (const window of TREND_WINDOWS) {
    const stop = logger.time(`trends-${window.period}`);
    const startDate = new Date(new Date(asOfDate).getTime() - window.days * 86400000).toISOString().split('T')[0];

    const { rows } = await pool.query(
      `SELECT date, dimension, metrics FROM aurora_signatures
       WHERE date >= $1 AND date <= $2 ORDER BY date`,
      [startDate, asOfDate]
    );

    // Group by dimension, extract numeric metrics, build time series
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

    // Compute regression for each metric
    for (const [dimension, metrics] of dimMetrics) {
      for (const [metricKey, points] of metrics) {
        if (points.length < window.minSamples) { skipped++; continue; }

        const reg = linearRegression(points);
        if (reg.r2 < 0.05) { skipped++; continue; }

        const firstVal = reg.intercept;
        const lastVal = reg.slope * (window.days - 1) + reg.intercept;
        const changePct = firstVal !== 0 ? ((lastVal - firstVal) / Math.abs(firstVal)) * 100 : 0;
        const changePctPerDay = changePct / window.days;

        const direction = classifyDirection(reg.slope, reg.r2);
        const velocity = classifyVelocity(changePctPerDay);
        const metricFullName = `${dimension}/${metricKey}`;

        const narrative = `${metricFullName} is ${direction} ${velocity}ly over ${window.period}: ${Math.round(changePct)}% change (${Math.round(lastVal)} vs ${Math.round(firstVal)})`;

        await pool.query(
          `INSERT INTO aurora_trends (metric, period, direction, velocity, change_pct, current_value, baseline_value, significance, sigma, narrative, calculated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
           ON CONFLICT (metric, period) DO UPDATE SET
             direction = EXCLUDED.direction, velocity = EXCLUDED.velocity,
             change_pct = EXCLUDED.change_pct, current_value = EXCLUDED.current_value,
             baseline_value = EXCLUDED.baseline_value, significance = EXCLUDED.significance,
             sigma = EXCLUDED.sigma, narrative = EXCLUDED.narrative, calculated_at = NOW()`,
          [metricFullName, window.period, direction, velocity,
           Math.round(changePct * 10) / 10, Math.round(lastVal * 100) / 100,
           Math.round(firstVal * 100) / 100, Math.round(reg.r2 * 1000) / 1000,
           Math.round((reg.slope / (firstVal !== 0 ? Math.abs(firstVal) : 1)) * 100) / 100,
           narrative]
        );
        written++;
      }
    }
    stop();
  }

  logger.log(`Trends: ${written} written, ${skipped} skipped`);
  await jobLog(jobId, `Trends: ${written} written across ${TREND_WINDOWS.length} windows`);
  return { written, skipped };
}

// ── Phase 2: Change Point Detection ──────────────────────────

interface ChangePoint {
  metric: string;
  weekStart: string;
  zScore: number;
  direction: 'up' | 'down';
  oldAvg: number;
  newAvg: number;
}

async function detectChangePoints(
  pool: Pool, asOfDate: string, force: boolean, jobId: string
): Promise<ChangePoint[]> {
  const allChangePoints: ChangePoint[] = [];
  const lookbackStart = force ? '2009-01-01' : new Date(new Date(asOfDate).getTime() - 90 * 86400000).toISOString().split('T')[0];

  for (const km of KEY_METRICS) {
    const { rows } = await pool.query(
      `SELECT date, metrics->>$2 as val FROM aurora_signatures
       WHERE dimension = $1 AND date >= $3 AND metrics ? $2
       ORDER BY date`,
      [km.dimension, km.metric, lookbackStart]
    );

    if (rows.length < 12 * 7) continue; // Need 12+ weeks

    // Compute weekly averages
    const weeklyAvgs = new Map<string, { sum: number; count: number }>();
    for (const row of rows) {
      const d = row.date instanceof Date ? row.date.toISOString().split('T')[0] : String(row.date);
      const week = isoWeek(d);
      const val = parseFloat(row.val);
      if (isNaN(val)) continue;
      if (!weeklyAvgs.has(week)) weeklyAvgs.set(week, { sum: 0, count: 0 });
      const w = weeklyAvgs.get(week)!;
      w.sum += val; w.count++;
    }

    const weeks = [...weeklyAvgs.entries()]
      .map(([week, { sum, count }]) => ({ week, avg: sum / count }))
      .sort((a, b) => a.week.localeCompare(b.week));

    // Z-score each week vs prior N-week rolling window
    for (let i = CHANGE_POINT_LOOKBACK_WEEKS; i < weeks.length; i++) {
      const windowSlice = weeks.slice(i - CHANGE_POINT_LOOKBACK_WEEKS, i);
      const mean = windowSlice.reduce((s, w) => s + w.avg, 0) / windowSlice.length;
      const variance = windowSlice.reduce((s, w) => s + (w.avg - mean) ** 2, 0) / windowSlice.length;
      const stddev = Math.sqrt(variance);
      if (stddev === 0) continue;

      const z = (weeks[i].avg - mean) / stddev;
      if (Math.abs(z) >= CHANGE_POINT_THRESHOLD) {
        // Compute the week's start date from ISO week
        const [yearStr, weekStr] = weeks[i].week.split('-W');
        const jan4 = new Date(Date.UTC(parseInt(yearStr), 0, 4));
        const dayOfYear = (parseInt(weekStr) - 1) * 7;
        const weekDate = new Date(jan4.getTime() + (dayOfYear - jan4.getUTCDay() + 1) * 86400000);

        allChangePoints.push({
          metric: `${km.dimension}/${km.metric}`,
          weekStart: weekDate.toISOString().split('T')[0],
          zScore: Math.round(z * 100) / 100,
          direction: z > 0 ? 'up' : 'down',
          oldAvg: Math.round(mean * 100) / 100,
          newAvg: Math.round(weeks[i].avg * 100) / 100,
        });
      }
    }
  }

  // Cluster nearby change points (within CHANGE_POINT_CLUSTER_WEEKS)
  const clustered = clusterChangePoints(allChangePoints);

  logger.log(`Change points: ${allChangePoints.length} raw, ${clustered.length} after clustering`);
  await jobLog(jobId, `Change points: ${clustered.length} detected across ${KEY_METRICS.length} metrics`);
  return clustered;
}

function clusterChangePoints(points: ChangePoint[]): ChangePoint[] {
  if (points.length === 0) return [];

  const sorted = [...points].sort((a, b) => a.weekStart.localeCompare(b.weekStart));
  const clusters: ChangePoint[][] = [[sorted[0]]];

  for (let i = 1; i < sorted.length; i++) {
    const lastCluster = clusters[clusters.length - 1];
    const lastDate = new Date(lastCluster[lastCluster.length - 1].weekStart);
    const thisDate = new Date(sorted[i].weekStart);
    const diffWeeks = (thisDate.getTime() - lastDate.getTime()) / (7 * 86400000);

    if (diffWeeks <= CHANGE_POINT_CLUSTER_WEEKS) {
      lastCluster.push(sorted[i]);
    } else {
      clusters.push([sorted[i]]);
    }
  }

  // Keep the point with the highest |z-score| from each cluster
  return clusters.map(cluster =>
    cluster.reduce((best, p) => Math.abs(p.zScore) > Math.abs(best.zScore) ? p : best)
  );
}

// ── Phase 3: Life Chapter Detection ──────────────────────────

interface ChapterResult { detected: number; skipped: number; newChapterIds: number[] }

function classifyChapterType(shifts: ChangePoint[]): string {
  const metrics = shifts.map(s => s.metric);
  const directions = shifts.map(s => s.direction);

  const hasHealth = metrics.some(m => m.startsWith('health/'));
  const hasCommunication = metrics.some(m => m.startsWith('communication/'));
  const hasPhotos = metrics.some(m => m.startsWith('photos/'));
  const hasSearch = metrics.some(m => m.startsWith('search_behavior/'));
  const healthUp = shifts.some(s => s.metric.startsWith('health/') && s.direction === 'up');
  const healthDown = shifts.some(s => s.metric.startsWith('health/') && s.direction === 'down');
  const socialUp = shifts.some(s => s.metric.startsWith('communication/') && s.direction === 'up');

  if (healthUp && !healthDown) return 'health_focus';
  if (healthDown && !healthUp) return 'low_activity';
  if (socialUp) return 'social_expansion';
  if (hasPhotos && hasSearch) return 'exploration';
  if (directions.every(d => d === 'down')) return 'transition';
  if (shifts.length >= 3) return 'life_change';
  return 'routine';
}

function generateChapterTitle(type: string, shifts: ChangePoint[]): string {
  const titles: Record<string, string> = {
    health_focus: 'Health Focus Period',
    low_activity: 'Low Activity Period',
    social_expansion: 'Social Surge',
    exploration: 'Exploration Phase',
    transition: 'Transition Period',
    life_change: 'Major Life Change',
    travel: 'Travel Phase',
    routine: 'Routine Period',
  };
  return titles[type] || 'Behavioral Phase';
}

function generateChapterDescription(shifts: ChangePoint[]): string {
  const parts = shifts.slice(0, 3).map(s => {
    const pct = s.oldAvg !== 0 ? Math.round(((s.newAvg - s.oldAvg) / Math.abs(s.oldAvg)) * 100) : 0;
    const metricName = s.metric.split('/')[1].replace(/_/g, ' ');
    return `${metricName} ${s.direction === 'up' ? 'increased' : 'decreased'} ${Math.abs(pct)}%`;
  });
  return parts.join(', ') + '.';
}

async function detectLifeChapters(
  pool: Pool, changePoints: ChangePoint[], force: boolean, jobId: string
): Promise<ChapterResult> {
  const result: ChapterResult = { detected: 0, skipped: 0, newChapterIds: [] };

  if (changePoints.length < 2) {
    logger.logVerbose('Chapter detection: not enough change points');
    return result;
  }

  // Sort by date
  const sorted = [...changePoints].sort((a, b) => a.weekStart.localeCompare(b.weekStart));

  // Group change points that occur within 2 weeks as a single chapter boundary
  const boundaries: { date: string; shifts: ChangePoint[] }[] = [];
  let current = { date: sorted[0].weekStart, shifts: [sorted[0]] };

  for (let i = 1; i < sorted.length; i++) {
    const diffDays = (new Date(sorted[i].weekStart).getTime() - new Date(current.date).getTime()) / 86400000;
    if (diffDays <= 14) {
      current.shifts.push(sorted[i]);
    } else {
      boundaries.push(current);
      current = { date: sorted[i].weekStart, shifts: [sorted[i]] };
    }
  }
  boundaries.push(current);

  // Create chapters between consecutive boundaries
  for (let i = 0; i < boundaries.length; i++) {
    const startDate = boundaries[i].date;
    const endDate = i + 1 < boundaries.length ? boundaries[i + 1].date : null;
    const shifts = boundaries[i].shifts;

    // Check for existing chapter overlapping this period
    const existing = await pool.query(
      `SELECT id FROM aurora_life_chapters WHERE start_date = $1 AND chapter_type = $2`,
      [startDate, classifyChapterType(shifts)]
    );

    if (existing.rows.length > 0 && !force) {
      result.skipped++;
      continue;
    }

    const chapterType = classifyChapterType(shifts);
    const title = generateChapterTitle(chapterType, shifts);
    const description = generateChapterDescription(shifts);
    const confidence = Math.min(0.95, 0.3 + shifts.length * 0.15 + shifts.reduce((s, p) => s + Math.abs(p.zScore), 0) * 0.05);

    const metricsSnapshot: Record<string, number> = {};
    for (const s of shifts) {
      metricsSnapshot[s.metric] = s.newAvg;
    }

    const { rows } = await pool.query(
      `INSERT INTO aurora_life_chapters (chapter_type, title, description, start_date, end_date, confidence, evidence, enrichment_sources, metrics_snapshot)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT DO NOTHING
       RETURNING id`,
      [chapterType, title, description, startDate, endDate,
       Math.round(confidence * 100) / 100,
       JSON.stringify({ change_points: shifts }),
       '{}',
       JSON.stringify(metricsSnapshot)]
    );

    if (rows.length > 0) {
      result.detected++;
      result.newChapterIds.push(rows[0].id);
      logger.log(`Chapter: "${title}" starting ${startDate} (${chapterType}, confidence ${Math.round(confidence * 100)}%)`);
    }
  }

  await jobLog(jobId, `Chapters: ${result.detected} new, ${result.skipped} existing`);
  return result;
}

// ── Phase 4: Chapter Enrichment ──────────────────────────────

interface EnrichResult { enriched: number }

async function enrichChapters(pool: Pool, chapterIds: number[], jobId: string): Promise<EnrichResult> {
  let enriched = 0;

  for (const chapterId of chapterIds) {
    const { rows } = await pool.query(
      `SELECT id, chapter_type, start_date, end_date, enrichment_sources FROM aurora_life_chapters WHERE id = $1`,
      [chapterId]
    );
    if (rows.length === 0) continue;

    const chapter = rows[0];
    const sources: string[] = chapter.enrichment_sources || [];
    const endDate = chapter.end_date || new Date().toISOString().split('T')[0];

    // Check for flights during the chapter
    const flights = await pool.query(
      `SELECT COUNT(*) as cnt, array_agg(DISTINCT destination) as destinations
       FROM aurora_raw_flights
       WHERE COALESCE(actual_departure, scheduled_departure)::date BETWEEN $1 AND $2`,
      [chapter.start_date, endDate]
    );
    if (parseInt(flights.rows[0]?.cnt || '0') > 0) {
      sources.push('flights');
      if (chapter.chapter_type !== 'travel') {
        await pool.query(
          `UPDATE aurora_life_chapters SET chapter_type = 'travel', title = $2 WHERE id = $1`,
          [chapterId, `Travel Phase (${flights.rows[0].cnt} flights)`]
        );
      }
    }

    // Check photo volume
    const photos = await pool.query(
      `SELECT COUNT(*) as cnt FROM photo_metadata
       WHERE creation_date::date BETWEEN $1 AND $2`,
      [chapter.start_date, endDate]
    );
    const daySpan = Math.max(1, (new Date(endDate).getTime() - new Date(chapter.start_date).getTime()) / 86400000);
    const photosPerDay = parseInt(photos.rows[0]?.cnt || '0') / daySpan;
    if (photosPerDay > 5) { // Arbitrary threshold for notable photo activity
      sources.push('photos');
    }

    // Social media engagement during chapter
    try {
      const social = await pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE data_type = 'post') as fb_posts,
          COUNT(*) FILTER (WHERE data_type = 'comment') as fb_comments
        FROM aurora_raw_facebook
        WHERE timestamp::date BETWEEN $1 AND $2
      `, [chapter.start_date, endDate]);
      const ig = await pool.query(`
        SELECT COUNT(*) FILTER (WHERE data_type = 'like') as ig_likes
        FROM aurora_raw_instagram WHERE timestamp::date BETWEEN $1 AND $2
      `, [chapter.start_date, endDate]);
      const socialTotal = parseInt(social.rows[0]?.fb_posts || '0') + parseInt(social.rows[0]?.fb_comments || '0') + parseInt(ig.rows[0]?.ig_likes || '0');
      if (socialTotal > 50) sources.push('social_media');
    } catch { /* skip */ }

    // Spending patterns from gmail insights
    try {
      const purchases = await pool.query(`
        SELECT COUNT(*) as cnt FROM aurora_raw_gmail_insights
        WHERE category = 'purchase' AND extracted_date::date BETWEEN $1 AND $2
      `, [chapter.start_date, endDate]);
      if (parseInt(purchases.rows[0]?.cnt || '0') > 10) sources.push('spending');
    } catch { /* skip */ }

    // ARIA meta-signals (journal entries, proactive insights during chapter)
    try {
      const meta = await pool.query(`
        SELECT
          (SELECT COUNT(*) FROM aria_journal WHERE created_at::date BETWEEN $1 AND $2) as journals,
          (SELECT COUNT(*) FROM proactive_insights WHERE created_at::date BETWEEN $1 AND $2) as insights
      `, [chapter.start_date, endDate]);
      if (parseInt(meta.rows[0]?.journals || '0') > 3 || parseInt(meta.rows[0]?.insights || '0') > 5) {
        sources.push('aria_meta');
      }
    } catch { /* skip */ }

    // Multi-signal chapter validation: boost confidence if multiple enrichment sources agree
    const signalCount = sources.length;
    let adjustedConfidence = chapter.confidence;
    if (signalCount >= 3) {
      adjustedConfidence = Math.min(0.98, chapter.confidence + 0.15);
    } else if (signalCount >= 2) {
      adjustedConfidence = Math.min(0.95, chapter.confidence + 0.08);
    }

    // Find similar past chapters
    const similar = await pool.query(
      `SELECT id FROM aurora_life_chapters
       WHERE chapter_type = $1 AND id != $2
       ORDER BY ABS(confidence - (SELECT confidence FROM aurora_life_chapters WHERE id = $2)) LIMIT 3`,
      [chapter.chapter_type, chapterId]
    );
    const similarIds = similar.rows.map((r: { id: number }) => r.id);

    // Update enrichment + adjusted confidence
    await pool.query(
      `UPDATE aurora_life_chapters SET enrichment_sources = $1, similar_past_chapters = $2, confidence = $3 WHERE id = $4`,
      [sources, similarIds, Math.round(adjustedConfidence * 100) / 100, chapterId]
    );

    if (sources.length > 0) enriched++;
  }

  if (enriched > 0) logger.log(`Enrichment: ${enriched} chapters enriched`);
  await jobLog(jobId, `Enrichment: ${enriched} of ${chapterIds.length} chapters enriched`);
  return { enriched };
}

// ── Phase 5: Relationship Intelligence (WS6) ────────────────

interface RelationshipResult { analyzed: number; updated: number; facetimeCalls: number }

/** Extract hour-of-day (0-23) from timestamp */
function hourOfDay(ts: Date): number { return ts.getUTCHours(); }

/** Extract day-of-week name from timestamp */
function dayOfWeek(ts: Date): string {
  return ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][ts.getUTCDay()];
}

/** Bucket hours into readable labels */
function hourBucket(h: number): string {
  if (h < 6) return 'night';
  if (h < 12) return 'morning';
  if (h < 17) return 'afternoon';
  if (h < 21) return 'evening';
  return 'night';
}

/** Find top N items from a frequency map */
function topN(counts: Map<string, number>, n: number): string[] {
  return [...counts.entries()]
    .sort(([, a], [, b]) => b - a)
    .slice(0, n)
    .map(([k]) => k);
}

export async function analyzeRelationships(pool: Pool, weekStart: string, jobId: string): Promise<RelationshipResult> {
  const stop = logger.time('relationships');
  const periodEnd = weekStart;
  const periodStart = new Date(new Date(weekStart).getTime() - 90 * 86400000).toISOString().split('T')[0];
  const now = new Date(weekStart);

  // Get all identities with linked communication identifiers
  const { rows: identities } = await pool.query(`
    SELECT si.id as identity_id, si.display_name, si.kg_entity_id,
      array_agg(DISTINCT l.identifier) as identifiers,
      array_agg(DISTINCT l.platform) as platforms
    FROM aurora_social_identities si
    JOIN aurora_social_identity_links l ON l.identity_id = si.id
    WHERE si.display_name NOT IN ('Anonymous') -- Skip self
      AND si.is_person IS NOT FALSE -- Skip known non-person entries
    GROUP BY si.id, si.display_name, si.kg_entity_id
    HAVING COUNT(l.*) >= 1
  `);

  let analyzed = 0;
  let updated = 0;
  let totalFacetimeCalls = 0;

  logger.log(`Relationship analysis: ${identities.length} identities to process, period ${periodStart} to ${periodEnd}`);
  await jobLog(jobId, `Relationships: ${identities.length} identities, period ${periodStart}–${periodEnd}`);

  for (const identity of identities) {
    const identifiers = identity.identifiers as string[];
    if (identifiers.length === 0) continue;

    // Build placeholder list for IN clause
    const placeholders = identifiers.map((_, i) => `$${i + 3}`).join(',');

    // ── Cross-channel communication (90-day window) ──
    const commResult = await pool.query(`
      SELECT
        channel,
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE direction = 'sent') as sent,
        COUNT(*) FILTER (WHERE direction = 'received') as received,
        MIN(timestamp) as first_seen,
        MAX(timestamp) as last_seen,
        AVG(content_length) as avg_length
      FROM aurora_unified_communication
      WHERE contact_identifier IN (${placeholders})
        AND timestamp::date BETWEEN $1 AND $2
      GROUP BY channel
    `, [periodStart, periodEnd, ...identifiers]);

    // Also fetch per-message timestamps for hour/day analysis and response time
    const msgTimestamps = await pool.query(`
      SELECT timestamp, direction, thread_id, channel
      FROM aurora_unified_communication
      WHERE contact_identifier IN (${placeholders})
        AND timestamp::date BETWEEN $1 AND $2
      ORDER BY timestamp
    `, [periodStart, periodEnd, ...identifiers]);

    // ── Aggregate across channels ──
    let totalInteractions = 0;
    let totalSent = 0;
    let totalReceived = 0;
    let totalLength = 0;
    let lengthCount = 0;
    const channelActivity: Record<string, { total: number; sent: number; received: number }> = {};
    const channelsActive: string[] = [];
    let firstDate: Date | null = null;
    let lastDate: Date | null = null;

    for (const ch of commResult.rows) {
      const total = parseInt(ch.total);
      const sent = parseInt(ch.sent);
      const received = parseInt(ch.received);
      totalInteractions += total;
      totalSent += sent;
      totalReceived += received;
      totalLength += parseFloat(ch.avg_length || '0') * total;
      lengthCount += total;
      channelActivity[ch.channel] = { total, sent, received };
      channelsActive.push(ch.channel);

      const fs = new Date(ch.first_seen);
      const ls = new Date(ch.last_seen);
      if (!firstDate || fs < firstDate) firstDate = fs;
      if (!lastDate || ls > lastDate) lastDate = ls;
    }

    // Skip identities with no communication at all (calls-only handled below)
    const hasComm = commResult.rows.length > 0;

    // ── Voice/call data from aurora_unified_calls ──
    const callResult = await pool.query(`
      SELECT channel, COUNT(*) as calls, COALESCE(SUM(duration_seconds), 0) as duration,
        MIN(timestamp) as first_seen, MAX(timestamp) as last_seen
      FROM aurora_unified_calls
      WHERE contact_identifier IN (${placeholders})
        AND timestamp::date BETWEEN $1 AND $2
      GROUP BY channel
    `, [periodStart, periodEnd, ...identifiers]);

    for (const cr of callResult.rows) {
      const callCount = parseInt(cr.calls);
      if (callCount > 0) {
        totalInteractions += callCount;
        const chKey = cr.channel === 'plaud' ? 'plaud' : 'voice';
        channelActivity[chKey] = {
          total: (channelActivity[chKey]?.total || 0) + callCount,
          sent: channelActivity[chKey]?.sent || 0,
          received: (channelActivity[chKey]?.received || 0) + callCount,
        };
        if (!channelsActive.includes(chKey)) channelsActive.push(chKey);

        const cfs = new Date(cr.first_seen);
        const cls = new Date(cr.last_seen);
        if (!firstDate || cfs < firstDate) firstDate = cfs;
        if (!lastDate || cls > lastDate) lastDate = cls;
      }
    }

    // ── FaceTime calls from aurora_raw_facetime ──
    const facetimeResult = await pool.query(`
      SELECT COUNT(*) as calls, COALESCE(SUM(duration_seconds), 0) as duration,
        MIN(date_created) as first_seen, MAX(date_created) as last_seen
      FROM aurora_raw_facetime
      WHERE (from_handle = ANY($3::text[]) OR recipient = ANY($3::text[]))
        AND date_created::date BETWEEN $1 AND $2
    `, [periodStart, periodEnd, identifiers]);
    const ftCalls = parseInt(facetimeResult.rows[0]?.calls || '0');
    if (ftCalls > 0) {
      totalInteractions += ftCalls;
      totalFacetimeCalls += ftCalls;
      channelActivity['facetime'] = { total: ftCalls, sent: 0, received: ftCalls };
      if (!channelsActive.includes('facetime')) channelsActive.push('facetime');

      const ftFirst = new Date(facetimeResult.rows[0].first_seen);
      const ftLast = new Date(facetimeResult.rows[0].last_seen);
      if (!firstDate || ftFirst < firstDate) firstDate = ftFirst;
      if (!lastDate || ftLast > lastDate) lastDate = ftLast;
    }

    // Skip if no interactions at all across any channel
    if (totalInteractions === 0) continue;

    // ── Contact hour/day analysis ──
    const hourCounts = new Map<string, number>();
    const dayCounts = new Map<string, number>();
    for (const msg of msgTimestamps.rows) {
      const ts = new Date(msg.timestamp);
      const bucket = hourBucket(hourOfDay(ts));
      hourCounts.set(bucket, (hourCounts.get(bucket) || 0) + 1);
      const day = dayOfWeek(ts);
      dayCounts.set(day, (dayCounts.get(day) || 0) + 1);
    }
    const primaryContactHours = topN(hourCounts, 2);
    const primaryContactDays = topN(dayCounts, 3);

    // ── Response time estimation ──
    // Approximate by looking at sent→received and received→sent transitions within threads
    let responseTimeSum = 0;
    let responseTimeCount = 0;
    const threadMessages = new Map<string, { ts: Date; dir: string }[]>();
    for (const msg of msgTimestamps.rows) {
      const tid = msg.thread_id || msg.channel; // fall back to channel if no thread
      if (!threadMessages.has(tid)) threadMessages.set(tid, []);
      threadMessages.get(tid)!.push({ ts: new Date(msg.timestamp), dir: msg.direction });
    }
    for (const [, msgs] of threadMessages) {
      for (let i = 1; i < msgs.length; i++) {
        // Direction change = a response
        if (msgs[i].dir !== msgs[i - 1].dir) {
          const gap = (msgs[i].ts.getTime() - msgs[i - 1].ts.getTime()) / 1000; // seconds
          // Only count reasonable response times (< 24h, > 5s)
          if (gap > 5 && gap < 86400) {
            responseTimeSum += gap;
            responseTimeCount++;
          }
        }
      }
    }
    const avgResponseTimeSec = responseTimeCount > 0
      ? Math.round(responseTimeSum / responseTimeCount) : null;

    // ── Reciprocity (1.0 = balanced, <0.3 = one-sided) ──
    const reciprocity = (totalSent + totalReceived > 0)
      ? Math.round(Math.min(totalSent, totalReceived) / Math.max(totalSent, totalReceived, 1) * 100) / 100
      : 0;

    // ── Conversation depth (0-1, based on avg message length) ──
    const avgMsgLen = lengthCount > 0 ? totalLength / lengthCount : 0;
    const depthScore = Math.min(1, Math.round((avgMsgLen / 200) * 100) / 100);

    // ── Primary channel (highest volume) ──
    const primaryChannel = Object.entries(channelActivity)
      .sort(([, a], [, b]) => b.total - a.total)[0]?.[0] || null;

    // ── In-person ratio (Plaud/voice/facetime vs digital) ──
    const inPersonCount = (channelActivity['voice']?.total || 0) +
      (channelActivity['plaud']?.total || 0) + (channelActivity['facetime']?.total || 0);
    const inPersonRatio = totalInteractions > 0
      ? Math.round((inPersonCount / totalInteractions) * 100) / 100 : 0;

    // ── Frequency trend (compare last 30d rate vs prior 60d rate) ──
    const thirtyDaysAgo = new Date(now.getTime() - 30 * 86400000).toISOString().split('T')[0];
    const recentResult = await pool.query(`
      SELECT COUNT(*) as cnt FROM aurora_unified_communication
      WHERE contact_identifier IN (${placeholders}) AND timestamp::date BETWEEN $1 AND $2
    `, [thirtyDaysAgo, periodEnd, ...identifiers]);
    const olderResult = await pool.query(`
      SELECT COUNT(*) as cnt FROM aurora_unified_communication
      WHERE contact_identifier IN (${placeholders}) AND timestamp::date BETWEEN $1 AND $2
    `, [periodStart, thirtyDaysAgo, ...identifiers]);
    const recentCount = parseInt(recentResult.rows[0]?.cnt || '0');
    const olderCount = parseInt(olderResult.rows[0]?.cnt || '0');
    // Normalize to daily rate (30d recent vs 60d prior)
    const recentDailyRate = recentCount / 30;
    const olderDailyRate = olderCount / 60;
    let frequencyTrend: 'increasing' | 'stable' | 'decreasing' | 'silent';
    if (recentCount === 0 && olderCount === 0) frequencyTrend = 'silent';
    else if (olderDailyRate === 0) frequencyTrend = 'increasing';
    else if (recentDailyRate > olderDailyRate * 1.3) frequencyTrend = 'increasing';
    else if (recentDailyRate < olderDailyRate * 0.7) frequencyTrend = 'decreasing';
    else frequencyTrend = 'stable';

    // ── Recency: days since last contact ──
    const daysSinceLastContact = lastDate
      ? Math.round((now.getTime() - lastDate.getTime()) / 86400000) : 999;

    // ── Tier classification ──
    const weeklyRate = totalInteractions / 13; // 90 days ~= 13 weeks
    const channelCount = channelsActive.length;
    let tier: string;
    if (daysSinceLastContact > 30) {
      // Override: no contact in 30+ days = dormant regardless of historical volume
      tier = 'dormant';
    } else if (weeklyRate >= 10 && channelCount >= 3) {
      tier = 'inner_circle';
    } else if (weeklyRate >= 5 || (weeklyRate >= 2 && channelCount >= 2)) {
      tier = 'close';
    } else if (weeklyRate >= 1) {
      tier = 'regular';
    } else if (totalInteractions >= 3) {
      tier = 'acquaintance';
    } else {
      tier = 'dormant';
    }

    // ── Relationship type inference from channel mix ──
    const hasWorkChannels = channelsActive.includes('google_chat') || channelsActive.includes('gmail');
    const hasPersonalChannels = channelsActive.includes('imessage') ||
      channelsActive.includes('instagram') || channelsActive.includes('facebook') ||
      channelsActive.includes('facetime');
    let relType = 'unknown';
    if (hasWorkChannels && !hasPersonalChannels) relType = 'colleague';
    else if (hasPersonalChannels && !hasWorkChannels) relType = 'friend';
    else if (hasWorkChannels && hasPersonalChannels) relType = 'friend';

    // ── Health score (composite 0-1) ──
    const freqScore = Math.min(1, weeklyRate / 10);
    const recencyScore = daysSinceLastContact <= 7 ? 1.0 :
      daysSinceLastContact <= 14 ? 0.8 : daysSinceLastContact <= 30 ? 0.5 :
      daysSinceLastContact <= 60 ? 0.2 : 0;
    const responseScore = avgResponseTimeSec != null
      ? Math.min(1, Math.max(0, 1 - (avgResponseTimeSec / 7200))) // <2h = good
      : 0.5; // neutral if unknown
    const trendBonus = frequencyTrend === 'increasing' ? 0.1 :
      frequencyTrend === 'stable' ? 0.05 : 0;

    const healthScore = Math.round(
      (freqScore * 0.25 + reciprocity * 0.2 + depthScore * 0.15 +
       recencyScore * 0.15 + (channelCount / 5) * 0.1 +
       responseScore * 0.05 + trendBonus)
      * 100) / 100;

    const durationDays = firstDate && lastDate
      ? Math.round((lastDate.getTime() - firstDate.getTime()) / 86400000) : 0;

    // ── Tier confidence (higher with more channels + data points) ──
    const tierConfidence = Math.min(0.95,
      0.4 + channelCount * 0.1 + Math.min(0.2, totalInteractions / 200));

    // ── Entity ID from knowledge graph (if identity has one) ──
    const entityId = identity.kg_entity_id || null;

    // ── Upsert relationship ──
    await pool.query(`
      INSERT INTO aurora_relationships (
        identity_id, contact_name, entity_id, relationship_tier, relationship_type,
        tier_confidence, period_start, period_end, total_interactions,
        channel_activity, channels_active, primary_channel,
        primary_contact_hours, primary_contact_days,
        conversation_depth_score, reciprocity_score, in_person_ratio,
        frequency_trend, relationship_health_score,
        first_interaction_date, last_interaction_date,
        relationship_duration_days, updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,NOW())
      ON CONFLICT (identity_id, period_start) DO UPDATE SET
        contact_name = EXCLUDED.contact_name,
        entity_id = EXCLUDED.entity_id,
        relationship_tier = EXCLUDED.relationship_tier,
        relationship_type = EXCLUDED.relationship_type,
        tier_confidence = EXCLUDED.tier_confidence,
        total_interactions = EXCLUDED.total_interactions,
        channel_activity = EXCLUDED.channel_activity,
        channels_active = EXCLUDED.channels_active,
        primary_channel = EXCLUDED.primary_channel,
        primary_contact_hours = EXCLUDED.primary_contact_hours,
        primary_contact_days = EXCLUDED.primary_contact_days,
        conversation_depth_score = EXCLUDED.conversation_depth_score,
        reciprocity_score = EXCLUDED.reciprocity_score,
        in_person_ratio = EXCLUDED.in_person_ratio,
        frequency_trend = EXCLUDED.frequency_trend,
        relationship_health_score = EXCLUDED.relationship_health_score,
        last_interaction_date = EXCLUDED.last_interaction_date,
        relationship_duration_days = EXCLUDED.relationship_duration_days,
        updated_at = NOW()
    `, [
      identity.identity_id, identity.display_name, entityId,
      tier, relType, tierConfidence,
      periodStart, periodEnd, totalInteractions,
      JSON.stringify(channelActivity), channelsActive, primaryChannel,
      primaryContactHours.length > 0 ? primaryContactHours : null,
      primaryContactDays.length > 0 ? primaryContactDays : null,
      depthScore, reciprocity, inPersonRatio,
      frequencyTrend, healthScore,
      firstDate?.toISOString().split('T')[0],
      lastDate?.toISOString().split('T')[0],
      durationDays,
    ]);

    // Update identity tier
    await pool.query(
      `UPDATE aurora_social_identities SET tier = $1, relationship_type = $2, updated_at = NOW() WHERE id = $3`,
      [tier, relType, identity.identity_id]
    );

    analyzed++;
    if (tier !== 'dormant') updated++;
  }

  stop();
  logger.log(`Relationships: ${analyzed} analyzed, ${updated} active (non-dormant), ${totalFacetimeCalls} FaceTime calls`);
  await jobLog(jobId, `Relationships: ${analyzed} analyzed, ${updated} active, ${totalFacetimeCalls} FaceTime calls`);
  return { analyzed, updated, facetimeCalls: totalFacetimeCalls };
}

// ── Main handler ─────────────────────────────────────────────

export async function handleAuroraWeekly(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload || {};
  const jobStart = Date.now();

  const weekStart = (payload.week_start as string) || (() => {
    const d = new Date();
    d.setDate(d.getDate() - d.getDay());
    return d.toISOString().split('T')[0];
  })();
  const force = !!payload.force;

  logger.logVerbose('Entering handleAuroraWeekly', { weekStart, force });
  logger.log(`Weekly synthesis for week starting ${weekStart}`);
  await jobLog(job.id, `Processing week: ${weekStart}`);

  // Phase 1: Trend vectors
  let trendResult = { written: 0, skipped: 0 };
  try {
    const trendStop = logger.time('trend-vectors');
    trendResult = await computeTrendVectors(pool, weekStart, job.id);
    trendStop();
  } catch (err) {
    logger.logMinimal(`Trend computation failed: ${(err as Error).message}`);
  }

  // Phase 2: Change point detection
  let changePoints: ChangePoint[] = [];
  try {
    const cpStop = logger.time('change-points');
    changePoints = await detectChangePoints(pool, weekStart, force, job.id);
    cpStop();
  } catch (err) {
    logger.logMinimal(`Change point detection failed: ${(err as Error).message}`);
  }

  // Phase 3: Life chapter detection
  let chapterResult: ChapterResult = { detected: 0, skipped: 0, newChapterIds: [] };
  try {
    const chStop = logger.time('chapter-detection');
    chapterResult = await detectLifeChapters(pool, changePoints, force, job.id);
    chStop();
  } catch (err) {
    logger.logMinimal(`Chapter detection failed: ${(err as Error).message}`);
  }

  // Phase 4: Chapter enrichment
  let enrichResult: EnrichResult = { enriched: 0 };
  try {
    if (chapterResult.newChapterIds.length > 0) {
      const enStop = logger.time('chapter-enrichment');
      enrichResult = await enrichChapters(pool, chapterResult.newChapterIds, job.id);
      enStop();
    }
  } catch (err) {
    logger.logMinimal(`Chapter enrichment failed: ${(err as Error).message}`);
  }

  // Phase 5: Relationship intelligence
  let relResult: RelationshipResult = { analyzed: 0, updated: 0, facetimeCalls: 0 };
  try {
    const relStop = logger.time('relationships');
    relResult = await analyzeRelationships(pool, weekStart, job.id);
    relStop();
  } catch (err) {
    logger.logMinimal(`Relationship analysis failed: ${(err as Error).message}`);
    await jobLog(job.id, `ERROR in relationships: ${(err as Error).message}`);
  }

  // Phase 6: Relationship decay detection + KG bridging
  let decayResult = { decaying: 0, alerted: 0, kgFacts: 0 };
  try {
    const decayStop = logger.time('decay-detection');
    decayResult = await detectRelationshipDecay(pool, weekStart, job.id);
    decayStop();
  } catch (err) {
    logger.logMinimal(`Decay detection failed: ${(err as Error).message}`);
  }

  // Phase 7: Bridge Aurora findings into Knowledge Graph
  let kgBridgeResult = { written: 0 };
  try {
    const kgStop = logger.time('kg-bridge');
    kgBridgeResult = await bridgeAuroraToKG(pool, weekStart, job.id);
    kgStop();
  } catch (err) {
    logger.logMinimal(`KG bridge failed: ${(err as Error).message}`);
  }

  const elapsedMs = Date.now() - jobStart;
  logger.log(`Weekly complete: ${trendResult.written} trends, ${changePoints.length} change points, ${chapterResult.detected} chapters, ${relResult.analyzed} relationships, ${decayResult.decaying} decaying, ${kgBridgeResult.written} KG facts (${elapsedMs}ms)`);
  await jobLog(job.id, `Complete: ${trendResult.written} trends, ${chapterResult.detected} chapters, ${relResult.analyzed} relationships, ${decayResult.decaying} decaying, ${decayResult.alerted} alerts, ${kgBridgeResult.written} KG facts, ${elapsedMs}ms`);

  return {
    week_start: weekStart,
    trends_written: trendResult.written,
    trends_skipped: trendResult.skipped,
    change_points: changePoints.length,
    chapters_detected: chapterResult.detected,
    chapters_skipped: chapterResult.skipped,
    chapters_enriched: enrichResult.enriched,
    relationships_analyzed: relResult.analyzed,
    relationships_active: relResult.updated,
    relationships_facetime_calls: relResult.facetimeCalls,
    relationships_decaying: decayResult.decaying,
    decay_alerts: decayResult.alerted,
    kg_facts_bridged: kgBridgeResult.written,
    elapsed_ms: elapsedMs,
  };
}

// ── Phase 6: Relationship Decay Detection ──────────────────────

interface DecayResult { decaying: number; alerted: number; kgFacts: number }

export async function detectRelationshipDecay(
  pool: Pool, weekStart: string, jobId: string,
): Promise<DecayResult> {
  // Find contacts in close/regular tiers whose frequency is declining
  // Compare current period against previous period
  const { rows: decaying } = await pool.query<{
    contact_name: string;
    relationship_tier: string;
    frequency_trend: string;
    relationship_health_score: number;
    total_interactions: number;
    last_interaction_date: string;
    identity_id: number;
  }>(`
    SELECT r.contact_name, r.relationship_tier, r.frequency_trend,
           r.relationship_health_score, r.total_interactions,
           r.last_interaction_date::text, r.identity_id
    FROM aurora_relationships r
    JOIN aurora_social_identities si ON si.id = r.identity_id
    WHERE r.period_start = (SELECT MAX(period_start) FROM aurora_relationships)
      AND si.is_person = TRUE
      AND r.relationship_tier IN ('inner_circle', 'close', 'regular')
      AND (r.frequency_trend = 'decreasing' OR r.frequency_trend = 'silent')
    ORDER BY r.relationship_health_score ASC
  `);

  let alerted = 0;

  if (decaying.length > 0) {
    // Build decay alert for ARIA
    const lines = decaying.slice(0, 10).map(d => {
      const days = d.last_interaction_date
        ? Math.round((Date.now() - new Date(d.last_interaction_date).getTime()) / 86400000)
        : 999;
      return `• ${d.contact_name} (${d.relationship_tier}) — ${d.frequency_trend}, health ${d.relationship_health_score}, last contact ${days}d ago`;
    });

    const alertMsg = `[Relationship Decay Alert] ${decaying.length} close/regular contact(s) showing declining engagement:\n${lines.join('\n')}\n\nConsider reaching out to maintain these relationships.`;

    await pool.query(
      `INSERT INTO agent_inbox (to_agent, from_agent, message, priority)
       VALUES ('aria', 'system', $1, 1)`,
      [alertMsg],
    );
    alerted = 1;

    logger.log(`Decay alert: ${decaying.length} contacts declining, alert sent to ARIA`);
    await jobLog(jobId, `Decay: ${decaying.length} contacts declining — ${decaying.slice(0, 5).map(d => d.contact_name).join(', ')}`);
  }

  return { decaying: decaying.length, alerted, kgFacts: 0 };
}

// ── Phase 7: Bridge Aurora Findings into Knowledge Graph ───────

interface KGBridgeResult { written: number }

async function bridgeAuroraToKG(
  pool: Pool, weekStart: string, jobId: string,
): Promise<KGBridgeResult> {
  let written = 0;

  // Bridge significant trends into KG as behavioral facts
  const { rows: trends } = await pool.query<{
    metric: string;
    period: string;
    direction: string;
    velocity: number;
    change_pct: number;
    significance: number;
  }>(`
    SELECT metric, period, direction, velocity, change_pct, significance
    FROM aurora_trends
    WHERE calculated_at >= $1::date
      AND significance > 0.3
      AND direction != 'stable'
    ORDER BY significance DESC
    LIMIT 10
  `, [weekStart]);

  for (const trend of trends) {
    const key = `trend_${trend.metric}_${trend.period}_${weekStart}`;
    const value = `${trend.metric} is ${trend.direction} over ${trend.period} (${trend.change_pct > 0 ? '+' : ''}${trend.change_pct.toFixed(1)}%, significance ${trend.significance.toFixed(2)})`;

    await pool.query(`
      INSERT INTO knowledge_facts (domain, category, key, value, confidence, sources, valid_from)
      VALUES ('behavioral', 'trend', $1, $2, $3, ARRAY['aurora_weekly'], $4::date)
      ON CONFLICT DO NOTHING
    `, [key, value, Math.min(0.9, trend.significance), weekStart]);
    written++;
  }

  // Bridge top correlations
  const { rows: correlations } = await pool.query<{
    stream_a: string;
    stream_b: string;
    coefficient: number;
    direction: string;
  }>(`
    SELECT stream_a, stream_b, coefficient, direction
    FROM aurora_correlations
    WHERE last_validated >= $1::date
      AND ABS(coefficient) > 0.5
    ORDER BY ABS(coefficient) DESC
    LIMIT 5
  `, [weekStart]);

  for (const corr of correlations) {
    const key = `correlation_${corr.stream_a}_${corr.stream_b}_${weekStart}`;
    const value = `${corr.stream_a} and ${corr.stream_b} are ${corr.direction}ly correlated (r=${corr.coefficient.toFixed(2)})`;

    await pool.query(`
      INSERT INTO knowledge_facts (domain, category, key, value, confidence, sources, valid_from)
      VALUES ('behavioral', 'correlation', $1, $2, $3, ARRAY['aurora_weekly'], $4::date)
      ON CONFLICT DO NOTHING
    `, [key, value, Math.min(0.9, Math.abs(corr.coefficient)), weekStart]);
    written++;
  }

  if (written > 0) {
    logger.log(`Bridged ${written} Aurora findings to KG (${trends.length} trends, ${correlations.length} correlations)`);
    await jobLog(jobId, `KG bridge: ${written} behavioral facts written`);
  }

  return { written };
}
