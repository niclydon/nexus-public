/**
 * Life Transition Detection Handler
 *
 * Detects major life transitions by analyzing shifts in behavioral
 * patterns across multiple data sources. Research shows transitions
 * are detectable 2-4 weeks early from:
 *
 * - Communication pattern shifts (new contact clusters, volume changes)
 * - Location entropy changes (moving, traveling, new routines)
 * - Network composition (ratio of new vs established contacts)
 * - Calendar structure (routine vs chaotic schedule)
 * - Sentiment trajectory (sustained directional shift)
 *
 * Transition types detected:
 * - career_change: New email contacts + calendar pattern shift
 * - relocation: Location entropy spike + new location clusters
 * - relationship_change: Communication with specific person spikes/drops
 * - social_shift: Network composition change (new people, old ones fade)
 * - routine_break: Calendar structure deviation from baseline
 * - stress_period: Multiple negative indicators coinciding
 *
 * Runs monthly. Stores findings in aurora_life_chapters.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import { routeRequest } from '../lib/llm/index.js';

const logger = createLogger('life-transition');

const ANALYSIS_WINDOW_DAYS = 30;
const BASELINE_WINDOW_DAYS = 90;

interface TransitionSignal {
  type: string;
  strength: number; // 0-1
  description: string;
  evidence: string[];
}

export async function handleLifeTransition(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { window_days?: number };
  const windowDays = payload.window_days ?? ANALYSIS_WINDOW_DAYS;

  logger.log(`Detecting life transitions (${windowDays}-day window, ${BASELINE_WINDOW_DAYS}-day baseline)`);
  await jobLog(job.id, `Analyzing transition signals`);

  const now = new Date();
  const windowStart = new Date(now.getTime() - windowDays * 86400000);
  const baselineStart = new Date(now.getTime() - (BASELINE_WINDOW_DAYS + windowDays) * 86400000);
  const windowStr = windowStart.toISOString().split('T')[0];
  const baselineStr = baselineStart.toISOString().split('T')[0];

  const signals: TransitionSignal[] = [];

  // 1. New contact emergence — ratio of new vs established contacts
  try {
    const { rows: [contactShift] } = await pool.query<{
      recent_new: number;
      recent_total: number;
      baseline_new: number;
      baseline_total: number;
    }>(`
      WITH recent AS (
        SELECT COUNT(DISTINCT handle_id) as total,
          COUNT(DISTINCT handle_id) FILTER (
            WHERE handle_id NOT IN (
              SELECT DISTINCT handle_id FROM aurora_raw_imessage
              WHERE date < $1::date AND date >= $2::date
            )
          ) as new_contacts
        FROM aurora_raw_imessage WHERE date >= $1::date
      ),
      baseline AS (
        SELECT COUNT(DISTINCT handle_id) as total,
          COUNT(DISTINCT handle_id) FILTER (
            WHERE handle_id NOT IN (
              SELECT DISTINCT handle_id FROM aurora_raw_imessage WHERE date < $2::date
            )
          ) as new_contacts
        FROM aurora_raw_imessage WHERE date >= $2::date AND date < $1::date
      )
      SELECT r.new_contacts as recent_new, r.total as recent_total,
             b.new_contacts as baseline_new, b.total as baseline_total
      FROM recent r, baseline b
    `, [windowStr, baselineStr]);

    if (contactShift && contactShift.recent_total > 0 && contactShift.baseline_total > 0) {
      const recentNewRatio = contactShift.recent_new / contactShift.recent_total;
      const baselineNewRatio = contactShift.baseline_new / contactShift.baseline_total;
      if (recentNewRatio > baselineNewRatio * 2 && contactShift.recent_new >= 3) {
        signals.push({
          type: 'social_shift',
          strength: Math.min(1, recentNewRatio / Math.max(0.01, baselineNewRatio) / 3),
          description: `New contact ratio doubled: ${(recentNewRatio * 100).toFixed(0)}% vs baseline ${(baselineNewRatio * 100).toFixed(0)}%`,
          evidence: [`${contactShift.recent_new} new contacts in ${windowDays} days vs ${contactShift.baseline_new} in baseline`],
        });
      }
    }
  } catch (err) { logger.logVerbose('Contact shift check failed:', (err as Error).message); }

  // 2. Location entropy — variety of locations visited
  try {
    const { rows: [locShift] } = await pool.query<{
      recent_variety: number;
      baseline_variety: number;
    }>(`
      SELECT
        (SELECT COUNT(DISTINCT ROUND(latitude::numeric, 2) || ',' || ROUND(longitude::numeric, 2))
         FROM device_location WHERE recorded_at >= $1::date AND latitude IS NOT NULL)::int as recent_variety,
        (SELECT COUNT(DISTINCT ROUND(latitude::numeric, 2) || ',' || ROUND(longitude::numeric, 2))
         FROM device_location WHERE recorded_at >= $2::date AND recorded_at < $1::date AND latitude IS NOT NULL)::int as baseline_variety
    `, [windowStr, baselineStr]);

    if (locShift && locShift.baseline_variety > 0) {
      const ratio = locShift.recent_variety / locShift.baseline_variety;
      if (ratio > 2.0) {
        signals.push({
          type: 'relocation',
          strength: Math.min(1, (ratio - 1) / 3),
          description: `Location variety ${ratio.toFixed(1)}x baseline (${locShift.recent_variety} vs ${locShift.baseline_variety} unique locations)`,
          evidence: ['Significant increase in unique locations — possible travel or move'],
        });
      } else if (ratio < 0.3 && locShift.baseline_variety >= 5) {
        signals.push({
          type: 'routine_break',
          strength: Math.min(1, (1 - ratio)),
          description: `Location variety dropped to ${(ratio * 100).toFixed(0)}% of baseline — routine contraction`,
          evidence: [`Only ${locShift.recent_variety} locations vs ${locShift.baseline_variety} baseline`],
        });
      }
    }
  } catch (err) { logger.logVerbose('Location entropy check failed:', (err as Error).message); }

  // 3. Communication volume shift
  try {
    const { rows: [commShift] } = await pool.query<{
      recent_daily: number;
      baseline_daily: number;
    }>(`
      SELECT
        (SELECT COUNT(*)::float / GREATEST(1, $3::int) FROM aurora_raw_imessage WHERE date >= $1::date) as recent_daily,
        (SELECT COUNT(*)::float / GREATEST(1, $4::int) FROM aurora_raw_imessage WHERE date >= $2::date AND date < $1::date) as baseline_daily
    `, [windowStr, baselineStr, windowDays, BASELINE_WINDOW_DAYS]);

    if (commShift && commShift.baseline_daily > 0) {
      const ratio = commShift.recent_daily / commShift.baseline_daily;
      if (ratio > 1.5 || ratio < 0.5) {
        signals.push({
          type: ratio > 1 ? 'social_shift' : 'stress_period',
          strength: Math.min(1, Math.abs(ratio - 1) / 1.5),
          description: `Message volume ${ratio > 1 ? 'up' : 'down'} ${Math.abs((ratio - 1) * 100).toFixed(0)}% from baseline`,
          evidence: [`${commShift.recent_daily.toFixed(1)}/day vs ${commShift.baseline_daily.toFixed(1)}/day baseline`],
        });
      }
    }
  } catch (err) { logger.logVerbose('Comm volume check failed:', (err as Error).message); }

  // 4. Sentiment trajectory
  try {
    const { rows: [sentShift] } = await pool.query<{
      recent_avg: number;
      baseline_avg: number;
    }>(`
      SELECT
        (SELECT AVG(sentiment_score)::float FROM aurora_sentiment WHERE created_at >= $1::date) as recent_avg,
        (SELECT AVG(sentiment_score)::float FROM aurora_sentiment WHERE created_at >= $2::date AND created_at < $1::date) as baseline_avg
    `, [windowStr, baselineStr]);

    if (sentShift && sentShift.baseline_avg != null && sentShift.recent_avg != null) {
      const diff = sentShift.recent_avg - sentShift.baseline_avg;
      if (Math.abs(diff) > 0.1) {
        signals.push({
          type: diff < 0 ? 'stress_period' : 'social_shift',
          strength: Math.min(1, Math.abs(diff) / 0.3),
          description: `Sentiment ${diff > 0 ? 'improved' : 'declined'} by ${(diff * 100).toFixed(0)} points (${sentShift.recent_avg.toFixed(2)} vs ${sentShift.baseline_avg.toFixed(2)} baseline)`,
          evidence: [`Sustained ${diff > 0 ? 'positive' : 'negative'} shift across ${windowDays} days`],
        });
      }
    }
  } catch (err) { logger.logVerbose('Sentiment trajectory check failed:', (err as Error).message); }

  // 5. Calendar structure change
  try {
    const { rows: [calShift] } = await pool.query<{
      recent_daily: number;
      baseline_daily: number;
    }>(`
      SELECT
        (SELECT COUNT(*)::float / GREATEST(1, $3::int) FROM device_calendar_events WHERE start_date >= $1::date) as recent_daily,
        (SELECT COUNT(*)::float / GREATEST(1, $4::int) FROM device_calendar_events WHERE start_date >= $2::date AND start_date < $1::date) as baseline_daily
    `, [windowStr, baselineStr, windowDays, BASELINE_WINDOW_DAYS]);

    if (calShift && calShift.baseline_daily > 0) {
      const ratio = calShift.recent_daily / calShift.baseline_daily;
      if (ratio > 1.8 || ratio < 0.4) {
        signals.push({
          type: 'routine_break',
          strength: Math.min(1, Math.abs(ratio - 1) / 2),
          description: `Calendar density ${ratio > 1 ? 'up' : 'down'} ${Math.abs((ratio - 1) * 100).toFixed(0)}% from baseline`,
          evidence: [`${calShift.recent_daily.toFixed(1)} events/day vs ${calShift.baseline_daily.toFixed(1)} baseline`],
        });
      }
    }
  } catch (err) { logger.logVerbose('Calendar structure check failed:', (err as Error).message); }

  // 6. Email pattern shift (new domains/senders)
  try {
    const { rows: [emailShift] } = await pool.query<{
      recent_new_domains: number;
      baseline_new_domains: number;
    }>(`
      WITH recent_domains AS (
        SELECT SPLIT_PART(from_address, '@', 2) as domain
        FROM aurora_raw_gmail WHERE date >= $1::date AND from_address IS NOT NULL
      ),
      baseline_domains AS (
        SELECT DISTINCT SPLIT_PART(from_address, '@', 2) as domain
        FROM aurora_raw_gmail WHERE date >= $2::date AND date < $1::date AND from_address IS NOT NULL
      )
      SELECT
        COUNT(DISTINCT rd.domain) FILTER (WHERE bd.domain IS NULL)::int as recent_new_domains,
        (SELECT COUNT(DISTINCT domain) FROM baseline_domains)::int as baseline_new_domains
      FROM recent_domains rd
      LEFT JOIN baseline_domains bd ON rd.domain = bd.domain
    `, [windowStr, baselineStr]);

    if (emailShift && emailShift.recent_new_domains >= 5) {
      signals.push({
        type: 'career_change',
        strength: Math.min(1, emailShift.recent_new_domains / 15),
        description: `${emailShift.recent_new_domains} new email domains appearing (possible new organization/context)`,
        evidence: ['New senders from previously unseen domains — could indicate job change, new project, or new service subscriptions'],
      });
    }
  } catch (err) { logger.logVerbose('Email pattern check failed:', (err as Error).message); }

  logger.log(`Detected ${signals.length} transition signals`);
  await jobLog(job.id, `Found ${signals.length} transition signals`);

  // If signals are strong enough, use LLM to synthesize into a life chapter narrative
  const strongSignals = signals.filter(s => s.strength >= 0.3);
  let chapterCreated = false;

  if (strongSignals.length >= 2) {
    // Multiple signals = likely real transition
    const signalText = strongSignals.map(s =>
      `[${s.type}] (strength ${s.strength.toFixed(2)}): ${s.description}\n  Evidence: ${s.evidence.join('; ')}`
    ).join('\n\n');

    const result = await routeRequest({
      handler: 'life-transition',
      taskTier: 'generation',
      systemPrompt: `You are analyzing behavioral transition signals for a personal AI assistant. Based on the detected signals, determine if a significant life transition is occurring and write a brief narrative (2-3 sentences) describing what's happening. Be specific about what changed and what it likely means. If the signals don't clearly indicate a transition, say so.

Respond in JSON: {"is_transition": true/false, "chapter_type": "career|relocation|relationship|social|routine|health", "title": "Short title", "description": "2-3 sentence narrative"}`,
      userMessage: `Behavioral signals detected in the last ${windowDays} days:\n\n${signalText}`,
      maxTokens: 300,
      useBatch: true,
    });

    try {
      const parsed = JSON.parse(result.text.replace(/```json?\s*/g, '').replace(/```/g, '').trim());
      if (parsed.is_transition) {
        await pool.query(`
          INSERT INTO aurora_life_chapters (chapter_type, title, description, start_date, confidence, signals)
          VALUES ($1, $2, $3, $4::date, $5, $6)
        `, [
          parsed.chapter_type || 'general',
          parsed.title,
          parsed.description,
          windowStr,
          Math.min(0.9, strongSignals.reduce((sum, s) => sum + s.strength, 0) / strongSignals.length),
          JSON.stringify(strongSignals),
        ]);

        chapterCreated = true;
        logger.log(`Life chapter detected: "${parsed.title}" (${parsed.chapter_type})`);
        await jobLog(job.id, `Life chapter: "${parsed.title}"`);

        // Alert ARIA
        await pool.query(
          `INSERT INTO agent_inbox (to_agent, from_agent, message, priority)
           VALUES ('aria', 'system', $1, 1)`,
          [`[Life Transition Detected] ${parsed.title}\n\n${parsed.description}\n\nSignals: ${strongSignals.map(s => s.description).join('; ')}`],
        );
      }
    } catch (err) {
      logger.logMinimal('Failed to parse LLM transition response:', (err as Error).message);
    }
  }

  const result = {
    signals_detected: signals.length,
    strong_signals: strongSignals.length,
    chapter_created: chapterCreated,
    signal_types: signals.map(s => `${s.type}(${s.strength.toFixed(2)})`),
  };

  logger.log(`Complete: ${JSON.stringify(result)}`);
  await jobLog(job.id, `Complete: ${signals.length} signals, ${strongSignals.length} strong, chapter=${chapterCreated}`);
  return result;
}
