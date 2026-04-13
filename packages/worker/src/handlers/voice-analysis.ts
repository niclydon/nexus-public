/**
 * Voice Analysis Handler
 *
 * Computes per-person speaking metrics from voice-print diarization data
 * in the aria database and upserts results into person_voice_metrics (nexus).
 *
 * Metrics: segment count, total speaking time, avg segment duration,
 * conversation count, dominance ratio, most active hour, voice confidence.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import pg from 'pg';

const logger = createLogger('voice-analysis');

function getAriaPool(): pg.Pool {
  const nexusUrl = process.env.DATABASE_URL ?? '';
  const ariaUrl = process.env.VOICE_DB_URL ?? nexusUrl.replace(/\/nexus\b/, '/aria');
  return new pg.Pool({ connectionString: ariaUrl, max: 3 });
}

interface PersonMetrics {
  person_id: number;
  person_name: string;
  total_segments: number;
  total_speaking_seconds: number;
  avg_segment_duration: number;
  conversations_count: number;
  voice_confidence_avg: number | null;
}

interface DominanceRow {
  person_id: number;
  media_file_id: number;
  person_seconds: number;
  total_seconds: number;
}

interface ActiveHourRow {
  person_id: number;
  hour: number;
  segment_count: number;
}

export async function handleVoiceAnalysis(job: TempoJob): Promise<Record<string, unknown>> {
  const nexusPool = getPool();
  const ariaPool = getAriaPool();

  try {
    logger.log('Starting voice analysis');
    await jobLog(job.id, 'Connecting to aria database for diarization data');

    // 1. Aggregate per-person basic metrics
    const { rows: personMetrics } = await ariaPool.query<PersonMetrics>(`
      SELECT
        ds.person_id,
        p.name AS person_name,
        COUNT(*)::INTEGER AS total_segments,
        COALESCE(SUM(ds.end_time - ds.start_time), 0)::REAL AS total_speaking_seconds,
        COALESCE(AVG(ds.end_time - ds.start_time), 0)::REAL AS avg_segment_duration,
        COUNT(DISTINCT ds.media_file_id)::INTEGER AS conversations_count,
        AVG(ds.voice_confidence)::REAL AS voice_confidence_avg
      FROM diarization_segments ds
      JOIN persons p ON p.id = ds.person_id
      WHERE ds.person_id IS NOT NULL
      GROUP BY ds.person_id, p.name
      ORDER BY total_speaking_seconds DESC
    `);

    if (personMetrics.length === 0) {
      logger.log('No persons with diarization segments found');
      await jobLog(job.id, 'No diarization data found');
      return { persons_processed: 0 };
    }

    logger.log(`Found ${personMetrics.length} persons with diarization data`);
    await jobLog(job.id, `Computing metrics for ${personMetrics.length} persons`);

    // 2. Compute per-person per-media dominance ratios
    const { rows: dominanceRows } = await ariaPool.query<DominanceRow>(`
      WITH per_person_media AS (
        SELECT
          ds.person_id,
          ds.media_file_id,
          SUM(ds.end_time - ds.start_time) AS person_seconds
        FROM diarization_segments ds
        WHERE ds.person_id IS NOT NULL
        GROUP BY ds.person_id, ds.media_file_id
      ),
      per_media_total AS (
        SELECT
          media_file_id,
          SUM(end_time - start_time) AS total_seconds
        FROM diarization_segments
        WHERE person_id IS NOT NULL
        GROUP BY media_file_id
      )
      SELECT
        ppm.person_id,
        ppm.media_file_id,
        ppm.person_seconds::REAL AS person_seconds,
        pmt.total_seconds::REAL AS total_seconds
      FROM per_person_media ppm
      JOIN per_media_total pmt ON pmt.media_file_id = ppm.media_file_id
      WHERE pmt.total_seconds > 0
    `);

    // Average dominance ratio per person
    const dominanceMap = new Map<number, number[]>();
    for (const row of dominanceRows) {
      const ratio = row.person_seconds / row.total_seconds;
      const existing = dominanceMap.get(row.person_id);
      if (existing) {
        existing.push(ratio);
      } else {
        dominanceMap.set(row.person_id, [ratio]);
      }
    }

    const avgDominance = new Map<number, number>();
    for (const [personId, ratios] of dominanceMap) {
      const sum = ratios.reduce((a, b) => a + b, 0);
      avgDominance.set(personId, sum / ratios.length);
    }

    // 3. Most active hour per person (hour with most segments)
    const { rows: activeHourRows } = await ariaPool.query<ActiveHourRow>(`
      WITH hourly AS (
        SELECT
          ds.person_id,
          EXTRACT(HOUR FROM mf.created_at)::INTEGER AS hour,
          COUNT(*)::INTEGER AS segment_count
        FROM diarization_segments ds
        JOIN media_files mf ON mf.id = ds.media_file_id
        WHERE ds.person_id IS NOT NULL
          AND mf.created_at IS NOT NULL
        GROUP BY ds.person_id, EXTRACT(HOUR FROM mf.created_at)
      )
      SELECT DISTINCT ON (person_id)
        person_id, hour, segment_count
      FROM hourly
      ORDER BY person_id, segment_count DESC
    `);

    const activeHourMap = new Map<number, number>();
    for (const row of activeHourRows) {
      activeHourMap.set(row.person_id, row.hour);
    }

    // 4. Map voice-print person names to aurora_social_identities
    const { rows: nexusPersons } = await nexusPool.query<{
      id: number;
      display_name: string;
    }>(`SELECT id, display_name FROM aurora_social_identities WHERE is_person = TRUE`);

    const nameToNexusId = new Map<string, number>();
    for (const p of nexusPersons) {
      nameToNexusId.set(p.display_name.toLowerCase(), p.id);
    }

    // 5. Upsert into person_voice_metrics
    let upserted = 0;
    let skipped = 0;

    for (const pm of personMetrics) {
      const nexusId = nameToNexusId.get(pm.person_name.toLowerCase());
      if (!nexusId) {
        logger.logVerbose(`No nexus person match for "${pm.person_name}", using voice-print ID ${pm.person_id}`);
        skipped++;
        continue;
      }

      const dominance = avgDominance.get(pm.person_id) ?? null;
      const activeHour = activeHourMap.get(pm.person_id) ?? null;

      await nexusPool.query(
        `INSERT INTO person_voice_metrics
          (person_id, contact_name, total_segments, total_speaking_seconds,
           avg_segment_duration, conversations_count, avg_dominance_ratio,
           most_active_hour, voice_confidence_avg, computed_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
         ON CONFLICT (person_id) DO UPDATE SET
           contact_name = EXCLUDED.contact_name,
           total_segments = EXCLUDED.total_segments,
           total_speaking_seconds = EXCLUDED.total_speaking_seconds,
           avg_segment_duration = EXCLUDED.avg_segment_duration,
           conversations_count = EXCLUDED.conversations_count,
           avg_dominance_ratio = EXCLUDED.avg_dominance_ratio,
           most_active_hour = EXCLUDED.most_active_hour,
           voice_confidence_avg = EXCLUDED.voice_confidence_avg,
           computed_at = NOW()`,
        [
          nexusId,
          pm.person_name,
          pm.total_segments,
          pm.total_speaking_seconds,
          pm.avg_segment_duration,
          pm.conversations_count,
          dominance,
          activeHour,
          pm.voice_confidence_avg,
        ],
      );
      upserted++;
    }

    const result = {
      persons_found: personMetrics.length,
      upserted,
      skipped_no_match: skipped,
    };

    logger.log(`Complete: ${JSON.stringify(result)}`);
    await jobLog(job.id, `Voice analysis complete: ${upserted} upserted, ${skipped} skipped (no name match)`);
    return result;

  } finally {
    await ariaPool.end();
  }
}
