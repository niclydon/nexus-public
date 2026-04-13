/**
 * Photo Social Analysis Handler
 *
 * Analyzes co-occurrence patterns in photos — which people appear together,
 * how often, where, and over what date range. Resolves person names from
 * photo_metadata.people[] to canonical IDs in aurora_social_identities.
 *
 * Processes BATCH_SIZE photos per job, self-chains if more remain.
 * Results upserted into person_co_occurrence table.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('photo-social-analysis');

const BATCH_SIZE = 5000;

interface PhotoRow {
  id: string;
  people: string[];
  location_name: string | null;
  taken_at: Date | null;
}

interface CoOccurrence {
  count: number;
  locations: Set<string>;
  firstSeen: Date | null;
  lastSeen: Date | null;
}

export async function handlePhotoSocialAnalysis(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { batch_size?: number; offset?: number };
  const batchSize = payload.batch_size ?? BATCH_SIZE;
  const offset = payload.offset ?? 0;

  logger.log(`Starting photo social analysis (offset=${offset}, batch=${batchSize})`);
  await jobLog(job.id, `Analyzing photo co-occurrence (offset=${offset}, batch=${batchSize})`);

  // Ensure the target table exists
  await pool.query(`
    CREATE TABLE IF NOT EXISTS person_co_occurrence (
      person_a_id INTEGER NOT NULL,
      person_b_id INTEGER NOT NULL,
      co_occurrence_count INTEGER DEFAULT 0,
      common_locations TEXT[],
      first_seen DATE,
      last_seen DATE,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (person_a_id, person_b_id)
    )
  `);
  logger.logVerbose('Ensured person_co_occurrence table exists');

  // Fetch photos with 2+ people
  const { rows: photos } = await pool.query<PhotoRow>(`
    SELECT id, people, location_name, taken_at
    FROM photo_metadata
    WHERE people IS NOT NULL AND array_length(people, 1) >= 2
    ORDER BY id
    LIMIT $1 OFFSET $2
  `, [batchSize + 1, offset]); // fetch one extra to detect if more remain

  const hasMore = photos.length > batchSize;
  const batch = hasMore ? photos.slice(0, batchSize) : photos;

  if (batch.length === 0) {
    logger.log('No photos with 2+ people found at this offset');
    return { status: 'complete', processed: 0, pairs_upserted: 0 };
  }

  logger.log(`Fetched ${batch.length} photos with 2+ people`);

  // Build identity lookup: display_name (lowered) → id
  const { rows: identities } = await pool.query<{ id: number; display_name: string }>(`
    SELECT id, display_name FROM aurora_social_identities WHERE display_name IS NOT NULL
  `);

  const nameToId = new Map<string, number>();
  for (const ident of identities) {
    nameToId.set(ident.display_name.toLowerCase(), ident.id);
  }
  logger.logVerbose(`Loaded ${nameToId.size} social identities for name resolution`);

  // Accumulate co-occurrence pairs
  // Key: "minId:maxId" to ensure consistent ordering
  const pairs = new Map<string, CoOccurrence>();
  let unresolvedNames = 0;
  let photosProcessed = 0;

  for (const photo of batch) {
    // Resolve each person name to an ID
    const resolvedIds: number[] = [];
    for (const name of photo.people) {
      const id = nameToId.get(name.toLowerCase());
      if (id != null) {
        resolvedIds.push(id);
      } else {
        unresolvedNames++;
      }
    }

    // Deduplicate IDs within the same photo
    const uniqueIds = [...new Set(resolvedIds)];
    if (uniqueIds.length < 2) continue;

    photosProcessed++;

    // Generate all pairs
    for (let i = 0; i < uniqueIds.length; i++) {
      for (let j = i + 1; j < uniqueIds.length; j++) {
        const a = Math.min(uniqueIds[i], uniqueIds[j]);
        const b = Math.max(uniqueIds[i], uniqueIds[j]);
        const key = `${a}:${b}`;

        let pair = pairs.get(key);
        if (!pair) {
          pair = { count: 0, locations: new Set(), firstSeen: null, lastSeen: null };
          pairs.set(key, pair);
        }

        pair.count++;

        if (photo.location_name) {
          pair.locations.add(photo.location_name);
        }

        if (photo.taken_at) {
          if (!pair.firstSeen || photo.taken_at < pair.firstSeen) {
            pair.firstSeen = photo.taken_at;
          }
          if (!pair.lastSeen || photo.taken_at > pair.lastSeen) {
            pair.lastSeen = photo.taken_at;
          }
        }
      }
    }
  }

  logger.log(`Found ${pairs.size} unique pairs from ${photosProcessed} photos (${unresolvedNames} unresolved name occurrences)`);

  // Upsert all pairs
  let upserted = 0;
  for (const [key, data] of pairs) {
    const [aStr, bStr] = key.split(':');
    const personA = parseInt(aStr, 10);
    const personB = parseInt(bStr, 10);
    const locationsArr = [...data.locations];

    await pool.query(`
      INSERT INTO person_co_occurrence (person_a_id, person_b_id, co_occurrence_count, common_locations, first_seen, last_seen, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, NOW())
      ON CONFLICT (person_a_id, person_b_id) DO UPDATE SET
        co_occurrence_count = person_co_occurrence.co_occurrence_count + EXCLUDED.co_occurrence_count,
        common_locations = (
          SELECT ARRAY(SELECT DISTINCT unnest FROM unnest(
            array_cat(person_co_occurrence.common_locations, EXCLUDED.common_locations)
          ))
        ),
        first_seen = LEAST(person_co_occurrence.first_seen, EXCLUDED.first_seen),
        last_seen = GREATEST(person_co_occurrence.last_seen, EXCLUDED.last_seen),
        updated_at = NOW()
    `, [personA, personB, data.count, locationsArr, data.firstSeen, data.lastSeen]);
    upserted++;
  }

  logger.log(`Upserted ${upserted} co-occurrence pairs`);

  // Self-chain if more photos remain
  if (hasMore) {
    const { rows: existing } = await pool.query(
      `SELECT id FROM tempo_jobs WHERE job_type = 'photo-social-analysis' AND status = 'pending' LIMIT 1`,
    );
    if (existing.length === 0) {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
         VALUES ('photo-social-analysis', $1, 'pending', 0, 3)`,
        [JSON.stringify({
          batch_size: batchSize,
          offset: offset + batchSize,
          triggered_by: 'self-chain',
        })],
      );
      logger.logVerbose(`Self-chained next batch at offset ${offset + batchSize}`);
    } else {
      logger.logVerbose('Skipped self-chain — pending job already exists');
    }
  }

  await jobLog(job.id, `Processed ${photosProcessed} photos → ${upserted} pairs upserted (${hasMore ? 'more remain' : 'complete'})`);

  return {
    status: hasMore ? 'chunk_complete' : 'complete',
    photos_in_batch: batch.length,
    photos_with_resolved_pairs: photosProcessed,
    pairs_upserted: upserted,
    unresolved_name_occurrences: unresolvedNames,
    has_more: hasMore,
  };
}
