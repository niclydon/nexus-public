/**
 * Photo History Analyze handler.
 *
 * Processes described photos from photo_metadata to extract personal knowledge
 * about the owner: places visited, trips, life events, activities, and interests.
 * Uses a watermark to track progress, processing photos in batches.
 *
 * Photos are clustered by day and location proximity (~50km) before analysis.
 * Extracted facts are written to owner_profile via upsertProfileFacts.
 * If more photos remain, auto-enqueues another job to continue.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';
import { upsertProfileFacts, type ProfileFact } from '../lib/owner-profile.js';

const logger = createLogger('photo-history');

const WATERMARK_SOURCE = 'photo_history_analysis';
const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_MIN_YEAR = 1980;
const CLUSTER_RADIUS_KM = 50;

interface PhotoRow {
  id: number;
  local_identifier: string;
  taken_at: Date;
  latitude: number | null;
  longitude: number | null;
  pixel_width: number | null;
  pixel_height: number | null;
  media_type: string | null;
  description: string;
  source: string | null;
}

interface PhotoCluster {
  date: string; // YYYY-MM-DD
  lat: number | null;
  lon: number | null;
  photos: PhotoRow[];
}

interface ExtractedFact {
  domain: string;
  category: string;
  key: string;
  value: string;
  confidence: number;
  valid_from?: string;
  metadata?: Record<string, unknown>;
}

interface AnalysisResult {
  facts: ExtractedFact[];
}

const SYSTEM_PROMPT = `You are ARIA, a personal AI assistant analyzing photo metadata from your owner the owner's personal photo library. You are given descriptions, dates, and GPS coordinates of photos grouped by day and location.

Your task is to extract meaningful personal facts about the owner's life from these photos. Focus on:

1. **places/visited** -- Places the owner has visited. Include city, country, and type of place (restaurant, beach, mountain, museum, park, etc.)
2. **events/travel** -- Trips and vacations (multi-day travel to a destination)
3. **events/life** -- Life events visible in photos (birthdays, weddings, graduations, holidays, moving, etc.)
4. **events/social** -- Social gatherings, parties, dinners with friends/family
5. **interests/activities** -- Activities and hobbies shown (hiking, cooking, photography, sports, gaming, etc.)
6. **interests/food** -- Food and dining preferences
7. **people/relationships** -- People who appear frequently, their relationship to the owner if inferable (e.g. "the owner frequently photographs with [Person]" not facts about [Person] himself)
8. **lifestyle/home** -- Home, living situation, pets visible in photos

CRITICAL — IDENTITY DISAMBIGUATION:
- These are photos from the owner's photo library. You may extract facts about both the owner AND people important to him.
- Always make the subject of each fact explicit. If a fact is about another person, name them clearly.
- For the "people/relationships" domain, include both the owner's relationship to the person AND notable facts about them (e.g. "[Person] is the owner's close friend" or "[Person] visited Paris in March 2024").
- Do NOT attribute other people's activities or attributes to the owner. If someone else is the subject, say so explicitly.

RULES:
- Only extract facts that are clearly supported by the photo descriptions. Do NOT fabricate.
- Use specific, descriptive keys like "paris_france_mar2019" not generic ones like "trip1".
- Values should be concise but informative statements.
- Confidence should reflect how certain you are (0.5 = somewhat likely, 0.8 = quite confident, 0.95 = very clear).
- Include valid_from as ISO date (YYYY-MM-DD) when the date is known from photo timestamps.
- Include lat/lon in metadata when GPS coordinates are available.
- Prefer fewer high-quality facts over many vague ones.
- For travel, try to identify the destination and approximate duration from the date range of photos.
- If photos are mundane (screenshots, random objects) with no extractable personal info, return empty facts array.

Respond with a JSON object (no markdown fences):
{
  "facts": [
    {
      "domain": "places",
      "category": "visited",
      "key": "paris_france_mar2019",
      "value": "Visited Paris, France in March 2019. Photos show Eiffel Tower, Louvre Museum, and local cafes.",
      "confidence": 0.9,
      "valid_from": "2019-03-15",
      "metadata": {"lat": 48.8566, "lon": 2.3522}
    }
  ]
}`;

/**
 * Get the current watermark (last analyzed photo ID).
 */
async function getWatermark(): Promise<{ lastId: number; totalProcessed: number }> {
  const pool = getPool();
  try {
    const { rows } = await pool.query<{ metadata: Record<string, unknown> }>(
      `SELECT metadata FROM context_watermarks WHERE source = $1`,
      [WATERMARK_SOURCE]
    );
    if (rows.length > 0 && rows[0].metadata) {
      return {
        lastId: Number(rows[0].metadata.last_id) || 0,
        totalProcessed: Number(rows[0].metadata.total_processed) || 0,
      };
    }
    return { lastId: 0, totalProcessed: 0 };
  } catch {
    return { lastId: 0, totalProcessed: 0 };
  }
}

/**
 * Update the watermark to track progress.
 */
async function updateWatermark(lastId: number, totalProcessed: number): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO context_watermarks (source, last_checked_at, last_change_detected_at, metadata)
     VALUES ($1, NOW(), NOW(), $2)
     ON CONFLICT (source) DO UPDATE SET
       last_checked_at = NOW(),
       last_change_detected_at = NOW(),
       metadata = $2`,
    [WATERMARK_SOURCE, JSON.stringify({ last_id: lastId, total_processed: totalProcessed })]
  );
}

/**
 * Fetch described photos not yet analyzed.
 */
async function fetchPhotos(afterId: number, batchSize: number, minYear: number): Promise<PhotoRow[]> {
  const pool = getPool();
  const { rows } = await pool.query<PhotoRow>(
    `SELECT id, local_identifier, taken_at, latitude, longitude, pixel_width, pixel_height,
            media_type, description, source
     FROM photo_metadata
     WHERE id > $1
       AND description IS NOT NULL
       AND taken_at >= $2
     ORDER BY id ASC
     LIMIT $3`,
    [afterId, `${minYear}-01-01`, batchSize]
  );
  return rows;
}

/**
 * Calculate distance between two GPS coordinates in km (Haversine formula).
 */
function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

/**
 * Cluster photos by day and location proximity.
 * Photos on the same day within ~50km are grouped together.
 */
function clusterPhotos(photos: PhotoRow[]): PhotoCluster[] {
  const clusters: PhotoCluster[] = [];

  for (const photo of photos) {
    const date = new Date(photo.taken_at).toISOString().slice(0, 10);
    const lat = photo.latitude;
    const lon = photo.longitude;

    // Try to find an existing cluster for this day + location
    let matched = false;
    for (const cluster of clusters) {
      if (cluster.date !== date) continue;

      // If neither has GPS, group by day only
      if (lat == null && cluster.lat == null) {
        cluster.photos.push(photo);
        matched = true;
        break;
      }

      // If one has GPS and the other doesn't, group by day
      if (lat == null || cluster.lat == null) {
        cluster.photos.push(photo);
        // Update cluster location if this photo has one
        if (lat != null && cluster.lat == null) {
          cluster.lat = lat;
          cluster.lon = lon;
        }
        matched = true;
        break;
      }

      // Both have GPS -- check distance
      const dist = haversineKm(lat!, lon!, cluster.lat, cluster.lon!);
      if (dist <= CLUSTER_RADIUS_KM) {
        cluster.photos.push(photo);
        matched = true;
        break;
      }
    }

    if (!matched) {
      clusters.push({
        date,
        lat,
        lon,
        photos: [photo],
      });
    }
  }

  return clusters;
}

/**
 * Format a cluster for LLM analysis.
 */
function formatCluster(cluster: PhotoCluster): string {
  const lines: string[] = [];
  lines.push(`Date: ${cluster.date}`);
  if (cluster.lat != null && cluster.lon != null) {
    lines.push(`Location: ${cluster.lat.toFixed(4)}, ${cluster.lon.toFixed(4)}`);
  }
  lines.push(`Photos (${cluster.photos.length}):`);
  lines.push('---');

  for (const photo of cluster.photos) {
    const time = new Date(photo.taken_at).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
    const parts: string[] = [`[${time}]`];
    if (photo.media_type && photo.media_type !== 'image') {
      parts.push(`(${photo.media_type})`);
    }
    parts.push(photo.description);
    if (photo.latitude != null && photo.longitude != null) {
      parts.push(`[GPS: ${photo.latitude.toFixed(4)}, ${photo.longitude.toFixed(4)}]`);
    }
    lines.push(parts.join(' '));
  }

  return lines.join('\n');
}

/**
 * Merge small clusters on the same day to reduce LLM calls.
 * Groups clusters from the same day into batches for analysis.
 */
function mergeClustersForAnalysis(clusters: PhotoCluster[]): PhotoCluster[][] {
  const byDate = new Map<string, PhotoCluster[]>();
  for (const cluster of clusters) {
    if (!byDate.has(cluster.date)) {
      byDate.set(cluster.date, []);
    }
    byDate.get(cluster.date)!.push(cluster);
  }

  const batches: PhotoCluster[][] = [];
  let currentBatch: PhotoCluster[] = [];
  let currentPhotoCount = 0;

  for (const [, dayClusters] of byDate) {
    const dayPhotoCount = dayClusters.reduce((sum, c) => sum + c.photos.length, 0);

    // If adding this day would exceed ~30 photos per LLM call, start a new batch
    if (currentPhotoCount > 0 && currentPhotoCount + dayPhotoCount > 30) {
      batches.push(currentBatch);
      currentBatch = [];
      currentPhotoCount = 0;
    }

    currentBatch.push(...dayClusters);
    currentPhotoCount += dayPhotoCount;
  }

  if (currentBatch.length > 0) {
    batches.push(currentBatch);
  }

  return batches;
}

/**
 * Analyze a batch of clusters and extract profile facts.
 */
async function analyzeBatch(clusters: PhotoCluster[]): Promise<ExtractedFact[]> {
  const formatted = clusters.map(formatCluster).join('\n\n========\n\n');
  const totalPhotos = clusters.reduce((sum, c) => sum + c.photos.length, 0);

  // Skip batches with very sparse descriptions
  const totalDescLength = clusters.reduce(
    (sum, c) => sum + c.photos.reduce((s, p) => s + p.description.length, 0),
    0
  );
  if (totalDescLength < 30) {
    logger.log(`Skipping batch: descriptions too short (${totalDescLength} chars)`);
    return [];
  }

  try {
    const result = await routeRequest({
      handler: 'photo-history-analyze',
      taskTier: 'generation',
      systemPrompt: SYSTEM_PROMPT,
      userMessage: `Analyze these photo clusters and extract meaningful personal facts:\n\n${formatted}`,
      maxTokens: 4096,
    });

    logger.log(
      `Analyzed ${totalPhotos} photos across ${clusters.length} cluster(s) via ${result.model} (${result.provider}, ${result.estimatedCostCents}c)`
    );

    // Parse response -- handle markdown fences if present
    let text = result.text.trim();
    if (text.startsWith('```')) {
      text = text.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
    }

    const parsed: AnalysisResult = JSON.parse(text);
    return Array.isArray(parsed.facts) ? parsed.facts : [];
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal(`Failed to analyze batch: ${msg}`);
    return [];
  }
}

/**
 * Main handler.
 */
export async function handlePhotoHistoryAnalyze(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = (job.payload || {}) as {
    batch_size?: number;
    min_year?: number;
  };
  const batchSize = payload.batch_size ?? DEFAULT_BATCH_SIZE;
  const minYear = payload.min_year ?? DEFAULT_MIN_YEAR;

  logger.log(`Starting job ${job.id} (batch_size=${batchSize}, min_year=${minYear})`);

  // 1. Get current watermark
  const { lastId, totalProcessed } = await getWatermark();
  logger.log(`Watermark: last_id=${lastId}, total_processed=${totalProcessed}`);

  // 2. Fetch described photos not yet analyzed
  const photos = await fetchPhotos(lastId, batchSize, minYear);

  if (photos.length === 0) {
    logger.log('No new described photos to analyze');
    return { analyzed: 0, facts_extracted: 0, facts_written: 0 };
  }

  logger.log(
    `Fetched ${photos.length} photo(s) (ids ${photos[0].id}-${photos[photos.length - 1].id})`
  );

  // 3. Cluster photos by day and location proximity
  const clusters = clusterPhotos(photos);
  logger.log(`${clusters.length} cluster(s) from ${photos.length} photos`);

  // 4. Merge clusters into analysis batches
  const batches = mergeClustersForAnalysis(clusters);
  logger.log(`${batches.length} analysis batch(es)`);

  // 5. Analyze each batch
  const allFacts: ExtractedFact[] = [];

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    logger.log(
      `Analyzing batch ${i + 1}/${batches.length} (${batch.length} clusters, ${batch.reduce((s, c) => s + c.photos.length, 0)} photos)`
    );
    const facts = await analyzeBatch(batch);
    allFacts.push(...facts);
  }

  logger.log(`Total facts extracted: ${allFacts.length}`);

  // 6. Write facts to owner_profile
  let written = 0;
  if (allFacts.length > 0) {
    const profileFacts: ProfileFact[] = allFacts.map((f) => ({
      domain: f.domain,
      category: f.category,
      key: f.key,
      value: f.value,
      confidence: f.confidence,
      source: 'photos',
      validFrom: f.valid_from,
      metadata: f.metadata,
    }));

    written = await upsertProfileFacts(profileFacts);
    logger.log(`Wrote ${written}/${allFacts.length} facts to owner_profile`);
  }

  // 7. Update watermark
  const maxId = photos[photos.length - 1].id;
  const newTotal = totalProcessed + photos.length;
  await updateWatermark(maxId, newTotal);

  // 8. Check if more photos remain and auto-enqueue
  const pool = getPool();
  const { rows: remaining } = await pool.query<{ count: string }>(
    `SELECT COUNT(*) as count FROM photo_metadata
     WHERE id > $1 AND description IS NOT NULL AND taken_at >= $2`,
    [maxId, `${minYear}-01-01`]
  );
  const remainingCount = parseInt(remaining[0]?.count ?? '0', 10);

  if (remainingCount > 0) {
    logger.log(`${remainingCount} more photo(s) to process, enqueuing continuation job`);
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, max_attempts)
       VALUES ($1, $2, 1)`,
      ['photo-history-analyze', JSON.stringify({ batch_size: batchSize, min_year: minYear })]
    );
  }

  // 9. Log event
  logEvent({
    action: `Photo history analysis: processed ${photos.length} photos, extracted ${allFacts.length} facts, wrote ${written} to owner_profile`,
    component: 'photos',
    category: 'self_maintenance',
    metadata: {
      photos_analyzed: photos.length,
      clusters: clusters.length,
      batches: batches.length,
      facts_extracted: allFacts.length,
      facts_written: written,
      remaining: remainingCount,
      watermark_id: maxId,
      total_processed: newTotal,
    },
  });

  logger.log(
    `Complete: ${photos.length} photos, ${allFacts.length} facts, ${written} written, ${remainingCount} remaining (${newTotal} total processed)`
  );

  return {
    photos_analyzed: photos.length,
    clusters: clusters.length,
    facts_extracted: allFacts.length,
    facts_written: written,
    remaining: remainingCount,
    watermark_id: maxId,
    total_processed: newTotal,
  };
}
