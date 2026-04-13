/**
 * Reverse Geocode — batch resolves GPS coordinates to location names.
 *
 * Groups photos by rounded coordinates (~1km precision), geocodes each
 * unique location once, then updates all matching photos.
 * Uses Nominatim (OpenStreetMap) — free, 1 req/sec rate limit.
 *
 * Processes BATCH_SIZE unique locations per job, self-chains if more remain.
 */
import { getPool, createLogger } from '@nexus/core';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/reverse-geocode');

const BATCH_SIZE = 100; // unique locations per job
const RATE_LIMIT_MS = 1100; // Nominatim: 1 req/sec
const NOMINATIM_URL = 'https://nominatim.openstreetmap.org/reverse';

export async function handleReverseGeocode(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Get unique ungeooded locations (rounded to 2 decimal places ~1km)
  const { rows: locations } = await pool.query<{
    lat_round: number;
    lon_round: number;
    photo_count: number;
  }>(
    `SELECT
      ROUND(latitude::numeric, 2)::float as lat_round,
      ROUND(longitude::numeric, 2)::float as lon_round,
      COUNT(*) as photo_count
     FROM photo_metadata
     WHERE latitude IS NOT NULL AND location_name IS NULL
     GROUP BY lat_round, lon_round
     ORDER BY photo_count DESC
     LIMIT $1`,
    [BATCH_SIZE],
  );

  if (locations.length === 0) {
    logger.log('No more locations to geocode');
    return { status: 'complete', remaining: 0 };
  }

  logger.log(`Geocoding ${locations.length} unique locations (covering ${locations.reduce((s, l) => s + Number(l.photo_count), 0)} photos)`);

  let geocoded = 0;
  let photosUpdated = 0;
  let errors = 0;

  for (const loc of locations) {
    try {
      const locationName = await nominatimReverse(loc.lat_round, loc.lon_round);

      if (locationName) {
        // Update all photos near this location
        const result = await pool.query(
          `UPDATE photo_metadata
           SET location_name = $1
           WHERE latitude IS NOT NULL AND location_name IS NULL
             AND ROUND(latitude::numeric, 2) = $2
             AND ROUND(longitude::numeric, 2) = $3`,
          [locationName, loc.lat_round, loc.lon_round],
        );
        photosUpdated += result.rowCount ?? 0;
        geocoded++;

        if (geocoded % 20 === 0) {
          logger.logVerbose(`Progress: ${geocoded}/${locations.length} locations, ${photosUpdated} photos updated`);
        }
      } else {
        // Mark as "Unknown location" so we don't retry forever
        await pool.query(
          `UPDATE photo_metadata
           SET location_name = 'Unknown location'
           WHERE latitude IS NOT NULL AND location_name IS NULL
             AND ROUND(latitude::numeric, 2) = $1
             AND ROUND(longitude::numeric, 2) = $2`,
          [loc.lat_round, loc.lon_round],
        );
        errors++;
      }

      // Rate limit
      await new Promise(r => setTimeout(r, RATE_LIMIT_MS));
    } catch (err) {
      logger.logVerbose(`Geocode error for (${loc.lat_round}, ${loc.lon_round}):`, (err as Error).message);
      errors++;
      await new Promise(r => setTimeout(r, RATE_LIMIT_MS));
    }
  }

  // Check remaining
  const { rows: remaining } = await pool.query<{ count: string }>(
    `SELECT COUNT(DISTINCT (ROUND(latitude::numeric, 2), ROUND(longitude::numeric, 2))) as count
     FROM photo_metadata WHERE latitude IS NOT NULL AND location_name IS NULL`,
  );
  const remainCount = parseInt(remaining[0].count, 10);

  // Self-chain if more to process
  if (remainCount > 0) {
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
       VALUES ('reverse-geocode', '{}', 'nexus', 0, 3, NOW() + INTERVAL '5 seconds')`,
    );
    logger.log(`Geocoded ${geocoded} locations (${photosUpdated} photos), ${errors} errors, ${remainCount} remaining — chaining`);
  } else {
    logger.log(`Geocoding complete: ${geocoded} locations, ${photosUpdated} photos updated, ${errors} errors`);
  }

  // Regenerate descriptions for newly geocoded photos
  if (photosUpdated > 0) {
    await pool.query(
      `UPDATE photo_metadata
       SET description =
         CASE
           WHEN array_length(people, 1) > 0 THEN
             CASE WHEN media_type = 'video' THEN 'Video' ELSE 'Photo' END
             || ' with ' || array_to_string(people, ' and ')
             || ' in ' || location_name
             || CASE WHEN taken_at IS NOT NULL THEN ' on ' || to_char(taken_at, 'Month DD, YYYY') ELSE '' END
             || '.'
           ELSE
             CASE WHEN media_type = 'video' THEN 'Video' ELSE 'Photo' END
             || ' in ' || location_name
             || CASE WHEN taken_at IS NOT NULL THEN ' on ' || to_char(taken_at, 'Month DD, YYYY') ELSE '' END
             || '.'
         END,
         described_at = NOW()
       WHERE location_name IS NOT NULL
         AND description LIKE 'Photo at coordinates%'`,
    );
  }

  return {
    geocoded,
    photos_updated: photosUpdated,
    errors,
    remaining: remainCount,
  };
}

async function nominatimReverse(lat: number, lon: number): Promise<string | null> {
  try {
    const response = await fetch(
      `${NOMINATIM_URL}?lat=${lat}&lon=${lon}&format=json&zoom=14&addressdetails=1`,
      {
        headers: {
          'User-Agent': 'Nexus/1.0 (personal project)',
          Accept: 'application/json',
        },
        signal: AbortSignal.timeout(10_000),
      },
    );

    if (!response.ok) {
      if (response.status === 429) {
        // Rate limited — wait extra
        await new Promise(r => setTimeout(r, 5000));
      }
      return null;
    }

    const data = await response.json() as {
      display_name?: string;
      address?: {
        city?: string;
        town?: string;
        village?: string;
        municipality?: string;
        state?: string;
        country?: string;
      };
    };

    // Build a clean location name: City, State, Country
    const addr = data.address;
    if (!addr) return data.display_name ?? null;

    const city = addr.city ?? addr.town ?? addr.village ?? addr.municipality ?? '';
    const state = addr.state ?? '';
    const country = addr.country ?? '';

    const parts = [city, state, country].filter(Boolean);
    return parts.length > 0 ? parts.join(', ') : data.display_name ?? null;
  } catch {
    return null;
  }
}
