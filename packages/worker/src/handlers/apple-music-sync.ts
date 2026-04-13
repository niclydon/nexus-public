/**
 * Apple Music Biome Sync — ingests play history from iOS Biome data on Dev-Server.
 *
 * Flow:
 *   1. SSH to Dev-Server to run the Biome parser script
 *   2. SCP the resulting JSON to local /tmp
 *   3. Parse plays array, INSERT with dedup on (played_at, track_name, artist_name)
 *
 * The Biome parser runs on a 6h cron on Dev-Server, but this handler triggers
 * a fresh parse each run to catch any new data.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import { execSync } from 'child_process';
import fs from 'fs';

const logger = createLogger('apple-music-sync');

const BIOME_JSON_PATH = '/tmp/apple-music-biome.json';

interface BiomePlay {
  timestamp: string;
  track_name: string;
  artist_name: string | null;
  album_name: string | null;
  genre: string | null;
  duration_seconds: number | null;
  store_track_id: string | null;
  store_album_id: string | null;
  output_device: string | null;
  source_file: string | null;
}

export async function handleAppleMusicSync(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Step 1: Run Biome parser on Dev-Server
  logger.log('Running Biome parser on Dev-Server...');
  const stopParse = logger.time('biome-parser-ssh');
  try {
    execSync('ssh dev-server "~/biome-venv/bin/python3 ~/ingest-apple-music-biome.py"', {
      timeout: 60_000,
      stdio: 'pipe',
    });
  } catch (err) {
    const msg = `Biome parser failed: ${(err as Error).message}`;
    logger.logMinimal(msg);
    await jobLog(job.id, msg, 'error');
    return { error: msg };
  }
  stopParse();

  // Step 2: Copy results from Dev-Server
  logger.logVerbose('Copying Biome JSON from Dev-Server...');
  const stopCopy = logger.time('biome-scp');
  try {
    execSync(`scp dev-server:${BIOME_JSON_PATH} ${BIOME_JSON_PATH}`, {
      timeout: 30_000,
      stdio: 'pipe',
    });
  } catch (err) {
    const msg = `SCP failed: ${(err as Error).message}`;
    logger.logMinimal(msg);
    await jobLog(job.id, msg, 'error');
    return { error: msg };
  }
  stopCopy();

  // Step 3: Parse JSON
  let plays: BiomePlay[];
  try {
    const raw = fs.readFileSync(BIOME_JSON_PATH, 'utf-8');
    const data = JSON.parse(raw);
    plays = data.plays ?? [];
    logger.log(`Parsed ${plays.length} plays from Biome data`);
  } catch (err) {
    const msg = `JSON parse failed: ${(err as Error).message}`;
    logger.logMinimal(msg);
    await jobLog(job.id, msg, 'error');
    return { error: msg };
  }

  if (plays.length === 0) {
    await jobLog(job.id, 'No plays found in Biome data');
    logger.log('No plays to ingest');
    return { inserted: 0, total_parsed: 0, status: 'empty' };
  }

  // Step 4: Batch insert with dedup
  let inserted = 0;
  let errors = 0;
  const stopInsert = logger.time('db-insert');

  for (const play of plays) {
    if (!play.timestamp || !play.track_name) {
      logger.logVerbose('Skipping play with missing timestamp or track_name');
      continue;
    }

    try {
      const result = await pool.query(
        `INSERT INTO apple_music_plays
           (played_at, track_name, artist_name, album_name, genre,
            duration_seconds, store_track_id, store_album_id, output_device, source_file)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (played_at, track_name, artist_name) DO NOTHING`,
        [
          play.timestamp,
          play.track_name,
          play.artist_name ?? null,
          play.album_name ?? null,
          play.genre ?? null,
          play.duration_seconds ?? null,
          play.store_track_id ?? null,
          play.store_album_id ?? null,
          play.output_device ?? null,
          play.source_file ?? null,
        ],
      );
      if (result.rowCount && result.rowCount > 0) inserted++;
    } catch (err) {
      errors++;
      logger.logDebug(`Insert error for "${play.track_name}":`, (err as Error).message);
    }
  }
  stopInsert();

  const dupes = plays.length - inserted - errors;
  const summary = `Apple Music sync: ${inserted} new, ${dupes} dupes, ${errors} errors (${plays.length} total)`;
  await jobLog(job.id, summary);
  logger.log(summary);

  // Log to ingestion pipeline
  if (inserted > 0) {
    try {
      await pool.query(
        `INSERT INTO ingestion_log (source_key, source_id, timestamp, category, summary, metadata)
         VALUES ('apple_music_biome', $1, NOW(), 'music', $2, $3)
         ON CONFLICT (source_key, source_id) DO NOTHING`,
        [
          `biome-sync-${new Date().toISOString().slice(0, 16)}`,
          summary,
          JSON.stringify({ inserted, dupes, errors, total: plays.length }),
        ],
      );
    } catch (err) {
      logger.logVerbose('Ingestion log insert failed:', (err as Error).message);
    }
  }

  return { inserted, dupes, errors, total_parsed: plays.length, status: 'complete' };
}
