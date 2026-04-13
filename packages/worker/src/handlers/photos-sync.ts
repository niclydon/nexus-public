/**
 * Photos Sync — polls Apple Photos library via MCP on Dev-Server.
 * Runs every 15 minutes. Searches for recent photos, pushes metadata
 * through ingestion pipeline. Does NOT transfer actual image data.
 *
 * The Apple Photos MCP returns JSON (unlike Gmail/Calendar which return text).
 * Date filtering has a timezone bug, so we use the photo_metadata table
 * to track what's already synced and fetch by limit + dedup.
 *
 * Self-chains after completion.
 */
import { getPool, createLogger } from '@nexus/core';
import { callMcpTool } from '../lib/mcp-client-manager.js';
import { ingest } from '../lib/ingestion-pipeline.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/photos-sync');

const INTERVAL_SECONDS = 900; // 15 minutes
const BATCH_SIZE = 50;

export async function handlePhotosSync(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  try {
    logger.logVerbose('Starting photos sync');

    // Search recent photos (MCP date filter has timezone bug, so fetch by limit)
    let searchResult: string;
    try {
      searchResult = await callMcpTool('apple-photos', 'search_photos', {
        limit: BATCH_SIZE,
      });
    } catch (err) {
      logger.logMinimal('Photos MCP error:', (err as Error).message);
      return { error: (err as Error).message, checked: false };
    }

    // Parse JSON response
    let photos: Record<string, unknown>[];
    try {
      const parsed = JSON.parse(searchResult);
      if (parsed.error) {
        logger.logMinimal('Photos MCP returned error:', parsed.error);
        return { error: parsed.error, checked: false };
      }
      photos = Array.isArray(parsed) ? parsed : (parsed.photos ?? []);
    } catch {
      logger.log('Photos returned non-JSON:', searchResult.slice(0, 300));
      return { checked: true, new_photos: 0 };
    }

    if (photos.length === 0) {
      logger.logVerbose('No photos returned');
      await pool.query(
        `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active' WHERE source_key = 'apple_photos'`,
      );
      return { checked: true, new_photos: 0 };
    }

    // Filter out photos we already have
    const uuids = photos.map(p => String(p.uuid ?? '')).filter(Boolean);
    const { rows: existing } = await pool.query<{ local_identifier: string }>(
      `SELECT local_identifier FROM photo_metadata WHERE local_identifier = ANY($1)`,
      [uuids],
    );
    const existingIds = new Set(existing.map(r => r.local_identifier));
    const newPhotos = photos.filter(p => !existingIds.has(String(p.uuid)));

    if (newPhotos.length === 0) {
      logger.logVerbose(`All ${photos.length} photos already synced`);
      await pool.query(
        `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active' WHERE source_key = 'apple_photos'`,
      );
      return { checked: true, new_photos: 0, already_synced: photos.length };
    }

    logger.log(`${newPhotos.length} new photos to ingest (${existingIds.size} already synced)`);

    // Push through ingestion pipeline
    const result = await ingest('apple_photos', newPhotos);

    logger.log(`Photos sync: ${result.ingested} new, ${result.skipped} dupes, ${result.enrichments} enrichments`);

    return {
      checked: true,
      new_photos: result.ingested,
      duplicates: result.skipped,
      enrichments_queued: result.enrichments,
      already_synced: existingIds.size,
    };
  } finally {
    // Self-chain with pile-up guard — skip if one is already pending.
    try {
      const existing = await pool.query(
        `SELECT id FROM tempo_jobs WHERE job_type = 'photos-sync' AND status = 'pending' LIMIT 1`,
      );
      if (existing.rows.length === 0) {
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
           VALUES ('photos-sync', '{}', 'nexus', 2, 3, NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
        );
      }
    } catch (err) {
      logger.logMinimal('Failed to self-chain photos-sync:', (err as Error).message);
    }
  }
}
