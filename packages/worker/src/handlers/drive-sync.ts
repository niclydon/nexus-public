/**
 * Google Drive Sync — indexes files from Google Drive via MCP.
 *
 * Runs every 30 minutes. Uses google-drive__search with rawQuery to find
 * recently modified files, indexes them in drive_file_index, and pushes
 * high-value documents through the ingestion pipeline.
 *
 * Self-chains after completion.
 */
import { getPool, createLogger } from '@nexus/core';
import { callMcpTool } from '../lib/mcp-client-manager.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/drive-sync');

const INTERVAL_SECONDS = 1800; // 30 minutes
const MAX_FILES_PER_CYCLE = 50;

// File types worth ingesting for knowledge
const INGESTABLE_MIME_TYPES = new Set([
  'application/vnd.google-apps.document',
  'application/vnd.google-apps.spreadsheet',
  'application/pdf',
  'text/plain',
  'text/markdown',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
]);

interface DriveFile {
  id: string;
  name: string;
  mimeType: string;
  modifiedTime?: string;
  createdTime?: string;
  size?: string;
  parents?: string[];
}

/**
 * Parse the MCP text response into structured file objects.
 *
 * Google Drive MCP returns lines like:
 *   Name (mimeType) [id: xxx, path: yyy] [created: ..., modified: ...]
 *
 * Also extracts the pageToken for pagination if present.
 */
function parseSearchResults(text: string): { files: DriveFile[]; pageToken: string | null } {
  const files: DriveFile[] = [];

  // Try JSON first
  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) return { files: parsed, pageToken: null };
    if (parsed.files && Array.isArray(parsed.files)) return { files: parsed.files, pageToken: parsed.pageToken ?? null };
  } catch { /* not JSON, parse text */ }

  // Parse text format line by line
  // Pattern: Name (mimeType) [id: xxx, path: yyy] [created: ..., modified: ...]
  const linePattern = /^(.+?)\s+\(([^)]+)\)\s+\[id:\s*([^,\]]+)(?:,\s*path:\s*([^\]]*))?\]\s*(?:\[created:\s*([^,\]]+)(?:,\s*modified:\s*([^\]]+))?\])?/;

  for (const line of text.split('\n')) {
    const match = line.match(linePattern);
    if (match) {
      files.push({
        id: match[3].trim(),
        name: match[1].trim(),
        mimeType: match[2].trim(),
        createdTime: match[5]?.trim(),
        modifiedTime: match[6]?.trim(),
        parents: match[4] ? [match[4].trim()] : undefined,
      });
    }
  }

  // Extract pageToken
  const tokenMatch = text.match(/pageToken:\s*(\S+)/);
  return { files, pageToken: tokenMatch?.[1] ?? null };
}

export async function handleDriveSync(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  try {
    // Get watermark
    const { rows } = await pool.query<{ last_sync_at: Date | null }>(
      `SELECT last_sync_at FROM data_source_registry WHERE source_key = 'cloud_drive'`,
    );

    const lastSync = rows[0]?.last_sync_at;
    const modifiedAfter = lastSync
      ? lastSync.toISOString().split('T')[0].replace(/-/g, '-')
      : new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    // Raw query to Google Drive API — search for recently modified files
    const query = `modifiedTime > '${modifiedAfter}T00:00:00' and trashed = false`;
    logger.logVerbose(`Searching Drive: ${query}`);

    let searchResult: string;
    try {
      searchResult = await callMcpTool('google-drive', 'search', {
        query,
        rawQuery: true,
        pageSize: MAX_FILES_PER_CYCLE,
      });
    } catch (err) {
      logger.logMinimal('Drive MCP search error:', (err as Error).message);
      return { error: (err as Error).message, checked: false };
    }

    const { files, pageToken } = parseSearchResults(searchResult);
    if (files.length === 0) {
      logger.logVerbose('No new/modified files found');
      await pool.query(
        `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active' WHERE source_key = 'cloud_drive'`,
      );
      return { checked: true, new_files: 0 };
    }

    logger.log(`Found ${files.length} files to index${pageToken ? ' (more pages available)' : ''}`);

    let indexed = 0;
    let ingested = 0;

    for (const file of files) {
      // Upsert into drive_file_index
      const filePath = file.parents?.[0] ?? null;
      const { rows: existing } = await pool.query(
        `SELECT id FROM drive_file_index WHERE source = 'google_drive' AND file_id = $1`,
        [file.id],
      );
      if (existing.length > 0) {
        await pool.query(
          `UPDATE drive_file_index SET file_name = $1, mime_type = $2, modified_at = $3, file_path = $4, created_at = $5, synced_at = NOW()
           WHERE source = 'google_drive' AND file_id = $6`,
          [file.name, file.mimeType, file.modifiedTime ?? null, filePath, file.createdTime ?? null, file.id],
        );
      } else {
        await pool.query(
          `INSERT INTO drive_file_index (source, file_id, file_name, mime_type, modified_at, file_path, created_at, synced_at)
           VALUES ('google_drive', $1, $2, $3, $4, $5, $6, NOW())`,
          [file.id, file.name, file.mimeType, file.modifiedTime ?? null, filePath, file.createdTime ?? null],
        );
      }
      indexed++;

      // Mark ingestable files — attempt content read if scope allows
      console.log(`[drive-sync-debug] File: ${file.name} | mime: '${file.mimeType}' | ingestable: ${INGESTABLE_MIME_TYPES.has(file.mimeType)}`);
      if (INGESTABLE_MIME_TYPES.has(file.mimeType)) {
        // Flag as high-value in the index
        await pool.query(
          `UPDATE drive_file_index SET is_data_goldmine = true, goldmine_category = 'document'
           WHERE source = 'google_drive' AND file_id = $1`,
          [file.id],
        );

        // Content ingestion handled by separate drive-ingest job
        // (drive-sync indexes fast, drive-ingest reads content for goldmine files)
      }
    }

    // Update watermark
    await pool.query(
      `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active', enabled = true WHERE source_key = 'cloud_drive'`,
    );

    logger.log(`Cycle complete: ${indexed} indexed, ${ingested} ingested`);

    // Self-chain
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, executor, next_run_at)
       VALUES ('drive-sync', 'nexus', NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
    );

    return { checked: true, files_found: files.length, indexed, ingested };
  } catch (err) {
    logger.logMinimal('Drive sync error:', (err as Error).message);
    // Still self-chain on error so we retry
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, executor, next_run_at)
       VALUES ('drive-sync', 'nexus', NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
    );
    return { error: (err as Error).message };
  }
}
