/**
 * Google Drive Ingest — reads content from goldmine files and ingests into KG pipeline.
 *
 * Processes files flagged as is_data_goldmine=true and ingested=false by drive-sync.
 * Reads content via the tool execution proxy (localhost:7702), stores in ingestion_log.
 * Self-chains if more files remain.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';

const logger = createLogger('drive-ingest');

const BATCH_SIZE = 5; // Files per job execution
// Read from env so this works from any host. Secondary-Server workers reach Primary-Server's
// tool server via Thunderbolt at primary-tb.example.io:7702 (set in TOOL_SERVER_URL,
// resolves via direct link). Falls back to localhost for the primary worker on Primary-Server.
const TOOL_SERVER = process.env.TOOL_SERVER_URL || 'http://localhost:7702';

const TOOL_MAP: Record<string, { toolId: string; paramKey: string }> = {
  'application/vnd.google-apps.document': { toolId: 'google-drive__readGoogleDoc', paramKey: 'documentId' },
  'application/vnd.google-apps.spreadsheet': { toolId: 'google-drive__getGoogleSheetContent', paramKey: 'spreadsheetId' },
};

export async function handleDriveIngest(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Find un-ingested goldmine files
  const { rows: files } = await pool.query<{
    id: number; file_id: string; file_name: string; mime_type: string; modified_at: Date | null;
  }>(
    `SELECT id, file_id, file_name, mime_type, modified_at
     FROM drive_file_index
     WHERE source = 'google_drive' AND is_data_goldmine = true AND ingested = false
     ORDER BY modified_at DESC NULLS LAST
     LIMIT $1`,
    [BATCH_SIZE],
  );

  if (files.length === 0) {
    logger.log('No un-ingested goldmine files');
    return { status: 'complete', ingested: 0 };
  }

  logger.log(`Processing ${files.length} goldmine files`);

  let ingested = 0;
  let errors = 0;

  for (const file of files) {
    const mapping = TOOL_MAP[file.mime_type];
    if (!mapping) {
      logger.logVerbose(`No reader for mime type: ${file.mime_type} (${file.file_name})`);
      continue;
    }

    try {
      logger.log(`Reading: ${file.file_name}`);

      const resp = await fetch(`${TOOL_SERVER}/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          agent_id: 'collector',
          tool_id: mapping.toolId,
          params: { [mapping.paramKey]: file.file_id },
        }),
        signal: AbortSignal.timeout(30_000),
      });

      const data = await resp.json() as { result?: string; error?: string };
      const content = data.result ?? '';

      if (content.length > 50 && !content.startsWith('Error') && !content.startsWith('BLOCKED')) {
        await pool.query(
          `INSERT INTO ingestion_log (source_key, source_id, category, summary, metadata, timestamp)
           VALUES ('cloud_drive', $1, 'document', $2, $3, $4)
           ON CONFLICT DO NOTHING`,
          [file.file_id,
           (file.file_name + ': ' + content).slice(0, 5000),
           JSON.stringify({ mime_type: file.mime_type, file_id: file.file_id, content_length: content.length }),
           file.modified_at ?? new Date()],
        );

        await pool.query(
          `UPDATE drive_file_index SET ingested = true, ingested_at = NOW()
           WHERE id = $1`,
          [file.id],
        );

        ingested++;
        logger.log(`Ingested: ${file.file_name} (${content.length} chars)`);
      } else {
        logger.logMinimal(`Empty/error content for ${file.file_name}: ${content.slice(0, 100)}`);
        errors++;
      }
    } catch (err) {
      logger.logMinimal(`Failed: ${file.file_name}: ${(err as Error).message.slice(0, 100)}`);
      errors++;
    }
  }

  // Self-chain if more files remain
  const { rows: [{ remaining }] } = await pool.query<{ remaining: string }>(
    `SELECT count(*)::text as remaining FROM drive_file_index
     WHERE source = 'google_drive' AND is_data_goldmine = true AND ingested = false`,
  );

  if (parseInt(remaining) > 0) {
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, executor) VALUES ('drive-ingest', 'nexus')`,
    );
    logger.log(`${remaining} files remaining — chained next batch`);
  }

  logger.log(`Complete: ${ingested} ingested, ${errors} errors`);
  return { ingested, errors, remaining: parseInt(remaining) };
}
