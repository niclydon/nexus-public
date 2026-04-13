/**
 * Apple Notes Sync — pulls notes from Apple Notes SQLite DB on Dev-Server.
 *
 * Reads NoteStore.sqlite via SSH, decodes gzip-compressed protobuf content,
 * and pushes through the ingestion pipeline. Self-chains every 30 minutes.
 *
 * Content decoding: ZICNOTEDATA.ZDATA → gunzip → strip protobuf binary → text
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import { execSync } from 'child_process';
import zlib from 'zlib';

const logger = createLogger('notes-sync');

const INTERVAL_SECONDS = 30 * 60; // 30 minutes
const NOTES_DB = '~/Library/Group\\ Containers/group.com.apple.notes/NoteStore.sqlite';
const APPLE_EPOCH_OFFSET = 978307200; // seconds between Unix epoch and Apple epoch (2001-01-01)

interface RawNote {
  zpk: number;
  identifier: string;
  title: string | null;
  created: number; // Apple epoch seconds
  modified: number;
  deleted: number;
  folder: string | null;
  data_hex: string | null;
  has_checklist: number;
  latitude: number | null;
  longitude: number | null;
}

/**
 * Decode Apple Notes protobuf content.
 * ZDATA is gzip-compressed protobuf. After gunzip, text content is
 * interspersed with binary protobuf framing. We extract readable text
 * by filtering for printable character runs.
 */
function decodeNoteContent(hexData: string): string {
  try {
    const buf = Buffer.from(hexData, 'hex');
    const decompressed = zlib.gunzipSync(buf);

    const lines = decompressed
      .toString('utf-8')
      .replace(/[\x00-\x08\x0b\x0c\x0e-\x1f]/g, '\n')
      .split('\n')
      .map(l => l.trim())
      .filter(l => {
        if (l.length < 2) return false;
        if (/^\(/.test(l)) return false; // protobuf checklist markers
        const printable = l.replace(/[^\x20-\x7e\u00a0-\uffff]/g, '');
        return printable.length / l.length > 0.7; // >70% printable chars
      });

    // Find where the real content ends — once we hit 3+ consecutive
    // lines of mostly binary, the text portion is over
    let binaryRunCount = 0;
    let cutoff = lines.length;
    for (let i = 0; i < lines.length; i++) {
      const printableRatio = lines[i].replace(/[^\x20-\x7e\u00a0-\uffff]/g, '').length / lines[i].length;
      if (printableRatio < 0.5 || lines[i].length < 3) {
        binaryRunCount++;
        if (binaryRunCount >= 3) {
          cutoff = i - 2;
          break;
        }
      } else {
        binaryRunCount = 0;
      }
    }

    return lines.slice(0, cutoff).join('\n').trim();
  } catch (err) {
    logger.logVerbose('Failed to decode note content:', (err as Error).message);
    return '';
  }
}

function appleToDate(appleTimestamp: number): Date {
  return new Date((appleTimestamp + APPLE_EPOCH_OFFSET) * 1000);
}

export async function handleNotesSync(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Honor the data_source_registry.enabled flag so disabling the source via
  // SQL pauses the every-30-min SSH-to-Dev-Server failure loop without needing a
  // code change. Resumes automatically when enabled flips back to true.
  try {
    const sourceCheck = await pool.query<{ enabled: boolean }>(
      `SELECT enabled FROM data_source_registry WHERE source_key = 'apple_notes'`,
    );
    if (sourceCheck.rows.length > 0 && !sourceCheck.rows[0].enabled) {
      logger.logVerbose('apple_notes disabled in data_source_registry, skipping');
      await jobLog(job.id, 'Skipped: apple_notes source disabled', 'info');
      // Self-chain with a long backoff instead of the healthy 5-min cadence.
      try {
        const existing = await pool.query(
          `SELECT id FROM tempo_jobs WHERE job_type = 'notes-sync' AND status = 'pending' LIMIT 1`,
        );
        if (existing.rows.length === 0) {
          await pool.query(
            `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
             VALUES ('notes-sync', '{}', 'nexus', 1, 3, NOW() + INTERVAL '6 hours')`,
          );
        }
      } catch {
        // non-fatal
      }
      return { skipped: true, reason: 'data_source_disabled' };
    }
  } catch {
    // Non-fatal — continue with normal sync
  }

  try {
    logger.log('Starting Apple Notes sync from Dev-Server');
    await jobLog(job.id, 'Querying Apple Notes DB on Dev-Server');

    // Query all non-deleted notes with their content
    // SQL file pre-deployed to /tmp/notes-query.sql on Dev-Server
    let rawOutput: string;
    try {
      const notesDbPath = "/home/user/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite";
      rawOutput = execSync(
        `ssh dev-server "sqlite3 -separator '§§§' '${notesDbPath}' < /tmp/notes-query.sql"`,
        { timeout: 60000, encoding: 'utf-8', maxBuffer: 50 * 1024 * 1024 },
      );
    } catch (err) {
      logger.logMinimal('Failed to query Apple Notes DB:', (err as Error).message);
      return { error: 'SSH/sqlite3 failed', checked: false };
    }

    const rows = rawOutput.trim().split('\n').filter(Boolean);
    if (rows.length === 0) {
      logger.log('No notes found');
      return { checked: true, synced: 0 };
    }

    logger.log(`Found ${rows.length} notes from Dev-Server`);

    // Parse and decode
    const notes: RawNote[] = rows.map(row => {
      const parts = row.split('§§§');
      return {
        zpk: parseInt(parts[0]) || 0,
        identifier: parts[1] || '',
        title: parts[2] || null,
        created: parseFloat(parts[3]) || 0,
        modified: parseFloat(parts[4]) || 0,
        deleted: parseInt(parts[5]) || 0,
        folder: parts[6] || null,
        data_hex: parts[7] || null,
        has_checklist: parseInt(parts[8]) || 0,
        latitude: parts[9] ? parseFloat(parts[9]) : null,
        longitude: parts[10] ? parseFloat(parts[10]) : null,
      };
    }).filter(n => n.identifier);

    // Debug first note
    if (notes.length > 0) {
      const first = notes[0];
      logger.logVerbose(`First note: zpk=${first.zpk} id=${first.identifier} title="${first.title}" hex_len=${first.data_hex?.length ?? 0}`);
    }
    logger.log(`Parsed ${notes.length} notes, decoding content`);
    await jobLog(job.id, `Processing ${notes.length} notes`);

    let ingested = 0;
    let updated = 0;
    let skipped = 0;

    for (const note of notes) {
      const bodyText = note.data_hex ? decodeNoteContent(note.data_hex) : '';
      const createdAt = note.created ? appleToDate(note.created) : new Date();
      const modifiedAt = note.modified ? appleToDate(note.modified) : createdAt;

      if (!bodyText && !note.title) {
        skipped++;
        continue;
      }

      try {
        // Upsert into apple_notes
        const { rows: result } = await pool.query<{ id: string; was_updated: boolean }>(
          `INSERT INTO apple_notes (
            note_identifier, zpk, title, folder, body_text,
            created_at, modified_at, is_deleted,
            has_attachments, latitude, longitude,
            metadata, synced_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
          ON CONFLICT (note_identifier) DO UPDATE SET
            title = EXCLUDED.title,
            body_text = EXCLUDED.body_text,
            folder = EXCLUDED.folder,
            modified_at = EXCLUDED.modified_at,
            is_deleted = EXCLUDED.is_deleted,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            metadata = EXCLUDED.metadata,
            synced_at = NOW()
          RETURNING id, (xmax = 0) AS was_updated`,
          [
            note.identifier, note.zpk, note.title, note.folder, bodyText,
            createdAt, modifiedAt, note.deleted > 0,
            false, note.latitude, note.longitude,
            JSON.stringify({
              has_checklist: note.has_checklist > 0,
              word_count: bodyText.split(/\s+/).filter(Boolean).length,
            }),
          ],
        );

        if (result.length > 0) {
          if (result[0].was_updated) ingested++;
          else updated++;
        }

        // Also insert into ingestion_log for pipeline tracking
        await pool.query(
          `INSERT INTO ingestion_log (source_key, source_id, timestamp, category, summary, metadata)
           VALUES ('apple_notes', $1, $2, 'document', $3, $4)
           ON CONFLICT (source_key, source_id) DO NOTHING`,
          [note.identifier, createdAt, note.title ?? '(Untitled)',
           JSON.stringify({ folder: note.folder })],
        );

      } catch (err) {
        logger.logVerbose(`Failed to ingest note ${note.identifier}:`, (err as Error).message);
        skipped++;
      }
    }

    // Update data source registry
    await pool.query(
      `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active'
       WHERE source_key = 'apple_notes'`,
    ).catch(() => {});

    const result = {
      checked: true,
      total_from_source: notes.length,
      ingested,
      updated,
      skipped,
    };

    logger.log(`Notes sync: ${ingested} new, ${updated} updated, ${skipped} skipped out of ${notes.length}`);
    await jobLog(job.id, `Notes sync: ${ingested} new, ${updated} updated, ${skipped} skipped`);
    return result;

  } finally {
    // Self-chain with pile-up guard — skip if one is already pending.
    try {
      const existing = await pool.query(
        `SELECT id FROM tempo_jobs WHERE job_type = 'notes-sync' AND status = 'pending' LIMIT 1`,
      );
      if (existing.rows.length === 0) {
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
           VALUES ('notes-sync', '{}', 'nexus', 1, 3, NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
        );
      }
    } catch (err) {
      logger.logMinimal('Failed to self-chain notes-sync:', (err as Error).message);
    }
  }
}
