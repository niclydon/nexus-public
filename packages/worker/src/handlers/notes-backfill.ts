/**
 * Apple Notes Backfill — LLM classification of ingested notes.
 *
 * Processes one unclassified note per job execution. For each note:
 * 1. Sends content to LLM for classification (note type, summary, entities)
 * 2. Updates apple_notes with classification results
 * 3. Queues appropriate enrichments based on note type
 * 4. Self-chains for next note until all are classified
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import { routeRequest } from '../lib/llm/router.js';

const logger = createLogger('notes-backfill');

interface NoteRow {
  id: string;
  note_identifier: string;
  title: string | null;
  body_text: string | null;
  folder: string | null;
  latitude: number | null;
  longitude: number | null;
  metadata: Record<string, unknown>;
}

interface Classification {
  note_type: string;
  summary: string;
  entities: Array<{ name: string; type: string }>;
  checklist_items?: string[];
  location_name?: string;
  topics: string[];
  skip_enrichment: boolean;
}

export async function handleNotesBackfill(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Find next unclassified note
  const { rows } = await pool.query<NoteRow>(
    `SELECT id, note_identifier, title, body_text, folder, latitude, longitude, metadata
     FROM apple_notes
     WHERE classified_at IS NULL AND body_text IS NOT NULL AND LENGTH(body_text) > 5
     ORDER BY created_at ASC
     LIMIT 1`,
  );

  if (rows.length === 0) {
    logger.log('All notes classified');
    await jobLog(job.id, 'All notes classified — backfill complete');
    return { status: 'complete', remaining: 0 };
  }

  const note = rows[0];
  const hasLocation = !!(note.latitude && note.longitude);
  const hasChecklist = !!(note.metadata as Record<string, unknown>)?.has_checklist;

  logger.log(`Classifying note: "${note.title}" (${note.id.slice(0, 8)})`);
  await jobLog(job.id, `Classifying: ${note.title ?? '(Untitled)'}`);

  // Build classification prompt
  const prompt = `You are classifying an Apple Note for a personal knowledge system. Analyze the content and respond with ONLY valid JSON.

Title: ${note.title ?? '(Untitled)'}
Folder: ${note.folder ?? '(None)'}
Has GPS coordinates: ${hasLocation}
Has checklist formatting: ${hasChecklist}
Content (first 2000 chars):
${(note.body_text ?? '').slice(0, 2000)}

Respond with this exact JSON structure:
{
  "note_type": "text|checklist|location|travel|pdf|certificate|log_file|todo|reference|unknown",
  "summary": "1-2 sentence description of what this note contains and why the person saved it",
  "entities": [{"name": "person or place name", "type": "person|place|org|topic"}],
  "checklist_items": ["item1", "item2"],
  "location_name": "place name if this is a location/travel note",
  "topics": ["travel", "work", "shopping", "packing", "reference"],
  "skip_enrichment": false
}

Rules:
- note_type "travel" for notes about places visited/to visit, packing lists for trips, location pins
- note_type "checklist" for shopping lists, to-do lists
- note_type "reference" for saved info like phone numbers, codes, certificates
- note_type "log_file" for technical logs, system data
- skip_enrichment = true for certificates, log files, and system-generated content with no personal meaning
- Extract ALL person names, place names, and organizations mentioned in the content`;

  try {
    const response = await routeRequest({
      systemPrompt: 'You are a note classifier. Respond with ONLY valid JSON, no markdown wrapping.',
      userMessage: prompt,
      taskTier: 'classification',
      handler: 'notes-backfill',
      maxTokens: 500,
    });

    let classification: Classification;
    try {
      const jsonMatch = response.text.match(/\{[\s\S]*\}/);
      if (!jsonMatch) throw new Error('No JSON in response');
      classification = JSON.parse(jsonMatch[0]);
    } catch {
      logger.logVerbose('Failed to parse LLM classification, using defaults');
      classification = {
        note_type: 'unknown',
        summary: note.title ?? 'Unclassified note',
        entities: [],
        topics: [],
        skip_enrichment: false,
      };
    }

    // Update note with classification
    await pool.query(
      `UPDATE apple_notes SET
        note_type = $1,
        classification = $2,
        checklist_items = $3,
        location_name = $4,
        classified_at = NOW()
      WHERE id = $5`,
      [
        classification.note_type,
        JSON.stringify(classification),
        classification.checklist_items ? JSON.stringify(classification.checklist_items) : null,
        classification.location_name ?? null,
        note.id,
      ],
    );

    // Queue enrichments based on note type (unless skip_enrichment)
    if (!classification.skip_enrichment) {
      // Embedding for all non-skipped notes
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, executor, priority)
         VALUES ('embed-backfill', $1, 'nexus', 0)`,
        [JSON.stringify({ table: 'apple_notes', batch_size: 10 })],
      ).catch(() => {});

      // Knowledge extraction for text, travel, and reference notes
      if (['text', 'travel', 'location', 'reference', 'todo'].includes(classification.note_type)) {
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, executor, priority)
           VALUES ('knowledge-backfill', $1, 'nexus', 0)`,
          [JSON.stringify({ source: 'apple_notes', note_id: note.id })],
        ).catch(() => {});
      }
    }

    logger.log(`Classified "${note.title}" as ${classification.note_type}: ${classification.summary.slice(0, 100)}`);

    // Count remaining
    const { rows: [countRow] } = await pool.query<{ remaining: string }>(
      `SELECT COUNT(*)::text AS remaining FROM apple_notes WHERE classified_at IS NULL AND body_text IS NOT NULL AND LENGTH(body_text) > 5`,
    );
    const remaining = parseInt(countRow.remaining);

    // Self-chain if more to process
    if (remaining > 0) {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, executor, priority)
         VALUES ('notes-backfill', '{}', 'nexus', 0)`,
      );
    }

    await jobLog(job.id, `Classified as ${classification.note_type}. ${remaining} remaining.`);

    return {
      note_id: note.id,
      title: note.title,
      note_type: classification.note_type,
      summary: classification.summary,
      remaining,
    };

  } catch (err) {
    logger.logMinimal(`Classification failed for "${note.title}":`, (err as Error).message);
    // Mark as classified with error to avoid infinite retry
    await pool.query(
      `UPDATE apple_notes SET note_type = 'error', classification = $1, classified_at = NOW() WHERE id = $2`,
      [JSON.stringify({ error: (err as Error).message }), note.id],
    );
    return { error: (err as Error).message, note_id: note.id };
  }
}
