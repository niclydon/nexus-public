/**
 * Person Photo Sync Handler
 *
 * Pulls person thumbnails from Apple Photos MCP (list_persons tool)
 * and stores them on aurora_social_identities for use in the Chancery UI.
 *
 * Runs periodically (daily or on-demand). Matches Apple Photos person
 * names to existing aurora_social_identities by display_name.
 *
 * The Apple Photos MCP returns person names with thumbnail data.
 * We store the thumbnail as a base64 data URL or file path.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { callMcpTool } from '../lib/mcp-client-manager.js';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('person-photo-sync');

export async function handlePersonPhotoSync(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  logger.log('Syncing person photos from Apple Photos');
  await jobLog(job.id, 'Calling Apple Photos list_persons');

  // Call Apple Photos MCP to get persons with thumbnails
  let result: string;
  try {
    result = await callMcpTool('apple-photos', 'list_persons', {});
  } catch (err) {
    logger.logMinimal('Apple Photos MCP error:', (err as Error).message);
    return { error: (err as Error).message };
  }

  // Parse response — the MCP returns text with person data
  let persons: Array<{ name: string; photo_count?: number; thumbnail?: string; id?: string }>;
  try {
    const parsed = JSON.parse(result);
    persons = Array.isArray(parsed) ? parsed : parsed.persons ?? parsed.people ?? [];
  } catch {
    // Try to extract JSON from text response
    const jsonMatch = result.match(/\[[\s\S]*\]/);
    if (jsonMatch) {
      try {
        persons = JSON.parse(jsonMatch[0]);
      } catch {
        logger.logMinimal('Failed to parse Apple Photos response:', result.slice(0, 300));
        return { error: 'parse_error', raw_length: result.length };
      }
    } else {
      logger.log('Apple Photos returned non-JSON:', result.slice(0, 300));
      return { error: 'no_json', raw_length: result.length };
    }
  }

  logger.log(`Found ${persons.length} persons from Apple Photos`);
  await jobLog(job.id, `Found ${persons.length} Apple Photos persons`);

  // Ensure photo_url column exists
  try {
    await pool.query(`ALTER TABLE aurora_social_identities ADD COLUMN IF NOT EXISTS photo_url TEXT`);
  } catch {
    // Column may already exist
  }

  let matched = 0;
  let updated = 0;

  for (const person of persons) {
    if (!person.name || person.name.length < 2) continue;

    // Match by name (case-insensitive)
    const { rows } = await pool.query(
      `SELECT id, photo_url FROM aurora_social_identities
       WHERE LOWER(display_name) = LOWER($1) AND is_person = TRUE
       LIMIT 1`,
      [person.name]
    );

    if (rows.length > 0) {
      matched++;
      const existing = rows[0];

      // Update photo if we have one and it's not already set
      if (person.thumbnail && (!existing.photo_url || existing.photo_url === '')) {
        await pool.query(
          `UPDATE aurora_social_identities SET photo_url = $1, updated_at = NOW() WHERE id = $2`,
          [person.thumbnail, existing.id]
        );
        updated++;
      }
    }
  }

  const result_data = {
    apple_photos_persons: persons.length,
    matched_to_nexus: matched,
    photos_updated: updated,
  };

  logger.log(`Complete: ${JSON.stringify(result_data)}`);
  await jobLog(job.id, `Complete: ${matched} matched, ${updated} photos updated`);
  return result_data;
}
