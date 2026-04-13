/**
 * Generate Merge Candidates Handler
 *
 * Scans all entity types for near-duplicate records and creates
 * merge candidates for human review in the Chancery merge review page.
 *
 * Entity types:
 * - person: aurora_social_identities with similar display_names or shared identifiers
 * - knowledge_entity: knowledge_entities with similar canonical_names within same type
 * - photo_person: unmatched photo people[] names → existing persons
 * - contact_person: unlinked contacts → existing persons by phone/email
 *
 * Runs weekly or on-demand. Uses ON CONFLICT to avoid duplicate candidates.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('generate-merge-candidates');

const MIN_CONFIDENCE = 0.6;

export async function handleGenerateMergeCandidates(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { types?: string[] };
  const types = payload.types ?? ['person', 'knowledge_entity', 'photo_person', 'contact_person'];

  let totalCandidates = 0;

  // ── Person candidates ──
  if (types.includes('person')) {
    try {
      // Find persons with similar display_names
      const { rows } = await pool.query(`
        SELECT a.id as a_id, b.id as b_id, a.display_name as a_name, b.display_name as b_name,
          similarity(LOWER(a.display_name), LOWER(b.display_name)) as name_sim
        FROM aurora_social_identities a
        JOIN aurora_social_identities b ON a.id < b.id
        WHERE a.is_person = TRUE AND b.is_person = TRUE
          AND similarity(LOWER(a.display_name), LOWER(b.display_name)) > 0.6
          AND NOT EXISTS (SELECT 1 FROM merge_candidates mc WHERE mc.entity_type = 'person'
            AND mc.record_a_id = a.id::text AND mc.record_b_id = b.id::text)
        LIMIT 500
      `);

      for (const row of rows) {
        const confidence = row.name_sim * 0.8; // name similarity weighted
        if (confidence >= MIN_CONFIDENCE) {
          await pool.query(
            `INSERT INTO merge_candidates (entity_type, record_a_id, record_b_id, confidence, match_reason)
             VALUES ('person', $1, $2, $3, $4)
             ON CONFLICT (entity_type, record_a_id, record_b_id) DO NOTHING`,
            [String(row.a_id), String(row.b_id), confidence,
             JSON.stringify({ name_similarity: row.name_sim, a_name: row.a_name, b_name: row.b_name })]
          );
          totalCandidates++;
        }
      }

      // Also find persons sharing phone/email across different identities
      const { rows: sharedIds } = await pool.query(`
        SELECT a.identity_id as a_id, b.identity_id as b_id, a.identifier
        FROM aurora_social_identity_links a
        JOIN aurora_social_identity_links b ON a.identifier = b.identifier AND a.identity_id < b.identity_id
        JOIN aurora_social_identities sa ON sa.id = a.identity_id AND sa.is_person = TRUE
        JOIN aurora_social_identities sb ON sb.id = b.identity_id AND sb.is_person = TRUE
        WHERE NOT EXISTS (SELECT 1 FROM merge_candidates mc WHERE mc.entity_type = 'person'
          AND mc.record_a_id = a.identity_id::text AND mc.record_b_id = b.identity_id::text)
        LIMIT 100
      `);

      for (const row of sharedIds) {
        await pool.query(
          `INSERT INTO merge_candidates (entity_type, record_a_id, record_b_id, confidence, match_reason)
           VALUES ('person', $1, $2, 0.95, $3)
           ON CONFLICT (entity_type, record_a_id, record_b_id) DO NOTHING`,
          [String(row.a_id), String(row.b_id),
           JSON.stringify({ shared_identifier: row.identifier })]
        );
        totalCandidates++;
      }

      logger.log(`Person candidates: ${rows.length} name matches, ${sharedIds.length} shared identifiers`);
      await jobLog(job.id, `Person: ${rows.length} name + ${sharedIds.length} identifier matches`);
    } catch (err) {
      logger.logMinimal(`Person candidate generation failed: ${(err as Error).message}`);
      // pg_trgm might not be installed — fall back to exact prefix matching
      await jobLog(job.id, `Person candidates failed (pg_trgm may not be installed): ${(err as Error).message}`);
    }
  }

  // ── Knowledge entity candidates ──
  if (types.includes('knowledge_entity')) {
    // Exact name duplicates within same type
    const { rows: dupes } = await pool.query(`
      SELECT entity_type, canonical_name, array_agg(id ORDER BY fact_count DESC) as ids, COUNT(*) as cnt
      FROM knowledge_entities
      WHERE fact_count >= 0
      GROUP BY entity_type, canonical_name
      HAVING COUNT(*) > 1
      LIMIT 500
    `);

    for (const dupe of dupes) {
      const ids = dupe.ids as string[];
      for (let i = 0; i < ids.length - 1; i++) {
        await pool.query(
          `INSERT INTO merge_candidates (entity_type, record_a_id, record_b_id, confidence, match_reason)
           VALUES ('knowledge_entity', $1, $2, 1.0, $3)
           ON CONFLICT (entity_type, record_a_id, record_b_id) DO NOTHING`,
          [ids[0], ids[i + 1], JSON.stringify({ exact_name_match: dupe.canonical_name, entity_type: dupe.entity_type })]
        );
        totalCandidates++;
      }
    }

    logger.log(`KG entity candidates: ${dupes.length} duplicate name groups`);
    await jobLog(job.id, `KG entities: ${dupes.length} duplicate groups`);
  }

  // ── Photo person candidates ──
  if (types.includes('photo_person')) {
    // Find photo people names not linked to any identity
    const { rows: unmatched } = await pool.query(`
      SELECT DISTINCT name, COUNT(*) as photo_count
      FROM (SELECT unnest(people) as name FROM photo_metadata WHERE array_length(people, 1) > 0) sub
      WHERE name NOT IN (SELECT display_name FROM aurora_social_identities WHERE is_person = TRUE)
        AND LENGTH(name) > 1
      GROUP BY name
      ORDER BY photo_count DESC
      LIMIT 200
    `);

    for (const row of unmatched) {
      // Find closest matching person by name
      try {
        const { rows: matches } = await pool.query(`
          SELECT id, display_name, similarity(LOWER(display_name), LOWER($1)) as sim
          FROM aurora_social_identities
          WHERE is_person = TRUE AND similarity(LOWER(display_name), LOWER($1)) > 0.5
          ORDER BY sim DESC LIMIT 1
        `, [row.name]);

        if (matches.length > 0) {
          const confidence = matches[0].sim * 0.9;
          await pool.query(
            `INSERT INTO merge_candidates (entity_type, record_a_id, record_b_id, confidence, match_reason)
             VALUES ('photo_person', $1, $2, $3, $4)
             ON CONFLICT (entity_type, record_a_id, record_b_id) DO NOTHING`,
            [String(matches[0].id), row.name, confidence,
             JSON.stringify({ photo_name: row.name, person_name: matches[0].display_name, similarity: matches[0].sim, photo_count: row.photo_count })]
          );
          totalCandidates++;
        }
      } catch {
        // pg_trgm not available — skip fuzzy matching for photos
      }
    }

    logger.log(`Photo person candidates: ${unmatched.length} unmatched names checked`);
    await jobLog(job.id, `Photo people: ${unmatched.length} names checked`);
  }

  // ── Contact → person candidates ──
  if (types.includes('contact_person')) {
    // Find contacts not linked to any identity
    const { rows: unlinked } = await pool.query(`
      SELECT c.id, c.display_name, c.phone_numbers, c.email_addresses
      FROM contacts c
      WHERE NOT EXISTS (
        SELECT 1 FROM aurora_social_identities si WHERE si.aria_contact_id = c.id
      )
    `);

    for (const contact of unlinked) {
      // Try to match by phone/email
      const phones = (contact.phone_numbers || []) as string[];
      const emails = (contact.email_addresses || []) as string[];
      const allIdentifiers = [...phones.map((p: string) => p.replace(/[\s\-\(\)\.]/g, '')), ...emails.map((e: string) => e.toLowerCase())];

      for (const identifier of allIdentifiers) {
        if (identifier.length < 3) continue;
        const { rows: matches } = await pool.query(
          `SELECT identity_id FROM aurora_social_identity_links WHERE identifier = $1 LIMIT 1`,
          [identifier]
        );

        if (matches.length > 0) {
          await pool.query(
            `INSERT INTO merge_candidates (entity_type, record_a_id, record_b_id, confidence, match_reason)
             VALUES ('contact_person', $1, $2, 0.9, $3)
             ON CONFLICT (entity_type, record_a_id, record_b_id) DO NOTHING`,
            [String(matches[0].identity_id), contact.id,
             JSON.stringify({ shared_identifier: identifier, contact_name: contact.display_name })]
          );
          totalCandidates++;
          break; // One match is enough
        }
      }
    }

    logger.log(`Contact candidates: ${unlinked.length} unlinked contacts checked`);
    await jobLog(job.id, `Contacts: ${unlinked.length} unlinked checked`);
  }

  const result = { candidates_generated: totalCandidates };
  logger.log(`Complete: ${totalCandidates} total candidates generated`);
  await jobLog(job.id, `Complete: ${totalCandidates} candidates`);
  return result;
}
