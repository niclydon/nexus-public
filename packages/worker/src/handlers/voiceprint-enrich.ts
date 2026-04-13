/**
 * Voice-Print Person Enrichment Handler
 *
 * Syncs person data from the voice-print system (aria database) into
 * Nexus's canonical person entities (aurora_social_identities).
 *
 * What it does:
 * 1. Match voice-print persons to aurora_social_identities by name
 * 2. Import phone number → person mappings from media_files.caller_phone
 * 3. Create new aurora_social_identities for voice persons not yet in Nexus
 * 4. Import voice profile centroids (256-dim embeddings)
 *
 * The voice-print tables live in the `aria` database, same PostgreSQL
 * instance. We connect via a separate pool using VOICE_DB_URL or
 * by modifying the DATABASE_URL to point at aria.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import pg from 'pg';

const logger = createLogger('voiceprint-enrich');

function getAriaPool(): pg.Pool {
  // Derive aria DB URL from nexus DATABASE_URL
  const nexusUrl = process.env.DATABASE_URL ?? '';
  const ariaUrl = process.env.VOICE_DB_URL ?? nexusUrl.replace(/\/nexus\b/, '/aria');
  return new pg.Pool({ connectionString: ariaUrl, max: 3 });
}

interface VoicePerson {
  id: number;
  name: string;
  notes: string | null;
}

interface PhoneMapping {
  person_id: number;
  person_name: string;
  phone: string;
}

export async function handleVoiceprintEnrich(job: TempoJob): Promise<Record<string, unknown>> {
  const nexusPool = getPool();
  const ariaPool = getAriaPool();

  try {
    logger.log('Starting voice-print person enrichment');
    await jobLog(job.id, 'Connecting to aria database for voice-print data');

    // 1. Load all voice-print persons
    const { rows: vpPersons } = await ariaPool.query<VoicePerson>(
      `SELECT id, name, notes FROM persons ORDER BY name`
    );
    logger.log(`Found ${vpPersons.length} voice-print persons`);
    await jobLog(job.id, `Found ${vpPersons.length} voice-print persons`);

    // 2. Load RELIABLE phone → person mappings from media_files
    // Only trust: 3+ unique calls from same number, not toll-free, top 2 per person
    const { rows: phoneMappings } = await ariaPool.query<PhoneMapping>(`
      WITH ranked AS (
        SELECT p.id as person_id, p.name as person_name, mf.caller_phone as phone,
          COUNT(DISTINCT mf.id) as call_count,
          ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY COUNT(DISTINCT mf.id) DESC) as rank
        FROM diarization_segments ds
        JOIN media_files mf ON mf.id = ds.media_file_id
        JOIN persons p ON p.id = ds.person_id
        WHERE mf.caller_phone IS NOT NULL
          AND ds.person_id IS NOT NULL
          AND p.name != 'Anonymous'
          AND mf.caller_phone !~ '^\\+1(800|888|877|866|855|844|833)'
        GROUP BY p.id, p.name, mf.caller_phone
        HAVING COUNT(DISTINCT mf.id) >= 3
      )
      SELECT person_id, person_name, phone FROM ranked WHERE rank <= 2
      ORDER BY person_name
    `);
    logger.log(`Found ${phoneMappings.length} reliable phone → person mappings (3+ calls, top 2 per person)`);

    // 3. Load existing aurora_social_identities for matching
    const { rows: existingPersons } = await nexusPool.query<{
      id: number;
      display_name: string;
    }>(`SELECT id, display_name FROM aurora_social_identities WHERE is_person = TRUE`);

    // Build name lookup (case-insensitive)
    const nameToNexusId = new Map<string, number>();
    for (const p of existingPersons) {
      nameToNexusId.set(p.display_name.toLowerCase(), p.id);
    }

    let matched = 0;
    let created = 0;
    let phonesAdded = 0;

    // 4. For each voice-print person, find or create a Nexus person
    const vpToNexusMap = new Map<number, number>(); // voiceprint_id → nexus_id

    for (const vp of vpPersons) {
      const nexusId = nameToNexusId.get(vp.name.toLowerCase());

      if (nexusId) {
        vpToNexusMap.set(vp.id, nexusId);
        matched++;
      } else {
        // Create new person in aurora_social_identities
        const { rows: [newPerson] } = await nexusPool.query<{ id: number }>(
          `INSERT INTO aurora_social_identities (display_name, is_person, notes)
           VALUES ($1, TRUE, $2)
           ON CONFLICT DO NOTHING
           RETURNING id`,
          [vp.name, vp.notes ? `From voice-print: ${vp.notes}` : 'Imported from voice-print']
        );

        if (newPerson) {
          vpToNexusMap.set(vp.id, newPerson.id);
          nameToNexusId.set(vp.name.toLowerCase(), newPerson.id);
          created++;
        }
      }
    }

    logger.log(`Person matching: ${matched} matched, ${created} created`);
    await jobLog(job.id, `Persons: ${matched} matched to existing, ${created} new created`);

    // 5. Import phone number → person mappings as identity links
    for (const mapping of phoneMappings) {
      const nexusId = vpToNexusMap.get(mapping.person_id);
      if (!nexusId) continue;

      // Normalize phone number to E.164
      let phone = mapping.phone.replace(/[\s\-\(\)\.]/g, '');
      if (/^[2-9]\d{9}$/.test(phone)) phone = '+1' + phone;
      if (/^1[2-9]\d{9}$/.test(phone)) phone = '+' + phone;

      if (phone.length < 7) continue;

      const result = await nexusPool.query(
        `INSERT INTO aurora_social_identity_links (identity_id, platform, identifier, identifier_type, match_confidence)
         VALUES ($1, 'phone', $2, 'phone', 0.85)
         ON CONFLICT DO NOTHING`,
        [nexusId, phone]
      );

      if (result.rowCount && result.rowCount > 0) phonesAdded++;
    }

    logger.log(`Phone numbers: ${phonesAdded} new links added`);
    await jobLog(job.id, `Phone links: ${phonesAdded} new`);

    // 6. Import Google Voice contact_name → phone mappings from aria
    let gvLinked = 0;
    try {
      const { rows: gvMappings } = await ariaPool.query<{
        contact_name: string;
        phone_number: string;
      }>(`
        SELECT DISTINCT contact_name, phone_number
        FROM aurora_raw_google_voice
        WHERE contact_name IS NOT NULL AND phone_number IS NOT NULL
          AND contact_name != '' AND LENGTH(phone_number) >= 7
        ORDER BY contact_name
      `);

      for (const gv of gvMappings) {
        const nexusId = nameToNexusId.get(gv.contact_name.toLowerCase());
        if (!nexusId) continue;

        let phone = gv.phone_number.replace(/[\s\-\(\)\.]/g, '');
        if (/^[2-9]\d{9}$/.test(phone)) phone = '+1' + phone;
        if (/^1[2-9]\d{9}$/.test(phone)) phone = '+' + phone;

        const result = await nexusPool.query(
          `INSERT INTO aurora_social_identity_links (identity_id, platform, identifier, identifier_type, match_confidence)
           VALUES ($1, 'google_voice', $2, 'phone', 0.9)
           ON CONFLICT DO NOTHING`,
          [nexusId, phone]
        );
        if (result.rowCount && result.rowCount > 0) gvLinked++;
      }

      logger.log(`Google Voice: ${gvLinked} phone links from contact names`);
      await jobLog(job.id, `Google Voice: ${gvLinked} phone links`);
    } catch (err) {
      logger.logVerbose('Google Voice enrichment skipped:', (err as Error).message);
    }

    const result = {
      voiceprint_persons: vpPersons.length,
      matched_to_existing: matched,
      new_persons_created: created,
      phone_links_added: phonesAdded,
      google_voice_links: gvLinked,
    };

    logger.log(`Complete: ${JSON.stringify(result)}`);
    await jobLog(job.id, `Complete: ${matched} matched, ${created} created, ${phonesAdded + gvLinked} phone links`);
    return result;

  } finally {
    await ariaPool.end();
  }
}
