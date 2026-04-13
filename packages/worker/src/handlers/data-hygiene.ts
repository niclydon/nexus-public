/**
 * Data Hygiene Handler
 *
 * LLM-powered data quality auditor. Scans tables for inconsistencies,
 * misclassified entries, and noise — then fixes them.
 *
 * Phase 1: aurora_social_identities — classify is_person via LLM
 * Phase 1b: aurora_social_identities RESTORE — LLM-flip is_person=false back
 *            to true for entries misclassified by Phase 1. See
 *            docs/is-person-restoration.md for design + safety rails.
 * Phase 2: knowledge_entities — dedup, merge, type correction
 * Phase 3: contacts — dedup, merge with social identities
 *
 * Uses classification tier (cheap, fast) for yes/no decisions.
 * Processes in batches to stay within token limits.
 *
 * Payload options:
 *   target?: string     — which table/phase to audit (default: 'social-identities')
 *   batch_size?: number — entries per LLM call (default: 50)
 *   dry_run?: boolean   — log findings without making changes (default: false)
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import { routeRequest } from '../lib/llm/index.js';

const logger = createLogger('data-hygiene');

export async function handleDataHygiene(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as {
    target?: string;
    batch_size?: number;
    dry_run?: boolean;
  };

  const target = payload.target ?? 'social-identities';
  const batchSize = payload.batch_size ?? 50;
  const dryRun = payload.dry_run ?? false;

  logger.log(`Data hygiene: target=${target}, batch_size=${batchSize}, dry_run=${dryRun}`);
  await jobLog(job.id, `Starting data hygiene: ${target}`);

  switch (target) {
    case 'social-identities':
      return auditSocialIdentities(job, batchSize, dryRun);
    case 'social-identities-restore':
      return restoreSocialIdentities(job, batchSize, dryRun, payload as RestorePayload);
    case 'knowledge-entities':
      return auditKnowledgeEntities(job, batchSize, dryRun);
    case 'auto-merge-contacts':
      return autoMergeHighConfidence(job);
    default:
      throw new Error(`Unknown data-hygiene target: ${target}`);
  }
}

// ---------------------------------------------------------------------------
// Phase 1: Social Identity Classification
// ---------------------------------------------------------------------------

interface IdentityRow {
  id: number;
  display_name: string;
  identifiers: string[];
  platforms: string[];
}

interface RestorePayload {
  target?: string;
  batch_size?: number;
  dry_run?: boolean;
  /** Max number of batches to process this run. Default 60 (= 3000 entries at batch_size 50). */
  max_batches?: number;
  /** If an LLM batch wants to flip more than this fraction of entries, skip it as a safety rail. Default 0.7. */
  safety_rail_max_flip_rate?: number;
}

interface RestoreCandidate {
  id: number;
  display_name: string;
  identifiers: string[];
  platforms: string[];
  sent_to_nic: number;
  received_from_nic: number;
  sample_messages: string[];
  first_seen: string | null;
  last_seen: string | null;
}

async function auditSocialIdentities(
  job: TempoJob,
  batchSize: number,
  dryRun: boolean,
): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Get unaudited identities still marked as person.
  // SKIP rows with strong evidence of personhood:
  //   - photo_metadata.person_ids reference (someone tagged them in a photo)
  //   - aria_contact_id (linked to a real iCloud contact)
  //   - >= 3 distinct platforms (rich cross-channel presence)
  // The LLM was previously flipping these to false, mass-deleting real people.
  const { rows: identities } = await pool.query<IdentityRow>(`
    SELECT si.id, si.display_name,
      array_agg(DISTINCT l.identifier) as identifiers,
      array_agg(DISTINCT l.platform) as platforms
    FROM aurora_social_identities si
    LEFT JOIN aurora_social_identity_links l ON l.identity_id = si.id
    WHERE si.is_person = TRUE
      AND si.aria_contact_id IS NULL
      AND NOT EXISTS (
        SELECT 1 FROM photo_metadata pm WHERE si.id = ANY(pm.person_ids)
      )
    GROUP BY si.id, si.display_name
    HAVING count(DISTINCT l.platform) < 3
    ORDER BY si.id
  `);

  if (identities.length === 0) {
    logger.log('No identities to audit');
    return { target: 'social-identities', audited: 0, flagged: 0 };
  }

  logger.log(`Auditing ${identities.length} identities in batches of ${batchSize}`);
  await jobLog(job.id, `Auditing ${identities.length} social identities`);

  // Hard-skip obvious human name patterns BEFORE the LLM ever sees them. The
  // LLM was misclassifying "Joshua.Malenick" / "andrea.curley" username-style
  // names as non-persons because of the dot. These regexes catch the patterns
  // we know are human and prevent any chance of accidental flipping.
  const HUMAN_NAME_PATTERNS = [
    /^[A-Z][a-z]+\s+[A-Z][a-z]+/,                      // "First Last"
    /^[A-Z][a-z]+\.[A-Z]?[a-z]+$/,                     // "First.Last" / "First.last"
    /^[a-z][a-z]+\.[a-z]+$/,                           // "first.last"
    /^[A-Z][a-z]+_[A-Z]?[a-z]+$/,                      // "First_Last"
    /^[A-Z][a-z]+\s+[A-Z]\.\s+[A-Z][a-z]+$/,           // "First M. Last"
  ];
  function looksLikeHumanName(name: string): boolean {
    return HUMAN_NAME_PATTERNS.some((p) => p.test(name));
  }

  const preFiltered = identities.filter((id) => !looksLikeHumanName(id.display_name));
  if (preFiltered.length < identities.length) {
    logger.log(`Pre-filter: skipped ${identities.length - preFiltered.length} obvious human name patterns`);
  }

  let totalFlagged = 0;
  let totalAudited = 0;

  for (let i = 0; i < preFiltered.length; i += batchSize) {
    const batch = preFiltered.slice(i, i + batchSize);

    // Format batch for LLM
    const entries = batch.map((id, idx) => {
      const ids = (id.identifiers || []).filter(Boolean).join(', ');
      return `${idx + 1}. "${id.display_name}" — identifiers: [${ids}] platforms: [${(id.platforms || []).filter(Boolean).join(', ')}]`;
    }).join('\n');

    const result = await routeRequest({
      handler: 'data-hygiene',
      taskTier: 'classification',
      systemPrompt: `You are a data quality auditor for a personal contact database. You must identify entries that are CLEARLY NOT a real person.

NOT A PERSON means the entry is OBVIOUSLY one of:
- An automated email sender (noreply@, notifications@, alerts@, newsletter@, do-not-reply@)
- A brand or company name without any human identifier (Amazon, Uber, Netflix, Bank of America, United Airlines, Capital One)
- A system/service account (helpdesk, support, admin, postmaster, root)
- A hash-based identifier with no human name (random alphanumeric strings, ticket IDs, marketplace seller codes)
- A pure phone number with no name attached
- A bot or automated system clearly named as such

REAL PERSON includes ANYTHING that:
- Has a first name (even alone like "Erin" or "Brodey")
- Has a recognizable human name pattern, INCLUDING:
    * "FirstName LastName" → "Joshua Malenick"
    * "FirstName.LastName" → "Joshua.Malenick" (Notion/Slack/email handle format — STILL A PERSON)
    * "firstname.lastname" → "joshua.malenick" (lowercase username — STILL A PERSON)
    * "firstname_lastname" → "joshua_malenick" (STILL A PERSON)
    * "FirstName M LastName" or "FirstName Middle LastName"
- Could plausibly be a friend, family member, coworker, or acquaintance — even if you don't know who they are
- Has a nickname or partial name

The dot, underscore, or period in a name is NOT a sign of being a non-person. It's a username separator. People are routinely stored with names like "andrea.curley" or "Mike_Johnson" — these are real humans.

CRITICAL: When in doubt, classify as REAL PERSON. False negatives (real people being flipped to non-person) destroy data permanently. False positives (a brand staying as a person) are easily reverted later. Err strongly on the side of keeping entries as people.

Respond with ONLY a JSON array of the entry numbers that are CLEARLY NOT persons. Example: [3, 7]
If you're not certain, respond with: []`,
      userMessage: `Classify each entry:\n\n${entries}`,
      maxTokens: 500,
      useBatch: true,
    });

    // Parse response — extract array of indices
    const notPersonIds = parseClassificationResponse(result.text, batch.length);

    if (notPersonIds.length > 0) {
      const flaggedNames = notPersonIds.map(idx => batch[idx - 1]?.display_name).filter(Boolean);
      logger.log(`Batch ${Math.floor(i / batchSize) + 1}: flagged ${notPersonIds.length} non-persons: ${flaggedNames.slice(0, 5).join(', ')}${flaggedNames.length > 5 ? '...' : ''}`);

      if (!dryRun) {
        const dbIds = notPersonIds
          .map(idx => batch[idx - 1]?.id)
          .filter((id): id is number => id != null);

        if (dbIds.length > 0) {
          await pool.query(
            `UPDATE aurora_social_identities SET is_person = FALSE WHERE id = ANY($1::int[])`,
            [dbIds],
          );
        }
      }

      totalFlagged += notPersonIds.length;
    }

    totalAudited += batch.length;

    if ((i / batchSize) % 5 === 4) {
      await jobLog(job.id, `Progress: ${totalAudited}/${preFiltered.length} audited, ${totalFlagged} flagged`);
    }
  }

  const result = {
    target: 'social-identities',
    audited: totalAudited,
    flagged: totalFlagged,
    dry_run: dryRun,
  };

  logger.log(`Complete: ${JSON.stringify(result)}`);
  await jobLog(job.id, `Complete: ${totalAudited} audited, ${totalFlagged} flagged as non-person${dryRun ? ' (DRY RUN)' : ''}`);
  return result;
}

function parseClassificationResponse(text: string, maxIdx: number): number[] {
  // Extract JSON array from response — handle markdown code blocks too
  const cleaned = text.replace(/```json?\s*/g, '').replace(/```/g, '').trim();

  // Find array in response
  const arrayMatch = cleaned.match(/\[[\d\s,]*\]/);
  if (!arrayMatch) return [];

  try {
    const parsed = JSON.parse(arrayMatch[0]) as number[];
    // Validate: must be numbers within range
    return parsed.filter(n => typeof n === 'number' && n >= 1 && n <= maxIdx);
  } catch {
    logger.logMinimal(`Failed to parse classification response: ${cleaned.slice(0, 200)}`);
    return [];
  }
}

// ---------------------------------------------------------------------------
// Phase 1b: Social Identity RESTORATION
// ---------------------------------------------------------------------------
//
// Counterpart to auditSocialIdentities. auditSocialIdentities scans is_person=true
// entries and flips some to false; this scans is_person=false entries and flips
// some back to true for the ones that were misclassified.
//
// Design + rationale: docs/is-person-restoration.md
//
// Safety rails baked in:
//   * dry_run default is TRUE — caller must explicitly opt into writes
//   * skip any identity with 'manual_review' marker in notes
//   * skip batches where the LLM wants to flip >70 % of entries (the LLM
//     probably misread the prompt — better to investigate than restore blindly)
//   * cap total batches per job run to prevent runaway cost/rate-limit issues
//   * every flip writes an audit note with a rollback marker

async function restoreSocialIdentities(
  job: TempoJob,
  batchSize: number,
  dryRun: boolean,
  payload: RestorePayload,
): Promise<Record<string, unknown>> {
  const pool = getPool();
  const maxBatches = payload.max_batches ?? 60;
  const safetyRail = payload.safety_rail_max_flip_rate ?? 0.7;
  // Default for this phase is dry_run=true (explicit opt-in to writes).
  // Respect the generic dry_run flag but also honor absence-of-flag as dryRun.
  const effectiveDryRun = payload.dry_run === false ? false : true;

  logger.log(
    `Restore: batch_size=${batchSize}, max_batches=${maxBatches}, ` +
    `safety_rail=${safetyRail}, dry_run=${effectiveDryRun}`,
  );
  await jobLog(job.id, `Starting social-identities restoration (dry_run=${effectiveDryRun})`);

  // Pull candidates, ordered by comm volume (highest ROI first). Only consider
  // identities that actually have comm rows — classifying identities with zero
  // comm volume is a waste of LLM budget (and hard to judge from display name
  // alone).
  const { rows: candidates } = await pool.query<RestoreCandidate>(`
    WITH identity_comm AS (
      SELECT l.identity_id,
             COUNT(*) FILTER (WHERE c.direction = 'sent')     AS sent_to_nic,
             COUNT(*) FILTER (WHERE c.direction = 'received') AS received_from_nic,
             MIN(c.timestamp) AS first_seen,
             MAX(c.timestamp) AS last_seen
      FROM aurora_social_identity_links l
      JOIN aurora_unified_communication c ON c.contact_identifier = l.identifier
      GROUP BY l.identity_id
    ),
    sample_msgs AS (
      SELECT DISTINCT ON (l.identity_id, c.source_id)
             l.identity_id,
             LEFT(COALESCE(c.content_text, ''), 200) AS msg
      FROM aurora_social_identity_links l
      JOIN aurora_unified_communication c ON c.contact_identifier = l.identifier
      WHERE c.content_text IS NOT NULL AND length(c.content_text) > 0
      ORDER BY l.identity_id, c.source_id, c.timestamp DESC
    ),
    samples_agg AS (
      SELECT identity_id, (ARRAY_AGG(msg ORDER BY identity_id))[1:3] AS sample_messages
      FROM sample_msgs GROUP BY identity_id
    )
    SELECT si.id,
           si.display_name,
           COALESCE(array_agg(DISTINCT l.identifier) FILTER (WHERE l.identifier IS NOT NULL), '{}') AS identifiers,
           COALESCE(array_agg(DISTINCT l.platform)   FILTER (WHERE l.platform   IS NOT NULL), '{}') AS platforms,
           COALESCE(ic.sent_to_nic, 0)::int      AS sent_to_nic,
           COALESCE(ic.received_from_nic, 0)::int AS received_from_nic,
           COALESCE(sa.sample_messages, '{}')     AS sample_messages,
           ic.first_seen::text AS first_seen,
           ic.last_seen::text  AS last_seen
    FROM aurora_social_identities si
    LEFT JOIN aurora_social_identity_links l ON l.identity_id = si.id
    LEFT JOIN identity_comm ic  ON ic.identity_id = si.id
    LEFT JOIN samples_agg  sa   ON sa.identity_id = si.id
    WHERE si.is_person = false
      AND (si.notes IS NULL OR si.notes NOT ILIKE '%manual_review%')
      AND ic.identity_id IS NOT NULL
    GROUP BY si.id, si.display_name, ic.sent_to_nic, ic.received_from_nic,
             sa.sample_messages, ic.first_seen, ic.last_seen
    ORDER BY (COALESCE(ic.sent_to_nic, 0) + COALESCE(ic.received_from_nic, 0)) DESC
    LIMIT $1
  `, [maxBatches * batchSize]);

  if (candidates.length === 0) {
    logger.log('Restore: no candidates');
    return { target: 'social-identities-restore', audited: 0, restored: 0 };
  }

  logger.log(`Restore: ${candidates.length} candidates queued in batches of ${batchSize}`);
  await jobLog(job.id, `Restore: ${candidates.length} candidates queued`);

  let totalAudited = 0;
  let totalRestored = 0;
  let batchIdx = 0;
  const skippedBatches: number[] = [];

  for (let i = 0; i < candidates.length; i += batchSize) {
    batchIdx++;
    if (batchIdx > maxBatches) break;

    const batch = candidates.slice(i, i + batchSize);

    // Format batch for LLM. Each entry gets name, identifiers, platforms,
    // comm volume, first/last seen, and up to 3 sample messages truncated.
    const entries = batch.map((c, idx) => {
      const identifiers = (c.identifiers || []).filter(Boolean).slice(0, 4).join(', ');
      const platforms   = (c.platforms || []).filter(Boolean).join(', ');
      const samples = (c.sample_messages || [])
        .filter(Boolean)
        .slice(0, 3)
        .map((m) => `    > ${m.replace(/\s+/g, ' ').trim()}`)
        .join('\n');
      return `${idx + 1}. "${c.display_name}"\n` +
             `   identifiers: [${identifiers}]\n` +
             `   platforms: [${platforms}]\n` +
             `   volume: sent_to_nic=${c.sent_to_nic}, received_from_nic=${c.received_from_nic}\n` +
             `   first_seen: ${c.first_seen || 'unknown'}, last_seen: ${c.last_seen || 'unknown'}\n` +
             (samples ? `   samples:\n${samples}` : '   samples: (none)');
    }).join('\n\n');

    const result = await routeRequest({
      handler: 'data-hygiene-restore',
      taskTier: 'classification',
      systemPrompt: `You are restoring mistakenly-flipped entries in a personal contact database. Every entry below was marked NOT-A-PERSON by a prior automated pass, but many were real people. Your job: identify which ones should be flipped BACK to "is a real person".

REAL PERSON indicators:
- Has a recognizable human first or full name (including "First.Last", "first_last", username styles)
- Has bidirectional, conversational exchanges with the user (both sent AND received)
- Sample messages read like personal messages: casual tone, personal content, "hey", "what's up", names, emojis
- Short-form personal conversations

NOT A PERSON (leave alone):
- Display name contains: noreply, no-reply, notifications, alerts, support, help, system, admin, automated, do-not-reply, campaigns, newsletter, marketing, receipts, orders, shipping, billing
- Brand / business / service: "Amazon", "Uber", "Netflix", "Chase", "United Airlines", "Apple Inc", "Meta for Business", any "<Brand> Customer Service"
- Email domains like @email.foo, @notify.foo, @alerts.foo, @campaigns.foo, @communications.foo
- Toll-free US numbers (+1800, +1833, +1844, +1855, +1866, +1877, +1888) or short codes
- Display name is a domain fragment, ticket id, chat-room id (chat<digits>), or random alphanumeric
- Sample messages are transactional only: "Your order", "Payment received", "Verification code", "Click to unsubscribe", "Your package"

CRITICAL:
- When in doubt, LEAVE AS NOT-A-PERSON. We can flip more later; a wrongly-flipped bot pollutes every relationship analysis downstream.
- Do NOT flip based on comm volume alone — newsletters have high volume. Weight sample messages heavily.
- Ignore any "tier" or "relationship_type" metadata if present — the prior pass poisoned these labels on bot rows.

Respond with ONLY a JSON array of entry numbers that should be RESTORED to real-person status. Example: [1, 4, 7].
If nothing in this batch should be restored: [].`,
      userMessage: `Classify each entry:\n\n${entries}`,
      maxTokens: 500,
      useBatch: false, // real-time for faster feedback during initial runs
    });

    const restoreIds = parseClassificationResponse(result.text, batch.length);
    const flipRate = restoreIds.length / batch.length;

    // Safety rail: if the LLM wants to flip too high a fraction, skip the batch.
    if (flipRate > safetyRail) {
      logger.logMinimal(
        `Restore batch ${batchIdx}: safety rail tripped (${restoreIds.length}/${batch.length} = ${(flipRate * 100).toFixed(0)}% flip rate > ${(safetyRail * 100).toFixed(0)}%). Skipping.`,
      );
      await jobLog(
        job.id,
        `Batch ${batchIdx}: SAFETY RAIL tripped (${(flipRate * 100).toFixed(0)}% flip rate > ${(safetyRail * 100).toFixed(0)}%) — skipped`,
      );
      skippedBatches.push(batchIdx);
      totalAudited += batch.length;
      continue;
    }

    if (restoreIds.length > 0) {
      const flaggedNames = restoreIds.map((idx) => batch[idx - 1]?.display_name).filter(Boolean);
      logger.log(
        `Restore batch ${batchIdx}: flagged ${restoreIds.length} for restore: ${flaggedNames.slice(0, 5).join(', ')}${flaggedNames.length > 5 ? '…' : ''}`,
      );

      if (!effectiveDryRun) {
        const dbIds = restoreIds
          .map((idx) => batch[idx - 1]?.id)
          .filter((id): id is number => id != null);

        if (dbIds.length > 0) {
          await pool.query(
            `UPDATE aurora_social_identities
             SET is_person = true,
                 updated_at = NOW(),
                 notes = COALESCE(notes || E'\\n', '') ||
                         '[${new Date().toISOString().slice(0, 10)}] is_person restored via data-hygiene social-identities-restore (batch ' || $2 || ')'
             WHERE id = ANY($1::int[])`,
            [dbIds, batchIdx],
          );
          totalRestored += dbIds.length;
        }
      } else {
        totalRestored += restoreIds.length; // count for reporting even in dry run
      }
    }

    totalAudited += batch.length;

    if (batchIdx % 5 === 0) {
      await jobLog(
        job.id,
        `Progress: ${totalAudited}/${candidates.length} audited, ${totalRestored} ${effectiveDryRun ? 'would-restore' : 'restored'}`,
      );
    }
  }

  const result = {
    target: 'social-identities-restore',
    audited: totalAudited,
    restored: totalRestored,
    skipped_batches: skippedBatches,
    dry_run: effectiveDryRun,
  };

  logger.log(`Restore complete: ${JSON.stringify(result)}`);
  await jobLog(
    job.id,
    `Restore complete: ${totalAudited} audited, ${totalRestored} ${effectiveDryRun ? 'would-restore' : 'restored'}${skippedBatches.length ? `, ${skippedBatches.length} batches skipped by safety rail` : ''}`,
  );
  return result;
}

// ---------------------------------------------------------------------------
// Phase 2: Knowledge Entity Dedup (merge-based, no data loss)
// ---------------------------------------------------------------------------

async function auditKnowledgeEntities(
  job: TempoJob,
  _batchSize: number,
  dryRun: boolean,
): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Find duplicate canonical_names — keep the one with the most facts
  const { rows: dupes } = await pool.query<{
    canonical_name: string;
    count: number;
  }>(`
    SELECT canonical_name, COUNT(*)::int as count
    FROM knowledge_entities
    WHERE fact_count >= 0
    GROUP BY canonical_name
    HAVING COUNT(*) > 1
    ORDER BY count DESC
  `);

  if (dupes.length === 0) {
    logger.log('No duplicate knowledge entities found');
    return { target: 'knowledge-entities', dupes_found: 0, merged: 0 };
  }

  logger.log(`Found ${dupes.length} duplicate entity names to merge`);
  await jobLog(job.id, `Found ${dupes.length} duplicate entity names`);

  let merged = 0;

  for (const dupe of dupes) {
    // Get all entities with this name, ordered by fact_count desc (keep the richest)
    const { rows: entities } = await pool.query<{
      id: string;
      entity_type: string;
      fact_count: number;
      summary: string | null;
      aliases: string[];
    }>(`
      SELECT id, entity_type, fact_count, summary, aliases
      FROM knowledge_entities
      WHERE canonical_name = $1 AND fact_count >= 0
      ORDER BY fact_count DESC, created_at ASC
    `, [dupe.canonical_name]);

    if (entities.length <= 1) continue;

    const keeper = entities[0]; // highest fact_count
    const duplicates = entities.slice(1);

    if (dryRun) {
      logger.logVerbose(`Would merge ${duplicates.length} dupes of "${dupe.canonical_name}" into ${keeper.id}`);
      merged += duplicates.length;
      continue;
    }

    for (const dup of duplicates) {
      // Move all fact links from duplicate to keeper
      // ON CONFLICT: if the fact is already linked to keeper, just drop the duplicate link
      await pool.query(`
        INSERT INTO knowledge_fact_entities (fact_id, entity_id, role)
        SELECT fact_id, $1, role FROM knowledge_fact_entities WHERE entity_id = $2
        ON CONFLICT (fact_id, entity_id) DO NOTHING
      `, [keeper.id, dup.id]);

      // Merge aliases: add duplicate's name and aliases to keeper
      const newAliases = new Set([
        ...(keeper.aliases || []),
        dupe.canonical_name,  // the name itself (already on keeper, but dedup via SET)
        ...(dup.aliases || []),
      ]);
      // Remove the keeper's canonical_name from aliases (it's the main name)
      newAliases.delete(dupe.canonical_name);

      await pool.query(`
        UPDATE knowledge_entities
        SET aliases = $2,
            fact_count = (SELECT COUNT(*) FROM knowledge_fact_entities WHERE entity_id = $1)::int,
            summary = CASE WHEN summary IS NULL THEN $3 ELSE summary END,
            updated_at = NOW()
        WHERE id = $1
      `, [keeper.id, Array.from(newAliases), dup.summary]);

      // Re-point conversation chunks to keeper before deleting
      await pool.query(
        `UPDATE knowledge_conversation_chunks SET entity_id = $1 WHERE entity_id = $2`,
        [keeper.id, dup.id],
      );

      // Replace dangling entity_id in knowledge_summaries UUID arrays
      await pool.query(
        `UPDATE knowledge_summaries
         SET entity_ids = array_replace(entity_ids, $2, $1)
         WHERE $2 = ANY(entity_ids)`,
        [keeper.id, dup.id],
      );

      // Delete the duplicate entity (cascade removes its now-empty fact_entities rows)
      await pool.query(`DELETE FROM knowledge_entities WHERE id = $1`, [dup.id]);

      merged++;
    }

    if (merged % 50 === 0 && merged > 0) {
      await jobLog(job.id, `Progress: ${merged} duplicates merged`);
    }
  }

  const result = {
    target: 'knowledge-entities',
    dupes_found: dupes.length,
    merged,
    dry_run: dryRun,
  };

  logger.log(`Complete: ${JSON.stringify(result)}`);
  await jobLog(job.id, `Complete: ${dupes.length} duplicate names, ${merged} entities merged${dryRun ? ' (DRY RUN)' : ''}`);
  return result;
}

// ---------------------------------------------------------------------------
// Auto-merge high-confidence candidates (called by Pipeline agent)
// ---------------------------------------------------------------------------

async function autoMergeHighConfidence(
  job: TempoJob,
): Promise<Record<string, unknown>> {
  const pool = getPool();
  const MIN_AUTO_CONFIDENCE = 0.9;

  // Auto-link contact_person candidates (name + shared identifier = guaranteed match)
  // Use >= MIN-0.001 to dodge float4 precision: confidence is stored as `real`,
  // and 0.9 → ~0.89999998 which fails a strict `>= 0.9` comparison.
  const { rows: contactCandidates } = await pool.query(`
    SELECT id, record_a_id, record_b_id FROM merge_candidates
    WHERE entity_type = 'contact_person' AND status = 'pending' AND confidence >= $1
  `, [MIN_AUTO_CONFIDENCE - 0.001]);

  let contactsLinked = 0;
  for (const mc of contactCandidates) {
    try {
      // Link contact to person
      await pool.query(
        `UPDATE aurora_social_identities SET aria_contact_id = $1::uuid, updated_at = NOW()
         WHERE id = $2::int AND aria_contact_id IS NULL`,
        [mc.record_b_id, mc.record_a_id]
      );

      // Add phone/email from contact as identity links
      const { rows: [contact] } = await pool.query(
        `SELECT phone_numbers, email_addresses FROM contacts WHERE id = $1::uuid`, [mc.record_b_id]
      );
      if (contact) {
        const phones = contact.phone_numbers as unknown[];
        const emails = contact.email_addresses as unknown[];
        if (Array.isArray(phones)) {
          for (const p of phones) {
            const phone = String(p).replace(/[\s\-\(\)\.\"]/g, '');
            if (phone.length >= 7) {
              await pool.query(
                `INSERT INTO aurora_social_identity_links (identity_id, platform, identifier, identifier_type, match_confidence)
                 VALUES ($1::int, 'phone', $2, 'phone', 0.9) ON CONFLICT DO NOTHING`,
                [mc.record_a_id, phone]
              );
            }
          }
        }
        if (Array.isArray(emails)) {
          for (const e of emails) {
            await pool.query(
              `INSERT INTO aurora_social_identity_links (identity_id, platform, identifier, identifier_type, match_confidence)
               VALUES ($1::int, 'email', $2, 'email', 0.9) ON CONFLICT DO NOTHING`,
              [mc.record_a_id, String(e).toLowerCase()]
            );
          }
        }
      }

      await pool.query(
        `UPDATE merge_candidates SET status = 'merged', decided_by = 'system', decided_at = NOW() WHERE id = $1`,
        [mc.id]
      );
      contactsLinked++;
    } catch (err) {
      logger.logVerbose(`Auto-link failed for candidate ${mc.id}:`, (err as Error).message);
    }
  }

  // Auto-merge person duplicates at very high confidence (0.95+).
  // Use 0.949 instead of 0.95 — confidence is stored as float4 and 0.95
  // becomes ~0.94999998807, failing a strict `>= 0.95` check. This silent
  // off-by-precision was hiding every shared-identifier auto-merge.
  let personsMerged = 0;
  const { rows: personCandidates } = await pool.query(`
    SELECT id, record_a_id, record_b_id FROM merge_candidates
    WHERE entity_type = 'person' AND status = 'pending' AND confidence >= 0.949
  `);

  for (const mc of personCandidates) {
    try {
      const { entityMerge } = await import('@nexus/core');
      const result = await entityMerge.executeMerge({
        candidateId: mc.id,
        keeperId: String(mc.record_a_id),
        mergedId: String(mc.record_b_id),
        entityType: 'person',
        mergedBy: 'system',
      });
      if (result.success) personsMerged++;
    } catch (err) {
      logger.logVerbose(`Auto-merge failed for candidate ${mc.id}:`, (err as Error).message);
    }
  }

  const result = { contacts_linked: contactsLinked, persons_merged: personsMerged };
  logger.log(`Auto-merge: ${contactsLinked} contacts linked, ${personsMerged} persons merged`);
  await jobLog(job.id, `Auto-merge: ${contactsLinked} contacts, ${personsMerged} persons`);
  return result;
}
