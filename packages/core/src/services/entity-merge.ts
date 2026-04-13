/**
 * Entity Merge Service
 *
 * Shared merge/undo/link logic for all entity types. Used by both
 * the API routes (human-initiated merges) and the worker (auto-merges).
 * All merges run in transactions for safety.
 */
import { getPool } from '../db.js';
import { createLogger } from '../logger.js';

const logger = createLogger('entity-merge');

export interface MergeRequest {
  candidateId: string;
  keeperId: string;
  mergedId: string;
  entityType: string;
  fieldChoices?: Record<string, 'a' | 'b' | 'both'>;
  mergedBy?: string;
}

export interface MergeResult {
  success: boolean;
  auditId?: string;
  affectedRows?: Record<string, number>;
  error?: string;
}

// ── Person Merge ────────────────────────────────────────────

async function mergePerson(req: MergeRequest): Promise<MergeResult> {
  const pool = getPool();
  const client = await pool.connect();
  const affected: Record<string, number> = {};

  try {
    await client.query('BEGIN');

    const keeperId = parseInt(req.keeperId);
    const mergedId = parseInt(req.mergedId);

    // Snapshot both records
    const { rows: [keeper] } = await client.query(
      `SELECT * FROM aurora_social_identities WHERE id = $1`, [keeperId]
    );
    const { rows: [merged] } = await client.query(
      `SELECT * FROM aurora_social_identities WHERE id = $1`, [mergedId]
    );
    if (!keeper || !merged) throw new Error(`Person not found: keeper=${keeperId} merged=${mergedId}`);

    const { rows: keeperLinks } = await client.query(
      `SELECT * FROM aurora_social_identity_links WHERE identity_id = $1`, [keeperId]
    );
    const { rows: mergedLinks } = await client.query(
      `SELECT * FROM aurora_social_identity_links WHERE identity_id = $1`, [mergedId]
    );

    const snapshot = { keeper, merged, keeperLinks, mergedLinks };

    // 1. Move identity links
    const r1 = await client.query(
      `UPDATE aurora_social_identity_links SET identity_id = $1 WHERE identity_id = $2
       AND identifier NOT IN (SELECT identifier FROM aurora_social_identity_links WHERE identity_id = $1)`,
      [keeperId, mergedId]
    );
    affected.identity_links = r1.rowCount ?? 0;

    // Delete remaining (conflicting) links from merged
    await client.query(
      `DELETE FROM aurora_social_identity_links WHERE identity_id = $1`, [mergedId]
    );

    // 2. Move relationships
    const r2 = await client.query(
      `UPDATE aurora_relationships SET identity_id = $1 WHERE identity_id = $2`, [keeperId, mergedId]
    );
    affected.relationships = r2.rowCount ?? 0;

    // 3. Move sentiment
    const r3 = await client.query(
      `UPDATE aurora_sentiment SET person_id = $1 WHERE person_id = $2`, [keeperId, mergedId]
    );
    affected.sentiment = r3.rowCount ?? 0;

    // 4. Move KG entities
    const r4 = await client.query(
      `UPDATE knowledge_entities SET person_id = $1 WHERE person_id = $2`, [keeperId, mergedId]
    );
    affected.kg_entities = r4.rowCount ?? 0;

    // 5. Replace in photo person_ids arrays
    const r5 = await client.query(
      `UPDATE photo_metadata SET person_ids = array_replace(person_ids, $2, $1) WHERE $2 = ANY(person_ids)`,
      [keeperId, mergedId]
    );
    affected.photos = r5.rowCount ?? 0;

    // 6. Apply field choices to keeper
    if (req.fieldChoices) {
      const updates: string[] = [];
      const params: unknown[] = [keeperId];
      let idx = 2;

      for (const [field, choice] of Object.entries(req.fieldChoices)) {
        if (choice === 'b' && field in merged) {
          updates.push(`${field} = $${idx}`);
          params.push(merged[field]);
          idx++;
        }
      }

      if (updates.length > 0) {
        await client.query(
          `UPDATE aurora_social_identities SET ${updates.join(', ')}, updated_at = NOW() WHERE id = $1`,
          params
        );
      }
    }

    // 7. Soft-delete merged record
    await client.query(
      `UPDATE aurora_social_identities SET is_person = FALSE, notes = COALESCE(notes, '') || ' [merged into ' || $1 || ']', updated_at = NOW() WHERE id = $2`,
      [keeperId, mergedId]
    );

    // 8. Insert audit log
    const { rows: [audit] } = await client.query(
      `INSERT INTO merge_audit_log (merge_candidate_id, entity_type, keeper_id, merged_id, pre_merge_snapshot, field_choices, merged_by)
       VALUES ($1, 'person', $2, $3, $4, $5, $6) RETURNING id`,
      [req.candidateId, String(keeperId), String(mergedId), JSON.stringify(snapshot), JSON.stringify(req.fieldChoices ?? {}), req.mergedBy ?? 'owner']
    );

    // 9. Update candidate status
    await client.query(
      `UPDATE merge_candidates SET status = 'merged', decided_by = $1, decided_at = NOW() WHERE id = $2`,
      [req.mergedBy ?? 'owner', req.candidateId]
    );

    await client.query('COMMIT');
    logger.log(`Person merge: ${merged.display_name} → ${keeper.display_name} (${Object.entries(affected).map(([k, v]) => `${k}=${v}`).join(', ')})`);
    return { success: true, auditId: audit.id, affectedRows: affected };

  } catch (err) {
    await client.query('ROLLBACK');
    const msg = err instanceof Error ? err.message : String(err);
    logger.logMinimal(`Person merge failed: ${msg}`);
    return { success: false, error: msg };
  } finally {
    client.release();
  }
}

// ── Knowledge Entity Merge ──────────────────────────────────

async function mergeKnowledgeEntity(req: MergeRequest): Promise<MergeResult> {
  const pool = getPool();
  const client = await pool.connect();
  const affected: Record<string, number> = {};

  try {
    await client.query('BEGIN');

    // Snapshot
    const { rows: [keeper] } = await client.query(`SELECT * FROM knowledge_entities WHERE id = $1`, [req.keeperId]);
    const { rows: [merged] } = await client.query(`SELECT * FROM knowledge_entities WHERE id = $1`, [req.mergedId]);
    if (!keeper || !merged) throw new Error(`Entity not found`);

    const snapshot = { keeper, merged };

    // 1. Move fact links
    const r1 = await client.query(
      `INSERT INTO knowledge_fact_entities (fact_id, entity_id, role)
       SELECT fact_id, $1, role FROM knowledge_fact_entities WHERE entity_id = $2
       ON CONFLICT (fact_id, entity_id) DO NOTHING`,
      [req.keeperId, req.mergedId]
    );
    affected.fact_links = r1.rowCount ?? 0;

    // 2. Move conversation chunks
    const r2 = await client.query(
      `UPDATE knowledge_conversation_chunks SET entity_id = $1 WHERE entity_id = $2`,
      [req.keeperId, req.mergedId]
    );
    affected.conversation_chunks = r2.rowCount ?? 0;

    // 3. Replace in summary entity_ids arrays
    await client.query(
      `UPDATE knowledge_summaries SET entity_ids = array_replace(entity_ids, $2::uuid, $1::uuid) WHERE $2::uuid = ANY(entity_ids)`,
      [req.keeperId, req.mergedId]
    );

    // 4. Merge aliases
    const allAliases = new Set([
      ...(keeper.aliases || []),
      ...(merged.aliases || []),
      merged.canonical_name,
    ]);
    allAliases.delete(keeper.canonical_name);

    // 5. Update keeper
    const keepSummary = req.fieldChoices?.summary === 'b' ? merged.summary : keeper.summary;
    await client.query(
      `UPDATE knowledge_entities SET
         aliases = $2,
         summary = COALESCE($3, summary),
         fact_count = (SELECT COUNT(*) FROM knowledge_fact_entities WHERE entity_id = $1)::int,
         updated_at = NOW()
       WHERE id = $1`,
      [req.keeperId, Array.from(allAliases), keepSummary]
    );

    // 6. Delete merged
    await client.query(`DELETE FROM knowledge_entities WHERE id = $1`, [req.mergedId]);
    affected.deleted = 1;

    // 7. Audit log
    const { rows: [audit] } = await client.query(
      `INSERT INTO merge_audit_log (merge_candidate_id, entity_type, keeper_id, merged_id, pre_merge_snapshot, field_choices, merged_by)
       VALUES ($1, 'knowledge_entity', $2, $3, $4, $5, $6) RETURNING id`,
      [req.candidateId, req.keeperId, req.mergedId, JSON.stringify(snapshot), JSON.stringify(req.fieldChoices ?? {}), req.mergedBy ?? 'owner']
    );

    await client.query(
      `UPDATE merge_candidates SET status = 'merged', decided_by = $1, decided_at = NOW() WHERE id = $2`,
      [req.mergedBy ?? 'owner', req.candidateId]
    );

    await client.query('COMMIT');
    logger.log(`KG entity merge: "${merged.canonical_name}" → "${keeper.canonical_name}"`);
    return { success: true, auditId: audit.id, affectedRows: affected };

  } catch (err) {
    await client.query('ROLLBACK');
    return { success: false, error: err instanceof Error ? err.message : String(err) };
  } finally {
    client.release();
  }
}

// ── Photo Person Link (not a merge — links photo name to existing person) ──

async function linkPhotoPerson(req: MergeRequest): Promise<MergeResult> {
  const pool = getPool();
  const personId = parseInt(req.keeperId);
  const photoName = req.mergedId; // the raw name string from photos

  // Add person_id to all photos containing this name
  const result = await pool.query(
    `UPDATE photo_metadata SET person_ids = array_append(person_ids, $1)
     WHERE $2 = ANY(people) AND NOT ($1 = ANY(person_ids))`,
    [personId, photoName]
  );

  await pool.query(
    `UPDATE merge_candidates SET status = 'merged', decided_by = $1, decided_at = NOW() WHERE id = $2`,
    [req.mergedBy ?? 'owner', req.candidateId]
  );

  logger.log(`Photo person link: "${photoName}" → person ${personId} (${result.rowCount} photos)`);
  return { success: true, affectedRows: { photos: result.rowCount ?? 0 } };
}

// ── Contact → Person Link ──────────────────────────────────

async function linkContactPerson(req: MergeRequest): Promise<MergeResult> {
  const pool = getPool();
  const personId = parseInt(req.keeperId);
  const contactId = req.mergedId;

  // Link the contact to the person
  await pool.query(
    `UPDATE aurora_social_identities SET aria_contact_id = $1, updated_at = NOW() WHERE id = $2`,
    [contactId, personId]
  );

  // Add contact's phone/email as identity links
  const { rows: [contact] } = await pool.query(
    `SELECT phone_numbers, email_addresses FROM contacts WHERE id = $1`, [contactId]
  );

  if (contact) {
    const phones = (contact.phone_numbers || []) as string[];
    const emails = (contact.email_addresses || []) as string[];

    for (const phone of phones) {
      const normalized = phone.replace(/[\s\-\(\)\.]/g, '');
      if (normalized.length >= 7) {
        await pool.query(
          `INSERT INTO aurora_social_identity_links (identity_id, platform, identifier, identifier_type, match_confidence)
           VALUES ($1, 'phone', $2, 'phone', 0.9) ON CONFLICT DO NOTHING`,
          [personId, normalized]
        );
      }
    }
    for (const email of emails) {
      await pool.query(
        `INSERT INTO aurora_social_identity_links (identity_id, platform, identifier, identifier_type, match_confidence)
         VALUES ($1, 'email', $2, 'email', 0.9) ON CONFLICT DO NOTHING`,
        [personId, email.toLowerCase()]
      );
    }
  }

  await pool.query(
    `UPDATE merge_candidates SET status = 'merged', decided_by = $1, decided_at = NOW() WHERE id = $2`,
    [req.mergedBy ?? 'owner', req.candidateId]
  );

  logger.log(`Contact → Person link: contact ${contactId} → person ${personId}`);
  return { success: true };
}

// ── Undo ────────────────────────────────────────────────────

export async function undoMerge(auditId: string): Promise<MergeResult> {
  const pool = getPool();

  const { rows: [audit] } = await pool.query(
    `SELECT * FROM merge_audit_log WHERE id = $1 AND undone_at IS NULL`, [auditId]
  );

  if (!audit) return { success: false, error: 'Audit record not found or already undone' };

  const daysSince = (Date.now() - new Date(audit.merged_at).getTime()) / 86400000;
  if (daysSince > 30) return { success: false, error: 'Undo window expired (30 days)' };

  if (audit.entity_type === 'person') {
    const snapshot = audit.pre_merge_snapshot as { keeper: Record<string, unknown>; merged: Record<string, unknown>; mergedLinks: Array<Record<string, unknown>> };

    // Restore the soft-deleted person
    await pool.query(
      `UPDATE aurora_social_identities SET is_person = TRUE, notes = REPLACE(COALESCE(notes, ''), ' [merged into ' || $1 || ']', ''), updated_at = NOW() WHERE id = $2`,
      [audit.keeper_id, audit.merged_id]
    );

    // Restore original identity links
    for (const link of (snapshot.mergedLinks || [])) {
      await pool.query(
        `INSERT INTO aurora_social_identity_links (identity_id, platform, identifier, identifier_type, match_confidence)
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`,
        [audit.merged_id, link.platform, link.identifier, link.identifier_type, link.match_confidence]
      );
    }

    // Note: We don't reverse every FK change — that's complex and error-prone.
    // The restored person will re-accumulate data on the next aurora-weekly run.
  }

  // Mark as undone
  await pool.query(`UPDATE merge_audit_log SET undone_at = NOW() WHERE id = $1`, [auditId]);
  await pool.query(
    `UPDATE merge_candidates SET status = 'pending', decided_by = NULL, decided_at = NULL WHERE id = $1`,
    [audit.merge_candidate_id]
  );

  logger.log(`Undo merge: ${audit.entity_type} ${audit.merged_id} restored`);
  return { success: true };
}

// ── Dispatch ────────────────────────────────────────────────

export async function executeMerge(req: MergeRequest): Promise<MergeResult> {
  switch (req.entityType) {
    case 'person': return mergePerson(req);
    case 'knowledge_entity': return mergeKnowledgeEntity(req);
    case 'photo_person': return linkPhotoPerson(req);
    case 'contact_person': return linkContactPerson(req);
    default: return { success: false, error: `Unknown entity type: ${req.entityType}` };
  }
}

// ── Dismiss / Defer ─────────────────────────────────────────

export async function dismissCandidate(candidateId: string): Promise<void> {
  const pool = getPool();
  const { rows: [candidate] } = await pool.query(
    `UPDATE merge_candidates SET skip_count = skip_count + 1,
       status = CASE WHEN skip_count >= 2 THEN 'dismissed' ELSE status END,
       decided_at = CASE WHEN skip_count >= 2 THEN NOW() ELSE decided_at END,
       decided_by = CASE WHEN skip_count >= 2 THEN 'owner' ELSE decided_by END
     WHERE id = $1 RETURNING status, skip_count`,
    [candidateId]
  );
  logger.logVerbose(`Dismiss candidate ${candidateId}: skip_count=${candidate?.skip_count}, status=${candidate?.status}`);
}

export async function deferCandidate(candidateId: string): Promise<void> {
  const pool = getPool();
  await pool.query(
    `UPDATE merge_candidates SET status = 'deferred', decided_at = NOW() WHERE id = $1`,
    [candidateId]
  );
}
