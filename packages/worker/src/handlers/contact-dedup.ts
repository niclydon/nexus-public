/**
 * Contact deduplication — scans for duplicates, auto-merges high-confidence pairs.
 */
import { getPool, createLogger } from '@nexus/core';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/contact-dedup');

interface ContactForDedup {
  id: string;
  display_name: string;
  given_name: string | null;
  family_name: string | null;
  phone_numbers: string[];
  email_addresses: string[];
  relationship: string | null;
  context: string | null;
}

interface DuplicatePair {
  contact_a_id: string;
  contact_b_id: string;
  similarity_score: number;
  match_signals: Record<string, unknown>;
}

function normalizePhone(phone: string): string {
  return phone.replace(/[^0-9]/g, '').slice(-10);
}

function nameSimilarity(a: string, b: string): number {
  const la = a.toLowerCase().trim();
  const lb = b.toLowerCase().trim();
  if (la === lb) return 1.0;
  if (la.includes(lb) || lb.includes(la)) return 0.8;
  const maxLen = Math.max(la.length, lb.length);
  if (maxLen === 0) return 0;
  const m = la.length, n = lb.length;
  const dp: number[][] = Array.from({ length: m + 1 }, () => Array(n + 1).fill(0));
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] = Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1] + (la[i-1]===lb[j-1]?0:1));
    }
  }
  return Math.max(0, 1 - dp[m][n] / maxLen);
}

export async function handleContactDedup(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  logger.log('Starting contact deduplication scan');

  const { rows: contacts } = await pool.query<ContactForDedup>(
    `SELECT id, display_name, given_name, family_name, phone_numbers,
            email_addresses, relationship, context
     FROM contacts ORDER BY display_name`,
  );

  logger.log(`Scanning ${contacts.length} contacts for duplicates`);
  if (contacts.length < 2) return { pairs_detected: 0, auto_merged: 0, total_contacts: contacts.length };

  const phoneIndex = new Map<string, string[]>();
  const emailIndex = new Map<string, string[]>();

  for (const c of contacts) {
    for (const phone of c.phone_numbers) {
      const norm = normalizePhone(phone);
      if (norm.length >= 7) {
        const ids = phoneIndex.get(norm) || [];
        ids.push(c.id);
        phoneIndex.set(norm, ids);
      }
    }
    for (const email of c.email_addresses) {
      const norm = email.toLowerCase().trim();
      if (norm) {
        const ids = emailIndex.get(norm) || [];
        ids.push(c.id);
        emailIndex.set(norm, ids);
      }
    }
  }

  const contactMap = new Map(contacts.map(c => [c.id, c]));
  const candidatePairs = new Map<string, DuplicatePair>();

  function pairKey(a: string, b: string): string {
    return a < b ? `${a}|${b}` : `${b}|${a}`;
  }

  for (const [phone, ids] of phoneIndex.entries()) {
    if (ids.length < 2) continue;
    for (let i = 0; i < ids.length; i++) {
      for (let j = i + 1; j < ids.length; j++) {
        const key = pairKey(ids[i], ids[j]);
        if (!candidatePairs.has(key)) {
          candidatePairs.set(key, {
            contact_a_id: ids[i] < ids[j] ? ids[i] : ids[j],
            contact_b_id: ids[i] < ids[j] ? ids[j] : ids[i],
            similarity_score: 0,
            match_signals: {},
          });
        }
        const pair = candidatePairs.get(key)!;
        pair.match_signals.phone_match = true;
        if (!pair.match_signals.shared_phones) pair.match_signals.shared_phones = [];
        (pair.match_signals.shared_phones as string[]).push(phone);
      }
    }
  }

  for (const [email, ids] of emailIndex.entries()) {
    if (ids.length < 2) continue;
    for (let i = 0; i < ids.length; i++) {
      for (let j = i + 1; j < ids.length; j++) {
        const key = pairKey(ids[i], ids[j]);
        if (!candidatePairs.has(key)) {
          candidatePairs.set(key, {
            contact_a_id: ids[i] < ids[j] ? ids[i] : ids[j],
            contact_b_id: ids[i] < ids[j] ? ids[j] : ids[i],
            similarity_score: 0,
            match_signals: {},
          });
        }
        const pair = candidatePairs.get(key)!;
        pair.match_signals.email_match = true;
        if (!pair.match_signals.shared_emails) pair.match_signals.shared_emails = [];
        (pair.match_signals.shared_emails as string[]).push(email);
      }
    }
  }

  for (const pair of candidatePairs.values()) {
    const a = contactMap.get(pair.contact_a_id);
    const b = contactMap.get(pair.contact_b_id);
    if (!a || !b) continue;
    let score = 0;
    if (pair.match_signals.phone_match) score += 0.5;
    if (pair.match_signals.email_match) score += 0.3;
    const ns = nameSimilarity(a.display_name, b.display_name);
    pair.match_signals.name_similarity = ns;
    score += ns * 0.2;
    pair.similarity_score = Math.min(score, 1.0);
  }

  const validPairs = Array.from(candidatePairs.values()).filter(p => p.similarity_score >= 0.6);
  logger.log(`Found ${validPairs.length} candidate duplicate pairs`);

  let pairsDetected = 0;
  let autoMerged = 0;

  for (const pair of validPairs) {
    try {
      const { rowCount } = await pool.query(
        `INSERT INTO contact_merge_queue (contact_a_id, contact_b_id, similarity_score, match_signals)
         VALUES (LEAST($1::uuid, $2::uuid), GREATEST($1::uuid, $2::uuid), $3, $4)
         ON CONFLICT ON CONSTRAINT idx_contact_merge_pair_unique DO NOTHING`,
        [pair.contact_a_id, pair.contact_b_id, pair.similarity_score, JSON.stringify(pair.match_signals)],
      );
      if (rowCount && rowCount > 0) {
        pairsDetected++;
        if (pair.similarity_score >= 0.95) {
          try {
            await autoMerge(pool, pair.contact_a_id, pair.contact_b_id);
            await pool.query(
              `UPDATE contact_merge_queue SET status = 'auto_merged', resolved_at = NOW()
               WHERE contact_a_id = LEAST($1::uuid, $2::uuid)
                 AND contact_b_id = GREATEST($1::uuid, $2::uuid) AND status = 'pending'`,
              [pair.contact_a_id, pair.contact_b_id],
            );
            autoMerged++;
          } catch (err) {
            logger.log(`Auto-merge failed: ${(err as Error).message}`);
          }
        }
      }
    } catch { /* ok */ }
  }

  logger.log(`Dedup complete: ${pairsDetected} new pairs, ${autoMerged} auto-merged`);
  return { pairs_detected: pairsDetected, auto_merged: autoMerged, total_contacts: contacts.length };
}

async function autoMerge(pool: ReturnType<typeof getPool>, contactAId: string, contactBId: string): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(`SELECT * FROM contacts WHERE id = ANY($1)`, [[contactAId, contactBId]]);
    const a = rows.find((r: { id: string }) => r.id === contactAId);
    const b = rows.find((r: { id: string }) => r.id === contactBId);
    if (!a || !b) throw new Error('Contact not found');

    await client.query(
      `UPDATE contacts SET phone_numbers=$2, email_addresses=$3, organizations=$4,
              relationship=$5, context=$6, birthday=$7, notes=$8, updated_at=NOW()
       WHERE id = $1`,
      [contactAId,
       [...new Set([...a.phone_numbers, ...b.phone_numbers])],
       [...new Set([...a.email_addresses, ...b.email_addresses])],
       [...new Set([...(a.organizations || []), ...(b.organizations || [])])],
       a.relationship || b.relationship,
       [a.context, b.context].filter(Boolean).join('; ') || null,
       a.birthday || b.birthday,
       [a.notes, b.notes].filter(Boolean).join('; ') || null],
    );
    await client.query(`UPDATE knowledge_entities SET contact_id=$1 WHERE contact_id=$2`, [contactAId, contactBId]);
    await client.query(`DELETE FROM contacts WHERE id = $1`, [contactBId]);
    await client.query(
      `UPDATE contact_merge_queue SET status='rejected', resolved_at=NOW()
       WHERE status='pending' AND (contact_a_id=$1 OR contact_b_id=$1)`,
      [contactBId],
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}
