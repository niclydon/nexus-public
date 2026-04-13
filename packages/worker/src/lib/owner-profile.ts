/**
 * Owner profile management — supersession-based fact storage.
 * NOTE: owner_profile is archived — Knowledge Graph is the primary store.
 * These functions exist for backward compatibility with existing handlers.
 */
import { getPool, createLogger } from '@nexus/core';

const logger = createLogger('owner-profile');

export interface ProfileFact {
  domain: string;
  category: string;
  key: string;
  value: string;
  confidence?: number;
  source: string;
  validFrom?: string;
  validUntil?: string;
  metadata?: Record<string, unknown>;
}

export async function upsertProfileFacts(facts: ProfileFact[]): Promise<number> {
  logger.log('owner_profile is archived — skipping write. Use Knowledge Graph instead.');
  return 0;
}

export async function upsertProfileFact(fact: ProfileFact): Promise<void> {
  logger.log('owner_profile is archived — skipping write. Use Knowledge Graph instead.');
}

export async function getProfileFacts(domain: string, category?: string): Promise<ProfileFact[]> {
  const pool = getPool();
  let sql = 'SELECT * FROM owner_profile WHERE domain = $1 AND superseded_by IS NULL';
  const params: unknown[] = [domain];
  if (category) { sql += ' AND category = $2'; params.push(category); }
  sql += ' ORDER BY confidence DESC, updated_at DESC';
  const { rows } = await pool.query(sql, params);
  return rows;
}

export async function getOwnerProfileContext(): Promise<string> {
  return '';
}

export async function getOwnerName(): Promise<string> {
  return 'Owner';
}
