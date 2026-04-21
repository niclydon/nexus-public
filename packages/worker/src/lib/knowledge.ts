/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * Knowledge Graph — Core Library
 *
 * Provides the unified fact ingestion pipeline that replaces direct writes
 * to core_memory and owner_profile. All data sources flow through ingestFact().
 *
 * Key functions:
 *   ingestFact()       — Write a fact with entity resolution + embedding
 *   ingestFacts()      — Batch version
 *   resolveEntities()  — Match entity hints to canonical entities
 *   searchFacts()      — Semantic search across all facts
 *   getEntityFacts()   — Get all facts for an entity
 */
import { getPool, createLogger, platformMode } from '@nexus/core';
import type { PoolClient } from 'pg';

const logger = createLogger('knowledge');

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
export interface EntityHint {
  name: string;
  type: 'person' | 'place' | 'topic' | 'event' | 'thing';
  aliases?: string[];
  role?: 'subject' | 'object' | 'location' | 'topic';
  contactId?: string;
  metadata?: Record<string, unknown>;
}

export interface FactInput {
  domain: string;
  category: string;
  key: string;
  value: string;
  confidence?: number;
  source: string;
  validFrom?: string | Date;
  validUntil?: string | Date;
  entityHints?: EntityHint[];
  skipEmbedding?: boolean;
}

interface ResolvedEntity {
  id: string;
  role: string;
  isNew: boolean;
}

// ---------------------------------------------------------------------------
// Embedding Router
// ---------------------------------------------------------------------------

/**
 * Content classes that determine which embedding model to use.
 * The router selects the model based on the content class.
 */
export type EmbeddingContentClass = 'fact' | 'conversation' | 'email' | 'transcript' | 'journal' | 'narrative';

interface EmbeddingConfig {
  model: string;
  dimensions: number;
  provider: 'google' | 'openai' | 'local';
}

// Forge embedding server on Primary-Server (qwen3-embed-8b, 768 dims)
const LOCAL_EMBED_URL = process.env.LOCAL_EMBED_URL || 'http://localhost:8642';
const EMBED_API_KEY = process.env.FORGE_API_KEY || '';

// Default configs (used if DB config is unavailable)
const EMBEDDING_DEFAULTS: Record<EmbeddingContentClass, EmbeddingConfig> = {
  fact: { model: 'qwen3-embed-8b', dimensions: 768, provider: 'local' },
  conversation: { model: 'qwen3-embed-8b', dimensions: 768, provider: 'local' },
  email: { model: 'qwen3-embed-8b', dimensions: 768, provider: 'local' },
  transcript: { model: 'qwen3-embed-8b', dimensions: 768, provider: 'local' },
  journal: { model: 'qwen3-embed-8b', dimensions: 768, provider: 'local' },
  narrative: { model: 'qwen3-embed-8b', dimensions: 768, provider: 'local' },
};

// Cache DB config for 5 min
let embeddingConfigCache: { configs: Record<string, EmbeddingConfig>; loadedAt: number } | null = null;

async function getEmbeddingConfig(contentClass: EmbeddingContentClass): Promise<EmbeddingConfig> {
  // Try DB config with caching
  if (!embeddingConfigCache || Date.now() - embeddingConfigCache.loadedAt > 300_000) {
    try {
      const pool = getPool();
      const { rows } = await pool.query<{
        content_class: string;
        embedding_model: string;
        embedding_dimensions: number;
        provider: string;
      }>('SELECT content_class, embedding_model, embedding_dimensions, provider FROM knowledge_embedding_config');

      const configs: Record<string, EmbeddingConfig> = {};
      for (const row of rows) {
        configs[row.content_class] = {
          model: row.embedding_model,
          dimensions: row.embedding_dimensions,
          provider: row.provider as 'google' | 'openai' | 'local',
        };
      }
      embeddingConfigCache = { configs, loadedAt: Date.now() };
    } catch {
      logger.logDebug('Could not load embedding config from DB — using defaults');
    }
  }

  return embeddingConfigCache?.configs[contentClass] || EMBEDDING_DEFAULTS[contentClass];
}

/**
 * Classify content to determine which embedding tier to use.
 * Used by the import pipeline and other writers to auto-route content.
 */
export function classifyContentForEmbedding(params: {
  sourceType?: string;    // 'imessage', 'email', 'google_voice', 'import', 'chat', etc.
  mimeType?: string;      // 'message/rfc822', 'text/plain', etc.
  textLength?: number;    // character count
  messageCount?: number;  // number of messages (for grouped conversations)
  fileType?: string;      // 'mbox', 'json', 'csv', etc.
}): EmbeddingContentClass {
  // Explicit source type mappings
  if (params.sourceType === 'imessage' || params.sourceType === 'google_voice') return 'conversation';
  if (params.sourceType === 'email' || params.mimeType === 'message/rfc822') return 'email';
  if (params.sourceType === 'transcript' || params.sourceType === 'voicemail') return 'transcript';
  if (params.sourceType === 'journal') return 'journal';
  if (params.sourceType === 'life_narration') return 'narrative';

  // File type hints from imports
  if (params.fileType === 'mbox' || params.fileType === 'eml') return 'email';

  // Heuristic: multi-message or long content → conversation tier
  if (params.messageCount && params.messageCount > 5) return 'conversation';
  if (params.textLength && params.textLength > 1000) return 'conversation';

  // Default to fact tier
  return 'fact';
}

// ---------------------------------------------------------------------------
// Embedding generation — multi-provider
// ---------------------------------------------------------------------------

/**
 * Generate an embedding using the appropriate model for the content class.
 */
export async function generateEmbedding(text: string, contentClass: EmbeddingContentClass = 'fact'): Promise<number[] | null> {
  const config = await getEmbeddingConfig(contentClass);

  if (config.provider === 'local') {
    return generateLocalEmbedding(text, config);
  } else if (config.provider === 'google') {
    return generateGoogleEmbedding(text, config);
  } else if (config.provider === 'openai') {
    return generateOpenAIEmbedding(text, config);
  }

  logger.logMinimal(`Unknown embedding provider: ${config.provider}`);
  return null;
}

/**
 * Generate embedding via Forge server on Primary-Server (free, fast).
 * Falls back to Google if Forge is unreachable.
 *
 * During Creative Mode the Forge embed backend is intentionally
 * offline, so skip the Forge call entirely and go straight to Google.
 * This saves ~15 useless Forge requests per hour during creative mode
 * (every knowledge-backfill chunk was hitting /embed, failing, and
 * falling back anyway — observed via Forge's /api/usage).
 */
async function generateLocalEmbedding(text: string, config: EmbeddingConfig): Promise<number[] | null> {
  // Short-circuit: during Creative Mode, Forge /embed is always 503.
  // Skip it and fall straight through to the Google fallback.
  if (await platformMode.isCreativeMode()) {
    return generateGoogleEmbedding(text, { ...config, model: 'gemini-embedding-001', provider: 'google' });
  }

  try {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (EMBED_API_KEY) headers['Authorization'] = `Bearer ${EMBED_API_KEY}`;
    const response = await fetch(`${LOCAL_EMBED_URL}/embed`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ texts: [text] }),
      signal: AbortSignal.timeout(60_000),
    });

    if (!response.ok) {
      const body = await response.text();
      logger.logDebug(`Local embedding error ${response.status}: ${body.slice(0, 200)}`);
      // Fall back to Google free tier
      return generateGoogleEmbedding(text, { ...config, model: 'gemini-embedding-001', provider: 'google' });
    }

    const data = await response.json() as { embeddings?: number[][] };
    return data.embeddings?.[0] ?? null;
  } catch {
    // Local server unreachable — fall back to Google
    logger.logDebug('Local embed server unreachable — falling back to Google');
    return generateGoogleEmbedding(text, { ...config, model: 'gemini-embedding-001', provider: 'google' });
  }
}

async function generateGoogleEmbedding(text: string, config: EmbeddingConfig): Promise<number[] | null> {
  const apiKey = process.env.GOOGLE_GENAI_API_KEY;
  if (!apiKey) {
    logger.logDebug('No GOOGLE_GENAI_API_KEY — skipping embedding');
    return null;
  }

  try {
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${config.model}:embedContent?key=${apiKey}`;
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: `models/${config.model}`,
        content: { parts: [{ text }] },
        outputDimensionality: config.dimensions,
      }),
    });

    if (!response.ok) {
      const body = await response.text();
      logger.logMinimal(`Google embedding error ${response.status}: ${body.slice(0, 200)}`);
      return null;
    }

    const data = await response.json() as { embedding?: { values?: number[] } };
    return data.embedding?.values ?? null;
  } catch (err) {
    logger.logMinimal('Google embedding failed:', err instanceof Error ? err.message : err);
    return null;
  }
}

async function generateOpenAIEmbedding(text: string, config: EmbeddingConfig): Promise<number[] | null> {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    logger.logDebug('No OPENAI_API_KEY — skipping OpenAI embedding, falling back to Google');
    // Fallback to Google free tier rather than failing
    return generateGoogleEmbedding(text, EMBEDDING_DEFAULTS.fact);
  }

  try {
    const response = await fetch('https://api.openai.com/v1/embeddings', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: config.model,
        input: text,
        dimensions: config.dimensions,
      }),
    });

    if (!response.ok) {
      const body = await response.text();
      logger.logMinimal(`OpenAI embedding error ${response.status}: ${body.slice(0, 200)}`);
      return null;
    }

    const data = await response.json() as { data?: Array<{ embedding?: number[] }> };
    return data.data?.[0]?.embedding ?? null;
  } catch (err) {
    logger.logMinimal('OpenAI embedding failed:', err instanceof Error ? err.message : err);
    return null;
  }
}

/**
 * Create a searchable text string from a fact for embedding.
 */
function factToEmbeddingText(fact: FactInput): string {
  return `${fact.domain} ${fact.category}: ${fact.key} — ${fact.value}`;
}

// ---------------------------------------------------------------------------
// Date normalization (shared with owner-profile.ts)
// ---------------------------------------------------------------------------
function normalizeDate(dateStr: string | Date | undefined | null): string | null {
  if (!dateStr) return null;
  if (dateStr instanceof Date) return isNaN(dateStr.getTime()) ? null : dateStr.toISOString();
  const s = dateStr.trim();
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s;
  if (/^\d{4}-\d{2}$/.test(s)) return `${s}-01`;
  if (/^\d{4}$/.test(s)) return `${s}-01-01`;
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d.toISOString();
}

// ---------------------------------------------------------------------------
// Entity Resolution
// ---------------------------------------------------------------------------

/**
 * Normalize a name for fuzzy matching: lowercase, trim, collapse whitespace.
 */
function normalizeName(name: string): string {
  return name.toLowerCase().trim().replace(/\s+/g, ' ');
}

/**
 * Normalize a phone number for matching: strip everything except digits,
 * remove leading country code 1 if present.
 *
 * All these formats normalize to the same 10-digit string:
 *   +15551234567, 5551234567, (555) 123-4567, (555)-123-4567, 1-555-123-4567
 */
export function normalizePhone(phone: string): string {
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) return digits.slice(1);
  return digits;
}

/**
 * Check if a string looks like a phone number.
 */
function isPhoneNumber(s: string): boolean {
  return /^\+?\d[\d\s()\-\.]+$/.test(s.trim()) && s.replace(/\D/g, '').length >= 7;
}

/**
 * Normalize aliases before storage: phone numbers get normalized to 10-digit,
 * everything else gets lowercased and trimmed. Deduplicates.
 */
function normalizeAliases(aliases: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const alias of aliases) {
    if (!alias || !alias.trim()) continue;
    const normalized = isPhoneNumber(alias) ? normalizePhone(alias) : alias.trim();
    if (normalized && !seen.has(normalized.toLowerCase())) {
      seen.add(normalized.toLowerCase());
      result.push(normalized);
    }
  }
  return result;
}

/**
 * Name similarity scoring with awareness of family name collisions.
 *
 * Returns 0.0-1.0 with special handling:
 * - Exact match → 1.0
 * - First name only matching full name → 0.7 (NOT 0.8, since "Michael [Family]"
 *   could be 3 different people if only first name matches)
 * - Same last name, different first name → capped at 0.5 (prevents auto-merge
 *   of family members who share surnames)
 * - Nickname/variant detected → 0.65 (flag for review, not auto-merge)
 */
function nameSimilarity(a: string, b: string): number {
  const na = normalizeName(a);
  const nb = normalizeName(b);
  if (na === nb) return 1.0;

  const partsA = na.split(' ');
  const partsB = nb.split(' ');

  // Both have first + last name
  if (partsA.length >= 2 && partsB.length >= 2) {
    const firstA = partsA[0], lastA = partsA[partsA.length - 1];
    const firstB = partsB[0], lastB = partsB[partsB.length - 1];

    // Same last name, same first name (with possible middle name difference)
    if (lastA === lastB && firstA === firstB) return 0.95;

    // Same last name, different first name → likely family members, DO NOT auto-merge
    if (lastA === lastB && firstA !== firstB) return 0.45;
  }

  // One is a single name that matches part of the other (e.g. "Jane" vs "Jane Smith")
  if (partsA.length === 1 && partsB.length >= 2 && partsB[0] === partsA[0]) return 0.7;
  if (partsB.length === 1 && partsA.length >= 2 && partsA[0] === partsB[0]) return 0.7;

  // General substring check (e.g. "JSmith" containing "smith")
  if (na.length > 3 && nb.length > 3 && (na.includes(nb) || nb.includes(na))) return 0.65;

  // Bigram (Dice coefficient) similarity for everything else
  const bigramsA = new Set<string>();
  for (let i = 0; i < na.length - 1; i++) bigramsA.add(na.slice(i, i + 2));
  const bigramsB = new Set<string>();
  for (let i = 0; i < nb.length - 1; i++) bigramsB.add(nb.slice(i, i + 2));

  if (bigramsA.size === 0 || bigramsB.size === 0) return 0;

  let intersection = 0;
  for (const bg of bigramsA) {
    if (bigramsB.has(bg)) intersection++;
  }

  return (2 * intersection) / (bigramsA.size + bigramsB.size);
}

// Thresholds for entity resolution
// Auto-merge: only when we're very confident (phone match, exact name, or score >= 0.85)
// Review queue: moderate similarity that needs human eyes (0.55-0.85)
const AUTO_MERGE_THRESHOLD = 0.85;      // Raised from 0.75 — safer for family name collisions
const REVIEW_QUEUE_THRESHOLD = 0.55;     // Flag for review between this and auto-merge

/**
 * Resolve entity hints to canonical entity IDs. Creates new entities when needed.
 * Auto-merges high-confidence matches, flags low-confidence near-matches for review.
 *
 * Resolution order (strongest signal first):
 * 1. Phone number match (definitive for people)
 * 2. Contact ID match (from iOS contacts)
 * 3. Exact name match (case-insensitive)
 * 4. Alias match (nicknames, emails)
 * 5. Fuzzy name match (with family-aware disambiguation)
 * 6. Create new entity
 */
export async function resolveEntities(
  hints: EntityHint[],
  client: PoolClient
): Promise<ResolvedEntity[]> {
  const resolved: ResolvedEntity[] = [];

  for (const hint of hints) {
    const role = hint.role || 'subject';
    const normalizedHintAliases = normalizeAliases(hint.aliases || []);

    // 1. Try phone number match first (strongest signal for people)
    const phoneAliases = normalizedHintAliases.filter(a => /^\d{7,}$/.test(a));
    if (phoneAliases.length > 0) {
      const { rows: phoneMatch } = await client.query<{ id: string }>(
        `SELECT id FROM knowledge_entities
         WHERE entity_type = $1
         AND EXISTS (
           SELECT 1 FROM unnest(aliases) AS a
           WHERE REGEXP_REPLACE(a, '[^0-9]', '', 'g') = ANY($2::text[])
         )
         LIMIT 1`,
        [hint.type, phoneAliases]
      );

      if (phoneMatch.length > 0) {
        logger.logDebug(`Phone match for "${hint.name}" → ${phoneMatch[0].id}`);
        await mergeAliases(phoneMatch[0].id, normalizedHintAliases, client);
        // Also update canonical_name if the hint has a more complete name
        await maybeUpgradeCanonicalName(phoneMatch[0].id, hint.name, client);
        resolved.push({ id: phoneMatch[0].id, role, isNew: false });
        continue;
      }
    }

    // 2. Try contact_id match (from iOS contacts)
    if (hint.contactId) {
      const { rows: contactMatch } = await client.query<{ id: string }>(
        `SELECT id FROM knowledge_entities
         WHERE entity_type = 'person' AND contact_id = $1
         LIMIT 1`,
        [hint.contactId]
      );

      if (contactMatch.length > 0) {
        logger.logDebug(`Contact match for "${hint.name}" → ${contactMatch[0].id}`);
        await mergeAliases(contactMatch[0].id, normalizedHintAliases, client);
        resolved.push({ id: contactMatch[0].id, role, isNew: false });
        continue;
      }
    }

    // 3. Try exact name match (case-insensitive)
    const { rows: exact } = await client.query<{ id: string }>(
      `SELECT id FROM knowledge_entities
       WHERE entity_type = $1 AND LOWER(canonical_name) = LOWER($2)
       LIMIT 1`,
      [hint.type, hint.name.trim()]
    );

    if (exact.length > 0) {
      logger.logDebug(`Exact match for "${hint.name}" → ${exact[0].id}`);
      await mergeAliases(exact[0].id, normalizedHintAliases, client);
      resolved.push({ id: exact[0].id, role, isNew: false });
      continue;
    }

    // 4. Try alias match (nicknames, emails — non-phone)
    const nonPhoneAliases = normalizedHintAliases.filter(a => !/^\d{7,}$/.test(a));
    if (nonPhoneAliases.length > 0) {
      const { rows: aliasMatch } = await client.query<{ id: string }>(
        `SELECT id FROM knowledge_entities
         WHERE entity_type = $1
         AND EXISTS (
           SELECT 1 FROM unnest(aliases) AS a
           WHERE LOWER(a) = ANY($2::text[])
         )
         LIMIT 1`,
        [hint.type, nonPhoneAliases]
      );

      if (aliasMatch.length > 0) {
        logger.logDebug(`Alias match for "${hint.name}" → ${aliasMatch[0].id}`);
        await mergeAliases(aliasMatch[0].id, normalizedHintAliases, client);
        resolved.push({ id: aliasMatch[0].id, role, isNew: false });
        continue;
      }
    }

    // 5. Try fuzzy name match (with family-aware disambiguation)
    const { rows: candidates } = await client.query<{ id: string; canonical_name: string; aliases: string[] }>(
      `SELECT id, canonical_name, aliases FROM knowledge_entities
       WHERE entity_type = $1
       ORDER BY fact_count DESC
       LIMIT 200`,
      [hint.type]
    );

    let bestMatch: { id: string; score: number; name: string } | null = null;
    for (const candidate of candidates) {
      const score = nameSimilarity(hint.name, candidate.canonical_name);
      // Also check aliases
      let aliasScore = 0;
      for (const alias of candidate.aliases || []) {
        aliasScore = Math.max(aliasScore, nameSimilarity(hint.name, alias));
      }
      const finalScore = Math.max(score, aliasScore);

      if (finalScore > (bestMatch?.score ?? 0)) {
        bestMatch = { id: candidate.id, score: finalScore, name: candidate.canonical_name };
      }
    }

    if (bestMatch && bestMatch.score >= AUTO_MERGE_THRESHOLD) {
      logger.logVerbose(`Fuzzy auto-merge "${hint.name}" → "${bestMatch.name}" (score: ${bestMatch.score.toFixed(2)})`);
      await mergeAliases(bestMatch.id, [...(hint.aliases || []), hint.name], client);
      resolved.push({ id: bestMatch.id, role, isNew: false });
      continue;
    }

    if (bestMatch && bestMatch.score >= REVIEW_QUEUE_THRESHOLD) {
      // Flag for review but still create a new entity
      logger.logVerbose(`Near-match flagged: "${hint.name}" ↔ "${bestMatch.name}" (score: ${bestMatch.score.toFixed(2)})`);
      // We'll create the new entity and add to merge queue after
    }

    // 6. Create new entity
    const { rows: newEntity } = await client.query<{ id: string }>(
      `INSERT INTO knowledge_entities (entity_type, canonical_name, aliases, contact_id, metadata, first_seen_at, last_seen_at)
       VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
       RETURNING id`,
      [
        hint.type,
        hint.name.trim(),
        normalizedHintAliases,
        hint.contactId || null,
        JSON.stringify(hint.metadata || {}),
      ]
    );

    logger.logVerbose(`Created new entity "${hint.name}" (${hint.type}) → ${newEntity[0].id}`);
    resolved.push({ id: newEntity[0].id, role, isNew: true });

    // If there was a near-match, add to merge review queue
    if (bestMatch && bestMatch.score >= REVIEW_QUEUE_THRESHOLD) {
      await client.query(
        `INSERT INTO knowledge_entity_merge_queue (entity_a_id, entity_b_id, similarity_score, match_signals)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING`,
        [
          newEntity[0].id,
          bestMatch.id,
          bestMatch.score,
          JSON.stringify({
            new_name: hint.name,
            existing_name: bestMatch.name,
            match_type: 'fuzzy_name',
          }),
        ]
      );
    }
  }

  return resolved;
}

/**
 * Merge new aliases into an existing entity, normalizing and deduplicating.
 * Phone numbers are stored as normalized 10-digit strings.
 */
async function mergeAliases(entityId: string, newAliases: string[], client: PoolClient): Promise<void> {
  if (newAliases.length === 0) return;

  // Normalize before storing
  const normalized = normalizeAliases(newAliases);
  if (normalized.length === 0) return;

  await client.query(
    `UPDATE knowledge_entities
     SET aliases = (
       SELECT array_agg(DISTINCT a)
       FROM unnest(aliases || $2::text[]) AS a
       WHERE a IS NOT NULL AND a != ''
     ),
     updated_at = NOW()
     WHERE id = $1`,
    [entityId, normalized]
  );
}

/**
 * Upgrade canonical_name if the new name is more complete (e.g., has more parts).
 * "Jane" → "Jane Smith" is an upgrade. "Jane Smith" → "Jane" is not.
 */
async function maybeUpgradeCanonicalName(entityId: string, newName: string, client: PoolClient): Promise<void> {
  const { rows } = await client.query<{ canonical_name: string }>(
    `SELECT canonical_name FROM knowledge_entities WHERE id = $1`,
    [entityId]
  );
  if (rows.length === 0) return;

  const current = rows[0].canonical_name;
  const newParts = newName.trim().split(/\s+/);
  const currentParts = current.split(/\s+/);

  // Upgrade if new name has more parts and contains the old name
  if (newParts.length > currentParts.length && normalizeName(newName).includes(normalizeName(current))) {
    await client.query(
      `UPDATE knowledge_entities SET canonical_name = $2, updated_at = NOW() WHERE id = $1`,
      [entityId, newName.trim()]
    );
    logger.logVerbose(`Upgraded canonical name: "${current}" → "${newName.trim()}"`);
  }
}

// ---------------------------------------------------------------------------
// Fact Ingestion
// ---------------------------------------------------------------------------

/**
 * Ingest a single fact into the knowledge graph.
 *
 * Pipeline:
 * 1. Resolve entity hints to canonical entities
 * 2. Upsert fact (dedup by domain/category/key)
 * 3. Link fact to entities
 * 4. Generate embedding (async, non-blocking on failure)
 * 5. Update entity stats (fact_count, last_seen_at)
 * 6. Mark affected summaries as stale
 */
export async function ingestFact(fact: FactInput): Promise<string | null> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // 1. Resolve entities
    const entities = fact.entityHints
      ? await resolveEntities(fact.entityHints, client)
      : [];

    // 2. Upsert fact
    const factId = await upsertFact(fact, client);

    if (!factId) {
      // Fact was a lower-confidence duplicate — no-op
      await client.query('COMMIT');
      return null;
    }

    // 3. Link fact to entities
    for (const entity of entities) {
      await client.query(
        `INSERT INTO knowledge_fact_entities (fact_id, entity_id, role)
         VALUES ($1, $2, $3)
         ON CONFLICT (fact_id, entity_id) DO UPDATE SET role = EXCLUDED.role`,
        [factId, entity.id, entity.role]
      );
    }

    // 4. Update entity stats
    const entityIds = entities.map(e => e.id);
    if (entityIds.length > 0) {
      await client.query(
        `UPDATE knowledge_entities
         SET fact_count = (
           SELECT COUNT(*) FROM knowledge_fact_entities WHERE entity_id = knowledge_entities.id
         ),
         last_seen_at = NOW(),
         updated_at = NOW()
         WHERE id = ANY($1::uuid[])`,
        [entityIds]
      );

      // 5. Mark affected summaries as stale
      await client.query(
        `UPDATE knowledge_summaries
         SET stale = TRUE
         WHERE entity_ids && $1::uuid[]
           AND stale = FALSE`,
        [entityIds]
      );
    }

    await client.query('COMMIT');

    // 6. Generate embedding (non-blocking — update after commit)
    if (!fact.skipEmbedding) {
      generateAndStoreEmbedding(factId, fact).catch(err => {
        logger.logMinimal(`Failed to generate embedding for fact ${factId}:`, err instanceof Error ? err.message : err);
      });
    }

    logger.logDebug(`Ingested fact ${fact.domain}/${fact.category}/${fact.key} → ${factId} (${entities.length} entities)`);
    return factId;
  } catch (err) {
    await client.query('ROLLBACK');
    // Unique constraint race (concurrent ingestion of same domain/category/key) — safe to ignore
    if (err instanceof Error && err.message.includes('idx_kf_active')) {
      logger.logDebug(`Concurrent duplicate for ${fact.domain}/${fact.key}, skipping`);
      return null;
    }
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Upsert a fact into knowledge_facts. Returns the fact ID, or null if skipped
 * (lower-confidence duplicate).
 */
async function upsertFact(fact: FactInput, client: PoolClient): Promise<string | null> {
  const confidence = fact.confidence ?? 0.5;

  // Find existing active fact
  const { rows: existing } = await client.query<{
    id: string;
    value: string;
    confidence: number;
    evidence_count: number;
    sources: string[];
  }>(
    `SELECT id, value, confidence, evidence_count, sources
     FROM knowledge_facts
     WHERE domain = $1 AND category = $2 AND key = $3 AND superseded_by IS NULL
     LIMIT 1`,
    [fact.domain, fact.category, fact.key]
  );

  if (existing.length === 0) {
    // New fact — insert
    const { rows } = await client.query<{ id: string }>(
      `INSERT INTO knowledge_facts (domain, category, key, value, confidence, sources, valid_from, valid_until, last_confirmed_at)
       VALUES ($1, $2, $3, $4, $5, ARRAY[$6]::text[], $7, $8, NOW())
       RETURNING id`,
      [
        fact.domain, fact.category, fact.key, fact.value,
        confidence, fact.source,
        normalizeDate(fact.validFrom), normalizeDate(fact.validUntil),
      ]
    );
    return rows[0].id;
  }

  const old = existing[0];

  if (old.value === fact.value) {
    // Same value — bump evidence, merge source
    await client.query(
      `UPDATE knowledge_facts SET
         evidence_count = evidence_count + 1,
         sources = (SELECT array_agg(DISTINCT s) FROM unnest(sources || ARRAY[$2]::text[]) s),
         confidence = GREATEST(confidence, $3),
         last_confirmed_at = NOW(),
         updated_at = NOW(),
         valid_from = COALESCE($4, valid_from),
         valid_until = COALESCE($5, valid_until)
       WHERE id = $1`,
      [old.id, fact.source, confidence, normalizeDate(fact.validFrom), normalizeDate(fact.validUntil)]
    );
    return old.id;
  }

  if (confidence >= old.confidence) {
    // Different value, higher/equal confidence — supersede
    const { rows: newRows } = await client.query<{ id: string }>(
      `INSERT INTO knowledge_facts (domain, category, key, value, confidence, evidence_count, sources, valid_from, valid_until, last_confirmed_at)
       VALUES ($1, $2, $3, $4, $5, 1, ARRAY[$6]::text[], $7, $8, NOW())
       RETURNING id`,
      [
        fact.domain, fact.category, fact.key, fact.value,
        confidence, fact.source,
        normalizeDate(fact.validFrom), normalizeDate(fact.validUntil),
      ]
    );

    await client.query(
      `UPDATE knowledge_facts SET superseded_by = $1, updated_at = NOW() WHERE id = $2`,
      [newRows[0].id, old.id]
    );

    logger.logVerbose(`Superseded fact ${fact.domain}/${fact.key}`);
    return newRows[0].id;
  }

  // Lower confidence — just bump evidence on existing
  await client.query(
    `UPDATE knowledge_facts SET
       evidence_count = evidence_count + 1,
       sources = (SELECT array_agg(DISTINCT s) FROM unnest(sources || ARRAY[$2]::text[]) s),
       last_confirmed_at = NOW(),
       updated_at = NOW()
     WHERE id = $1`,
    [old.id, fact.source]
  );
  return old.id;
}

/**
 * Generate and store embedding for a fact (fire-and-forget after commit).
 */
async function generateAndStoreEmbedding(factId: string, fact: FactInput): Promise<void> {
  const text = factToEmbeddingText(fact);
  const embedding = await generateEmbedding(text, 'fact');
  if (!embedding) return;

  const pool = getPool();
  await pool.query(
    `UPDATE knowledge_facts SET embedding = $2::vector WHERE id = $1`,
    [factId, `[${embedding.join(',')}]`]
  );
  logger.logDebug(`Stored embedding for fact ${factId}`);
}

// ---------------------------------------------------------------------------
// Conversation Chunk Ingestion
// ---------------------------------------------------------------------------

export interface ConversationChunkInput {
  sourceType: string;           // 'imessage', 'email', 'google_voice', 'import'
  sourceIdentifier: string;     // phone number, email address, import ID
  entityId?: string;            // pre-resolved entity ID
  entityHint?: EntityHint;      // if entity needs resolution
  chunkText: string;            // the conversation text
  chunkIndex: number;           // position in sequence
  messageCount: number;
  timeStart?: string;
  timeEnd?: string;
  metadata?: Record<string, unknown>;
}

/**
 * Ingest a conversation chunk with high-quality embedding.
 * Uses the embedding router to select the appropriate model.
 */
export async function ingestConversationChunk(chunk: ConversationChunkInput): Promise<string> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // Resolve entity if needed
    let entityId = chunk.entityId || null;
    if (!entityId && chunk.entityHint) {
      const resolved = await resolveEntities([chunk.entityHint], client);
      entityId = resolved[0]?.id || null;
    }

    // Insert chunk (embedding added after commit)
    const { rows } = await client.query<{ id: string }>(
      `INSERT INTO knowledge_conversation_chunks
         (source_type, source_identifier, entity_id, chunk_text, chunk_index,
          message_count, time_start, time_end, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       RETURNING id`,
      [
        chunk.sourceType,
        chunk.sourceIdentifier,
        entityId,
        chunk.chunkText,
        chunk.chunkIndex,
        chunk.messageCount,
        chunk.timeStart || null,
        chunk.timeEnd || null,
        JSON.stringify(chunk.metadata || {}),
      ]
    );

    const chunkId = rows[0].id;

    // Update entity last_seen_at
    if (entityId) {
      await client.query(
        `UPDATE knowledge_entities SET last_seen_at = NOW(), updated_at = NOW() WHERE id = $1`,
        [entityId]
      );
    }

    await client.query('COMMIT');

    // Generate embedding (fire-and-forget — uses the content classifier)
    const contentClass = classifyContentForEmbedding({
      sourceType: chunk.sourceType,
      messageCount: chunk.messageCount,
      textLength: chunk.chunkText.length,
    });

    generateAndStoreChunkEmbedding(chunkId, chunk.chunkText, contentClass).catch(err => {
      logger.logMinimal(`Failed to embed chunk ${chunkId}:`, err instanceof Error ? err.message : err);
    });

    logger.logDebug(`Ingested conversation chunk ${chunkId} (${chunk.sourceType}/${chunk.sourceIdentifier}, index ${chunk.chunkIndex})`);
    return chunkId;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function generateAndStoreChunkEmbedding(
  chunkId: string,
  text: string,
  contentClass: EmbeddingContentClass
): Promise<void> {
  const embedding = await generateEmbedding(text, contentClass);
  if (!embedding) return;

  const pool = getPool();
  await pool.query(
    `UPDATE knowledge_conversation_chunks SET embedding = $2::vector WHERE id = $1`,
    [chunkId, `[${embedding.join(',')}]`]
  );
  logger.logDebug(`Stored ${contentClass} embedding for chunk ${chunkId}`);
}

/**
 * Batch ingest conversation chunks. Returns count of successful writes.
 */
export async function ingestConversationChunks(chunks: ConversationChunkInput[]): Promise<{ written: number; errors: number }> {
  let written = 0;
  let errors = 0;

  for (const chunk of chunks) {
    try {
      await ingestConversationChunk(chunk);
      written++;
    } catch (err) {
      errors++;
      const msg = err instanceof Error ? err.message : String(err);
      logger.logMinimal(`Failed to ingest chunk ${chunk.sourceType}/${chunk.sourceIdentifier}#${chunk.chunkIndex}: ${msg}`);
    }
  }

  logger.log(`Conversation chunk ingestion: ${written} written, ${errors} errors out of ${chunks.length}`);
  return { written, errors };
}

// ---------------------------------------------------------------------------
// Batch Ingestion
// ---------------------------------------------------------------------------

/**
 * Ingest multiple facts. Returns count of successful writes.
 * Processes sequentially to maintain entity resolution consistency.
 */
export async function ingestFacts(facts: FactInput[]): Promise<{ written: number; errors: number }> {
  let written = 0;
  let errors = 0;

  for (const fact of facts) {
    try {
      const id = await ingestFact(fact);
      if (id) written++;
    } catch (err) {
      errors++;
      const msg = err instanceof Error ? err.message : String(err);
      logger.logMinimal(`Failed to ingest ${fact.domain}/${fact.category}/${fact.key}: ${msg}`);
    }
  }

  logger.log(`Batch ingestion complete: ${written} written, ${errors} errors out of ${facts.length} facts`);
  return { written, errors };
}

// ---------------------------------------------------------------------------
// Semantic Search
// ---------------------------------------------------------------------------

/**
 * Unified search across both knowledge_facts and knowledge_conversation_chunks.
 * Uses the appropriate embedding model for the query based on what we're searching.
 */
export async function searchKnowledge(params: {
  query: string;
  domain?: string;
  entityId?: string;
  limit?: number;
  minConfidence?: number;
  includeConversations?: boolean;
}): Promise<{
  facts: Array<{
    id: string;
    domain: string;
    category: string;
    key: string;
    value: string;
    confidence: number;
    evidence_count: number;
    similarity?: number;
    result_type: 'fact';
  }>;
  conversations: Array<{
    id: string;
    source_type: string;
    source_identifier: string;
    chunk_text: string;
    time_start: string | null;
    time_end: string | null;
    message_count: number;
    similarity?: number;
    result_type: 'conversation';
  }>;
}> {
  const pool = getPool();
  const limit = params.limit || 20;
  const minConfidence = params.minConfidence || 0;
  const includeConversations = params.includeConversations !== false;

  const result: Awaited<ReturnType<typeof searchKnowledge>> = { facts: [], conversations: [] };

  // Search facts with fact-tier embedding
  const factEmbedding = await generateEmbedding(params.query, 'fact');

  if (factEmbedding) {
    const embeddingStr = `[${factEmbedding.join(',')}]`;
    let query = `
      SELECT f.id, f.domain, f.category, f.key, f.value, f.confidence, f.evidence_count,
             1 - (f.embedding <=> $1::vector) as similarity
      FROM knowledge_facts f
      WHERE f.superseded_by IS NULL
        AND f.embedding IS NOT NULL
        AND f.confidence >= $2
    `;
    const queryParams: unknown[] = [embeddingStr, minConfidence];
    let paramIdx = 3;

    if (params.domain) {
      query += ` AND f.domain = $${paramIdx}`;
      queryParams.push(params.domain);
      paramIdx++;
    }
    if (params.entityId) {
      query += ` AND EXISTS (SELECT 1 FROM knowledge_fact_entities kfe WHERE kfe.fact_id = f.id AND kfe.entity_id = $${paramIdx})`;
      queryParams.push(params.entityId);
      paramIdx++;
    }

    query += ` ORDER BY f.embedding <=> $1::vector LIMIT $${paramIdx}`;
    queryParams.push(limit);

    const { rows } = await pool.query(query, queryParams);
    result.facts = rows.map(r => ({ ...r, result_type: 'fact' as const }));
  } else {
    // Fallback to text search for facts
    let query = `
      SELECT f.id, f.domain, f.category, f.key, f.value, f.confidence, f.evidence_count
      FROM knowledge_facts f
      WHERE f.superseded_by IS NULL AND f.confidence >= $1
        AND (f.key ILIKE $2 OR f.value ILIKE $2 OR f.category ILIKE $2)
    `;
    const queryParams: unknown[] = [minConfidence, `%${params.query}%`];
    let paramIdx = 3;

    if (params.domain) {
      query += ` AND f.domain = $${paramIdx}`;
      queryParams.push(params.domain);
      paramIdx++;
    }
    if (params.entityId) {
      query += ` AND EXISTS (SELECT 1 FROM knowledge_fact_entities kfe WHERE kfe.fact_id = f.id AND kfe.entity_id = $${paramIdx})`;
      queryParams.push(params.entityId);
      paramIdx++;
    }
    query += ` ORDER BY f.confidence DESC, f.evidence_count DESC LIMIT $${paramIdx}`;
    queryParams.push(limit);

    const { rows } = await pool.query(query, queryParams);
    result.facts = rows.map(r => ({ ...r, result_type: 'fact' as const }));
  }

  // Search conversation chunks with conversation-tier embedding
  if (includeConversations) {
    const convEmbedding = await generateEmbedding(params.query, 'conversation');

    if (convEmbedding) {
      const embeddingStr = `[${convEmbedding.join(',')}]`;
      let query = `
        SELECT c.id, c.source_type, c.source_identifier, c.chunk_text, c.time_start, c.time_end,
               c.message_count, 1 - (c.embedding <=> $1::vector) as similarity
        FROM knowledge_conversation_chunks c
        WHERE c.embedding IS NOT NULL
      `;
      const queryParams: unknown[] = [embeddingStr];
      let paramIdx = 2;

      if (params.entityId) {
        query += ` AND c.entity_id = $${paramIdx}`;
        queryParams.push(params.entityId);
        paramIdx++;
      }

      query += ` ORDER BY c.embedding <=> $1::vector LIMIT $${paramIdx}`;
      queryParams.push(limit);

      const { rows } = await pool.query(query, queryParams);
      result.conversations = rows.map(r => ({ ...r, result_type: 'conversation' as const }));
    } else {
      // Fallback to text search for conversations
      let query = `
        SELECT c.id, c.source_type, c.source_identifier, c.chunk_text, c.time_start, c.time_end, c.message_count
        FROM knowledge_conversation_chunks c
        WHERE c.chunk_text ILIKE $1
      `;
      const queryParams: unknown[] = [`%${params.query}%`];
      let paramIdx = 2;

      if (params.entityId) {
        query += ` AND c.entity_id = $${paramIdx}`;
        queryParams.push(params.entityId);
        paramIdx++;
      }
      query += ` ORDER BY c.time_end DESC NULLS LAST LIMIT $${paramIdx}`;
      queryParams.push(limit);

      const { rows } = await pool.query(query, queryParams);
      result.conversations = rows.map(r => ({ ...r, result_type: 'conversation' as const }));
    }
  }

  return result;
}

// Keep backward-compatible alias
export const searchFacts = async (params: Parameters<typeof searchKnowledge>[0]) => {
  const result = await searchKnowledge({ ...params, includeConversations: false });
  return result.facts;
};

// ---------------------------------------------------------------------------
// Entity Queries
// ---------------------------------------------------------------------------

/**
 * Get all active facts for a specific entity.
 */
export async function getEntityFacts(entityId: string): Promise<Array<{
  id: string;
  domain: string;
  category: string;
  key: string;
  value: string;
  confidence: number;
  evidence_count: number;
  role: string;
}>> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT f.id, f.domain, f.category, f.key, f.value, f.confidence, f.evidence_count, kfe.role
     FROM knowledge_facts f
     JOIN knowledge_fact_entities kfe ON kfe.fact_id = f.id
     WHERE kfe.entity_id = $1 AND f.superseded_by IS NULL
     ORDER BY f.confidence DESC, f.evidence_count DESC`,
    [entityId]
  );
  return rows;
}

/**
 * List entities by type with fact counts.
 */
export async function listEntities(params: {
  type?: string;
  limit?: number;
  minFacts?: number;
}): Promise<Array<{
  id: string;
  entity_type: string;
  canonical_name: string;
  fact_count: number;
  summary: string | null;
  first_seen_at: string;
  last_seen_at: string;
}>> {
  const pool = getPool();
  let query = `SELECT id, entity_type, canonical_name, fact_count, summary, first_seen_at, last_seen_at
               FROM knowledge_entities WHERE fact_count >= $1`;
  const queryParams: unknown[] = [params.minFacts || 0];
  let paramIdx = 2;

  if (params.type) {
    query += ` AND entity_type = $${paramIdx}`;
    queryParams.push(params.type);
    paramIdx++;
  }

  query += ` ORDER BY fact_count DESC LIMIT $${paramIdx}`;
  queryParams.push(params.limit || 50);

  const { rows } = await pool.query(query, queryParams);
  return rows;
}

/**
 * Find an entity by name or alias.
 */
export async function findEntity(name: string, type?: string): Promise<{
  id: string;
  entity_type: string;
  canonical_name: string;
  aliases: string[];
  fact_count: number;
  summary: string | null;
} | null> {
  const pool = getPool();
  let query = `
    SELECT id, entity_type, canonical_name, aliases, fact_count, summary
    FROM knowledge_entities
    WHERE (LOWER(canonical_name) = LOWER($1) OR LOWER($1) = ANY(SELECT LOWER(a) FROM unnest(aliases) a))
  `;
  const queryParams: unknown[] = [name.trim()];

  if (type) {
    query += ` AND entity_type = $2`;
    queryParams.push(type);
  }

  query += ` ORDER BY fact_count DESC LIMIT 1`;

  const { rows } = await pool.query(query, queryParams);
  return rows[0] || null;
}

// ---------------------------------------------------------------------------
// Knowledge Map
// ---------------------------------------------------------------------------

/**
 * Render the compact knowledge map for context injection.
 * Returns a ~500 byte summary of what ARIA knows.
 */
export async function renderKnowledgeMap(): Promise<string> {
  const pool = getPool();

  // Entity counts by type
  const { rows: entityCounts } = await pool.query<{ entity_type: string; cnt: string; top_names: string }>(
    `SELECT entity_type, COUNT(*) as cnt,
            STRING_AGG(canonical_name, ', ' ORDER BY fact_count DESC) FILTER (WHERE rn <= 5) as top_names
     FROM (
       SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_type ORDER BY fact_count DESC) as rn
       FROM knowledge_entities
       WHERE fact_count > 0
     ) sub
     GROUP BY entity_type
     ORDER BY cnt DESC`
  );

  if (entityCounts.length === 0) {
    return '## What I Know\nKnowledge graph is being built. No entities indexed yet.';
  }

  // Total facts
  const { rows: [{ total_facts }] } = await pool.query<{ total_facts: string }>(
    `SELECT COUNT(*) as total_facts FROM knowledge_facts WHERE superseded_by IS NULL`
  );

  // Domain distribution
  const { rows: domains } = await pool.query<{ domain: string; cnt: string }>(
    `SELECT domain, COUNT(*) as cnt FROM knowledge_facts
     WHERE superseded_by IS NULL
     GROUP BY domain ORDER BY cnt DESC LIMIT 10`
  );

  const lines: string[] = ['## What I Know'];
  for (const ec of entityCounts) {
    const topStr = ec.top_names ? ` (top: ${ec.top_names})` : '';
    lines.push(`${ec.entity_type}: ${ec.cnt} entities${topStr}`);
  }

  lines.push(`Total facts: ${total_facts} across ${domains.length} domains (${domains.map(d => `${d.domain}: ${d.cnt}`).join(', ')})`);
  lines.push('I can search my full knowledge base for details on any of these.');

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Refresh materialized view
// ---------------------------------------------------------------------------
export async function refreshKnowledgeMap(): Promise<void> {
  const pool = getPool();
  await pool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY knowledge_map');
  logger.log('Refreshed knowledge_map materialized view');
}
