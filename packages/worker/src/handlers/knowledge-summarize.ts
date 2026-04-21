/**
 * Knowledge Summarize Handler
 *
 * Generates AI summaries for the knowledge graph:
 * 1. Entity summaries — relationship profiles, place descriptions, topic overviews
 * 2. Domain overviews — high-level summaries per knowledge domain
 * 3. Relationship summaries — dynamics between owner and key people
 *
 * Runs as a bulk job. Uses atomic claim to prevent duplicate work across
 * concurrent workers. Supports sprint mode for high-throughput backfill.
 *
 * Payload options:
 *   entity_ids?: string[]  — specific entities to summarize
 *   batch_size?: number    — entities per job (default 30, sprint default 100)
 *   concurrency?: number   — parallel LLM calls (default 1, sprint default 4)
 *   sprint?: boolean       — skip domain/relationship phases, bigger batches
 *   force?: boolean        — re-summarize even if fresh
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import { routeRequest } from '../lib/llm/index.js';

const logger = createLogger('knowledge-summarize');

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface EntityRow {
  id: string;
  entity_type: string;
  canonical_name: string;
  fact_count: number;
  summary: string | null;
  summary_updated_at: Date | null;
  aliases: string[];
  contact_id: string | null;
  metadata: Record<string, unknown>;
}

interface FactRow {
  domain: string;
  category: string;
  key: string;
  value: string;
  confidence: number;
  evidence_count: number;
  sources: string[];
  valid_from: Date | null;
  valid_until: Date | null;
}

// ---------------------------------------------------------------------------
// Concurrency helper
// ---------------------------------------------------------------------------

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>,
): Promise<(R | Error)[]> {
  const results: (R | Error)[] = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < items.length) {
      const i = nextIndex++;
      try {
        results[i] = await fn(items[i]);
      } catch (err) {
        results[i] = err instanceof Error ? err : new Error(String(err));
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()));
  return results;
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

export async function handleKnowledgeSummarize(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as {
    entity_ids?: string[];
    batch_size?: number;
    concurrency?: number;
    sprint?: boolean;
    force?: boolean;
  };
  const pool = getPool();

  const isSprint = payload.sprint === true;
  const batchSize = payload.batch_size ?? (isSprint ? 100 : 30);
  // Default concurrency 2 — the priority Llama slot can absorb 2 parallel
  // short summaries comfortably. Sequential (concurrency=1) hits timeouts at
  // 30 entities × ~10s = ~300s.
  const concurrency = payload.concurrency ?? (isSprint ? 4 : 2);

  let entitiesSummarized = 0;
  let domainSummariesCreated = 0;
  let relationshipSummariesCreated = 0;
  let errors = 0;

  // --- Step 1: Entity summaries ---

  let entities: EntityRow[];

  if (payload.entity_ids && payload.entity_ids.length > 0) {
    // Specific entities requested
    const { rows } = await pool.query<EntityRow>(
      `SELECT id, entity_type, canonical_name, fact_count, summary, summary_updated_at, aliases, contact_id, metadata
       FROM knowledge_entities WHERE id = ANY($1::uuid[])`,
      [payload.entity_ids]
    );
    entities = rows;
  } else {
    // Atomically claim entities by setting summary_updated_at, preventing
    // other concurrent jobs from selecting the same batch.
    const { rows } = await pool.query<EntityRow>(
      `UPDATE knowledge_entities
       SET summary_updated_at = NOW()
       WHERE id IN (
         SELECT id FROM knowledge_entities
         WHERE fact_count >= 5
           AND (summary IS NULL
                OR summary_updated_at IS NULL
                OR summary_updated_at < updated_at)
         ORDER BY fact_count DESC
         FOR UPDATE SKIP LOCKED
         LIMIT $1
       )
       RETURNING id, entity_type, canonical_name, fact_count, summary, summary_updated_at, aliases, contact_id, metadata`,
      [batchSize],
    );
    entities = rows;
  }

  logger.log(`Claimed ${entities.length} entities (batch_size=${batchSize}, concurrency=${concurrency}, sprint=${isSprint})`);
  await jobLog(job.id, `Claimed ${entities.length} entities for summarization`);

  if (entities.length === 0) {
    return { entities_summarized: 0, domain_overviews: 0, relationship_summaries: 0, errors: 0, message: 'no entities need summarization' };
  }

  // Process entities with concurrency
  const results = await mapWithConcurrency(entities, concurrency, async (entity) => {
    await generateEntitySummary(pool, entity);
    return entity.canonical_name;
  });

  for (let i = 0; i < results.length; i++) {
    if (results[i] instanceof Error) {
      errors++;
      const err = results[i] as Error;
      logger.logMinimal(`Failed to summarize entity "${entities[i].canonical_name}": ${err.message}`);
      await jobLog(job.id, `Failed to summarize "${entities[i].canonical_name}": ${err.message}`, 'error');
      // Clear the claim so another job can retry this entity
      await pool.query(
        `UPDATE knowledge_entities SET summary_updated_at = NULL WHERE id = $1 AND summary IS NULL`,
        [entities[i].id]
      );
    } else {
      entitiesSummarized++;
    }
  }

  await jobLog(job.id, `Entity summaries done: ${entitiesSummarized} summarized, ${errors} errors`);

  // --- Step 2 & 3: Domain overviews + Relationship summaries (skip in sprint mode) ---

  if (!isSprint) {
    try {
      domainSummariesCreated = await generateDomainOverviews(pool, payload.force || false);
      await jobLog(job.id, `Generated ${domainSummariesCreated} domain overview summaries`);
    } catch (err) {
      errors++;
      logger.logMinimal('Failed to generate domain overviews:', err instanceof Error ? err.message : err);
      await jobLog(job.id, `Failed to generate domain overviews: ${err instanceof Error ? err.message : err}`, 'error');
    }

    try {
      relationshipSummariesCreated = await generateRelationshipSummaries(pool, payload.force || false);
      await jobLog(job.id, `Generated ${relationshipSummariesCreated} relationship summaries`);
    } catch (err) {
      errors++;
      logger.logMinimal('Failed to generate relationship summaries:', err instanceof Error ? err.message : err);
      await jobLog(job.id, `Failed to generate relationship summaries: ${err instanceof Error ? err.message : err}`, 'error');
    }

    // Refresh materialized view
    try {
      await pool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY knowledge_map');
      logger.log('Refreshed knowledge_map materialized view');
    } catch (err) {
      logger.logVerbose('Could not refresh knowledge_map:', err instanceof Error ? err.message : err);
    }

    // Clear stale flags
    await pool.query(
      `UPDATE knowledge_summaries SET stale = FALSE, updated_at = NOW()
       WHERE stale = TRUE AND updated_at > NOW() - INTERVAL '1 hour'`
    );
  }

  const result = {
    entities_summarized: entitiesSummarized,
    domain_overviews: domainSummariesCreated,
    relationship_summaries: relationshipSummariesCreated,
    errors,
  };

  logger.log(`Complete: ${JSON.stringify(result)}`);
  await jobLog(job.id, `Complete: ${entitiesSummarized} entities, ${domainSummariesCreated} domains, ${relationshipSummariesCreated} relationships, ${errors} errors`);
  return result;
}

// ---------------------------------------------------------------------------
// Entity Summary Generation
// ---------------------------------------------------------------------------

async function generateEntitySummary(pool: ReturnType<typeof getPool>, entity: EntityRow): Promise<void> {
  // Get all facts for this entity
  const { rows: facts } = await pool.query<FactRow>(
    `SELECT f.domain, f.category, f.key, f.value, f.confidence, f.evidence_count, f.sources, f.valid_from, f.valid_until
     FROM knowledge_facts f
     JOIN knowledge_fact_entities kfe ON kfe.fact_id = f.id
     WHERE kfe.entity_id = $1 AND f.superseded_by IS NULL
     ORDER BY f.confidence DESC, f.evidence_count DESC
     LIMIT 200`,
    [entity.id]
  );

  if (facts.length < 3) {
    logger.logVerbose(`Skipping "${entity.canonical_name}" — only ${facts.length} facts`);
    return;
  }

  // Format facts for the LLM
  const factsText = formatFactsForLLM(facts);

  // Choose prompt based on entity type
  const systemPrompt = getEntitySummarySystemPrompt(entity.entity_type);
  const userMessage = buildEntitySummaryPrompt(entity, factsText, facts.length);

  const result = await routeRequest({
    handler: 'knowledge-summarize',
    taskTier: 'generation',
    systemPrompt,
    userMessage,
    maxTokens: 2400,
    // useBatch: false — real-time path. Generation tier currently routes to
    // Claude Haiku as primary (~1.5s/call). Batch API was the old config when
    // qwen3-next-chat-80b was primary; with Haiku it would be hours of latency for
    // ~$0.01 of savings per summary. Real-time wins.
    useBatch: false,
  });

  // Store summary on the entity
  await pool.query(
    `UPDATE knowledge_entities
     SET summary = $2, summary_updated_at = NOW(), updated_at = NOW()
     WHERE id = $1`,
    [entity.id, result.text.trim()]
  );

  logger.logVerbose(`Summarized "${entity.canonical_name}" (${entity.entity_type}, ${facts.length} facts, ${result.model})`);
}

function getEntitySummarySystemPrompt(entityType: string): string {
  const base = `You are a concise knowledge summarizer for a personal AI assistant called ARIA. Your job is to write compact, information-dense summaries that capture the most important facts about an entity. Write in third person. Be factual, not speculative. Omit filler. Target 100-300 words.`;

  switch (entityType) {
    case 'person':
      return `${base}\n\nFor people: capture relationship to the owner the owner, key personality traits, shared interests, communication patterns, important life events, and any notable dynamics. Mention nicknames or alternate names if relevant.`;
    case 'place':
      return `${base}\n\nFor places: capture significance to the owner, frequency of visits, what happens there, geographic context, and any memories or events associated with it.`;
    case 'topic':
      return `${base}\n\nFor topics: capture the owner's level of engagement, key opinions or preferences, how the topic has evolved over time, and connections to other aspects of life.`;
    case 'event':
      return `${base}\n\nFor events: capture what happened, when, who was involved, significance to the owner, and any lasting impact.`;
    case 'thing':
      return `${base}\n\nFor things: capture what it is, its significance to the owner, how it's used, and any notable history.`;
    default:
      return base;
  }
}

function buildEntitySummaryPrompt(entity: EntityRow, factsText: string, factCount: number): string {
  let prompt = `Summarize everything known about "${entity.canonical_name}" (${entity.entity_type}).\n\n`;

  if (entity.aliases.length > 0) {
    prompt += `Also known as: ${entity.aliases.join(', ')}\n\n`;
  }

  prompt += `Based on ${factCount} facts:\n\n${factsText}`;
  return prompt;
}

function formatFactsForLLM(facts: FactRow[]): string {
  // Group by domain for readability
  const grouped: Record<string, FactRow[]> = {};
  for (const f of facts) {
    if (!grouped[f.domain]) grouped[f.domain] = [];
    grouped[f.domain].push(f);
  }

  const sections: string[] = [];
  for (const [domain, domainFacts] of Object.entries(grouped)) {
    const lines = domainFacts.map(f => {
      let line = `  - ${f.key}: ${f.value}`;
      if (f.confidence >= 0.8) line += ' [high confidence]';
      if (f.evidence_count > 3) line += ` [${f.evidence_count}x confirmed]`;
      if (f.valid_from) line += ` [from ${f.valid_from.toISOString().slice(0, 10)}]`;
      return line;
    });
    sections.push(`[${domain}]\n${lines.join('\n')}`);
  }

  return sections.join('\n\n');
}

// ---------------------------------------------------------------------------
// Domain Overview Summaries
// ---------------------------------------------------------------------------

async function generateDomainOverviews(pool: ReturnType<typeof getPool>, force: boolean): Promise<number> {
  // Get domains with significant fact counts
  const { rows: domains } = await pool.query<{ domain: string; cnt: string }>(
    `SELECT domain, COUNT(*) as cnt FROM knowledge_facts
     WHERE superseded_by IS NULL
     GROUP BY domain HAVING COUNT(*) >= 10
     ORDER BY cnt DESC`
  );

  let created = 0;

  for (const { domain, cnt } of domains) {
    // Check if we already have a fresh summary
    if (!force) {
      const { rows: existing } = await pool.query<{ id: string }>(
        `SELECT id FROM knowledge_summaries
         WHERE summary_type = 'domain_overview' AND title = $1
         AND stale = FALSE AND updated_at > NOW() - INTERVAL '24 hours'
         LIMIT 1`,
        [domain]
      );
      if (existing.length > 0) continue;
    }

    // Get top facts for this domain
    const { rows: facts } = await pool.query<FactRow>(
      `SELECT domain, category, key, value, confidence, evidence_count, sources, valid_from, valid_until
       FROM knowledge_facts
       WHERE domain = $1 AND superseded_by IS NULL
       ORDER BY confidence DESC, evidence_count DESC
       LIMIT 100`,
      [domain]
    );

    const factsText = formatFactsForLLM(facts);

    const result = await routeRequest({
      handler: 'knowledge-summarize',
      taskTier: 'generation',
      systemPrompt: `You are a concise knowledge summarizer for a personal AI assistant. Write a compact overview (150-300 words) of the "${domain}" knowledge domain for the owner the owner. Highlight the most important facts, patterns, and themes. Write in a way that helps the AI assistant understand this area of the owner's life at a glance.`,
      userMessage: `Domain: ${domain}\nTotal facts: ${cnt}\n\nTop ${facts.length} facts:\n\n${factsText}`,
      maxTokens: 2400,
      useBatch: true,
    });

    // Delete-then-insert since there's no unique constraint on type+title
    await pool.query(
      `DELETE FROM knowledge_summaries WHERE summary_type = 'domain_overview' AND title = $1`,
      [domain]
    );
    await pool.query(
      `INSERT INTO knowledge_summaries (summary_type, title, content, fact_count, stale)
       VALUES ('domain_overview', $1, $2, $3, FALSE)`,
      [domain, result.text.trim(), parseInt(cnt)]
    );

    created++;
    logger.logVerbose(`Generated domain overview for "${domain}" (${cnt} facts)`);
  }

  return created;
}

// ---------------------------------------------------------------------------
// Relationship Summaries
// ---------------------------------------------------------------------------

async function generateRelationshipSummaries(pool: ReturnType<typeof getPool>, force: boolean): Promise<number> {
  // Get top people entities by fact count
  const { rows: people } = await pool.query<EntityRow>(
    `SELECT id, entity_type, canonical_name, fact_count, summary, summary_updated_at, aliases, contact_id, metadata
     FROM knowledge_entities
     WHERE entity_type = 'person' AND fact_count >= 10
     ORDER BY fact_count DESC
     LIMIT 20`
  );

  let created = 0;

  for (const person of people) {
    // Check if we already have a fresh relationship summary
    if (!force) {
      const { rows: existing } = await pool.query<{ id: string }>(
        `SELECT id FROM knowledge_summaries
         WHERE summary_type = 'relationship' AND $1 = ANY(entity_ids)
         AND stale = FALSE AND updated_at > NOW() - INTERVAL '48 hours'
         LIMIT 1`,
        [person.id]
      );
      if (existing.length > 0) continue;
    }

    // Get all facts linked to this person
    const { rows: facts } = await pool.query<FactRow>(
      `SELECT f.domain, f.category, f.key, f.value, f.confidence, f.evidence_count, f.sources, f.valid_from, f.valid_until
       FROM knowledge_facts f
       JOIN knowledge_fact_entities kfe ON kfe.fact_id = f.id
       WHERE kfe.entity_id = $1 AND f.superseded_by IS NULL
       ORDER BY f.confidence DESC, f.evidence_count DESC
       LIMIT 150`,
      [person.id]
    );

    if (facts.length < 5) continue;

    // Also get conversation chunk summaries if available
    const { rows: chunkStats } = await pool.query<{ cnt: string; earliest: Date | null; latest: Date | null }>(
      `SELECT COUNT(*) as cnt, MIN(time_start) as earliest, MAX(time_end) as latest
       FROM knowledge_conversation_chunks
       WHERE entity_id = $1`,
      [person.id]
    );

    const factsText = formatFactsForLLM(facts);
    let conversationContext = '';
    if (parseInt(chunkStats[0]?.cnt || '0') > 0) {
      conversationContext = `\n\nConversation history: ${chunkStats[0].cnt} conversation chunks spanning ${chunkStats[0].earliest?.toISOString().slice(0, 10) || '?'} to ${chunkStats[0].latest?.toISOString().slice(0, 10) || '?'}`;
    }

    const result = await routeRequest({
      handler: 'knowledge-summarize',
      taskTier: 'generation',
      systemPrompt: `You are a concise relationship summarizer for a personal AI assistant called ARIA. Write a compact relationship profile (200-400 words) that captures the dynamics between the owner the owner and this person. Include: relationship type, how they met or are connected, communication patterns, shared interests, key memories or events, current status of the relationship, and any notable dynamics or tensions. Write in third person. Be factual.`,
      userMessage: `Person: ${person.canonical_name}${person.aliases.length > 0 ? ` (also known as: ${person.aliases.join(', ')})` : ''}\nFact count: ${person.fact_count}\n\n${factsText}${conversationContext}`,
      maxTokens: 2400,
      useBatch: true,
    });

    // Delete existing and insert new
    await pool.query(
      `DELETE FROM knowledge_summaries WHERE summary_type = 'relationship' AND $1 = ANY(entity_ids)`,
      [person.id]
    );
    await pool.query(
      `INSERT INTO knowledge_summaries (summary_type, title, content, entity_ids, fact_count, stale)
       VALUES ('relationship', $1, $2, ARRAY[$3]::uuid[], $4, FALSE)`,
      [`Owner & ${person.canonical_name}`, result.text.trim(), person.id, person.fact_count]
    );

    created++;
    logger.logVerbose(`Generated relationship summary for "${person.canonical_name}" (${facts.length} facts)`);
  }

  return created;
}
