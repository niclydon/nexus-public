import { getPool } from '../db.js';
import { createLogger } from '../logger.js';

const logger = createLogger('semantic-search');

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type SearchCategory =
  | 'communication'
  | 'knowledge'
  | 'media'
  | 'social'
  | 'health'
  | 'biographical'
  | 'all';

export interface SearchOptions {
  query: string;
  categories?: SearchCategory[];
  limit?: number;
  rerank?: boolean;
  minSimilarity?: number;
}

export interface SearchResult {
  id: string;
  table: string;
  category: SearchCategory;
  text: string;
  timestamp: string | null;
  similarity: number;
  rerank_score?: number;
  topic_cluster_id?: string;
  topic_cluster_label?: string;
  topic_boosted?: boolean;
}

export interface MatchedTopic {
  cluster_id: string;
  label: string;
  level: number;
  similarity: number;
}

interface SearchResponse {
  results: SearchResult[];
  query_embedding_ms: number;
  search_ms: number;
  rerank_ms: number;
  tables_searched: number;
  matched_topics?: MatchedTopic[];
}

// ---------------------------------------------------------------------------
// Table configs
// ---------------------------------------------------------------------------

interface TableConfig {
  table: string;
  idColumn: string;
  idType: 'text' | 'integer' | 'uuid';
  textExpr: string;
  timestampExpr: string;
  category: Exclude<SearchCategory, 'all'>;
  extraWhere?: string;
}

const TABLE_CONFIGS: TableConfig[] = [
  // Communication
  {
    table: 'aurora_raw_imessage',
    idColumn: 'guid',
    idType: 'text',
    textExpr: `COALESCE(handle_id,'') || ': ' || COALESCE(text,'')`,
    timestampExpr: 'date',
    category: 'communication',
  },
  {
    table: 'aurora_raw_gmail',
    idColumn: 'id',
    idType: 'text',
    textExpr: `COALESCE(from_name,from_address,'') || ': ' || COALESCE(subject,'') || ' — ' || COALESCE(LEFT(body_text,300),'')`,
    timestampExpr: 'date',
    category: 'communication',
  },
  {
    table: 'aurora_raw_google_voice',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(contact_name,phone_number,'') || ': ' || COALESCE(message_text,'')`,
    timestampExpr: 'timestamp',
    category: 'communication',
  },
  {
    table: 'aurora_raw_google_chat',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(sender_name,sender_email,'') || ': ' || COALESCE(message_text,'')`,
    timestampExpr: 'timestamp',
    category: 'communication',
  },
  {
    table: 'bb_sms_messages',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(phone_number,'') || ': ' || COALESCE(message,'')`,
    timestampExpr: 'timestamp',
    category: 'communication',
  },
  {
    table: 'archived_sent_emails',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(subject,'') || ' — ' || COALESCE(LEFT(body_text,300),'')`,
    timestampExpr: 'sent_at',
    category: 'communication',
  },
  {
    table: 'site_guestbook',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(poster_name,'') || ' to ' || COALESCE(recipient_name,'') || ': ' || COALESCE(message_text,'')`,
    timestampExpr: 'posted_at',
    category: 'communication',
  },

  // Knowledge
  {
    table: 'knowledge_facts',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(domain,'') || '/' || COALESCE(category,'') || ': ' || COALESCE(key,'') || ' = ' || COALESCE(LEFT(value,500),'')`,
    timestampExpr: 'created_at',
    category: 'knowledge',
    extraWhere: 'AND superseded_by IS NULL',
  },
  {
    table: 'knowledge_entities',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(canonical_name,'') || COALESCE(' (' || entity_type || ')','') || COALESCE(' — ' || LEFT(summary,300),'')`,
    timestampExpr: 'created_at',
    category: 'knowledge',
  },
  {
    table: 'knowledge_conversation_chunks',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(LEFT(chunk_text,500),'')`,
    timestampExpr: 'time_start',
    category: 'knowledge',
  },

  // Media
  {
    table: 'blogger_posts',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(title,'') || ' — ' || COALESCE(LEFT(content_text,300),'')`,
    timestampExpr: 'published_at',
    category: 'media',
  },
  {
    table: 'music_library',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(track_name,'') || ' by ' || COALESCE(artist_name,'') || COALESCE(' — ' || album_name,'')`,
    timestampExpr: 'last_played_at',
    category: 'media',
  },
  {
    table: 'aurora_raw_chatgpt',
    idColumn: 'id',
    idType: 'text',
    textExpr: `COALESCE(conversation_title,'') || ' — ' || COALESCE(LEFT(content_text,300),'')`,
    timestampExpr: 'created_at',
    category: 'media',
  },
  {
    table: 'aurora_raw_claude',
    idColumn: 'id',
    idType: 'text',
    textExpr: `COALESCE(conversation_name,'') || ' — ' || COALESCE(LEFT(content_text,300),'')`,
    timestampExpr: 'created_at',
    category: 'media',
  },
  {
    table: 'aurora_raw_siri_interactions',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(domain,'') || '/' || COALESCE(type,'') || ' via ' || COALESCE(bundle_id,'')`,
    timestampExpr: 'start_date',
    category: 'media',
  },
  {
    table: 'apple_notes',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(title,'') || ' — ' || COALESCE(LEFT(body,300),'')`,
    timestampExpr: 'modified_at',
    category: 'media',
  },

  // Social
  {
    table: 'aurora_raw_facebook',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(sender,'') || ' (' || COALESCE(data_type,'') || '): ' || COALESCE(LEFT(content,300),'')`,
    timestampExpr: 'timestamp',
    category: 'social',
  },
  {
    table: 'aurora_raw_instagram',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(sender,'') || ' (' || COALESCE(data_type,'') || '): ' || COALESCE(LEFT(content,300),'')`,
    timestampExpr: 'timestamp',
    category: 'social',
  },
  {
    table: 'contacts',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(display_name,'') || COALESCE(' — ' || job_title,'') || COALESCE(' — ' || notes,'')`,
    timestampExpr: 'updated_at',
    category: 'social',
  },

  // Health
  {
    table: 'strava_activities',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(activity_name,'') || ' (' || COALESCE(activity_type,'') || ')' || COALESCE(' — ' || description,'')`,
    timestampExpr: 'activity_date',
    category: 'health',
  },
  {
    table: 'life_transactions',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(merchant,'') || ': ' || COALESCE(description,'') || COALESCE(' $' || amount,'') || COALESCE(' [' || category || ']','')`,
    timestampExpr: 'transaction_date',
    category: 'health',
  },
  {
    table: 'looki_moments',
    idColumn: 'id',
    idType: 'text',
    textExpr: `COALESCE(title,'') || ' — ' || COALESCE(transcript,description,summary,'')`,
    timestampExpr: 'start_time',
    category: 'health',
  },

  // Biographical
  {
    table: 'life_narration',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(LEFT(narrative,500),'')`,
    timestampExpr: 'created_at',
    category: 'biographical',
  },
  {
    table: 'aria_journal',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(LEFT(content,500),'')`,
    timestampExpr: 'created_at',
    category: 'biographical',
  },
  {
    table: 'proactive_insights',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(title,'') || ' — ' || COALESCE(LEFT(body,300),'')`,
    timestampExpr: 'created_at',
    category: 'biographical',
  },
  {
    table: 'photo_metadata',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(LEFT(description,500),'')`,
    timestampExpr: 'created_at',
    category: 'biographical',
  },
];

// ---------------------------------------------------------------------------
// Category → table filtering
// ---------------------------------------------------------------------------

const CATEGORY_MAP = new Map<Exclude<SearchCategory, 'all'>, TableConfig[]>();
for (const cfg of TABLE_CONFIGS) {
  const list = CATEGORY_MAP.get(cfg.category) ?? [];
  list.push(cfg);
  CATEGORY_MAP.set(cfg.category, list);
}

function resolveConfigs(categories: SearchCategory[]): TableConfig[] {
  if (categories.includes('all')) return TABLE_CONFIGS;
  const seen = new Set<string>();
  const out: TableConfig[] = [];
  for (const cat of categories) {
    if (cat === 'all') continue;
    for (const cfg of CATEGORY_MAP.get(cat) ?? []) {
      if (!seen.has(cfg.table)) {
        seen.add(cfg.table);
        out.push(cfg);
      }
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Forge helpers
// ---------------------------------------------------------------------------

function forgeEmbedUrl(): string {
  const base = process.env.FORGE_BASE_URL?.replace('/v1', '') || 'http://localhost:8642';
  return `${base}/embed`;
}

function forgeRerankUrl(): string {
  const base = process.env.FORGE_BASE_URL || 'http://localhost:8642/v1';
  return `${base}/rerank`;
}

function forgeHeaders(): Record<string, string> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (process.env.FORGE_API_KEY) {
    headers['Authorization'] = `Bearer ${process.env.FORGE_API_KEY}`;
  }
  return headers;
}

async function getEmbedding(text: string): Promise<number[]> {
  const res = await fetch(forgeEmbedUrl(), {
    method: 'POST',
    headers: forgeHeaders(),
    body: JSON.stringify({ texts: [text] }),
    signal: AbortSignal.timeout(10_000),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Forge embed failed (${res.status}): ${body.slice(0, 200)}`);
  }
  const json = (await res.json()) as { embeddings?: number[][] };
  if (json.embeddings?.[0]) return json.embeddings[0];
  throw new Error('Unexpected embed response shape');
}

async function rerank(
  query: string,
  documents: string[],
): Promise<{ index: number; relevance_score: number }[]> {
  const res = await fetch(forgeRerankUrl(), {
    method: 'POST',
    headers: forgeHeaders(),
    body: JSON.stringify({ query, documents }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Forge rerank failed (${res.status}): ${body.slice(0, 200)}`);
  }
  const json = (await res.json()) as { results: { index: number; score: number }[] };
  return json.results.map(r => ({ index: r.index, relevance_score: r.score }));
}

// ---------------------------------------------------------------------------
// Per-table query
// ---------------------------------------------------------------------------

async function searchTable(
  cfg: TableConfig,
  embeddingLiteral: string,
  perTableLimit: number,
  minSimilarity: number,
): Promise<SearchResult[]> {
  const pool = getPool();
  const sql = `
    SELECT
      ${cfg.idColumn}::text AS id,
      ${cfg.textExpr} AS text,
      ${cfg.timestampExpr}::text AS timestamp,
      1 - (embedding <=> $1::vector) AS similarity
    FROM ${cfg.table}
    WHERE embedding IS NOT NULL
      AND 1 - (embedding <=> $1::vector) >= $2
      ${cfg.extraWhere ?? ''}
    ORDER BY embedding <=> $1::vector
    LIMIT $3
  `;

  const { rows } = await pool.query(sql, [embeddingLiteral, minSimilarity, perTableLimit]);

  return rows.map((r: Record<string, unknown>) => ({
    id: r.id as string,
    table: cfg.table,
    category: cfg.category,
    text: (r.text as string) ?? '',
    timestamp: (r.timestamp as string) ?? null,
    similarity: Number(r.similarity),
  }));
}

// ---------------------------------------------------------------------------
// Main search
// ---------------------------------------------------------------------------

export async function search(options: SearchOptions): Promise<SearchResponse> {
  const {
    query,
    categories = ['all'],
    limit: rawLimit = 20,
    rerank: doRerank = true,
    minSimilarity = 0.3,
  } = options;

  const limit = Math.min(Math.max(rawLimit, 1), 100);
  const perTableLimit = limit * 2;
  const configs = resolveConfigs(categories);

  logger.logVerbose(
    `Searching ${configs.length} tables for "${query.slice(0, 80)}" (limit=${limit}, rerank=${doRerank})`,
  );

  // 1. Embed the query
  const stopEmbed = logger.time('embed');
  const embedStart = performance.now();
  let embedding: number[];
  try {
    embedding = await getEmbedding(query);
  } catch (err) {
    logger.logMinimal('Embedding failed:', (err as Error).message);
    throw err;
  }
  stopEmbed();
  const queryEmbeddingMs = Math.round(performance.now() - embedStart);

  const embeddingLiteral = `[${embedding.join(',')}]`;

  // 2. Parallel pgvector queries
  const searchStart = performance.now();
  const settled = await Promise.allSettled(
    configs.map((cfg) => searchTable(cfg, embeddingLiteral, perTableLimit, minSimilarity)),
  );

  let allResults: SearchResult[] = [];
  let tablesSearched = 0;

  for (let i = 0; i < settled.length; i++) {
    const outcome = settled[i];
    if (outcome.status === 'fulfilled') {
      tablesSearched++;
      allResults.push(...outcome.value);
      logger.logVerbose(`  ${configs[i].table}: ${outcome.value.length} results`);
    } else {
      logger.logMinimal(`Table ${configs[i].table} query failed:`, outcome.reason?.message ?? outcome.reason);
    }
  }

  allResults.sort((a, b) => b.similarity - a.similarity);
  const searchMs = Math.round(performance.now() - searchStart);
  logger.logVerbose(`Vector search: ${searchMs}ms across ${tablesSearched} tables, ${allResults.length} raw results`);

  // 2b. Cluster-aware boost: find which topic clusters the query maps to,
  // annotate every result with its cluster, and boost similarity for results
  // that fall in one of the top-3 query clusters. This makes "Tunisia trip"
  // surface items from the Tunisian-photos cluster even if their raw cosine
  // is slightly worse than unrelated items that share keywords.
  let matchedTopics: MatchedTopic[] | undefined;
  if (allResults.length > 0) {
    try {
      const pool = getPool();
      // Find top-3 nearest L2 (more specific) clusters by query embedding.
      // Fall back to L1 if there are no labeled L2 clusters.
      const { rows: topicRows } = await pool.query<{ id: string; label: string; level: number; sim: number }>(
        `SELECT id, label, level, 1 - (centroid <=> $1::vector) AS sim
         FROM topic_clusters
         WHERE label IS NOT NULL
         ORDER BY centroid <=> $1::vector
         LIMIT 3`,
        [embeddingLiteral],
      );
      matchedTopics = topicRows.map((r) => ({
        cluster_id: r.id,
        label: r.label,
        level: r.level,
        similarity: Number(r.sim),
      }));
      const topicIds = new Set(topicRows.map((r) => r.id));

      // Bulk-fetch cluster assignments for all candidate results in one query.
      // Use parameterized arrays so we don't have to escape SQL.
      const sourceTables = allResults.map((r) => r.table);
      const sourceIds = allResults.map((r) => String(r.id));
      const { rows: assignRows } = await pool.query<{
        source_table: string;
        source_id: string;
        cluster_id: string;
        label: string | null;
      }>(
        `SELECT ta.source_table, ta.source_id, ta.cluster_id, tc.label
         FROM topic_assignments ta
         JOIN topic_clusters tc ON tc.id = ta.cluster_id
         JOIN unnest($1::text[], $2::text[]) AS u(t, i)
           ON u.t = ta.source_table AND u.i = ta.source_id`,
        [sourceTables, sourceIds],
      );
      {

        const assignMap = new Map<string, { cluster_id: string; label: string | null }>();
        for (const a of assignRows) {
          assignMap.set(`${a.source_table}|${a.source_id}`, { cluster_id: a.cluster_id, label: a.label });
        }

        // Annotate + boost
        const BOOST = 0.05;
        for (const r of allResults) {
          const a = assignMap.get(`${r.table}|${String(r.id)}`);
          if (!a) continue;
          r.topic_cluster_id = a.cluster_id;
          r.topic_cluster_label = a.label ?? undefined;
          if (topicIds.has(a.cluster_id)) {
            r.similarity = Math.min(1, r.similarity + BOOST);
            r.topic_boosted = true;
          }
        }
        allResults.sort((a, b) => b.similarity - a.similarity);
      }
      logger.logVerbose(
        `Cluster boost: top topics = [${matchedTopics.map((t) => `"${t.label}"`).join(', ')}], ` +
        `boosted ${allResults.filter((r) => r.topic_boosted).length} results`,
      );
    } catch (err) {
      // Cluster boost is best-effort — never fail the search if topic_clusters
      // is empty or the query times out.
      logger.logMinimal('Cluster boost failed (non-fatal):', (err as Error).message);
    }
  }

  // 3. Optional rerank
  let rerankMs = 0;
  if (doRerank && allResults.length > 0) {
    const rerankStart = performance.now();
    const textsToRerank = allResults.slice(0, limit * 3).map((r) => r.text);

    try {
      const scores = await rerank(query, textsToRerank);
      const scoreMap = new Map<number, number>();
      for (const s of scores) {
        scoreMap.set(s.index, s.relevance_score);
      }

      for (let i = 0; i < Math.min(allResults.length, textsToRerank.length); i++) {
        allResults[i].rerank_score = scoreMap.get(i);
      }

      allResults.sort((a, b) => {
        const sa = a.rerank_score ?? a.similarity;
        const sb = b.rerank_score ?? b.similarity;
        return sb - sa;
      });

      rerankMs = Math.round(performance.now() - rerankStart);
      logger.logVerbose(`Rerank: ${rerankMs}ms for ${textsToRerank.length} documents`);
    } catch (err) {
      rerankMs = Math.round(performance.now() - rerankStart);
      logger.logMinimal('Rerank failed, using similarity order:', (err as Error).message);
    }
  }

  const results = allResults.slice(0, limit);

  logger.logDebug('Top results:', results.slice(0, 5).map((r) => ({
    table: r.table,
    sim: r.similarity.toFixed(3),
    rerank: r.rerank_score?.toFixed(3),
    text: r.text.slice(0, 80),
  })));

  logger.log(
    `Search complete: "${query.slice(0, 50)}" → ${results.length} results from ${tablesSearched} tables ` +
    `(embed=${queryEmbeddingMs}ms, search=${searchMs}ms, rerank=${rerankMs}ms)`,
  );

  return {
    results,
    query_embedding_ms: queryEmbeddingMs,
    search_ms: searchMs,
    rerank_ms: rerankMs,
    tables_searched: tablesSearched,
    matched_topics: matchedTopics,
  };
}
