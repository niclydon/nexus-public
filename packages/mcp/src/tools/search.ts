import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { getPool, semanticSearch } from '@nexus/core';

export function registerSearchTools(server: McpServer): void {
  // ── Semantic Search ─────────────────────────────────────
  server.tool(
    'nexus_search',
    'Semantic search across all 26 Nexus data sources (messages, emails, photos, knowledge, music, etc.)',
    {
      query: z.string().describe('Natural language search query'),
      categories: z.array(z.enum(['communication', 'knowledge', 'media', 'social', 'health', 'biographical', 'all']))
        .optional().describe('Filter by category (default: all)'),
      limit: z.number().optional().default(20).describe('Max results (1-100)'),
      rerank: z.boolean().optional().default(true).describe('Enable LLM reranking for better relevance'),
    },
    async ({ query, categories, limit, rerank }) => {
      try {
        const result = await semanticSearch.search({
          query,
          categories: categories as semanticSearch.SearchCategory[] | undefined,
          limit,
          rerank,
        });
        const summary = {
          total_found: result.results.length,
          tables_searched: result.tables_searched,
          timing: {
            embed_ms: result.query_embedding_ms,
            search_ms: result.search_ms,
            rerank_ms: result.rerank_ms,
          },
          matched_topics: result.matched_topics,
          results: result.results.map(r => ({
            table: r.table,
            category: r.category,
            text: r.text.slice(0, 500),
            timestamp: r.timestamp,
            similarity: +r.similarity.toFixed(3),
            rerank_score: r.rerank_score != null ? +r.rerank_score.toFixed(3) : undefined,
            topic: r.topic_cluster_label,
          })),
        };
        return { content: [{ type: 'text' as const, text: JSON.stringify(summary, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text' as const, text: `ERROR: ${(err as Error).message}` }] };
      }
    },
  );

  // ── Knowledge Lookup ────────────────────────────────────
  server.tool(
    'nexus_knowledge_lookup',
    'Look up knowledge graph entities and their facts by name or ID',
    {
      name: z.string().optional().describe('Fuzzy match on entity name'),
      entity_id: z.string().optional().describe('Exact entity UUID'),
      include_facts: z.boolean().optional().default(true).describe('Include linked facts'),
      limit: z.number().optional().default(10).describe('Max entities to return'),
    },
    async ({ name, entity_id, include_facts, limit }) => {
      if (!name && !entity_id) {
        return { content: [{ type: 'text' as const, text: 'ERROR: Provide name or entity_id' }] };
      }
      const pool = getPool();
      try {
        let entityRows: any[];
        if (entity_id) {
          const { rows } = await pool.query(
            `SELECT id, canonical_name, entity_type, aliases, summary, fact_count
             FROM knowledge_entities WHERE id = $1`,
            [entity_id],
          );
          entityRows = rows;
        } else {
          const { rows } = await pool.query(
            `SELECT id, canonical_name, entity_type, aliases, summary, fact_count
             FROM knowledge_entities
             WHERE canonical_name ILIKE '%' || $1 || '%'
                OR aliases::text ILIKE '%' || $1 || '%'
             ORDER BY fact_count DESC NULLS LAST
             LIMIT $2`,
            [name, limit],
          );
          entityRows = rows;
        }

        if (include_facts && entityRows.length > 0) {
          const entityIds = entityRows.map((e: any) => e.id);
          const { rows: factRows } = await pool.query(
            `SELECT fe.entity_id, f.key, f.value, f.domain, f.category, f.confidence
             FROM knowledge_fact_entities fe
             JOIN knowledge_facts f ON f.id = fe.fact_id
             WHERE fe.entity_id = ANY($1)
               AND f.superseded_by IS NULL
             ORDER BY f.confidence DESC NULLS LAST
             LIMIT 100`,
            [entityIds],
          );
          const factMap = new Map<string, any[]>();
          for (const f of factRows) {
            const list = factMap.get(f.entity_id) ?? [];
            list.push({ key: f.key, value: f.value?.slice(0, 300), domain: f.domain, category: f.category, confidence: f.confidence });
            factMap.set(f.entity_id, list);
          }
          for (const e of entityRows) {
            e.facts = factMap.get(e.id) ?? [];
          }
        }

        return { content: [{ type: 'text' as const, text: JSON.stringify({ entities: entityRows }, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text' as const, text: `ERROR: ${(err as Error).message}` }] };
      }
    },
  );
}
