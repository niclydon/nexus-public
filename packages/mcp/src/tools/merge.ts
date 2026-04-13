import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { getPool, entityMerge } from '@nexus/core';

export function registerMergeTools(server: McpServer): void {
  // ── Merge Candidates ────────────────────────────────────
  server.tool(
    'nexus_merge_candidates',
    'List pending entity merge candidates (duplicate people, knowledge entities)',
    {
      entity_type: z.enum(['person', 'knowledge_entity', 'photo_person', 'contact_person']).optional()
        .describe('Filter by entity type'),
      min_confidence: z.number().optional().default(0.6).describe('Minimum confidence threshold'),
      limit: z.number().optional().default(20).describe('Max candidates'),
    },
    async ({ entity_type, min_confidence, limit }) => {
      const pool = getPool();
      const conditions = ["status = 'pending'"];
      const params: unknown[] = [];
      let idx = 1;

      if (entity_type) {
        conditions.push(`entity_type = $${idx++}`);
        params.push(entity_type);
      }
      conditions.push(`confidence >= $${idx++}`);
      params.push(min_confidence);
      params.push(limit);

      const { rows } = await pool.query(
        `SELECT id, entity_type, record_a_id, record_a_name, record_b_id, record_b_name,
                confidence, reason, created_at
         FROM merge_candidates
         WHERE ${conditions.join(' AND ')}
         ORDER BY confidence DESC
         LIMIT $${idx}`,
        params,
      );
      return { content: [{ type: 'text' as const, text: JSON.stringify({ count: rows.length, candidates: rows }, null, 2) }] };
    },
  );

  // ── Merge Decide ────────────────────────────────────────
  server.tool(
    'nexus_merge_decide',
    'Merge, dismiss, or defer a merge candidate',
    {
      candidate_id: z.string().describe('Merge candidate UUID'),
      action: z.enum(['merge', 'dismiss', 'defer']).describe('What to do with this candidate'),
      keeper_id: z.string().optional().describe('For merge: which record to keep (record_a_id or record_b_id)'),
    },
    async ({ candidate_id, action, keeper_id }) => {
      try {
        if (action === 'dismiss') {
          await entityMerge.dismissCandidate(candidate_id);
          return { content: [{ type: 'text' as const, text: `Dismissed candidate ${candidate_id}` }] };
        }
        if (action === 'defer') {
          await entityMerge.deferCandidate(candidate_id);
          return { content: [{ type: 'text' as const, text: `Deferred candidate ${candidate_id}` }] };
        }

        // merge
        if (!keeper_id) {
          return { content: [{ type: 'text' as const, text: 'ERROR: keeper_id is required for merge action' }] };
        }

        // Look up the candidate to get both IDs and entity type
        const pool = getPool();
        const { rows } = await pool.query(
          `SELECT entity_type, record_a_id, record_b_id FROM merge_candidates WHERE id = $1`,
          [candidate_id],
        );
        if (rows.length === 0) {
          return { content: [{ type: 'text' as const, text: 'ERROR: Candidate not found' }] };
        }
        const { entity_type, record_a_id, record_b_id } = rows[0];
        const merged_id = keeper_id === record_a_id ? record_b_id : record_a_id;

        const result = await entityMerge.executeMerge({
          candidateId: candidate_id,
          keeperId: keeper_id,
          mergedId: merged_id,
          entityType: entity_type,
          mergedBy: 'owner',
        });
        return { content: [{ type: 'text' as const, text: JSON.stringify({ status: 'merged', ...result }, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text' as const, text: `ERROR: ${(err as Error).message}` }] };
      }
    },
  );
}
