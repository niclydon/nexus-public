import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { getPool } from '@nexus/core';

export function registerBiographyTools(server: McpServer): void {
  // ── Biography Queue ─────────────────────────────────────
  server.tool(
    'nexus_biography_queue',
    'List pending biographical inferences for review (hypotheses about life events, relationships, etc.)',
    {
      limit: z.number().optional().default(20).describe('Max items'),
    },
    async ({ limit }) => {
      const pool = getPool();
      const { rows } = await pool.query(
        `SELECT id, inference_type, hypothesis, confidence, subject_name,
                related_name, evidence_summary, rule_key, created_at
         FROM bio_inference_queue
         WHERE status = 'pending'
         ORDER BY confidence DESC, created_at ASC
         LIMIT $1`,
        [limit],
      );
      return { content: [{ type: 'text' as const, text: JSON.stringify({ count: rows.length, queue: rows }, null, 2) }] };
    },
  );

  // ── Biography Decide ────────────────────────────────────
  server.tool(
    'nexus_biography_decide',
    'Confirm, correct, reject, or skip a biographical inference',
    {
      inference_id: z.string().describe('Inference UUID'),
      status: z.enum(['confirmed', 'corrected', 'rejected', 'skipped']).describe('Decision'),
      notes: z.string().optional().describe('Optional correction notes'),
    },
    async ({ inference_id, status, notes }) => {
      const pool = getPool();
      const { rowCount } = await pool.query(
        `UPDATE bio_inference_queue
         SET status = $1, decided_at = NOW(), decided_by = 'owner', notes = $3
         WHERE id = $2 AND status = 'pending'`,
        [status, inference_id, notes ?? null],
      );
      if (rowCount === 0) {
        return { content: [{ type: 'text' as const, text: 'ERROR: Inference not found or already decided' }] };
      }
      return { content: [{ type: 'text' as const, text: `Inference ${inference_id} marked as ${status}` }] };
    },
  );
}
