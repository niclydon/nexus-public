import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { getPool, evalScorer } from '@nexus/core';

export function registerAgentTools(server: McpServer): void {
  // ── Agent Memories ──────────────────────────────────────
  server.tool(
    'nexus_agent_memories',
    "Read an agent's persistent memory bank (learned facts, preferences, baselines)",
    {
      agent_id: z.string().describe('Agent ID (e.g., aria, insight, pipeline)'),
      include_shared: z.boolean().optional().default(true).describe('Include memories shared by other agents'),
      limit: z.number().optional().default(50).describe('Max memories to return'),
    },
    async ({ agent_id, include_shared, limit }) => {
      const pool = getPool();
      const where = include_shared
        ? `WHERE (agent_id = $1 OR is_shared = true) AND is_active = true`
        : `WHERE agent_id = $1 AND is_active = true`;
      const { rows } = await pool.query(
        `SELECT id, agent_id, category, key, value, confidence,
                times_reinforced, source, created_at
         FROM agent_memory
         ${where}
         ORDER BY confidence DESC NULLS LAST, times_reinforced DESC
         LIMIT $2`,
        [agent_id, limit],
      );
      return { content: [{ type: 'text' as const, text: JSON.stringify({ agent_id, count: rows.length, memories: rows }, null, 2) }] };
    },
  );

  // ── Eval Scores ─────────────────────────────────────────
  server.tool(
    'nexus_eval_scores',
    'Get agent quality scores (task completion, faithfulness, hallucination, coherence, relevancy)',
    {
      agent_id: z.string().describe('Agent ID'),
      scorer: z.string().optional().describe('Filter by scorer type'),
      period: z.string().optional().default('7 days').describe("Time window (e.g., '7 days', '30 days')"),
      summary: z.boolean().optional().default(true).describe('Return aggregates instead of individual scores'),
    },
    async ({ agent_id, scorer, period, summary }) => {
      try {
        if (summary) {
          const result = await evalScorer.getScoreSummary(agent_id, period);
          return { content: [{ type: 'text' as const, text: JSON.stringify({ agent_id, period, summary: result }, null, 2) }] };
        }
        const result = await evalScorer.getRecentScores(agent_id, { scorer, minAge: period });
        return { content: [{ type: 'text' as const, text: JSON.stringify({ agent_id, period, scores: result }, null, 2) }] };
      } catch (err) {
        return { content: [{ type: 'text' as const, text: `ERROR: ${(err as Error).message}` }] };
      }
    },
  );

  // ── Decision Review ─────────────────────────────────────
  server.tool(
    'nexus_decision_review',
    'List recent agent decisions awaiting human feedback (thumbs up/down)',
    {
      agent_id: z.string().optional().describe('Filter by agent ID'),
      limit: z.number().optional().default(10).describe('Max decisions'),
    },
    async ({ agent_id, limit }) => {
      const pool = getPool();
      const whereClause = agent_id ? 'AND d.agent_id = $2' : '';
      const params: unknown[] = [limit];
      if (agent_id) params.push(agent_id);

      const { rows } = await pool.query(
        `SELECT d.id, d.agent_id, r.display_name,
                d.state_snapshot->>'status' as status,
                d.state_snapshot->>'summary' as summary,
                d.state_snapshot->'parsed_actions' as actions,
                d.duration_ms, d.created_at
         FROM agent_decisions d
         JOIN agent_registry r ON r.agent_id = d.agent_id
         LEFT JOIN agent_decision_feedback f ON f.decision_id = d.id
         WHERE f.id IS NULL
           AND d.created_at > NOW() - INTERVAL '7 days'
           ${whereClause}
         ORDER BY d.created_at DESC
         LIMIT $1`,
        params,
      );
      return { content: [{ type: 'text' as const, text: JSON.stringify(rows, null, 2) }] };
    },
  );

  // ── Decision Feedback ───────────────────────────────────
  server.tool(
    'nexus_decision_feedback',
    'Submit thumbs up (+1) or down (-1) on an agent decision',
    {
      decision_id: z.number().describe('Decision ID'),
      rating: z.number().describe('+1 for good, -1 for bad'),
      comment: z.string().optional().describe('Optional feedback note'),
    },
    async ({ decision_id, rating, comment }) => {
      if (rating !== 1 && rating !== -1) {
        return { content: [{ type: 'text' as const, text: 'ERROR: rating must be 1 or -1' }] };
      }
      const pool = getPool();
      const { rows } = await pool.query(
        `INSERT INTO agent_decision_feedback (decision_id, rating, comment, reviewer)
         VALUES ($1, $2, $3, 'owner')
         ON CONFLICT (decision_id) DO UPDATE SET rating = $2, comment = $3, reviewed_at = NOW()
         RETURNING id`,
        [decision_id, rating, comment ?? null],
      );
      return { content: [{ type: 'text' as const, text: `Feedback recorded (id: ${rows[0]?.id}) — ${rating === 1 ? 'thumbs up' : 'thumbs down'}` }] };
    },
  );
}
