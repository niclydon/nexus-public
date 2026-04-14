/**
 * Nexus MCP Server — exposes the agent platform to external AI clients.
 *
 * 24 tools across 8 domains:
 *   - Core: agent_status, send_message, list_decisions, query, job_status, approvals
 *   - Search: search, knowledge_lookup
 *   - Health: platform_health, forge_status, data_sources
 *   - Agents: agent_memories, eval_scores, decision_review, decision_feedback
 *   - Jobs: run_job
 *   - Merge: merge_candidates, merge_decide
 *   - People: people_lookup, contacts_search
 *   - Biography: biography_queue, biography_decide
 *   - Insights: insights, llm_costs
 *
 * Transport: Streamable HTTP (port 7701) + stdio (for Claude Desktop)
 */
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import express from 'express';
import { z } from 'zod';
import { getPool, shutdown, assertShowcaseDemoEnabled } from '@nexus/core';

import { registerSearchTools } from './tools/search.js';
import { registerHealthTools } from './tools/health.js';
import { registerAgentTools } from './tools/agents.js';
import { registerJobTools } from './tools/jobs.js';
import { registerMergeTools } from './tools/merge.js';
import { registerPeopleTools } from './tools/people.js';
import { registerBiographyTools } from './tools/biography.js';
import { registerInsightTools } from './tools/insights.js';

assertShowcaseDemoEnabled('nexus-mcp');

function createServer(): McpServer {
  const server = new McpServer({
    name: 'nexus-mcp',
    version: '2.0.0',
  });

  // ── Core Tools (original 6) ─────────────────────────────

  server.tool(
    'nexus_agent_status',
    'Get the current status of all Nexus agents or a specific agent',
    { agent_id: z.string().optional().describe('Specific agent ID, or omit for all agents') },
    async ({ agent_id }) => {
      const pool = getPool();
      if (agent_id) {
        const { rows } = await pool.query(
          `SELECT r.agent_id, r.display_name, r.role, r.last_check_in, r.schedule_interval_sec,
                  d.state_snapshot, d.duration_ms, d.created_at as last_decision_at
           FROM agent_registry r
           LEFT JOIN LATERAL (
             SELECT state_snapshot, duration_ms, created_at FROM agent_decisions
             WHERE agent_id = r.agent_id ORDER BY created_at DESC LIMIT 1
           ) d ON true
           WHERE r.agent_id = $1`,
          [agent_id],
        );
        return { content: [{ type: 'text' as const, text: JSON.stringify(rows[0] ?? { error: 'Agent not found' }, null, 2) }] };
      }

      const { rows } = await pool.query(
        `SELECT r.agent_id, r.display_name, r.role, r.last_check_in,
                d.state_snapshot->>'status' as status,
                d.state_snapshot->>'summary' as summary,
                d.duration_ms
         FROM agent_registry r
         LEFT JOIN LATERAL (
           SELECT state_snapshot, duration_ms FROM agent_decisions
           WHERE agent_id = r.agent_id ORDER BY created_at DESC LIMIT 1
         ) d ON true
         WHERE r.is_active AND r.executor = 'nexus'
         ORDER BY r.agent_id`,
      );
      return { content: [{ type: 'text' as const, text: JSON.stringify(rows, null, 2) }] };
    },
  );

  server.tool(
    'nexus_send_message',
    'Send an inbox message to a Nexus agent (triggers immediate wake-up)',
    {
      to_agent: z.string().describe('Agent ID to send to'),
      message: z.string().describe('Message text'),
      priority: z.number().default(1).describe('0=low, 1=normal, 2=high, 3=critical'),
    },
    async ({ to_agent, message, priority }) => {
      const pool = getPool();
      const { rows } = await pool.query(
        `INSERT INTO agent_inbox (to_agent, from_agent, message, priority)
         VALUES ($1, 'owner', $2, $3) RETURNING id`,
        [to_agent, message, priority],
      );
      return { content: [{ type: 'text' as const, text: `Message sent (id: ${rows[0].id}). Agent will be woken via LISTEN/NOTIFY.` }] };
    },
  );

  server.tool(
    'nexus_list_decisions',
    'List recent agent decisions with status and summary',
    {
      agent_id: z.string().optional().describe('Filter by agent ID'),
      limit: z.number().default(10).describe('Number of decisions to return'),
    },
    async ({ agent_id, limit }) => {
      const pool = getPool();
      const where = agent_id ? 'WHERE agent_id = $2' : '';
      const params: unknown[] = [limit];
      if (agent_id) params.push(agent_id);

      const { rows } = await pool.query(
        `SELECT agent_id, state_snapshot->>'status' as status,
                state_snapshot->>'summary' as summary,
                duration_ms, created_at
         FROM agent_decisions ${where}
         ORDER BY created_at DESC LIMIT $1`,
        params,
      );
      return { content: [{ type: 'text' as const, text: JSON.stringify(rows, null, 2) }] };
    },
  );

  server.tool(
    'nexus_query',
    'Run a read-only SQL query against the Nexus database',
    { sql: z.string().describe('SELECT query to execute') },
    async ({ sql }) => {
      const normalized = sql.trim().toLowerCase();
      if (!normalized.startsWith('select') && !normalized.startsWith('with')) {
        return { content: [{ type: 'text' as const, text: 'ERROR: Only SELECT queries allowed' }] };
      }
      try {
        const pool = getPool();
        const result = await pool.query(sql);
        const rows = result.rows.slice(0, 50);
        return { content: [{ type: 'text' as const, text: `${result.rowCount} rows:\n${JSON.stringify(rows, null, 2)}` }] };
      } catch (err) {
        return { content: [{ type: 'text' as const, text: `ERROR: ${(err as Error).message}` }] };
      }
    },
  );

  server.tool(
    'nexus_job_status',
    'Check the job queue health — pending, processing, failed counts',
    {},
    async () => {
      const pool = getPool();
      const { rows } = await pool.query(
        `SELECT status, COUNT(*) as count FROM tempo_jobs
         WHERE updated_at > NOW() - INTERVAL '1 hour'
         GROUP BY status ORDER BY count DESC`,
      );
      const { rows: stuck } = await pool.query(
        `SELECT job_type, COUNT(*) FROM tempo_jobs
         WHERE status = 'processing' AND updated_at < NOW() - INTERVAL '20 minutes'
         GROUP BY job_type`,
      );
      return { content: [{ type: 'text' as const, text: JSON.stringify({ queue: rows, stuck_jobs: stuck }, null, 2) }] };
    },
  );

  server.tool(
    'nexus_approvals',
    'List pending approvals, or approve/reject one by ID',
    {
      action: z.enum(['list', 'approve', 'reject']).default('list'),
      approval_id: z.number().optional().describe('ID to approve/reject'),
      reason: z.string().optional().describe('Reason for rejection'),
    },
    async ({ action, approval_id, reason }) => {
      const pool = getPool();

      if (action === 'list') {
        const { rows } = await pool.query(
          `SELECT id, agent_id, action_type, LEFT(params::text, 200) as params, reason, created_at
           FROM agent_pending_actions WHERE status = 'pending' ORDER BY created_at DESC`,
        );
        return { content: [{ type: 'text' as const, text: rows.length > 0 ? JSON.stringify(rows, null, 2) : 'No pending approvals.' }] };
      }

      if (!approval_id) {
        return { content: [{ type: 'text' as const, text: 'ERROR: approval_id is required for approve/reject' }] };
      }

      const status = action === 'approve' ? 'approved' : 'rejected';
      const { rows } = await pool.query(
        `UPDATE agent_pending_actions SET status = $1, decided_by = 'owner', decided_at = NOW(), decision_reason = $3
         WHERE id = $2 AND status = 'pending' RETURNING id, agent_id, action_type`,
        [status, approval_id, reason ?? null],
      );

      if (rows.length === 0) {
        return { content: [{ type: 'text' as const, text: 'Approval not found or already decided.' }] };
      }

      if (status === 'approved') {
        await pool.query(
          `INSERT INTO agent_inbox (to_agent, from_agent, message, priority)
           VALUES ($1, 'owner', $2, 2)`,
          [rows[0].agent_id, `Action approved: ${rows[0].action_type} (id: ${approval_id}). Execute on your next cycle.`],
        );
      }

      return { content: [{ type: 'text' as const, text: `${status}: ${rows[0].action_type} by ${rows[0].agent_id} (id: ${approval_id})` }] };
    },
  );

  // ── New Tool Domains ────────────────────────────────────

  registerSearchTools(server);
  registerHealthTools(server);
  registerAgentTools(server);
  registerJobTools(server);
  registerMergeTools(server);
  registerPeopleTools(server);
  registerBiographyTools(server);
  registerInsightTools(server);

  return server;
}

// ── Start ───────────────────────────────────────────────────

const port = parseInt(process.env.MCP_PORT ?? '7701', 10);
const isStdio = process.argv.includes('--stdio');

// In stdio mode, MCP protocol owns stdout. Redirect all console.log to
// stderr so the @nexus/core logger doesn't corrupt the JSON stream.
if (isStdio) {
  const origLog = console.log;
  console.log = (...args: unknown[]) => console.error(...args);
}

async function main() {
  const server = createServer();

  if (isStdio) {
    // stdio transport for Claude Desktop
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error(`Nexus MCP server running (stdio, 24 tools)`);
    return;
  }

  // HTTP transport for Claude Code and other MCP clients
  const app = express();

  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: undefined,
  });

  app.post('/mcp', async (req, res) => {
    await transport.handleRequest(req, res, req.body);
  });

  app.get('/mcp', async (_req, res) => {
    res.writeHead(405).end('Method Not Allowed. Use POST for MCP requests.');
  });

  app.delete('/mcp', async (_req, res) => {
    res.writeHead(405).end('Method Not Allowed.');
  });

  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', service: 'nexus-mcp', tools: 24, timestamp: new Date().toISOString() });
  });

  await server.connect(transport);

  app.listen(port, () => {
    console.log(`Nexus MCP server listening on port ${port} (24 tools)`);
  });
}

main().catch((err) => {
  console.error('MCP server error:', err);
  process.exit(1);
});

process.on('SIGINT', async () => {
  await shutdown();
  process.exit(0);
});
