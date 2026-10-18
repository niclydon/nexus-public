/**
 * Agent bus tools (public delta, October 2026).
 *
 * Adapted from the private platform's coordination layer for this public
 * snapshot. The private system grew a full coordination layer (registry,
 * conditions, typed stream) after the August 2026 snapshot; this module is
 * the stream piece, published as a representative sample: governed posts to
 * a shared agent bus, reads after a high-water mark, and per-caller
 * high-water marks.
 *
 * Adaptations from the private original, made for publication:
 *   - the caller map uses generic seat names (the private map names real
 *     seats and host aliases);
 *   - observability span naming was removed;
 *   - the backing schema ships with this snapshot as
 *     migrations/058-agent-bus.sql, including the per-caller Postgres
 *     roles the SET ROLE machinery expects.
 */
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from "zod/v3";
import { getPool } from '@nexus/core';

export type BusCaller = 'coordinator' | 'operator' | 'worker';
export const BUS_CALLERS: Record<string, { role: string; author: string }> = {
  coordinator: { role: 'coordinator_bus', author: 'coordinator' },
  operator: { role: 'operator_bus', author: 'operator' },
  worker: { role: 'worker_bus', author: 'worker' },
};

export function resolveBusCaller(caller: string): { role: string; author: string } | null {
  return BUS_CALLERS[caller.trim().toLowerCase()] ?? null;
}

function error(text: string) {
  return { content: [{ type: 'text' as const, text: `ERROR: ${text}` }] };
}

function requireApproval(userApproved: boolean | undefined, approvalText: string | undefined): string | null {
  if (userApproved !== true) return 'user_approved must be true';
  if (!approvalText?.trim()) return 'approval_text required';
  return null;
}

async function withBusRole<T>(caller: string, fn: (client: { query: (sql: string, params?: unknown[]) => Promise<{ rows: T[] }> }, identity: { role: string; author: string }) => Promise<T[]>): Promise<T[]> {
  const identity = resolveBusCaller(caller);
  if (!identity) throw new Error(`unknown bus caller: ${caller}`);
  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET ROLE ${identity.role}`);
    return await fn(client, identity);
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    try {
      await client.query('RESET ROLE');
      await client.query('COMMIT');
    } finally {
      client.release();
    }
  }
}

export function registerAgentBusTools(server: McpServer): void {
  server.tool(
    'agent_bus_post',
    'Post a governed status, intent, done, ask, or flag message to the agent coordination bus.',
    {
      caller: z.string().min(1).max(64),
      msg_type: z.enum(['status', 'intent', 'done', 'ask', 'flag']),
      body: z.string().min(1).max(8000),
      recipient: z.string().optional(),
      task: z.string().optional(),
      in_reply_to: z.number().int().positive().optional(),
      user_approved: z.boolean(),
      approval_text: z.string(),
    },
    async ({ caller, msg_type, body, recipient, task, in_reply_to, user_approved, approval_text }) => {
      const rejection = requireApproval(user_approved, approval_text);
      if (rejection) return error(rejection);
      const rows = await withBusRole(caller, (client, identity) => client.query(
        `INSERT INTO agent_bus.messages (author, recipient, msg_type, task, body, in_reply_to)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id, ts, author, recipient, msg_type, task, body, in_reply_to`,
        [identity.author, recipient ?? null, msg_type, task ?? null, body, in_reply_to ?? null],
      ).then((result) => result.rows));
      return { content: [{ type: 'text' as const, text: JSON.stringify(rows[0], null, 2) }] };
    },
  );

  server.tool(
    'agent_bus_read',
    'Read agent coordination bus messages after a high-water mark.',
    {
      caller: z.string().min(1).max(64),
      since_id: z.number().int().nonnegative().optional().default(0),
      limit: z.number().int().min(1).max(200).optional().default(50),
    },
    async ({ caller, since_id, limit }) => {
      const rows = await withBusRole(caller, (client) => client.query(
        `SELECT id, ts, author, recipient, msg_type, task, body, in_reply_to
           FROM agent_bus.messages
          WHERE id > $1
          ORDER BY id ASC
          LIMIT $2`,
        [since_id, limit],
      ).then((result) => result.rows));
      return { content: [{ type: 'text' as const, text: JSON.stringify({ since_id, count: rows.length, messages: rows }, null, 2) }] };
    },
  );

  server.tool(
    'agent_bus_hwm',
    'Get or set an agent coordination bus high-water mark.',
    {
      caller: z.string().min(1).max(64),
      action: z.enum(['get', 'set']).default('get'),
      last_seen_id: z.number().int().nonnegative().optional(),
      user_approved: z.boolean().optional(),
      approval_text: z.string().optional(),
    },
    async ({ caller, action, last_seen_id, user_approved, approval_text }) => {
      if (action === 'set') {
        const rejection = requireApproval(user_approved, approval_text);
        if (rejection) return error(rejection);
        if (last_seen_id === undefined) return error('last_seen_id is required when action=set');
      }
      const rows = await withBusRole(caller, (client, identity) => action === 'set'
        ? client.query(
          `INSERT INTO agent_bus.high_water_marks (author, last_seen_id)
           VALUES ($1, $2)
           ON CONFLICT (author) DO UPDATE SET last_seen_id = EXCLUDED.last_seen_id, updated_at = NOW()
           RETURNING author, last_seen_id, updated_at`,
          [identity.author, last_seen_id],
        ).then((result) => result.rows)
        : client.query(
          `SELECT author, last_seen_id, updated_at FROM agent_bus.high_water_marks WHERE author = $1`,
          [identity.author],
        ).then((result) => result.rows));
      return { content: [{ type: 'text' as const, text: JSON.stringify(rows[0] ?? null, null, 2) }] };
    },
  );
}
