import { query } from '../db.js';
import { createLogger } from '../logger.js';
import type { InboxMessage } from '../types/index.js';

const logger = createLogger('comms');

const INBOX_CAP = 10;
const INBOX_EXPIRY_DAYS = 7;

// Canonical agent IDs — normalize any variant to these
// Old names map to new names for backward compatibility
const AGENT_ALIASES: Record<string, string> = {
  aria: 'aria', ARIA: 'aria', Aria: 'aria',
  pipeline: 'pipeline', Pipeline: 'pipeline',
  infra: 'infra', Infra: 'infra',
  inference: 'inference', Inference: 'inference',
  coder: 'coder', Coder: 'coder',
  insight: 'insight', Insight: 'insight',
  circle: 'circle', Circle: 'circle',
  owner: 'owner',
  // Legacy name mappings (old → new)
  monitor: 'infra', Monitor: 'infra',
  analyst: 'insight', Analyst: 'insight',
  collector: 'pipeline', Collector: 'pipeline',
  fixer: 'coder', Fixer: 'coder',
  'model-ops': 'inference', 'model_ops': 'inference', 'Model Ops': 'inference', 'ModelOps': 'inference', 'model ops': 'inference',
  relationships: 'circle', Relationships: 'circle',
};

function normalizeAgentId(raw: string): string {
  return AGENT_ALIASES[raw] ?? raw.toLowerCase().replace(/_/g, '-');
}

export async function getUnreadInbox(agentId: string): Promise<InboxMessage[]> {
  logger.logVerbose('getUnreadInbox for:', agentId);

  // Expire old unread messages (FIFO decay)
  await query(
    `UPDATE agent_inbox SET read_at = NOW()
     WHERE to_agent = $1 AND read_at IS NULL
       AND created_at < NOW() - INTERVAL '1 day' * $2`,
    [agentId, INBOX_EXPIRY_DAYS],
  );

  const result = await query<InboxMessage>(
    `SELECT * FROM agent_inbox
     WHERE to_agent = $1 AND read_at IS NULL
     ORDER BY priority DESC, created_at DESC
     LIMIT $2`,
    [agentId, INBOX_CAP],
  );
  return result.rows;
}

export async function getInbox(
  agentId: string,
  opts: { limit?: number; includeRead?: boolean } = {},
): Promise<InboxMessage[]> {
  const { limit = 50, includeRead = false } = opts;
  logger.logVerbose('getInbox for:', agentId, 'includeRead:', includeRead);

  if (includeRead) {
    const result = await query<InboxMessage>(
      `SELECT * FROM agent_inbox WHERE to_agent = $1
       ORDER BY created_at DESC LIMIT $2`,
      [agentId, limit],
    );
    return result.rows;
  }

  const result = await query<InboxMessage>(
    `SELECT * FROM agent_inbox WHERE to_agent = $1 AND read_at IS NULL
     ORDER BY priority DESC, created_at DESC LIMIT $2`,
    [agentId, limit],
  );
  return result.rows;
}

export async function sendInbox(msg: {
  to_agent: string;
  from_agent: string;
  message: string;
  context?: Record<string, unknown>;
  priority?: number;
  trace_id?: string;
}): Promise<InboxMessage> {
  const toAgent = normalizeAgentId(msg.to_agent);
  const fromAgent = normalizeAgentId(msg.from_agent);

  // Validate recipient exists
  const recipientCheck = await query<{ agent_id: string }>(
    `SELECT agent_id FROM agent_registry WHERE agent_id = $1 AND is_active = true`,
    [toAgent],
  );

  if (recipientCheck.rows.length === 0) {
    // Route to undeliverable — log it and don't silently drop
    logger.logMinimal(`Undeliverable inbox: "${msg.to_agent}" (normalized: "${toAgent}") from ${fromAgent}`);
    const result = await query<InboxMessage>(
      `INSERT INTO agent_inbox (to_agent, from_agent, message, context, priority, trace_id)
       VALUES ('_undeliverable', $1, $2, $3, $4, $5)
       RETURNING *`,
      [
        fromAgent,
        `[undeliverable to "${msg.to_agent}"] ${msg.message}`,
        msg.context ? JSON.stringify(msg.context) : null,
        msg.priority ?? 0,
        msg.trace_id ?? null,
      ],
    );
    return result.rows[0];
  }

  logger.logVerbose('sendInbox from:', fromAgent, 'to:', toAgent);

  const result = await query<InboxMessage>(
    `INSERT INTO agent_inbox (to_agent, from_agent, message, context, priority, trace_id)
     VALUES ($1, $2, $3, $4, $5, $6)
     RETURNING *`,
    [
      toAgent,
      fromAgent,
      msg.message,
      msg.context ? JSON.stringify(msg.context) : null,
      msg.priority ?? 0,
      msg.trace_id ?? null,
    ],
  );

  logger.log('Inbox sent:', result.rows[0].id, 'from:', fromAgent, 'to:', toAgent);
  return result.rows[0];
}

export async function markInboxRead(messageId: number): Promise<void> {
  await query('UPDATE agent_inbox SET read_at = NOW() WHERE id = $1', [messageId]);
  logger.logVerbose('Marked inbox read:', messageId);
}

export async function markInboxActed(messageId: number): Promise<void> {
  await query(
    'UPDATE agent_inbox SET acted_at = NOW(), read_at = COALESCE(read_at, NOW()) WHERE id = $1',
    [messageId],
  );
  logger.logVerbose('Marked inbox acted:', messageId);
}
