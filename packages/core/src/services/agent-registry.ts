import crypto from 'node:crypto';
import bcrypt from 'bcrypt';
import { query } from '../db.js';
import { createLogger } from '../logger.js';
import type { Agent, AgentMemory, AgentDecision } from '../types/index.js';

const logger = createLogger('agent-registry');

export async function listAgents(activeOnly = true): Promise<Agent[]> {
  const stop = logger.time('listAgents');
  const result = await query<Agent>(
    activeOnly
      ? 'SELECT * FROM agent_registry WHERE is_active = true ORDER BY display_name'
      : 'SELECT * FROM agent_registry ORDER BY display_name',
  );
  stop();
  logger.logVerbose('Listed agents:', result.rowCount);
  return result.rows;
}

export async function getAgent(agentId: string): Promise<Agent | null> {
  logger.logVerbose('getAgent:', agentId);
  const result = await query<Agent>(
    'SELECT * FROM agent_registry WHERE agent_id = $1',
    [agentId],
  );
  return result.rows[0] ?? null;
}

export async function updateLastCheckIn(agentId: string): Promise<void> {
  await query(
    'UPDATE agent_registry SET last_check_in = NOW(), updated_at = NOW() WHERE agent_id = $1',
    [agentId],
  );
  logger.logVerbose('Updated last_check_in for:', agentId);
}

export async function generateApiKey(agentId: string): Promise<string> {
  const random = crypto.randomBytes(16).toString('hex');
  const key = `nexus_${agentId}_${random}`;
  const prefix = key.slice(0, 8);
  const hash = await bcrypt.hash(key, 10);

  await query(
    `UPDATE agent_registry
     SET api_key_hash = $1, api_key_prefix = $2, api_key_created_at = NOW(), updated_at = NOW()
     WHERE agent_id = $3`,
    [hash, prefix, agentId],
  );

  logger.log('Generated API key for agent:', agentId, 'prefix:', prefix);
  return key;
}

export async function getAgentMemories(
  agentId: string,
  opts: { includeShared?: boolean; limit?: number } = {},
): Promise<AgentMemory[]> {
  const { includeShared = true, limit = 100 } = opts;
  logger.logVerbose('getAgentMemories:', agentId, 'includeShared:', includeShared);

  const conditions = includeShared
    ? '(agent_id = $1 OR category = \'shared\') AND is_active = true'
    : 'agent_id = $1 AND is_active = true';

  const result = await query<AgentMemory>(
    `SELECT * FROM agent_memory WHERE ${conditions}
     ORDER BY confidence DESC, times_reinforced DESC LIMIT $2`,
    [agentId, limit],
  );

  logger.logVerbose('Loaded memories:', result.rowCount);
  return result.rows;
}

export async function logDecision(decision: Omit<AgentDecision, 'id' | 'created_at'>): Promise<number> {
  const result = await query<{ id: number }>(
    `INSERT INTO agent_decisions
       (agent_id, trace_id, state_snapshot, system_prompt_hash, user_message,
        llm_response, parsed_actions, execution_results, duration_ms)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     RETURNING id`,
    [
      decision.agent_id,
      decision.trace_id,
      JSON.stringify(decision.state_snapshot),
      decision.system_prompt_hash,
      decision.user_message,
      JSON.stringify(decision.llm_response),
      JSON.stringify(decision.parsed_actions),
      JSON.stringify(decision.execution_results),
      decision.duration_ms,
    ],
  );

  logger.logVerbose('Logged decision:', result.rows[0].id, 'for:', decision.agent_id);
  return result.rows[0].id;
}
