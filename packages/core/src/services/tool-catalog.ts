import { query } from '../db.js';
import { createLogger } from '../logger.js';
import type { PlatformTool, ToolGrant, ToolRequest } from '../types/index.js';

const logger = createLogger('tool-catalog');

export async function getToolsForAgent(agentId: string): Promise<PlatformTool[]> {
  logger.logVerbose('getToolsForAgent:', agentId);
  const stop = logger.time('getToolsForAgent');

  const result = await query<PlatformTool>(
    `SELECT t.* FROM platform_tools t
     INNER JOIN agent_tool_grants g ON t.tool_id = g.tool_id
     WHERE g.agent_id = $1 AND t.is_active = true
     ORDER BY t.category, t.display_name`,
    [agentId],
  );

  stop();
  logger.logVerbose('Tools for', agentId, ':', result.rowCount);
  return result.rows;
}

export async function searchTools(
  keyword: string,
  opts: { category?: string; limit?: number } = {},
): Promise<PlatformTool[]> {
  const { category, limit = 20 } = opts;
  logger.logVerbose('searchTools:', keyword, 'category:', category);

  const conditions = ['is_active = true'];
  const params: unknown[] = [];
  let paramIdx = 1;

  if (keyword) {
    conditions.push(`(display_name ILIKE $${paramIdx} OR description ILIKE $${paramIdx})`);
    params.push(`%${keyword}%`);
    paramIdx++;
  }

  if (category) {
    conditions.push(`category = $${paramIdx}`);
    params.push(category);
    paramIdx++;
  }

  params.push(limit);
  const result = await query<PlatformTool>(
    `SELECT * FROM platform_tools WHERE ${conditions.join(' AND ')}
     ORDER BY category, display_name LIMIT $${paramIdx}`,
    params,
  );

  return result.rows;
}

export async function checkGrant(agentId: string, toolId: string): Promise<boolean> {
  const result = await query<{ count: string }>(
    `SELECT COUNT(*) as count FROM agent_tool_grants
     WHERE agent_id = $1 AND tool_id = $2`,
    [agentId, toolId],
  );
  return parseInt(result.rows[0].count, 10) > 0;
}

export async function requestTool(
  agentId: string,
  toolId: string,
  reason: string | null,
): Promise<{ granted: boolean; request_id?: string }> {
  logger.log('Tool request from', agentId, 'for', toolId);

  // Check if tool exists and get risk level
  const toolResult = await query<PlatformTool>(
    'SELECT * FROM platform_tools WHERE tool_id = $1 AND is_active = true',
    [toolId],
  );

  if (toolResult.rows.length === 0) {
    logger.logVerbose('Tool not found:', toolId);
    return { granted: false };
  }

  const tool = toolResult.rows[0];

  // Auto-approve read_only tools
  if (tool.risk_level === 'read_only') {
    await query(
      `INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by)
       VALUES ($1, $2, 'auto')
       ON CONFLICT (agent_id, tool_id) DO NOTHING`,
      [agentId, toolId],
    );
    logger.log('Auto-approved read_only tool', toolId, 'for', agentId);
    return { granted: true };
  }

  // Queue for approval
  const result = await query<{ id: string }>(
    `INSERT INTO tool_requests (agent_id, tool_id, reason)
     VALUES ($1, $2, $3) RETURNING id`,
    [agentId, toolId, reason],
  );

  logger.log('Queued tool request:', result.rows[0].id, 'risk:', tool.risk_level);
  return { granted: false, request_id: result.rows[0].id };
}

export async function approveTool(requestId: string, reviewedBy: string): Promise<void> {
  const result = await query<ToolRequest>(
    `UPDATE tool_requests SET status = 'approved', reviewed_by = $1, reviewed_at = NOW()
     WHERE id = $2 RETURNING agent_id, tool_id`,
    [reviewedBy, requestId],
  );

  if (result.rows.length > 0) {
    const { agent_id, tool_id } = result.rows[0];
    await query(
      `INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by)
       VALUES ($1, $2, $3)
       ON CONFLICT (agent_id, tool_id) DO NOTHING`,
      [agent_id, tool_id, reviewedBy],
    );
    logger.log('Approved tool', tool_id, 'for', agent_id, 'by', reviewedBy);
  }
}

export async function denyTool(requestId: string, reviewedBy: string): Promise<void> {
  await query(
    `UPDATE tool_requests SET status = 'denied', reviewed_by = $1, reviewed_at = NOW()
     WHERE id = $2`,
    [reviewedBy, requestId],
  );
  logger.log('Denied tool request:', requestId, 'by', reviewedBy);
}
