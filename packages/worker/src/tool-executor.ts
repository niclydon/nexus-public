import {
  createLogger,
  toolCatalog,
  comms,
  agentRegistry,
  query,
  evalScorer,
  semanticSearch,
  isShowcaseDemoEnabled,
  isShowcaseDangerousToolsAllowed,
  type PlatformTool,
} from '@nexus/core';
import { delegateToAgent } from './lib/delegate.js';
import { consolidateMemories } from './lib/summarize.js';
import type { ActionRequest } from './lib/llm-router.js';

const logger = createLogger('tool-executor');

type ToolHandler = (agentId: string, params: Record<string, unknown>) => Promise<string>;

const handlers: Record<string, ToolHandler> = {
  query_db: handleQueryDb,
  check_system: handleCheckSystem,
  library_search: handleLibrarySearch,
  inbox_send: handleInboxSend,
  remember: handleRemember,
  search_tools: handleSearchTools,
  request_tool: handleRequestTool,
  enqueue_job: handleEnqueueJob,
  create_task: handleCreateTask,
  update_task: handleUpdateTask,
  list_agents: handleListAgents,
  get_agent_status: handleGetAgentStatus,
  deliver_report: handleDeliverReport,
  send_email: handleSendEmail,
  forge_health: handleForgeHealth,
  requeue_stuck_jobs: handleRequeueStuckJobs,
  send_pushover: handleSendPushoverTool,
  pause_job_type: handlePauseJobType,
  resume_job_type: handleResumeJobType,
  get_queue_status: handleGetQueueStatus,
  restart_service: handleRestartService,
  restart_user_service: handleRestartUserService,
  update_llm_config: handleUpdateLlmConfig,
  read_file: handleReadFile,
  write_file: handleWriteFile,
  edit_file: handleEditFile,
  run_build: handleRunBuild,
  git_status: handleGitStatus,
  git_commit_push: handleGitCommitPush,
  create_pr: handleCreatePr,
  list_mcp_servers: handleListMcpServers,
  install_mcp_server: handleInstallMcpServer,
  ingest_data: handleIngestData,
  list_skills: handleListSkills,
  install_skill: handleInstallSkill,
  get_skill: handleGetSkill,
  score_decision: handleScoreDecision,
  get_eval_summary: handleGetEvalSummary,
  delegate_task: handleDelegateTask,
  looki_search_memories: handleLookiSearchMemories,
  get_model_health: handleGetModelHealth,
  reset_model_health: handleResetModelHealth,
  consolidate_memories: handleConsolidateMemories,
  get_checklist: handleGetChecklist,
  complete_checklist_item: handleCompleteChecklistItem,
  create_checklist_item: handleCreateChecklistItem,
  semantic_search: handleSemanticSearch,
};

const SHOWCASE_BLOCKED_TOOLS = new Set([
  'check_system',
  'query_db',
  'read_file',
  'write_file',
  'edit_file',
  'run_build',
  'git_commit_push',
  'create_pr',
  'restart_service',
  'restart_user_service',
  'install_mcp_server',
  'install_skill',
  'send_email',
]);

function isToolBlockedInShowcase(toolId: string): boolean {
  return isShowcaseDemoEnabled() && !isShowcaseDangerousToolsAllowed() && SHOWCASE_BLOCKED_TOOLS.has(toolId);
}

export async function executeAction(
  agentId: string,
  originalAction: ActionRequest,
  grantedTools: PlatformTool[],
  agentAutonomy?: string,
): Promise<string> {
  let action = originalAction;
  const toolId = action.action;
  logger.logVerbose('Executing', toolId, 'for', agentId);

  // Check grant
  const tool = grantedTools.find(t => t.tool_id === toolId);
  if (!tool) {
    logger.logVerbose('Tool not granted:', toolId, 'for', agentId);
    return `BLOCKED: You do not have access to tool '${toolId}'. Use search_tools to find available tools and request_tool to request access.`;
  }

  if (isToolBlockedInShowcase(toolId)) {
    logger.logMinimal('Blocked showcase tool execution:', toolId, 'for', agentId);
    return `BLOCKED: '${toolId}' is disabled in public showcase mode. Set NEXUS_SHOWCASE_ALLOW_DANGEROUS_TOOLS=true only for private testing.`;
  }

  // Approval gating — two levels:
  // 1. Per-tool: tool.approval_required = true → always queue, regardless of agent autonomy
  // 2. Per-agent: approval_gated agents → queue all non-read_only tools
  const needsApproval =
    tool.approval_required ||
    (agentAutonomy === 'approval_gated' && tool.risk_level !== 'read_only');

  if (needsApproval) {
    // Check if there's an approved action of this type (execute instead of re-queuing)
    const approved = await query<{ id: number }>(
      `SELECT id FROM agent_pending_actions
       WHERE agent_id = $1 AND action_type = $2 AND status = 'approved'
       ORDER BY decided_at DESC LIMIT 1`,
      [agentId, toolId],
    );

    if (approved.rows.length > 0) {
      // Pre-approved — check for modified params (edit response type)
      const approvedRow = await query<{ response_type: string | null; modified_params: unknown }>(
        `SELECT response_type, modified_params FROM agent_pending_actions WHERE id = $1`,
        [approved.rows[0].id],
      );
      const row = approvedRow.rows[0];

      // If reviewer edited params, use the modified version
      if (row?.response_type === 'edit' && row.modified_params) {
        const edited = typeof row.modified_params === 'string'
          ? JSON.parse(row.modified_params) as Record<string, unknown>
          : row.modified_params as Record<string, unknown>;
        action = { ...action, params: edited };
        logger.log(`Executing with modified params: ${toolId} (approval id: ${approved.rows[0].id})`);
      } else {
        logger.log(`Executing pre-approved action: ${toolId} (approval id: ${approved.rows[0].id})`);
      }

      await query(
        `UPDATE agent_pending_actions SET status = 'executed', decision_reason = 'Executed after approval' WHERE id = $1`,
        [approved.rows[0].id],
      );
      // Fall through to normal execution below
    } else {
      return await queueForApproval(agentId, action, tool);
    }
  }

  // MCP-provided tools — route through MCP client manager
  if (tool.provider_type === 'mcp' && tool.provider_config) {
    const config = tool.provider_config as { server: string; tool: string };
    const stop = logger.time(`mcp:${config.server}/${config.tool}`);
    try {
      const { callMcpTool } = await import('./lib/mcp-client-manager.js');
      const result = await callMcpTool(config.server, config.tool, action.params);
      stop();
      logger.logVerbose('MCP tool', toolId, 'result:', result.slice(0, 200));
      return result;
    } catch (err) {
      stop();
      const msg = (err as Error).message;
      logger.logMinimal('MCP tool', toolId, 'error:', msg);
      return `ERROR: ${toolId} failed: ${msg}`;
    }
  }

  // Built-in tools
  const handler = handlers[toolId];
  if (!handler) {
    logger.logVerbose('No handler for tool:', toolId);
    return `ERROR: No handler implemented for tool '${toolId}'`;
  }

  const stop = logger.time(`tool:${toolId}`);
  try {
    const result = await handler(agentId, action.params);
    stop();
    logger.logVerbose('Tool', toolId, 'result:', result.slice(0, 200));
    return result;
  } catch (err) {
    stop();
    const msg = (err as Error).message;
    logger.logMinimal('Tool', toolId, 'error:', msg);
    return `ERROR: ${toolId} failed: ${msg}`;
  }
}

// --- Tool Handlers ---

// Tables that ARIA is not allowed to query (infrastructure scope)
const ARIA_BLOCKED_TABLES = ['tempo_jobs', 'agent_decisions', 'data_source_registry', 'agent_registry', 'integration_health', 'mcp_servers'];

async function handleQueryDb(agentId: string, params: Record<string, unknown>): Promise<string> {
  const sql = String(params.sql ?? params.query ?? '');
  if (!sql) return 'ERROR: sql parameter is required (pass as "sql" or "query")';

  // Only allow SELECT queries
  const normalized = sql.trim().toLowerCase();
  if (!normalized.startsWith('select') && !normalized.startsWith('with')) {
    return 'BLOCKED: Only SELECT queries are allowed';
  }

  // ARIA is blocked from querying infrastructure tables
  if (agentId === 'aria') {
    const blockedTable = ARIA_BLOCKED_TABLES.find(t => normalized.includes(t));
    if (blockedTable) {
      return `BLOCKED: You do not have access to ${blockedTable}. Infrastructure monitoring is Infra's job. Focus on your personal assistant tasks.`;
    }
  }

  const result = await query(sql);
  const rows = result.rows.slice(0, 20);
  let output = JSON.stringify(rows);
  if (output.length > 3000) {
    output = output.slice(0, 3000) + '... (truncated)';
  }
  const extra = (result.rowCount ?? 0) > 20 ? ` (showing 20 of ${result.rowCount})` : '';
  return `${result.rowCount} rows${extra}:\n${output}`;
}

async function handleCheckSystem(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const { execSync } = await import('node:child_process');

  // Support both named checks and free-form commands
  const freeCommand = String(params.command ?? '').trim();
  const check = String(params.check ?? '').trim();

  if (freeCommand) {
    // Free-form command mode — block dangerous commands
    const blocked = ['rm ', 'rm\t', 'rmdir', 'kill ', 'pkill', 'reboot', 'shutdown', 'mkfs',
                     'dd ', 'mv ', 'chmod', 'chown', 'passwd', 'userdel', 'groupdel'];
    for (const b of blocked) {
      if (freeCommand.toLowerCase().includes(b)) {
        return `BLOCKED: command contains dangerous pattern "${b.trim()}"`;
      }
    }
    // Block sudo unless it's systemctl
    if (freeCommand.includes('sudo') && !freeCommand.match(/^sudo\s+systemctl\s/)) {
      return 'BLOCKED: sudo only allowed for systemctl commands';
    }
    // Block output redirection
    if (/\s>[>\s]/.test(freeCommand)) {
      return 'BLOCKED: output redirection (> or >>) is not allowed';
    }

    try {
      const output = execSync(freeCommand, { timeout: 15_000, encoding: 'utf-8', maxBuffer: 50_000 });
      return (output || '(no output)').trim().slice(0, 3000);
    } catch (err) {
      const msg = (err as Error).message;
      // Return stderr/stdout from failed command if available
      const e = err as { stderr?: string; stdout?: string };
      return (e.stderr || e.stdout || msg || '(command failed)').trim().slice(0, 2000);
    }
  }

  // Named check mode (backward compatible)
  const commands: Record<string, string> = {
    service_status: 'systemctl list-units --type=service --state=running --no-pager | head -30',
    disk_space: 'df -h / /home 2>/dev/null || df -h /',
    memory: 'free -h 2>/dev/null || vm_stat',
    uptime: 'uptime',
  };

  const checkName = check || 'uptime';
  const cmd = commands[checkName];
  if (!cmd) return `ERROR: Unknown check type '${checkName}'. Available: ${Object.keys(commands).join(', ')}, or pass {"command": "your shell command"}`;

  try {
    const output = execSync(cmd, { timeout: 10_000, encoding: 'utf-8' });
    return `${checkName}:\n${output.trim()}`;
  } catch (err) {
    return `ERROR: ${checkName} failed: ${(err as Error).message}`;
  }
}

async function handleLibrarySearch(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const searchQuery = String(params.query ?? '');
  if (!searchQuery) return 'ERROR: query parameter is required';

  // Library tables don't exist yet in Phase 1 — return placeholder
  return `Library search for '${searchQuery}': No articles found (library not yet populated)`;
}

// Infrastructure keywords that ARIA should not escalate to the owner
const INFRA_KEYWORDS = ['overdue', 'stuck job', 'failed job', 'service down', 'memory pressure', 'load average',
  'agent_decisions', 'tempo_jobs', 'high memory', 'inactive service', 'systemic inactivity',
  'empty cycle', 'forge issue', 'circuit breaker', 'processing stuck'];

async function handleInboxSend(agentId: string, params: Record<string, unknown>): Promise<string> {
  const toAgent = String(params.to_agent ?? '');
  const message = String(params.message ?? '');
  const priority = Number(params.priority ?? 0);

  if (!toAgent || !message) return 'ERROR: to_agent and message are required';

  // Block ARIA from escalating infrastructure issues to the owner
  if (agentId === 'aria' && toAgent === 'owner') {
    const msgLower = message.toLowerCase();
    const isInfra = INFRA_KEYWORDS.some(kw => msgLower.includes(kw));
    if (isInfra) {
      logger.logVerbose('Blocked ARIA→owner infra escalation:', message.slice(0, 100));
      return 'BLOCKED: Infrastructure issues should not be escalated to the owner. The specialized agents (Infra, Inference, Pipeline) handle their own domains. Focus on personal assistant tasks.';
    }
  }

  const msg = await comms.sendInbox({
    to_agent: toAgent,
    from_agent: agentId,
    message,
    priority,
  });

  return `Inbox message sent to ${toAgent} (id: ${msg.id})`;
}

async function handleRemember(agentId: string, params: Record<string, unknown>): Promise<string> {
  const content = String(params.content ?? '');
  const memoryType = String(params.memory_type ?? 'observation');
  const category = params.category ? String(params.category) : null;

  if (!content) return 'ERROR: content parameter is required';

  const result = await query<{ id: number }>(
    `INSERT INTO agent_memory (agent_id, memory_type, category, content, source)
     VALUES ($1, $2, $3, $4, 'self')
     RETURNING id`,
    [agentId, memoryType, category, content],
  );

  return `Memory saved (id: ${result.rows[0].id}, type: ${memoryType})`;
}

async function handleSearchTools(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const keyword = String(params.keyword ?? '');
  const category = params.category ? String(params.category) : undefined;
  const parts: string[] = [];

  // Search platform tools (built-in + MCP)
  const tools = await toolCatalog.searchTools(keyword, { category });
  if (tools.length > 0) {
    parts.push(`## Tools (${tools.length}):`);
    for (const t of tools) {
      parts.push(`- ${t.tool_id} [${t.risk_level}] (${t.category}): ${t.description}`);
    }
  }

  // Search skills
  if (keyword) {
    const skills = await query<{ skill_name: string; description: string; injection_mode: string }>(
      `SELECT skill_name, description, injection_mode FROM agent_skills
       WHERE is_active = true AND (skill_name ILIKE $1 OR description ILIKE $1)
       ORDER BY skill_name LIMIT 10`,
      [`%${keyword}%`],
    );
    if (skills.rows.length > 0) {
      parts.push(`\n## Skills (${skills.rows.length}):`);
      for (const s of skills.rows) {
        parts.push(`- ${s.skill_name} [${s.injection_mode}]: ${s.description ?? '(no description)'}`);
      }
    }
  }

  // Search installed MCP servers
  if (keyword) {
    const mcpServers = await query<{ server_name: string; display_name: string; tools_provided: string[] }>(
      `SELECT server_name, display_name, tools_provided FROM mcp_servers
       WHERE status IN ('installed', 'running') AND (server_name ILIKE $1 OR display_name ILIKE $1)
       ORDER BY server_name LIMIT 10`,
      [`%${keyword}%`],
    );
    if (mcpServers.rows.length > 0) {
      parts.push(`\n## MCP Servers (${mcpServers.rows.length}):`);
      for (const s of mcpServers.rows) {
        parts.push(`- ${s.server_name} (${s.display_name}): ${s.tools_provided?.join(', ') ?? 'tools not yet discovered'}`);
      }
    }
  }

  if (parts.length === 0) {
    return `No local results for "${keyword}". Use install_mcp_server or install_skill to add new capabilities, or search external marketplaces.`;
  }

  return parts.join('\n');
}

async function handleRequestTool(agentId: string, params: Record<string, unknown>): Promise<string> {
  const toolId = String(params.tool_id ?? '');
  const reason = params.reason ? String(params.reason) : null;

  if (!toolId) return 'ERROR: tool_id parameter is required';

  const result = await toolCatalog.requestTool(agentId, toolId, reason);
  if (result.granted) {
    return `Tool '${toolId}' granted (auto-approved, read-only).`;
  }
  if (result.request_id) {
    return `Tool '${toolId}' request submitted for review (request_id: ${result.request_id}).`;
  }
  return `Tool '${toolId}' not found or unavailable.`;
}

async function handleEnqueueJob(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const jobType = String(params.job_type ?? '');
  const payload = (params.payload as Record<string, unknown>) ?? {};
  const priority = Number(params.priority ?? 0);

  if (!jobType) return 'ERROR: job_type is required';

  // Validate job type against registered handlers — prevents agents from creating fake job types
  const { getRegisteredHandlers } = await import('./job-worker.js');
  const validTypes = getRegisteredHandlers();
  if (!validTypes.includes(jobType)) {
    logger.logMinimal(`Agent tried to enqueue unknown job type: ${jobType}`);
    return `ERROR: Unknown job type '${jobType}'. Valid types: ${validTypes.slice(0, 20).join(', ')}${validTypes.length > 20 ? '...' : ''}`;
  }

  const result = await query<{ id: string }>(
    `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
     VALUES ($1, $2, $3, 3)
     RETURNING id`,
    [jobType, JSON.stringify(payload), priority],
  );

  logger.log(`Job enqueued: ${jobType} (id: ${result.rows[0].id})`);
  return `Job enqueued: ${jobType} (id: ${result.rows[0].id}, priority: ${priority})`;
}

async function handleCreateTask(agentId: string, params: Record<string, unknown>): Promise<string> {
  const title = String(params.title ?? '');
  const description = params.description ? String(params.description) : null;
  const assignedTo = String(params.assigned_to ?? '');
  const priority = Number(params.priority ?? 0);

  if (!title || !assignedTo) return 'ERROR: title and assigned_to are required';

  // Dedup: check for existing open tasks with similar title for the same assignee
  const existing = await query<{ id: number; title: string }>(
    `SELECT id, title FROM agent_tasks
     WHERE assigned_to = $1 AND status IN ('open', 'in_progress')
       AND (title ILIKE $2 OR title ILIKE $3)
     LIMIT 1`,
    [assignedTo, `%${title.slice(0, 40)}%`, `%${title.split(' ').slice(0, 4).join(' ')}%`],
  );

  if (existing.rows.length > 0) {
    return `SKIPPED: Similar task already exists for ${assignedTo}: "${existing.rows[0].title}" (id: ${existing.rows[0].id}). Update it instead of creating a duplicate.`;
  }

  const result = await query<{ id: number }>(
    `INSERT INTO agent_tasks (title, description, assigned_to, assigned_by, priority)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING id`,
    [title, description, assignedTo, agentId, priority],
  );

  // Send inbox notification to the assigned agent
  await comms.sendInbox({
    to_agent: assignedTo,
    from_agent: agentId,
    message: `New task assigned: "${title}"${description ? ` — ${description}` : ''}`,
    priority: Math.min(priority, 2),
  });

  logger.log(`Task created: #${result.rows[0].id} "${title}" → ${assignedTo}`);
  return `Task created (id: ${result.rows[0].id}, assigned to: ${assignedTo})`;
}

async function handleUpdateTask(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const taskId = Number(params.task_id);
  const status = String(params.status ?? '');
  const result = params.result ? String(params.result) : null;

  if (!taskId || !status) return 'ERROR: task_id and status are required';

  const validStatuses = ['open', 'in_progress', 'completed', 'blocked', 'cancelled'];
  if (!validStatuses.includes(status)) return `ERROR: status must be one of: ${validStatuses.join(', ')}`;

  await query(
    `UPDATE agent_tasks SET status = $1, result = COALESCE($2, result),
     updated_at = NOW(), completed_at = CASE WHEN $1 IN ('completed','cancelled') THEN NOW() ELSE completed_at END
     WHERE id = $3`,
    [status, result, taskId],
  );

  return `Task #${taskId} updated to ${status}`;
}

async function handleListAgents(_agentId: string, _params: Record<string, unknown>): Promise<string> {
  const result = await query<{
    agent_id: string; display_name: string; role: string;
    last_check_in: Date | null; schedule_interval_sec: number | null;
  }>(
    `SELECT agent_id, display_name, role, last_check_in, schedule_interval_sec
     FROM agent_registry WHERE is_active = true AND executor = 'nexus'
     ORDER BY agent_id`,
  );

  const lines = result.rows.map(a => {
    const ago = a.last_check_in
      ? `${Math.round((Date.now() - new Date(a.last_check_in).getTime()) / 60000)}min ago`
      : 'never';
    return `- ${a.agent_id} (${a.display_name}): ${a.role} | last: ${ago} | every ${a.schedule_interval_sec ?? '?'}s`;
  });

  return `Active agents (${result.rowCount}):\n${lines.join('\n')}`;
}

async function handleGetAgentStatus(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const targetId = String(params.agent_id ?? '');
  if (!targetId) return 'ERROR: agent_id is required';

  const result = await query<{
    agent_id: string; state_snapshot: Record<string, unknown>;
    duration_ms: number; created_at: Date;
  }>(
    `SELECT agent_id, state_snapshot, duration_ms, created_at
     FROM agent_decisions WHERE agent_id = $1
     ORDER BY created_at DESC LIMIT 1`,
    [targetId],
  );

  if (result.rows.length === 0) return `No decisions found for agent: ${targetId}`;

  const d = result.rows[0];
  const snapshot = d.state_snapshot ?? {};
  const ago = Math.round((Date.now() - new Date(d.created_at).getTime()) / 60000);
  return `${targetId}: status=${snapshot.status ?? 'unknown'}, summary="${snapshot.summary ?? 'n/a'}", ${ago}min ago (${d.duration_ms}ms)`;
}

async function handleDeliverReport(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const { deliverReport } = await import('./lib/report-delivery.js');

  const reportType = String(params.report_type ?? '');
  const title = String(params.title ?? '');
  const body = String(params.body ?? '');
  const category = String(params.category ?? 'general');

  if (!reportType || !title || !body) return 'ERROR: report_type, title, and body are required';

  const deliveredVia = await deliverReport({ reportType, title, body, category });
  return `Report delivered via: ${deliveredVia.join(', ') || 'none (no subscription configured)'}`;
}

async function queueForApproval(agentId: string, action: ActionRequest, tool?: PlatformTool): Promise<string> {
  // Determine which interrupt options to offer based on tool type
  const isCodeChange = ['write_file', 'edit_file', 'git_commit_push', 'create_pr'].includes(action.action);
  const isServiceAction = ['restart_service', 'restart_user_service', 'manage_model'].includes(action.action);

  const allowEdit = isServiceAction || isCodeChange;  // the owner can modify params before approving
  const allowRespond = true;                           // the owner can always respond with guidance
  const allowIgnore = !isServiceAction;                // Can ignore non-critical actions

  // Build a human-readable description of what's being requested
  const description = action.reason
    ? `${action.action}: ${action.reason}`
    : `Agent "${agentId}" wants to execute "${action.action}" with params: ${JSON.stringify(action.params).slice(0, 300)}`;

  const result = await query<{ id: number }>(
    `INSERT INTO agent_pending_actions
       (agent_id, action_type, params, reason, status, description, allow_edit, allow_respond, allow_ignore)
     VALUES ($1, $2, $3, $4, 'pending', $5, $6, $7, $8)
     RETURNING id`,
    [agentId, action.action, JSON.stringify(action.params), action.reason ?? null,
     description, allowEdit, allowRespond, allowIgnore],
  );

  const id = result.rows[0].id;
  const options = [
    'accept (approve as-is)',
    allowEdit ? 'edit (modify params before executing)' : null,
    allowRespond ? 'respond (send guidance instead of approving)' : null,
    allowIgnore ? 'ignore (dismiss without action)' : null,
  ].filter(Boolean).join(', ');

  logger.log(`Queued for approval: ${action.action} by ${agentId} (id: ${id}) | options: ${options}`);
  return `AWAITING APPROVAL: Action '${action.action}' has been queued for review (id: ${id}). Reviewer can: ${options}. Do NOT claim this action is complete.`;
}

async function handleRequeueStuckJobs(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const jobType = params.job_type ? String(params.job_type) : null;
  const action = String(params.action ?? 'requeue');
  const staleMinutes = 20;

  const typeFilter = jobType ? 'AND job_type = $1' : '';
  const filterParams: unknown[] = jobType ? [jobType] : [];

  if (action === 'fail') {
    const result = await query(
      `UPDATE tempo_jobs SET status = 'failed', failed_at = NOW(), last_error = 'Stuck in processing — failed by monitor'
       WHERE status = 'processing' AND updated_at < NOW() - INTERVAL '${staleMinutes} minutes' ${typeFilter}`,
      filterParams,
    );
    return `Failed ${result.rowCount ?? 0} stuck job(s)${jobType ? ` of type ${jobType}` : ''}`;
  }

  const requeued = await query(
    `UPDATE tempo_jobs SET status = 'pending', claimed_at = NULL
     WHERE status = 'processing' AND updated_at < NOW() - INTERVAL '${staleMinutes} minutes'
       AND attempts < max_attempts ${typeFilter}`,
    filterParams,
  );
  const failed = await query(
    `UPDATE tempo_jobs SET status = 'failed', failed_at = NOW(), last_error = 'Stuck and max attempts reached'
     WHERE status = 'processing' AND updated_at < NOW() - INTERVAL '${staleMinutes} minutes'
       AND attempts >= max_attempts ${typeFilter}`,
    filterParams,
  );

  return `Requeued ${requeued.rowCount ?? 0}, failed ${failed.rowCount ?? 0} stuck job(s)${jobType ? ` of type ${jobType}` : ''}`;
}

async function handleSendEmail(agentId: string, params: Record<string, unknown>): Promise<string> {
  const { Resend } = await import('resend');
  const to = params.to ? (Array.isArray(params.to) ? params.to.map(String) : [String(params.to)]) : [];
  const subject = String(params.subject ?? '');
  const body = String(params.body ?? '');

  if (to.length === 0 || !subject || !body) return 'ERROR: to, subject, and body are required';

  // Get the agent's email address from config
  const configResult = await query<{ value: string }>(
    `SELECT value FROM agent_config WHERE agent_id = $1 AND key = 'email_address'`,
    [agentId],
  );

  let fromAddress = 'nexus@example.io';
  if (configResult.rows.length > 0) {
    const val = configResult.rows[0].value;
    fromAddress = typeof val === 'string' ? val.replace(/^"|"$/g, '') : String(val);
  }

  // Get agent display name for the From header
  const agentResult = await query<{ display_name: string }>(
    `SELECT display_name FROM agent_registry WHERE agent_id = $1`,
    [agentId],
  );
  const displayName = agentResult.rows[0]?.display_name ?? agentId;

  const resendKey = process.env.RESEND_API_KEY;
  if (!resendKey) return 'ERROR: RESEND_API_KEY not configured';

  const resend = new Resend(resendKey);
  const { data, error } = await resend.emails.send({
    from: `${displayName} <${fromAddress}>`,
    to,
    subject,
    text: body,
  });

  if (error) return `ERROR: ${error.message}`;

  logger.log(`Email sent by ${agentId} from ${fromAddress} to ${to.join(', ')}: "${subject}"`);
  return `Email sent from ${fromAddress} to ${to.join(', ')}: "${subject}" (id: ${data?.id})`;
}

async function handlePauseJobType(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const jobType = String(params.job_type ?? '');
  const reason = String(params.reason ?? 'Paused by monitor');
  if (!jobType) return 'ERROR: job_type is required';

  const result = await query(
    `UPDATE tempo_jobs SET status = 'cancelled', last_error = $2
     WHERE job_type = $1 AND status IN ('pending')`,
    [jobType, reason],
  );

  logger.log(`Paused job type ${jobType}: cancelled ${result.rowCount ?? 0} pending jobs`);
  return `Paused ${jobType}: cancelled ${result.rowCount ?? 0} pending job(s). Reason: ${reason}`;
}

async function handleResumeJobType(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const jobType = String(params.job_type ?? '');
  if (!jobType) return 'ERROR: job_type is required';

  const result = await query(
    `UPDATE tempo_jobs SET status = 'pending', last_error = NULL
     WHERE job_type = $1 AND status = 'cancelled' AND last_error LIKE 'Paused%'`,
    [jobType],
  );

  logger.log(`Resumed job type ${jobType}: restored ${result.rowCount ?? 0} jobs`);
  return `Resumed ${jobType}: restored ${result.rowCount ?? 0} job(s) to pending`;
}

async function handleGetQueueStatus(_agentId: string, _params: Record<string, unknown>): Promise<string> {
  const result = await query<{ job_type: string; status: string; count: string }>(
    `SELECT job_type, status, COUNT(*)::text as count
     FROM tempo_jobs
     WHERE updated_at > NOW() - INTERVAL '1 hour'
     GROUP BY job_type, status
     ORDER BY count::int DESC
     LIMIT 30`,
  );

  if (result.rows.length === 0) return 'Queue is empty (no jobs in the last hour)';

  const lines = result.rows.map(r => `${r.job_type}: ${r.status} (${r.count})`);
  return `Queue status (last hour):\n${lines.join('\n')}`;
}

// Per-agent daily Pushover budget. Critical alerts (priority>=2) bypass.
const PUSHOVER_DAILY_BUDGET = 8;

async function handleSendPushoverTool(agentId: string, params: Record<string, unknown>): Promise<string> {
  const { sendPushover } = await import('./lib/pushover.js');
  const title = String(params.title ?? '');
  const message = String(params.message ?? '');
  const priority = Number(params.priority ?? 0);

  if (!title || !message) return 'ERROR: title and message are required';

  // Fingerprint = agent + normalized message body content. Title is excluded
  // because agents (notably ARIA) use generic titles like "Proactive Insights"
  // that bypass title-based dedup. Also do a substring overlap check so
  // re-orderings of the same insight list still match.
  const normMsg = message.toLowerCase().replace(/\s+/g, ' ').trim();
  const fingerprint = `${agentId}|${normMsg.slice(0, 200)}`;
  const dupCheck = await query<{ ct: string; existing: string }>(
    `SELECT count(*)::text AS ct,
            string_agg(metadata->>'fingerprint', '|||') AS existing
     FROM event_log
     WHERE component = 'pushover'
       AND created_at > now() - interval '24 hours'
       AND metadata->>'agent_id' = $1`,
    [agentId],
  );
  if (parseInt(dupCheck.rows[0]?.ct ?? '0', 10) > 0) {
    // Check if any prior message shares ≥60 chars of contiguous content with this one
    const priors = (dupCheck.rows[0]?.existing ?? '').split('|||').filter(Boolean);
    const myContent = normMsg.slice(0, 200);
    const overlap = priors.some(p => {
      const pContent = p.split('|').slice(1).join('|');
      if (!pContent || pContent.length < 40) return false;
      // Check if 60+ char window of mine appears in any prior, or vice versa
      for (let i = 0; i + 60 <= myContent.length; i += 20) {
        if (pContent.includes(myContent.slice(i, i + 60))) return true;
      }
      for (let i = 0; i + 60 <= pContent.length; i += 20) {
        if (myContent.includes(pContent.slice(i, i + 60))) return true;
      }
      return false;
    });
    if (overlap) {
      return `SKIPPED: a Pushover with overlapping content was already sent in the last 24h. Mark the underlying insight(s) as 'acted' or 'dismissed' in proactive_insights instead of re-sending. Generic titles + reshuffled lists will still be caught by content fingerprint.`;
    }
  }

  // Daily budget check (per agent). Emergency priority (>=2) bypasses.
  if (priority < 2) {
    const budgetCheck = await query<{ ct: string }>(
      `SELECT count(*)::text AS ct FROM event_log
       WHERE component = 'pushover'
         AND created_at > now() - interval '24 hours'
         AND metadata->>'agent_id' = $1`,
      [agentId],
    );
    if (parseInt(budgetCheck.rows[0]?.ct ?? '0', 10) >= PUSHOVER_DAILY_BUDGET) {
      return `BUDGET_EXCEEDED: ${agentId} has used its ${PUSHOVER_DAILY_BUDGET}/day Pushover budget. Use inbox_send for non-critical items, or set priority=2 for emergencies.`;
    }
  }

  try {
    const result = await sendPushover({ title, message, priority: priority as -2 | -1 | 0 | 1 | 2 });
    // Record in event_log so future calls can dedup.
    const { logEvent } = await import('./lib/event-log.js');
    await logEvent({
      action: `Pushover: "${title}"`,
      component: 'pushover',
      category: 'interaction',
      metadata: { request_id: result.request, priority, agent_id: agentId, fingerprint },
    });
    return `Pushover sent: "${title}" (request: ${result.request})`;
  } catch (err) {
    return `ERROR: ${(err as Error).message}`;
  }
}

async function handleRestartService(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const { execSync } = await import('node:child_process');
  const service = String(params.service ?? '');
  if (!service) return 'ERROR: service name is required';

  // Allowlist of services that can be restarted
  const allowed = ['forge-api', 'nexus-worker', 'nexus-api', 'chancery-web', 'llama-server', 'llama-priority', 'llama-vlm', 'llama-embed'];
  if (!allowed.includes(service)) return `ERROR: Service '${service}' not in allowlist (${allowed.join(', ')})`;

  try {
    execSync(`sudo systemctl restart ${service}`, { timeout: 30_000, encoding: 'utf-8' });
    const status = execSync(`systemctl is-active ${service}`, { encoding: 'utf-8' }).trim();
    return `Restarted ${service}: ${status}`;
  } catch (err) {
    return `ERROR restarting ${service}: ${(err as Error).message}`;
  }
}

// Legacy — llama services are now system services. Redirect to restart_service.
async function handleRestartUserService(agentId: string, params: Record<string, unknown>): Promise<string> {
  return handleRestartService(agentId, params);
}

async function handleUpdateLlmConfig(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const tier = String(params.tier ?? '');
  const chainOrder = Number(params.chain_order ?? 0);
  const model = String(params.model ?? '');
  const provider = String(params.provider ?? '');
  const timeoutMs = params.timeout_ms ? Number(params.timeout_ms) : 30000;
  const isEnabled = params.is_enabled !== false;

  if (!tier || !model || !provider) return 'ERROR: tier, model, and provider are required';

  const result = await query(
    `INSERT INTO llm_model_config (tier, chain_order, model, provider, timeout_ms, is_enabled)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (tier, chain_order) DO UPDATE SET
       model = $3, provider = $4, timeout_ms = $5, is_enabled = $6, updated_at = NOW()`,
    [tier, chainOrder, model, provider, timeoutMs, isEnabled],
  );

  return `LLM config updated: ${tier}[${chainOrder}] = ${model} (${provider}, ${timeoutMs}ms, enabled=${isEnabled})`;
}

async function handleForgeHealth(_agentId: string, _params: Record<string, unknown>): Promise<string> {
  const forgeUrl = process.env.FORGE_URL ?? 'http://localhost:8642';
  try {
    const resp = await fetch(`${forgeUrl}/health`, { signal: AbortSignal.timeout(5000) });
    if (!resp.ok) return `Forge unhealthy: HTTP ${resp.status}`;
    const data = await resp.json() as Record<string, unknown>;
    return `Forge health: ${JSON.stringify(data)}`;
  } catch (err) {
    return `Forge unreachable: ${(err as Error).message}`;
  }
}

// ── Fixer tools (file/git operations) ──────────────────────────

const REPO_ROOT = process.env.NEXUS_REPO_PATH ?? '/opt/nexus/Projects/nexus';

function sanitizePath(path: string): string {
  // Prevent directory traversal
  const normalized = path.replace(/\.\./g, '').replace(/^\//, '');
  return `${REPO_ROOT}/${normalized}`;
}

async function handleReadFile(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const fs = await import('node:fs/promises');
  const filePath = sanitizePath(String(params.path ?? ''));
  if (!filePath) return 'ERROR: path is required';

  try {
    const content = await fs.readFile(filePath, 'utf-8');
    if (content.length > 10000) {
      return `File: ${params.path} (${content.length} chars, truncated)\n\n${content.slice(0, 10000)}\n\n[... truncated]`;
    }
    return `File: ${params.path} (${content.length} chars)\n\n${content}`;
  } catch (err) {
    return `ERROR: Cannot read ${params.path}: ${(err as Error).message}`;
  }
}

async function handleWriteFile(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const fs = await import('node:fs/promises');
  const path = await import('node:path');
  const filePath = sanitizePath(String(params.path ?? ''));
  const content = String(params.content ?? '');
  if (!filePath || !content) return 'ERROR: path and content are required';

  try {
    await fs.mkdir(path.dirname(filePath), { recursive: true });
    await fs.writeFile(filePath, content, 'utf-8');
    return `Written: ${params.path} (${content.length} chars)`;
  } catch (err) {
    return `ERROR: Cannot write ${params.path}: ${(err as Error).message}`;
  }
}

async function handleEditFile(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const fs = await import('node:fs/promises');
  const filePath = sanitizePath(String(params.path ?? ''));
  const oldStr = String(params.old_string ?? '');
  const newStr = String(params.new_string ?? '');
  if (!filePath || !oldStr) return 'ERROR: path and old_string are required';

  try {
    const content = await fs.readFile(filePath, 'utf-8');
    if (!content.includes(oldStr)) {
      return `ERROR: old_string not found in ${params.path}`;
    }
    const updated = content.replace(oldStr, newStr);
    await fs.writeFile(filePath, updated, 'utf-8');
    return `Edited: ${params.path} (replaced ${oldStr.length} chars with ${newStr.length} chars)`;
  } catch (err) {
    return `ERROR: Cannot edit ${params.path}: ${(err as Error).message}`;
  }
}

async function handleRunBuild(_agentId: string, _params: Record<string, unknown>): Promise<string> {
  const { execSync } = await import('node:child_process');
  try {
    const output = execSync('npm run build 2>&1', { cwd: REPO_ROOT, timeout: 60_000, encoding: 'utf-8' });
    if (output.includes('error TS')) {
      return `BUILD FAILED:\n${output.slice(-2000)}`;
    }
    return `BUILD SUCCESS:\n${output.slice(-500)}`;
  } catch (err) {
    return `BUILD FAILED: ${(err as Error).message}`;
  }
}

async function handleGitStatus(_agentId: string, _params: Record<string, unknown>): Promise<string> {
  const { execSync } = await import('node:child_process');
  try {
    const branch = execSync('git branch --show-current', { cwd: REPO_ROOT, encoding: 'utf-8' }).trim();
    const status = execSync('git status --short', { cwd: REPO_ROOT, encoding: 'utf-8' }).trim();
    return `Branch: ${branch}\n${status || '(clean working tree)'}`;
  } catch (err) {
    return `ERROR: ${(err as Error).message}`;
  }
}

async function handleGitCommitPush(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const { execSync } = await import('node:child_process');
  const message = String(params.message ?? '');
  const branch = String(params.branch ?? '');
  const files = (params.files as string[]) ?? [];
  if (!message) return 'ERROR: message is required';
  if (!branch) return 'ERROR: branch is required (e.g. fix/sentiment-emoji)';

  try {
    // Create and switch to branch
    try {
      execSync(`git checkout -b ${branch}`, { cwd: REPO_ROOT, encoding: 'utf-8' });
    } catch {
      // Branch might already exist
      execSync(`git checkout ${branch}`, { cwd: REPO_ROOT, encoding: 'utf-8' });
    }

    // Stage files
    if (files.length > 0) {
      execSync(`git add ${files.map(f => `"${f}"`).join(' ')}`, { cwd: REPO_ROOT, encoding: 'utf-8' });
    } else {
      execSync('git add -A', { cwd: REPO_ROOT, encoding: 'utf-8' });
    }

    // Commit
    const commitOutput = execSync(
      `git commit -m "${message.replace(/"/g, '\\"')}"`,
      { cwd: REPO_ROOT, encoding: 'utf-8' },
    );

    // Push branch
    execSync(`git push -u origin ${branch}`, { cwd: REPO_ROOT, encoding: 'utf-8', timeout: 30_000 });

    // Switch back to main
    execSync('git checkout main', { cwd: REPO_ROOT, encoding: 'utf-8' });

    return `Committed to branch ${branch} and pushed.\n${commitOutput.trim()}\nNow use create_pr to open a pull request.`;
  } catch (err) {
    // Try to get back to main on error
    try { execSync('git checkout main', { cwd: REPO_ROOT, encoding: 'utf-8' }); } catch { /* ok */ }
    return `ERROR: ${(err as Error).message}`;
  }
}

async function handleCreatePr(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const { execSync } = await import('node:child_process');
  const title = String(params.title ?? '');
  const body = String(params.body ?? '');
  const branch = String(params.branch ?? '');
  if (!title || !branch) return 'ERROR: title and branch are required';

  try {
    const output = execSync(
      `gh pr create --title "${title.replace(/"/g, '\\"')}" --body "${(body || 'Automated fix by Fixer agent').replace(/"/g, '\\"')}" --head ${branch} --base main`,
      { cwd: REPO_ROOT, encoding: 'utf-8', timeout: 30_000 },
    );
    return `PR created: ${output.trim()}`;
  } catch (err) {
    return `ERROR creating PR: ${(err as Error).message}`;
  }
}

// ── MCP server management tools ─────────────────────────────

async function handleListMcpServers(_agentId: string, _params: Record<string, unknown>): Promise<string> {
  const { listConnectedServers } = await import('./lib/mcp-client-manager.js');
  const connected = listConnectedServers();

  const dbResult = await query<{ server_name: string; display_name: string; status: string; tools_provided: string[] }>(
    `SELECT server_name, display_name, status, tools_provided FROM mcp_servers ORDER BY server_name`,
  );

  if (dbResult.rows.length === 0 && connected.length === 0) {
    return 'No MCP servers installed or connected.';
  }

  const lines = dbResult.rows.map(s => {
    const isConnected = connected.some(c => c.name === s.server_name);
    const tools = s.tools_provided?.length ? s.tools_provided.join(', ') : 'none';
    return `- ${s.server_name} (${s.display_name}): ${s.status}${isConnected ? ' [connected]' : ''} | tools: ${tools}`;
  });

  return `MCP servers (${dbResult.rows.length}):\n${lines.join('\n')}`;
}

async function handleInstallMcpServer(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const serverName = String(params.server_name ?? '');
  const installPackage = String(params.install_package ?? '');
  const description = String(params.description ?? '');

  if (!serverName || !installPackage) return 'ERROR: server_name and install_package are required';

  // Register in mcp_servers table
  await query(
    `INSERT INTO mcp_servers (server_name, display_name, description, install_type, install_package, transport, command, args, status, source)
     VALUES ($1, $1, $2, 'npm', $3, 'stdio', 'npx', ARRAY['-y', $3], 'installing', 'agent_request')
     ON CONFLICT (server_name) DO UPDATE SET install_package = $3, status = 'installing', updated_at = NOW()`,
    [serverName, description, installPackage],
  );

  // Install the package
  const { execSync } = await import('node:child_process');
  try {
    execSync(`npm install -g ${installPackage}`, { timeout: 120_000, encoding: 'utf-8' });
  } catch (err) {
    await query(`UPDATE mcp_servers SET status = 'error', status_message = $2 WHERE server_name = $1`,
      [serverName, (err as Error).message]);
    return `ERROR installing ${installPackage}: ${(err as Error).message}`;
  }

  // Update status and try to connect
  await query(`UPDATE mcp_servers SET status = 'installed' WHERE server_name = $1`, [serverName]);

  try {
    const { connectServer } = await import('./lib/mcp-client-manager.js');
    const result = await connectServer({
      serverName,
      command: 'npx',
      args: ['-y', installPackage],
    });
    return `Installed and connected ${serverName}: ${result.tools.length} tools available (${result.tools.join(', ')})`;
  } catch (err) {
    return `Installed ${serverName} but failed to connect: ${(err as Error).message}. Check env_vars.`;
  }
}

// ── Ingestion pipeline tool ──────────────────────────────────

async function handleIngestData(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const { ingest } = await import('./lib/ingestion-pipeline.js');
  const sourceKey = String(params.source_key ?? '');
  const data = params.data;

  if (!sourceKey) return 'ERROR: source_key is required';
  if (!data) return 'ERROR: data is required (array of raw records)';

  const records = Array.isArray(data) ? data : [data];

  try {
    const result = await ingest(sourceKey, records);
    return `Ingested ${sourceKey}: ${result.ingested} new, ${result.skipped} dupes, ${result.proactive} proactive actions, ${result.enrichments} enrichments queued`;
  } catch (err) {
    return `ERROR: ${(err as Error).message}`;
  }
}

// ── Skills management tools ──────────────────────────────────

async function handleListSkills(_agentId: string, _params: Record<string, unknown>): Promise<string> {
  const result = await query<{ skill_name: string; description: string; applicable_agents: string[] }>(
    `SELECT skill_name, description, applicable_agents FROM agent_skills WHERE is_active = true ORDER BY skill_name`,
  );

  if (result.rows.length === 0) return 'No skills installed.';

  const lines = result.rows.map(s => {
    const agents = s.applicable_agents?.length ? s.applicable_agents.join(', ') : 'all agents';
    return `- ${s.skill_name}: ${s.description ?? '(no description)'} [${agents}]`;
  });

  return `Installed skills (${result.rows.length}):\n${lines.join('\n')}`;
}

async function handleInstallSkill(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const skillName = String(params.skill_name ?? '');
  const description = String(params.description ?? '');
  const content = String(params.content ?? '');
  const sourceUrl = params.source_url ? String(params.source_url) : null;
  const applicableAgents = (params.applicable_agents as string[]) ?? [];

  if (!skillName || !content) return 'ERROR: skill_name and content are required';

  await query(
    `INSERT INTO agent_skills (skill_name, description, content, source, source_url, applicable_agents)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (skill_name) DO UPDATE SET
       description = $2, content = $3, source_url = $5, applicable_agents = $6, updated_at = NOW(), is_active = true`,
    [skillName, description, content, sourceUrl ? 'url' : 'manual', sourceUrl, applicableAgents],
  );

  logger.log(`Skill installed: ${skillName}`);
  return `Skill '${skillName}' installed. ${applicableAgents.length ? `Available to: ${applicableAgents.join(', ')}` : 'Available to all agents.'}`;
}

async function handleGetSkill(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const skillName = String(params.skill_name ?? '');
  if (!skillName) return 'ERROR: skill_name is required';

  const result = await query<{ content: string; description: string }>(
    `SELECT content, description FROM agent_skills WHERE skill_name = $1 AND is_active = true`,
    [skillName],
  );

  if (result.rows.length === 0) return `Skill '${skillName}' not found.`;

  return `# ${skillName}\n${result.rows[0].description ? `\n${result.rows[0].description}\n` : ''}\n${result.rows[0].content}`;
}

// ── Eval Scoring Tools ─────────────────────────────────────────────────────

async function handleScoreDecision(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const traceId = String(params.trace_id ?? '');
  if (!traceId) return 'ERROR: trace_id is required';

  const scorers = (params.scorers as string[] | undefined) ?? ['task_completion', 'faithfulness', 'coherence'];

  // Load the decision to score
  const decision = await query<{
    agent_id: string; user_message: string; llm_response: Record<string, unknown>;
    parsed_actions: Record<string, unknown>[]; execution_results: Record<string, unknown>[];
  }>(
    `SELECT agent_id, user_message, llm_response, parsed_actions, execution_results
     FROM agent_decisions WHERE trace_id = $1`,
    [traceId],
  );

  if (decision.rows.length === 0) return `ERROR: Decision not found for trace_id: ${traceId}`;

  const d = decision.rows[0];
  const response = d.llm_response ?? {};
  const output = [
    `Summary: ${(response as Record<string, unknown>).summary ?? 'n/a'}`,
    `Status: ${(response as Record<string, unknown>).status ?? 'n/a'}`,
    `Actions: ${JSON.stringify(d.parsed_actions ?? []).slice(0, 500)}`,
  ].join('\n');

  const results = (d.execution_results ?? []).map((r: Record<string, unknown>) => String(r.result ?? ''));
  const actions = (d.parsed_actions ?? []).map((a: Record<string, unknown>) => String(a.action ?? ''));

  const scores = await evalScorer.scoreAndPersist(
    scorers as Array<'task_completion' | 'faithfulness' | 'relevancy' | 'hallucination' | 'coherence'>,
    {
      agentId: d.agent_id,
      traceId,
      input: d.user_message ?? '',
      output,
      actions,
      results,
    },
  );

  if (scores.length === 0) return 'No scores generated — LLM may be unavailable.';

  const lines = scores.map(s => `${s.scorer}: ${s.score.toFixed(2)} — ${s.explanation}`);
  return `Scores for ${d.agent_id} (${traceId}):\n${lines.join('\n')}`;
}

async function handleGetEvalSummary(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const targetAgent = String(params.agent_id ?? '');
  if (!targetAgent) return 'ERROR: agent_id is required';

  const period = String(params.period ?? '7 days');
  const summary = await evalScorer.getScoreSummary(targetAgent, period);

  if (summary.length === 0) return `No eval scores found for ${targetAgent} in the last ${period}.`;

  const lines = summary.map(s =>
    `${s.scorer}: avg=${Number(s.avg_score).toFixed(2)}, min=${Number(s.min_score).toFixed(2)}, count=${s.count}`,
  );
  return `Eval summary for ${targetAgent} (${period}):\n${lines.join('\n')}`;
}

// ── Delegation & Memory Consolidation Tools ────────────────────────────────

async function handleDelegateTask(agentId: string, params: Record<string, unknown>): Promise<string> {
  const targetAgent = String(params.agent_id ?? '');
  const prompt = String(params.prompt ?? '');

  if (!targetAgent) return 'ERROR: agent_id is required (which agent to consult)';
  if (!prompt) return 'ERROR: prompt is required (what to ask the agent)';
  if (targetAgent === agentId) return 'ERROR: cannot delegate to yourself';

  const result = await delegateToAgent(agentId, targetAgent, prompt);

  if (!result.success) return result.response;
  return `[${result.agentId} responded in ${result.durationMs}ms]:\n${result.response}`;
}

async function handleConsolidateMemories(agentId: string, params: Record<string, unknown>): Promise<string> {
  const targetAgent = String(params.agent_id ?? agentId);
  const maxEntries = Number(params.max_entries ?? 50);

  const result = await consolidateMemories(targetAgent, { maxToConsolidate: maxEntries });

  if (result.consolidated === 0) {
    return `No consolidation needed for ${targetAgent} (fewer than 10 memories or no redundant clusters found).`;
  }

  return `Consolidated ${targetAgent} memories: ${result.consolidated} groups merged, ${result.removed} entries consolidated into fewer, higher-quality memories.`;
}

async function handleLookiSearchMemories(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const query = String(params.query ?? '');
  if (!query) return 'ERROR: query parameter is required';

  const apiKey = process.env.LOOKI_API_KEY;
  const baseUrl = process.env.LOOKI_BASE_URL || 'https://open.looki.ai/api/v1';
  if (!apiKey) return 'ERROR: LOOKI_API_KEY not configured';

  const searchParams = new URLSearchParams({ query });
  if (params.start_date) searchParams.set('start_date', String(params.start_date));
  if (params.end_date) searchParams.set('end_date', String(params.end_date));
  searchParams.set('page_size', String(params.page_size ?? 10));

  try {
    const resp = await fetch(`${baseUrl}/moments/search?${searchParams}`, {
      headers: { 'X-API-Key': apiKey, Accept: 'application/json' },
      signal: AbortSignal.timeout(15_000),
    });

    if (!resp.ok) return `ERROR: Looki API returned ${resp.status}`;

    const data = await resp.json() as { code: number; data?: { items?: unknown[] } };
    if (data.code !== 0) return `ERROR: Looki API error code ${data.code}`;

    const items = data.data?.items ?? [];
    if (items.length === 0) return `No Looki memories found for "${query}"`;

    const output = JSON.stringify(items);
    return output.length > 3000 ? output.slice(0, 3000) + '... (truncated)' : output;
  } catch (err) {
    return `ERROR: Looki search failed: ${(err as Error).message}`;
  }
}

async function handleGetModelHealth(_agentId: string, _params: Record<string, unknown>): Promise<string> {
  const { getModelHealth } = await import('./lib/llm/router.js');
  const health = getModelHealth();
  if (health.length === 0) return 'All models healthy — no circuit breakers active.';
  return JSON.stringify(health);
}

async function handleResetModelHealth(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const { resetModelHealth } = await import('./lib/llm/router.js');
  const model = params.model ? String(params.model) : undefined;
  const count = resetModelHealth(model);
  return model
    ? `Reset circuit breaker for ${model} (${count ? 'was disabled' : 'was not disabled'})`
    : `Reset all circuit breakers (${count} cleared)`;
}

async function handleGetChecklist(agentId: string, params: Record<string, unknown>): Promise<string> {
  const schedule = String(params.schedule ?? 'all');
  const where = schedule === 'all' ? '' : `AND schedule = '${schedule}'`;

  const result = await query(
    `SELECT id, schedule, title, description, last_completed_at::text, last_result
     FROM agent_checklists
     WHERE agent_id = $1 AND is_active = true ${where}
     ORDER BY priority DESC, schedule, title`,
    [agentId],
  );

  if (result.rows.length === 0) return 'No checklist items found.';

  const items = result.rows.map((r: Record<string, unknown>) => {
    const completed = r.last_completed_at ? `last: ${r.last_completed_at}` : 'never completed';
    return `ID: ${r.id} | [${r.schedule}] ${r.title} (${completed})\n  ${String(r.description ?? '').slice(0, 300)}`;
  });
  return items.join('\n\n');
}

async function handleCompleteChecklistItem(agentId: string, params: Record<string, unknown>): Promise<string> {
  const itemId = String(params.item_id ?? params.id ?? '');
  const result_text = String(params.result ?? 'completed');

  if (!itemId) return 'ERROR: id is required (pass the ID from get_checklist)';

  const res = await query(
    `UPDATE agent_checklists SET last_completed_at = NOW(), last_result = $1, updated_at = NOW()
     WHERE id = $2 AND agent_id = $3 RETURNING title`,
    [result_text, itemId, agentId],
  );

  if (res.rows.length === 0) return 'ERROR: checklist item not found or not yours';
  return `Completed: ${(res.rows[0] as Record<string, unknown>).title}`;
}

async function handleCreateChecklistItem(agentId: string, params: Record<string, unknown>): Promise<string> {
  const title = String(params.title ?? '');
  const description = String(params.description ?? '');
  const schedule = String(params.schedule ?? 'weekly');
  const checkQuery = params.check_query ? String(params.check_query) : null;
  const priority = Number(params.priority ?? 0);
  // ARIA can create checklist items for any agent; others can only create for themselves
  const targetAgent = (agentId === 'aria' && params.agent_id) ? String(params.agent_id) : agentId;

  if (!title) return 'ERROR: title is required';
  if (!['cycle', 'daily', 'weekly', 'monthly'].includes(schedule)) {
    return 'ERROR: schedule must be one of: cycle, daily, weekly, monthly';
  }

  // Prevent duplicates
  const existing = await query(
    `SELECT id FROM agent_checklists WHERE agent_id = $1 AND title = $2 AND is_active = true`,
    [targetAgent, title],
  );
  if (existing.rows.length > 0) {
    return `Already exists: "${title}" (id: ${(existing.rows[0] as Record<string, unknown>).id})`;
  }

  const res = await query(
    `INSERT INTO agent_checklists (agent_id, schedule, title, description, check_query, priority)
     VALUES ($1, $2, $3, $4, $5, $6)
     RETURNING id, title`,
    [targetAgent, schedule, title, description, checkQuery, priority],
  );

  const row = res.rows[0] as Record<string, unknown>;
  const forText = targetAgent !== agentId ? ` for ${targetAgent}` : '';
  return `Created checklist item${forText}: "${row.title}" (id: ${row.id}, schedule: ${schedule})`;
}

async function handleSemanticSearch(_agentId: string, params: Record<string, unknown>): Promise<string> {
  const q = params.query as string;
  if (!q) return 'Error: query parameter is required';

  const categories = params.categories
    ? (Array.isArray(params.categories) ? params.categories : [params.categories]) as semanticSearch.SearchCategory[]
    : undefined;
  const limit = params.limit ? Number(params.limit) : 10;

  const result = await semanticSearch.search({
    query: q,
    categories,
    limit,
    rerank: true,
    minSimilarity: 0.3,
  });

  if (result.results.length === 0) {
    return `No results found for "${q}" (searched ${result.tables_searched} tables)`;
  }

  const lines = result.results.map((r, i) => {
    const ts = r.timestamp ? ` [${r.timestamp.slice(0, 10)}]` : '';
    const score = r.rerank_score !== undefined
      ? `rerank=${r.rerank_score.toFixed(2)}`
      : `sim=${r.similarity.toFixed(3)}`;
    return `${i + 1}. [${r.table}]${ts} (${score}) ${r.text.slice(0, 200)}`;
  });

  return `Found ${result.results.length} results across ${result.tables_searched} tables ` +
    `(embed=${result.query_embedding_ms}ms, search=${result.search_ms}ms, rerank=${result.rerank_ms}ms):\n\n` +
    lines.join('\n');
}
