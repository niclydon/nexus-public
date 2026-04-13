import crypto from 'node:crypto';
import {
  createLogger, agentRegistry, toolCatalog, comms, query, soulLoader,
  type Agent, type PlatformTool, type InboxMessage, type AgentMemory, type AgentTask,
} from '@nexus/core';
import { callLlm, type LlmResponse, type ActionRequest } from './lib/llm-router.js';
import { executeAction } from './tool-executor.js';
import { summarizeStateIfNeeded, estimateTokens } from './lib/summarize.js';

const logger = createLogger('agent-runner');
const MAX_STEPS = 5;
// ARIA has more checklist items and needs more rounds to complete them all
const AGENT_MAX_STEPS: Record<string, number> = { aria: 8 };

const WORKING_MEMORY_START = '<working_memory>';
const WORKING_MEMORY_END = '</working_memory>';

// Cached platform preamble (loaded from agent_config once per process)
let cachedPreamble: string | null = null;

export interface CycleResult {
  agentId: string;
  traceId: string;
  steps: number;
  actions: ActionRequest[];
  results: string[];
  summary: string | null;
  status: string | null;
  durationMs: number;
}

/**
 * v2 response format from agents. Backward-compatible — if the agent
 * returns just {"actions": [...]}, the extra fields default to null.
 */
interface AgentResponse extends LlmResponse {
  summary?: string;
  status?: 'ok' | 'warning' | 'critical';
  confidence?: number;
  escalation?: {
    to: string;
    priority: 'low' | 'medium' | 'high' | 'critical';
    issue: string;
    evidence: string;
    impact: string;
    recommended_next_step: string;
  } | null;
}

export async function runAgentCycle(agentId: string): Promise<CycleResult> {
  const start = performance.now();
  const traceId = `${agentId}-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
  logger.log('Starting cycle for', agentId, 'trace:', traceId);

  const agent = await agentRegistry.getAgent(agentId);
  if (!agent) throw new Error(`Agent not found: ${agentId}`);
  if (!agent.is_active) throw new Error(`Agent is inactive: ${agentId}`);
  if (agent.executor !== 'nexus') throw new Error(`Agent not owned by nexus: ${agentId}`);

  const [tools, rawInbox, memories, tasks] = await Promise.all([
    toolCatalog.getToolsForAgent(agentId),
    comms.getUnreadInbox(agentId),
    agentRegistry.getAgentMemories(agentId),
    getOpenTasks(agentId),
  ]);

  // ARIA: only show messages from the owner and chat requests.
  // All agent-to-agent escalations are filtered — specialized agents handle their own domains.
  // ARIA's job is personal assistant work, not infrastructure triage.
  const inbox = agentId === 'aria'
    ? rawInbox.filter(m =>
        m.from_agent === 'owner' || (m.message ?? '').includes('[Chat Request]')
      )
    : rawInbox;

  const systemPrompt = await buildSystemPrompt(agent, tools, inbox.length > 0);
  const rawStateMessage = await buildStateMessage(agent, inbox, memories, tasks);

  // Per-agent LLM config (e.g., model-ops uses Secondary-Server for independence from Primary-Server LLM stack)
  const agentLlmConfig = await getAgentLlmConfig(agentId);

  // Auto-summarize state message if it exceeds token budget
  // This compresses memories when the state gets too large (many memories + inbox + tasks)
  const stateMessage = await summarizeStateIfNeeded(rawStateMessage, {
    tokenBudget: 4000,
    agentId,
    forgeUrl: agentLlmConfig.forgeUrl,
  });
  if (stateMessage !== rawStateMessage) {
    logger.logVerbose('State message summarized for', agentId,
      '|', estimateTokens(rawStateMessage), '→', estimateTokens(stateMessage), 'tokens',
    );
  }

  logger.logVerbose('Prompt assembled for', agentId,
    '| tools:', tools.length, '| inbox:', inbox.length,
    '| memories:', memories.length, '| tasks:', tasks.length,
    agentLlmConfig.forgeUrl ? `| llm: ${agentLlmConfig.forgeUrl}` : '',
  );

  // Multi-step cycle with conversation history
  const allActions: ActionRequest[] = [];
  const allResults: string[] = [];
  const conversationHistory: Array<{ role: 'user' | 'assistant'; content: string }> = [];
  let step = 0;
  let response = await callLlm({ systemPrompt, userMessage: stateMessage, agentId, ...agentLlmConfig }) as AgentResponse | null;

  if (!response) {
    logger.logMinimal('LLM returned null for', agentId);
    const durationMs = Math.round(performance.now() - start);
    await logDecision(agent, traceId, stateMessage, response, allActions, allResults, durationMs);
    return { agentId, traceId, steps: 0, actions: [], results: [], summary: null, status: null, durationMs };
  }

  // Handle structured escalation from initial response
  if (response.escalation) {
    await deliverEscalation(agentId, response.escalation, traceId);
  }

  const maxSteps = AGENT_MAX_STEPS[agentId] ?? MAX_STEPS;
  while (step < maxSteps) {
    step++;
    const actions = response.actions ?? [];
    if (actions.length === 0) break;

    const workActions: ActionRequest[] = [];
    const heldComms: ActionRequest[] = [];
    for (const a of actions) {
      if (a.action === 'inbox_send') {
        heldComms.push(a);
      } else {
        workActions.push(a);
      }
    }

    const workResults: string[] = [];
    if (workActions.length > 0) {
      for (const action of workActions) {
        const result = await executeAction(agentId, action, tools, agent.autonomy_level);
        workResults.push(result);
      }
      allActions.push(...workActions);
      allResults.push(...workResults);
    }

    const hasFailures = workResults.some(r =>
      r.includes('ERROR') || r.includes('FAILED') || r.includes('BLOCKED'),
    );

    if (workResults.length > 0 && step < maxSteps) {
      const resultsText = workResults.map(r => `- ${r}`).join('\n');
      let followup = `Results:\n${resultsText}`;
      if (hasFailures) {
        followup += '\nSome actions failed. Do NOT claim success for failed actions.';
      }

      // Build conversation history for context continuity
      conversationHistory.push(
        { role: 'assistant' as const, content: JSON.stringify(response) },
        { role: 'user' as const, content: followup },
      );

      response = await callLlm({ systemPrompt, userMessage: followup, agentId, history: conversationHistory.slice(0, -1), ...agentLlmConfig }) as AgentResponse | null;
      if (!response) break;

      // Handle escalation from follow-up responses too
      if (response.escalation) {
        await deliverEscalation(agentId, response.escalation, traceId);
      }
      continue;
    }

    if (heldComms.length > 0) {
      for (const action of heldComms) {
        if (hasFailures && action.action === 'inbox_send') {
          const msg = String(action.params.message ?? '');
          const falseClaims = ['updated', 'complete', 'done', 'fixed', 'live', 'deployed', 'committed'];
          if (falseClaims.some(claim => msg.toLowerCase().includes(claim))) {
            logger.logVerbose('Suppressing inbox message with false success claim for', agentId);
            allResults.push('SUPPRESSED: message claimed success but actions failed');
            continue;
          }
        }
        const result = await executeAction(agentId, action, tools, agent.autonomy_level);
        allActions.push(action);
        allResults.push(result);
      }
    }
    break;
  }

  if (step > 1) {
    logger.log('Multi-step cycle for', agentId, ':', step, 'steps,', allActions.length, 'actions');
  }

  const durationMs = Math.round(performance.now() - start);
  const summary = response?.summary ?? null;
  const status = response?.status ?? null;

  // Working Memory: extract from LLM response and persist across cycles
  await processWorkingMemory(agentId, response, status, allActions.length);

  await Promise.all([
    logDecision(agent, traceId, stateMessage, response, allActions, allResults, durationMs, summary, status),
    agentRegistry.updateLastCheckIn(agentId),
  ]);

  logger.log('Cycle complete for', agentId,
    '| status:', status ?? 'n/a',
    '| actions:', allActions.length,
    '| duration:', durationMs, 'ms',
  );

  return { agentId, traceId, steps: step, actions: allActions, results: allResults, summary, status, durationMs };
}

/**
 * Load the platform preamble from agent_config (cached per process).
 */
async function getPlatformPreamble(): Promise<string> {
  if (cachedPreamble !== null) return cachedPreamble;

  try {
    const result = await query<{ value: string }>(
      `SELECT value FROM agent_config WHERE agent_id = '_platform' AND key = 'preamble'`,
    );
    cachedPreamble = result.rows[0]?.value ?? '';
    // Strip JSON quotes if stored as jsonb text
    if (cachedPreamble.startsWith('"') && cachedPreamble.endsWith('"')) {
      cachedPreamble = JSON.parse(cachedPreamble);
    }
  } catch {
    cachedPreamble = '';
  }
  return cachedPreamble ?? '';
}

async function buildSystemPrompt(agent: Agent, tools: PlatformTool[], hasInbox: boolean): Promise<string> {
  const preamble = await getPlatformPreamble();

  // Split tools: internal tools get full listing, MCP tools get grouped by server
  const internalTools = tools.filter(t => t.provider_type === 'internal');
  const mcpTools = tools.filter(t => t.provider_type === 'mcp');

  const internalSection = internalTools.map(t =>
    `- ${t.tool_id}: ${t.description}`,
  ).join('\n');

  // Group MCP tools by server with just a count and sample tools
  const mcpByServer = new Map<string, PlatformTool[]>();
  for (const t of mcpTools) {
    const server = (t.provider_config as Record<string, string>)?.server ?? 'unknown';
    if (!mcpByServer.has(server)) mcpByServer.set(server, []);
    mcpByServer.get(server)!.push(t);
  }

  const mcpSection = Array.from(mcpByServer.entries()).map(([server, serverTools]) => {
    const sample = serverTools.slice(0, 3).map(t => t.tool_id.replace(`${server}__`, '')).join(', ');
    return `- ${server}: ${serverTools.length} tools (e.g. ${sample}). Use search_tools to find specific tools.`;
  }).join('\n');

  const parts: string[] = [];

  // Platform preamble first (shared context)
  if (preamble) {
    parts.push(preamble, '');
  }

  // Try soul package first, fall back to agent_registry.system_prompt
  const soulPrompt = await loadSoulPrompt(agent.agent_id, hasInbox);
  if (soulPrompt) {
    parts.push(soulPrompt, '');
  } else if (agent.system_prompt) {
    parts.push(agent.system_prompt, '');
  }

  // Available tools — internal tools in full, MCP tools grouped by server
  parts.push('## Available Tools (use directly)', internalSection, '');
  if (mcpSection) {
    parts.push('## MCP Tool Servers (use search_tools to discover, then call by tool_id)', mcpSection, '');
  }

  // Skills injection — three tiers:
  // 1. universal: full content in ALL agent prompts
  // 2. always: full content in applicable agent prompts
  // 3. on_demand: description only, agent uses get_skill to read full content
  const { universal, always, onDemand } = await getSkillsByTier(agent.agent_id);

  if (universal.length > 0) {
    for (const s of universal) {
      parts.push(`## Skill: ${s.skill_name}`, s.content, '');
    }
  }

  if (always.length > 0) {
    for (const s of always) {
      parts.push(`## Skill: ${s.skill_name}`, s.content, '');
    }
  }

  if (onDemand.length > 0) {
    parts.push('## Additional Skills (use get_skill to read, or search_tools to discover more)');
    for (const s of onDemand) {
      parts.push(`- ${s.skill_name}: ${s.description ?? '(no description)'}`);
    }
    parts.push('');
  }

  return parts.join('\n');
}

/**
 * Load prompt content from a soul package using progressive disclosure.
 * Returns null if no soul package exists for this agent.
 *
 * Level 2 (active): SOUL.md + IDENTITY.md — loaded every cycle
 * Level 3 (deep): adds AGENTS.md + REFERENCE.md + HEARTBEAT.md — loaded when inbox has messages
 */
async function loadSoulPrompt(agentId: string, hasInbox: boolean): Promise<string | null> {
  const level = hasInbox ? 'deep' : 'active';
  const soul = await soulLoader.loadSoulPackage(agentId, level);
  if (!soul) return null;

  const prompt = soulLoader.assembleSoulPrompt(soul, level);
  if (!prompt) return null;

  logger.logVerbose('Loaded soul package for', agentId, '| level:', level,
    '| files: SOUL.md', soul.identity ? '+ IDENTITY.md' : '',
    soul.agents && level === 'deep' ? '+ AGENTS.md' : '',
    soul.reference && level === 'deep' ? '+ REFERENCE.md' : '',
    soul.heartbeat && level === 'deep' ? '+ HEARTBEAT.md' : '',
  );

  return prompt;
}

async function buildStateMessage(
  agent: Agent,
  inbox: InboxMessage[],
  memories: AgentMemory[],
  tasks: AgentTask[],
): Promise<string> {
  const parts: string[] = [];

  const now = new Date();
  parts.push(`Current time: ${now.toISOString()}`);
  if (agent.last_check_in) {
    parts.push(`Last check-in: ${new Date(agent.last_check_in).toISOString()}`);
  }

  // Peer schedule awareness — only Infra monitors other agents
  if (agent.agent_id === 'infra') {
    try {
      const { rows: peers } = await query<{ agent_id: string; schedule_interval_sec: number; last_check_in: Date | null }>(
        `SELECT agent_id, schedule_interval_sec, last_check_in FROM agent_registry
         WHERE is_active = true AND executor = 'nexus' AND agent_id != $1
         ORDER BY agent_id`,
        [agent.agent_id],
      );
      if (peers.length > 0) {
        const peerLines = peers.map((p: { agent_id: string; schedule_interval_sec: number; last_check_in: Date | null }) => {
          const interval = p.schedule_interval_sec ? `every ${p.schedule_interval_sec >= 3600 ? `${Math.round(p.schedule_interval_sec / 3600)}h` : `${Math.round(p.schedule_interval_sec / 60)}m`}` : 'unscheduled';
          const ago = p.last_check_in ? `${Math.round((now.getTime() - new Date(p.last_check_in).getTime()) / 60000)}m ago` : 'never';
          const overdue = p.last_check_in && p.schedule_interval_sec
            ? (now.getTime() - new Date(p.last_check_in).getTime()) > p.schedule_interval_sec * 2000
            : false;
          return `  ${p.agent_id}: ${interval}, last ${ago}${overdue ? ' ⚠️ OVERDUE' : ''}`;
        });
        parts.push('', 'Agent schedules (only flag OVERDUE agents, others are normal):', ...peerLines);
      }
    } catch { /* non-critical */ }
  }

  // Inject working memory from previous cycle (short-term cross-cycle state)
  if (agent.working_memory) {
    parts.push('', '## Working Memory (from your previous cycle)');
    parts.push(agent.working_memory);
    parts.push('');
    parts.push('To update your working memory, include a <working_memory>...</working_memory> block in your response.');
    parts.push('To clear it (issue resolved), omit the block when reporting status "ok" with no actions.');
  }

  // Show approved actions awaiting execution
  const approvedActions = await getApprovedActions(agent.agent_id);
  if (approvedActions.length > 0) {
    parts.push('', '## Approved Actions (execute these NOW)');
    for (const a of approvedActions) {
      if (a.response_type === 'edit' && a.modified_params) {
        // Reviewer edited the params — use modified version
        parts.push(`- ACTION (params modified by reviewer): {"action": "${a.action_type}", "params": ${a.modified_params}, "reason": "executing approved action #${a.id} with modified params"}`);
      } else {
        // Standard approval — use original params
        parts.push(`- ACTION: {"action": "${a.action_type}", "params": ${a.params}, "reason": "executing approved action #${a.id}"}`);
      }
    }
  }

  if (inbox.length > 0) {
    parts.push('', '## Unread Inbox Messages');
    for (const msg of inbox) {
      // Include structured context if present
      const ctx = msg.context ? ` | context: ${JSON.stringify(msg.context).slice(0, 200)}` : '';
      parts.push(`- From ${msg.from_agent} (priority ${msg.priority}): ${msg.message}${ctx}`);
    }
  }

  if (tasks.length > 0) {
    parts.push('', '## Open Tasks');
    for (const task of tasks) {
      parts.push(`- [${task.status}] ${task.title} (assigned by ${task.assigned_by}, priority ${task.priority})`);
      if (task.description) parts.push(`  ${task.description}`);
    }
  }

  if (memories.length > 0) {
    parts.push('', '## Memories (sorted by confidence, most relevant first)');
    const shown = memories.slice(0, 15);
    for (const mem of shown) {
      parts.push(`- [${mem.memory_type}] ${mem.content}`);
    }
    if (memories.length > 15) {
      parts.push(`  (${memories.length - 15} older/lower-confidence memories omitted — use remember tool to search)`);
    }
  }

  if (inbox.length === 0 && tasks.length === 0) {
    parts.push('', 'No unread messages or open tasks. Run routine checks if appropriate, or return {"actions": []} if nothing to do.');
  }

  return parts.join('\n');
}

/**
 * Convert a structured escalation into an inbox message to the target agent.
 */
// Escalation dedup: suppress repeated escalations aggressively.
// Key insight: agents rephrase the same issue slightly differently each cycle,
// defeating content-based fingerprinting. Instead, deduplicate on
// (from_agent, to_agent, priority) — if the same agent already escalated at
// the same priority level recently, suppress regardless of content.
const escalationHistory = new Map<string, number>();
const ESCALATION_SUPPRESS_MS: Record<string, number> = {
  critical: 60 * 60 * 1000,    // 1 hour — critical from same agent suppressed for 1h
  high: 2 * 60 * 60 * 1000,    // 2 hours
  medium: 4 * 60 * 60 * 1000,  // 4 hours
  low: 6 * 60 * 60 * 1000,     // 6 hours
};

async function deliverEscalation(
  fromAgent: string,
  escalation: NonNullable<AgentResponse['escalation']>,
  traceId: string,
): Promise<void> {
  // Dedup on (from → to + priority) — content-agnostic
  const fingerprint = `${fromAgent}→${escalation.to}:${escalation.priority}`;
  const now = Date.now();
  const lastSent = escalationHistory.get(fingerprint) ?? 0;
  const suppressMs = ESCALATION_SUPPRESS_MS[escalation.priority] ?? 60 * 60 * 1000;

  if ((now - lastSent) < suppressMs) {
    logger.logVerbose(`Suppressing escalation from ${fromAgent} → ${escalation.to} [${escalation.priority}]: already escalated ${Math.round((now - lastSent) / 60000)}m ago`);
    return;
  }

  escalationHistory.set(fingerprint, now);

  const message = [
    `ESCALATION [${escalation.priority}]: ${escalation.issue}`,
    `Evidence: ${escalation.evidence}`,
    `Impact: ${escalation.impact}`,
    `Recommended: ${escalation.recommended_next_step}`,
  ].join('\n');

  await comms.sendInbox({
    to_agent: escalation.to,
    from_agent: fromAgent,
    message,
    priority: escalation.priority === 'critical' ? 3 : escalation.priority === 'high' ? 2 : 1,
    trace_id: traceId,
    context: { type: 'escalation', ...escalation },
  });

  logger.log(`Escalation from ${fromAgent} → ${escalation.to}: ${escalation.issue}`);
}

/**
 * Load per-agent LLM config from agent_config.
 * Supports: llm_url (override Forge URL), llm_model (override model).
 */
async function getAgentLlmConfig(agentId: string): Promise<{ forgeUrl?: string; model?: string }> {
  try {
    const result = await query<{ key: string; value: unknown }>(
      `SELECT key, value FROM agent_config WHERE agent_id = $1 AND key IN ('llm_url', 'llm_model')`,
      [agentId],
    );
    const config: { forgeUrl?: string; model?: string } = {};
    for (const row of result.rows) {
      const val = typeof row.value === 'string' ? row.value : JSON.stringify(row.value).replace(/^"|"$/g, '');
      if (row.key === 'llm_url') config.forgeUrl = val;
      if (row.key === 'llm_model') config.model = val;
    }
    return config;
  } catch {
    return {};
  }
}

async function getSkillsByTier(agentId: string): Promise<{
  universal: Array<{ skill_name: string; content: string }>;
  always: Array<{ skill_name: string; content: string }>;
  onDemand: Array<{ skill_name: string; description: string | null }>;
}> {
  try {
    const result = await query<{
      skill_name: string; description: string | null; content: string; injection_mode: string;
    }>(
      `SELECT skill_name, description, content, injection_mode FROM agent_skills
       WHERE is_active = true AND (applicable_agents = '{}' OR $1 = ANY(applicable_agents))
       ORDER BY skill_name`,
      [agentId],
    );

    return {
      universal: result.rows.filter(s => s.injection_mode === 'universal'),
      always: result.rows.filter(s => s.injection_mode === 'always'),
      onDemand: result.rows.filter(s => s.injection_mode === 'on_demand'),
    };
  } catch {
    return { universal: [], always: [], onDemand: [] };
  }
}

async function getApprovedActions(agentId: string): Promise<Array<{
  id: number; action_type: string; params: string;
  response_type: string | null; modified_params: string | null; response_text: string | null;
}>> {
  try {
    const result = await query<{
      id: number; action_type: string; params: unknown;
      response_type: string | null; modified_params: unknown; response_text: string | null;
    }>(
      `SELECT id, action_type, params::text as params, response_type,
              modified_params::text as modified_params, response_text
       FROM agent_pending_actions
       WHERE agent_id = $1 AND status = 'approved'
       ORDER BY decided_at ASC`,
      [agentId],
    );
    return result.rows.map(r => ({
      id: r.id,
      action_type: r.action_type,
      params: String(r.params),
      response_type: r.response_type,
      modified_params: r.modified_params ? String(r.modified_params) : null,
      response_text: r.response_text,
    }));
  } catch {
    return [];
  }
}

async function getOpenTasks(agentId: string): Promise<AgentTask[]> {
  const result = await query<AgentTask>(
    `SELECT * FROM agent_tasks
     WHERE assigned_to = $1 AND status IN ('open', 'in_progress', 'blocked')
     ORDER BY priority DESC, created_at DESC LIMIT 20`,
    [agentId],
  );
  return result.rows;
}

// ── Working Memory ──────────────────────────────────────────────────────────
//
// Agents can emit <working_memory>...</working_memory> blocks in their LLM
// responses to persist short-term structured state across cycles. Unlike
// agent_memory (permanent knowledge), working memory is ephemeral — it tracks
// in-progress investigations, multi-cycle task context, and running state
// that doesn't warrant a permanent memory entry.
//
// The framework:
// 1. Extracts <working_memory> content from the LLM response summary
// 2. Strips the tags from visible output
// 3. Persists to agent_registry.working_memory
// 4. Re-injects into the state message on the next cycle
// 5. Clears when the agent reports status=ok with zero actions (clean cycle)

/**
 * Extract working memory content from an LLM response string.
 * Returns the content between tags, or null if no tags found.
 */
function extractWorkingMemory(text: string): string | null {
  const start = text.indexOf(WORKING_MEMORY_START);
  if (start === -1) return null;

  const contentStart = start + WORKING_MEMORY_START.length;
  const end = text.indexOf(WORKING_MEMORY_END, contentStart);
  if (end === -1) return null;

  return text.substring(contentStart, end).trim();
}

/**
 * Remove working memory tags from text (so they don't appear in logged decisions).
 */
function stripWorkingMemoryTags(text: string): string {
  let result = '';
  let pos = 0;

  while (pos < text.length) {
    const start = text.indexOf(WORKING_MEMORY_START, pos);
    if (start === -1) {
      result += text.substring(pos);
      break;
    }
    result += text.substring(pos, start);
    const end = text.indexOf(WORKING_MEMORY_END, start + WORKING_MEMORY_START.length);
    if (end === -1) {
      result += text.substring(start);
      break;
    }
    pos = end + WORKING_MEMORY_END.length;
  }

  return result.trim();
}

/**
 * Process working memory after a cycle completes.
 * - If LLM emitted <working_memory> tags → persist the content
 * - If clean cycle (status=ok, no actions) and no new tags → clear working memory
 * - Otherwise → leave existing working memory unchanged
 */
async function processWorkingMemory(
  agentId: string,
  response: AgentResponse | null,
  status: string | null,
  actionCount: number,
): Promise<void> {
  if (!response) return;

  // Check for working memory in the summary field (most common place agents emit it)
  const summary = response.summary ?? '';
  const newWorkingMemory = extractWorkingMemory(summary);

  if (newWorkingMemory) {
    // Agent emitted new working memory — persist it
    await query(
      'UPDATE agent_registry SET working_memory = $1, updated_at = NOW() WHERE agent_id = $2',
      [newWorkingMemory, agentId],
    );
    // Strip tags from the summary so they don't appear in decision logs
    if (response.summary) {
      response.summary = stripWorkingMemoryTags(response.summary);
    }
    logger.logVerbose('Working memory updated for', agentId, '| length:', newWorkingMemory.length);
    return;
  }

  // Clean cycle (ok, no actions) → clear working memory if it exists
  if (status === 'ok' && actionCount === 0) {
    await query(
      'UPDATE agent_registry SET working_memory = NULL, updated_at = NOW() WHERE agent_id = $1 AND working_memory IS NOT NULL',
      [agentId],
    );
    logger.logVerbose('Working memory cleared for', agentId, '(clean cycle)');
  }
  // Otherwise: leave existing working memory unchanged for next cycle
}

async function logDecision(
  agent: Agent,
  traceId: string,
  stateMessage: string,
  response: AgentResponse | null,
  actions: ActionRequest[],
  results: string[],
  durationMs: number,
  summary?: string | null,
  status?: string | null,
): Promise<void> {
  try {
    await agentRegistry.logDecision({
      agent_id: agent.agent_id,
      trace_id: traceId,
      state_snapshot: {
        summary: summary ?? null,
        status: status ?? null,
        confidence: response?.confidence ?? null,
      },
      system_prompt_hash: null,
      user_message: stateMessage.slice(0, 5000),
      llm_response: response as Record<string, unknown> | null,
      parsed_actions: actions as unknown as Record<string, unknown>[],
      execution_results: results.map(r => ({ result: r })),
      duration_ms: durationMs,
    });
  } catch (err) {
    logger.logMinimal('Failed to log decision:', (err as Error).message);
  }
}
