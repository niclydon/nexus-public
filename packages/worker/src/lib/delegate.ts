import {
  createLogger, agentRegistry, toolCatalog, soulLoader, query, comms,
  type Agent, type PlatformTool,
} from '@nexus/core';
import { callLlm, type LlmResponse } from './llm-router.js';

const logger = createLogger('delegate');

/**
 * Synchronous sub-agent delegation.
 *
 * Spawns a single-shot LLM call using the target agent's soul package
 * and tools, with the provided prompt as context. Returns the target
 * agent's response as a string — no multi-step cycle, no tool execution.
 *
 * This enables real-time cross-agent coordination within a single cycle:
 * - Collector asks Model Ops "Is Forge healthy?" → gets answer in 5-10s
 * - ARIA asks Analyst "Summarize overnight activity" → gets summary
 * - Monitor asks Collector "What's the photo backlog?" → gets number
 *
 * The target agent does NOT execute tools — it only reasons about the prompt
 * using its soul context and any state it has. For tool execution, use
 * inbox_send + wait for the agent's next cycle.
 *
 * Costs: one LLM call (~2K tokens). On Forge local inference, $0.00.
 */

interface DelegateResult {
  success: boolean;
  response: string;
  agentId: string;
  durationMs: number;
}

export async function delegateToAgent(
  callerAgentId: string,
  targetAgentId: string,
  prompt: string,
): Promise<DelegateResult> {
  const start = performance.now();

  // Load target agent
  const targetAgent = await agentRegistry.getAgent(targetAgentId);
  if (!targetAgent) {
    return { success: false, response: `ERROR: Agent '${targetAgentId}' not found`, agentId: targetAgentId, durationMs: 0 };
  }
  if (!targetAgent.is_active) {
    return { success: false, response: `ERROR: Agent '${targetAgentId}' is inactive`, agentId: targetAgentId, durationMs: 0 };
  }

  // Load target agent's soul package for context
  const soul = await soulLoader.loadSoulPackage(targetAgentId, 'active');
  const soulPrompt = soul ? soulLoader.assembleSoulPrompt(soul, 'active') : null;
  const systemPrompt = soulPrompt ?? targetAgent.system_prompt ?? '';

  // Load target agent's tools (for context in the prompt, not for execution)
  const tools = await toolCatalog.getToolsForAgent(targetAgentId);
  const toolList = tools.map(t => `- ${t.tool_id}: ${t.description}`).join('\n');

  // Load target agent's recent memories for context
  const memories = await agentRegistry.getAgentMemories(targetAgentId, { limit: 10 });
  const memoryContext = memories.length > 0
    ? '\n\nYour memories:\n' + memories.slice(0, 10).map(m => `- [${m.memory_type}] ${m.content}`).join('\n')
    : '';

  // Build the delegation prompt
  const delegationSystem = [
    systemPrompt,
    '',
    '## Available Tools (reference only — you cannot execute tools in this delegation)',
    toolList,
    '',
    '## Delegation Context',
    `You are being consulted by agent "${callerAgentId}". They need your expertise.`,
    'Respond with a clear, concise answer. Do NOT return the standard JSON action format.',
    'Just answer the question directly as text.',
    memoryContext,
  ].join('\n');

  // Get per-agent LLM config for the target agent
  let forgeUrl: string | undefined;
  let model: string | undefined;
  try {
    const config = await query<{ key: string; value: unknown }>(
      `SELECT key, value FROM agent_config WHERE agent_id = $1 AND key IN ('llm_url', 'llm_model')`,
      [targetAgentId],
    );
    for (const row of config.rows) {
      const val = typeof row.value === 'string' ? row.value : JSON.stringify(row.value).replace(/^"|"$/g, '');
      if (row.key === 'llm_url') forgeUrl = val;
      if (row.key === 'llm_model') model = val;
    }
  } catch { /* use defaults */ }

  logger.log(`Delegation: ${callerAgentId} → ${targetAgentId} | prompt: ${prompt.slice(0, 100)}...`);

  try {
    const response = await callLlm({
      systemPrompt: delegationSystem,
      userMessage: prompt,
      agentId: `${callerAgentId}-to-${targetAgentId}`,
      forgeUrl,
      model,
      maxTokens: 1000,
      temperature: 0.1,
    });

    const durationMs = Math.round(performance.now() - start);

    if (!response) {
      // LLM slots busy — fall back to async inbox message
      logger.log(`Delegation sync failed (LLM null), falling back to async inbox: ${callerAgentId} → ${targetAgentId}`);
      try {
        await comms.sendInbox({
          to_agent: targetAgentId,
          from_agent: callerAgentId,
          message: `[Delegation Request] ${prompt}`,
          priority: 1,
        });
        return {
          success: true,
          response: `LLM slots were busy — your question was sent to ${targetAgentId}'s inbox as an async delegation. They will respond on their next cycle.`,
          agentId: targetAgentId,
          durationMs,
        };
      } catch {
        return { success: false, response: 'ERROR: LLM returned null and async fallback also failed', agentId: targetAgentId, durationMs };
      }
    }

    // Extract the text response — delegation responses are free-text, not JSON actions
    const resp = response as Record<string, unknown>;
    const text = String(resp.summary ?? resp.response ?? JSON.stringify(response));

    logger.log(`Delegation complete: ${callerAgentId} → ${targetAgentId} | ${durationMs}ms`);

    return { success: true, response: text, agentId: targetAgentId, durationMs };
  } catch (err) {
    const durationMs = Math.round(performance.now() - start);
    const msg = (err as Error).message;
    logger.logMinimal(`Delegation failed: ${callerAgentId} → ${targetAgentId}: ${msg}`);
    return { success: false, response: `ERROR: Delegation failed: ${msg}`, agentId: targetAgentId, durationMs };
  }
}
