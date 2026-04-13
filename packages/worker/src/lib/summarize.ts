import { createLogger } from '@nexus/core';
import { callLlm } from './llm-router.js';

const logger = createLogger('summarize');

/**
 * Auto-summarization for agent state messages.
 *
 * When the state message (inbox + tasks + memories + working memory) exceeds
 * a token budget, lower-priority sections are compressed before sending to
 * the LLM. This prevents context window overflow as agents accumulate state
 * over weeks/months of operation.
 *
 * Priority order (highest = kept full, lowest = summarized first):
 * 1. Approved actions (must execute exactly)
 * 2. Working memory (current investigation context)
 * 3. Unread inbox (immediate inputs)
 * 4. Open tasks (assigned work)
 * 5. Memories (long-term knowledge) ← summarized first
 *
 * The summarizer uses a lightweight LLM call to compress memories into a
 * concise digest. This costs one extra LLM call per cycle when the budget
 * is exceeded — rare in practice (only when memory count is very high).
 */

// Rough token estimate: ~4 chars per token for English
const CHARS_PER_TOKEN = 4;

// Default token budget for the state message (not counting system prompt)
const DEFAULT_STATE_TOKEN_BUDGET = 4000;

export interface SummarizeOptions {
  /** Max tokens for the state message. Default 4000. */
  tokenBudget?: number;
  /** Agent ID for LLM call logging. */
  agentId?: string;
  /** Override Forge URL for the summarization call. */
  forgeUrl?: string;
}

/**
 * Estimate token count from a string (rough, ~4 chars per token).
 */
export function estimateTokens(text: string): number {
  return Math.ceil(text.length / CHARS_PER_TOKEN);
}

/**
 * Summarize a state message if it exceeds the token budget.
 *
 * Identifies the "## Memories" section and compresses it if the total
 * message is over budget. Other sections are left intact.
 *
 * Returns the original message if within budget, or a compressed version
 * with memories summarized into a digest.
 */
export async function summarizeStateIfNeeded(
  stateMessage: string,
  opts: SummarizeOptions = {},
): Promise<string> {
  const budget = opts.tokenBudget ?? DEFAULT_STATE_TOKEN_BUDGET;
  const currentTokens = estimateTokens(stateMessage);

  if (currentTokens <= budget) {
    return stateMessage;
  }

  logger.logVerbose(
    'State message exceeds budget:',
    currentTokens, 'tokens vs', budget, 'budget. Summarizing memories.',
  );

  // Find the memories section
  const memoriesStart = stateMessage.indexOf('## Memories');
  if (memoriesStart === -1) {
    // No memories section to compress — return as-is
    logger.logVerbose('No memories section found, cannot summarize further');
    return stateMessage;
  }

  // Find the end of the memories section (next ## heading or end of string)
  const afterMemories = stateMessage.indexOf('\n## ', memoriesStart + 11);
  const memoriesEnd = afterMemories === -1 ? stateMessage.length : afterMemories;
  const memoriesSection = stateMessage.slice(memoriesStart, memoriesEnd);
  const memoriesTokens = estimateTokens(memoriesSection);

  // How much do we need to save?
  const overage = currentTokens - budget;
  if (memoriesTokens <= overage) {
    // Memories section alone can't save enough — drop it entirely
    logger.logVerbose('Dropping memories section entirely to fit budget');
    const before = stateMessage.slice(0, memoriesStart);
    const after = stateMessage.slice(memoriesEnd);
    return before + '## Memories\n(Memories omitted to fit context budget. Use query_db on agent_memory to recall specific knowledge.)\n' + after;
  }

  // Summarize memories via LLM
  const digest = await compressMemories(memoriesSection, overage, opts);
  if (!digest) {
    // Summarization failed — truncate instead
    logger.logVerbose('Summarization failed, truncating memories');
    const keepChars = (memoriesTokens - overage) * CHARS_PER_TOKEN;
    const truncated = memoriesSection.slice(0, keepChars) + '\n  ... (truncated to fit context budget)';
    return stateMessage.slice(0, memoriesStart) + truncated + stateMessage.slice(memoriesEnd);
  }

  const before = stateMessage.slice(0, memoriesStart);
  const after = stateMessage.slice(memoriesEnd);
  return before + digest + after;
}

/**
 * Compress a memories section using a lightweight LLM call.
 */
async function compressMemories(
  memoriesSection: string,
  targetSavingTokens: number,
  opts: SummarizeOptions,
): Promise<string | null> {
  const targetChars = Math.max(200, (estimateTokens(memoriesSection) - targetSavingTokens) * CHARS_PER_TOKEN);

  try {
    const response = await callLlm({
      systemPrompt: `You are a memory compression assistant. Condense the following agent memories into a brief digest that preserves the most important information. Target length: approximately ${targetChars} characters. Keep key facts, baselines, and recurring patterns. Drop one-off observations and redundant entries. Output the digest as a markdown section starting with "## Memories (compressed)".`,
      userMessage: memoriesSection,
      agentId: opts.agentId ?? 'summarizer',
      forgeUrl: opts.forgeUrl,
      maxTokens: Math.ceil(targetChars / CHARS_PER_TOKEN) + 100,
      temperature: 0.1,
    });

    if (!response) return null;

    const resp = response as Record<string, unknown>;
    const text = String(resp.summary ?? resp.response ?? JSON.stringify(response));
    if (!text || text.length < 50) return null;

    // If the response is JSON (from the standard response format), extract usable content
    if (text.startsWith('{')) {
      try {
        const parsed = JSON.parse(text) as Record<string, unknown>;
        return `## Memories (compressed)\n${String(parsed.summary ?? text)}`;
      } catch { /* not JSON, use as-is */ }
    }

    logger.logVerbose('Memories compressed:', estimateTokens(memoriesSection), '→', estimateTokens(text), 'tokens');
    return text.startsWith('## Memories') ? text : `## Memories (compressed)\n${text}`;
  } catch (err) {
    logger.logMinimal('Memory compression failed:', (err as Error).message);
    return null;
  }
}

/**
 * Consolidate agent memories by merging similar/redundant entries.
 *
 * This is the framework-assisted version of what the Improver agent does
 * manually. It identifies clusters of related memories and merges them
 * into consolidated entries with higher confidence.
 *
 * Designed to be called periodically (e.g., by the Analyst agent or
 * as a scheduled maintenance task), not every cycle.
 */
export async function consolidateMemories(
  agentId: string,
  opts: { maxToConsolidate?: number; forgeUrl?: string } = {},
): Promise<{ consolidated: number; removed: number }> {
  const { maxToConsolidate = 50, forgeUrl } = opts;
  const { query: dbQuery } = await import('@nexus/core');

  // Load all active memories for this agent
  const result = await dbQuery<{
    id: number; memory_type: string; category: string | null;
    content: string; confidence: number; times_reinforced: number;
  }>(
    `SELECT id, memory_type, category, content, confidence, times_reinforced
     FROM agent_memory
     WHERE agent_id = $1 AND is_active = true
     ORDER BY confidence DESC, times_reinforced DESC
     LIMIT $2`,
    [agentId, maxToConsolidate],
  );

  if (result.rows.length < 10) {
    // Not enough memories to warrant consolidation
    return { consolidated: 0, removed: 0 };
  }

  const memories = result.rows;
  const memoryList = memories.map((m, i) => `[${i}] (${m.memory_type}/${m.category ?? 'general'}) ${m.content}`).join('\n');

  // Ask LLM to identify clusters and suggest consolidations
  const response = await callLlm({
    systemPrompt: `You are a memory consolidation assistant. Given a list of agent memories, identify groups of related/redundant entries that should be merged into single consolidated memories.

For each group, output a JSON array of consolidation actions:
[
  {
    "keep_ids": [0, 3, 7],
    "merged_content": "The consolidated memory text",
    "category": "category_name"
  }
]

Rules:
- Only merge memories that are genuinely about the same topic/pattern
- The merged content should preserve all key facts from the originals
- Do not merge unrelated memories just to reduce count
- If no consolidation is needed, return an empty array: []
- Maximum 5 consolidation groups per call`,
    userMessage: `Agent: ${agentId}\n\nMemories:\n${memoryList}`,
    agentId: `consolidator-${agentId}`,
    forgeUrl,
    maxTokens: 1500,
    temperature: 0.1,
  });

  if (!response) return { consolidated: 0, removed: 0 };

  // Parse consolidation actions
  try {
    const resp = response as Record<string, unknown>;
    const text = String(resp.summary ?? JSON.stringify(response));
    const jsonMatch = text.match(/\[[\s\S]*\]/);
    if (!jsonMatch) return { consolidated: 0, removed: 0 };

    const actions = JSON.parse(jsonMatch[0]) as Array<{
      keep_ids: number[];
      merged_content: string;
      category?: string;
    }>;

    let consolidated = 0;
    let removed = 0;

    for (const action of actions) {
      if (!action.keep_ids?.length || !action.merged_content) continue;

      const memoryIds = action.keep_ids.map(i => memories[i]?.id).filter(Boolean);
      if (memoryIds.length < 2) continue;

      // Insert consolidated memory
      const maxConfidence = Math.max(...action.keep_ids.map(i => memories[i]?.confidence ?? 0.5));
      const totalReinforcements = action.keep_ids.reduce((sum, i) => sum + (memories[i]?.times_reinforced ?? 0), 0);

      await dbQuery(
        `INSERT INTO agent_memory (agent_id, memory_type, category, content, confidence, times_reinforced, source)
         VALUES ($1, 'consolidated', $2, $3, $4, $5, 'auto-consolidation')`,
        [agentId, action.category ?? 'general', action.merged_content, maxConfidence, totalReinforcements],
      );

      // Mark originals as superseded
      await dbQuery(
        `UPDATE agent_memory SET is_active = false WHERE id = ANY($1::int[])`,
        [memoryIds],
      );

      consolidated++;
      removed += memoryIds.length;
    }

    if (consolidated > 0) {
      logger.log(`Memory consolidation for ${agentId}: ${consolidated} groups, ${removed} memories merged`);
    }

    return { consolidated, removed };
  } catch (err) {
    logger.logMinimal('Memory consolidation parse error:', (err as Error).message);
    return { consolidated: 0, removed: 0 };
  }
}
