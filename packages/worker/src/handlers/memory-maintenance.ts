/**
 * Memory maintenance handler.
 *
 * Runs daily to review all active memories in core_memory, looking for
 * opportunities to consolidate duplicates, improve clarity, and streamline
 * wording -- while always erring on the side of remembering MORE, not less.
 *
 * Uses Claude to analyze the full memory set and produce targeted edits.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';
import { ingestFacts, type FactInput } from '../lib/knowledge.js';

const logger = createLogger('memory-maintenance');

interface MemoryRow {
  id: string;
  category: string;
  key: string;
  value: string;
  version: number;
}

interface MemoryAction {
  action: 'update' | 'merge';
  /** For 'update': the id of the memory to update. For 'merge': the ids being merged. */
  ids: string[];
  category: string;
  key: string;
  value: string;
  reason: string;
}

interface MemoryInsight {
  key: string;
  category: string;
  issue: 'stale' | 'enrichable' | 'contradictory' | 'low_value';
  details: string;
}

interface MaintenanceResult {
  analyzed: number;
  updated: number;
  merged: number;
  skipped: number;
  insights_logged: number;
  pkg_promoted: number;
  actions: Array<{ action: string; key: string; reason: string }>;
}

// LLM client management moved to the router (src/lib/llm/)

const SYSTEM_PROMPT = `You are a memory maintenance assistant for an AI companion named ARIA. Your job is to review ARIA's stored memories and suggest improvements that make them more efficient and useful, while preserving ALL important details.

ARIA's memories are stored as category/key/value triples. Categories are: people, preferences, goals, recurring, context, other.

Your goals:
1. **Merge duplicates**: If two memories contain overlapping information about the same topic, merge them into one comprehensive entry. Always keep the richer key name.
2. **Improve clarity**: Reword values to be clearer and more concise while retaining ALL factual details. A shorter phrasing that captures the same information is better.
3. **Fix categorization**: If a memory is in the wrong category, suggest moving it.
4. **Standardize format**: Make values consistent in tone and structure across the memory set.

CRITICAL RULES:
- NEVER suggest deleting a memory unless it is truly 100% redundant (every detail exists in another memory).
- When merging, the merged result MUST contain ALL details from BOTH original memories.
- Prefer remembering MORE over remembering less. When in doubt, keep the detail.
- Do NOT add information that isn't already present -- only reorganize and reword.
- Small improvements matter. Even slightly clearer wording is worth suggesting.
- If the memories are already well-organized and clear, return an empty actions array.

Additionally, for each memory:
- Score its usefulness (1-5) based on specificity, actionability, and recency
- Flag any stale memories (reference past events, old jobs, outdated info) with reason
- Flag memories that could be enriched with more context
- Identify any contradictory pairs of memories

Respond with a JSON object (no markdown fences) with this exact structure:
{
  "analysis": "Brief overall assessment of memory health",
  "actions": [
    {
      "action": "update",
      "ids": ["<id of memory to update>"],
      "category": "<category>",
      "key": "<key>",
      "value": "<improved value>",
      "reason": "Why this change improves the memory"
    },
    {
      "action": "merge",
      "ids": ["<id1>", "<id2>"],
      "category": "<category>",
      "key": "<key for merged memory>",
      "value": "<merged value containing ALL details from both>",
      "reason": "Why these should be merged"
    }
  ],
  "memory_insights": [
    { "key": "memory_key", "category": "category", "issue": "stale|enrichable|contradictory|low_value", "details": "explanation" }
  ]
}`;

/**
 * Fetch all active (non-superseded) memories.
 * Ordered by updated_at ASC so oldest (most likely to need maintenance) come first.
 * Cooldown is dynamic: 7 days normally, but 24 hours if total memories < 50
 * (avoids cold-start problem on young platforms where everything is recent).
 */
async function fetchActiveMemories(force?: boolean): Promise<MemoryRow[]> {
  const pool = getPool();

  // If force mode, skip the cooldown entirely
  if (force) {
    const { rows } = await pool.query<MemoryRow>(
      `SELECT id, category, key, value, version
       FROM core_memory
       WHERE superseded_by IS NULL
       ORDER BY updated_at ASC`
    );
    logger.log(`Force mode: returning all ${rows.length} active memories`);
    return rows;
  }

  // Count total active memories to determine cooldown
  const { rows: countRows } = await pool.query<{ count: string }>(
    `SELECT COUNT(*) as count FROM core_memory WHERE superseded_by IS NULL`
  );
  const totalActive = parseInt(countRows[0].count, 10);

  // Dynamic cooldown: 24h for young platforms (<50 memories), 7 days otherwise
  const cooldownInterval = totalActive < 50 ? '24 hours' : '7 days';

  const { rows } = await pool.query<MemoryRow>(
    `SELECT id, category, key, value, version
     FROM core_memory
     WHERE superseded_by IS NULL
       AND updated_at < NOW() - INTERVAL '${cooldownInterval}'
     ORDER BY updated_at ASC`
  );

  if (rows.length === 0 && totalActive > 0) {
    logger.logMinimal(`0 memories eligible (cooldown: ${cooldownInterval}) but ${totalActive} total active. All were recently updated.`);
  }

  return rows;
}

/**
 * Format memories into a readable block for the LLM.
 */
function formatMemoriesForAnalysis(memories: MemoryRow[]): string {
  const lines: string[] = [];
  let currentCategory = '';
  for (const m of memories) {
    if (m.category !== currentCategory) {
      if (currentCategory) lines.push('');
      lines.push(`[${m.category.toUpperCase()}]`);
      currentCategory = m.category;
    }
    lines.push(`  id=${m.id} | key="${m.key}" | value="${m.value}"`);
  }
  return lines.join('\n');
}

/**
 * Ask Claude to analyze memories and suggest improvements.
 */
async function analyzeMemories(memories: MemoryRow[]): Promise<{ actions: MemoryAction[]; insights: MemoryInsight[] }> {
  const formatted = formatMemoriesForAnalysis(memories);

  const result = await routeRequest({
    handler: 'memory-maintenance',
    taskTier: 'reasoning',
    systemPrompt: SYSTEM_PROMPT,
    userMessage: `Here are all ${memories.length} active memories. Review them and suggest improvements:\n\n${formatted}`,
    maxTokens: 6144,
    useBatch: true,
  });
  logger.log(`Analyzed via ${result.model} (${result.provider}, ${result.estimatedCostCents}¢)`);

  let text = result.text.trim();
  // Strip markdown fences if present
  if (text.startsWith('```')) {
    text = text.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
  }

  try {
    const parsed = JSON.parse(text);
    if (parsed.analysis) {
      logger.log(`Analysis: ${parsed.analysis}`);
    }
    const actions: MemoryAction[] = Array.isArray(parsed.actions) ? parsed.actions : [];
    const insights: MemoryInsight[] = Array.isArray(parsed.memory_insights) ? parsed.memory_insights : [];
    return { actions, insights };
  } catch (err) {
    logger.logMinimal('Failed to parse LLM response:', text.slice(0, 500));
    throw new Error('Failed to parse memory analysis response as JSON');
  }
}

/**
 * Apply a single memory action (update or merge) using the same versioning
 * scheme as ARIA's upsertMemory: insert new row, mark old row(s) as superseded.
 */
async function applyAction(action: MemoryAction): Promise<void> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    if (action.action === 'update' && action.ids.length === 1) {
      // Fetch the existing row to get its version
      const { rows: existing } = await client.query<MemoryRow>(
        `SELECT id, version FROM core_memory WHERE id = $1 AND superseded_by IS NULL`,
        [action.ids[0]]
      );

      if (existing.length === 0) {
        await client.query('ROLLBACK');
        logger.logMinimal(`Skipping update: memory ${action.ids[0]} not found or already superseded`);
        return;
      }

      // Insert updated version
      const { rows: newRows } = await client.query(
        `INSERT INTO core_memory (category, key, value, version)
         VALUES ($1, $2, $3, $4)
         RETURNING id`,
        [action.category, action.key, action.value, existing[0].version + 1]
      );

      // Mark old row as superseded
      await client.query(
        `UPDATE core_memory SET superseded_by = $1 WHERE id = $2`,
        [newRows[0].id, action.ids[0]]
      );
    } else if (action.action === 'merge' && action.ids.length >= 2) {
      // Verify all source memories still exist and are active
      const placeholders = action.ids.map((_, i) => `$${i + 1}`).join(', ');
      const { rows: existing } = await client.query<MemoryRow>(
        `SELECT id, version FROM core_memory WHERE id IN (${placeholders}) AND superseded_by IS NULL`,
        action.ids
      );

      if (existing.length < 2) {
        await client.query('ROLLBACK');
        logger.logMinimal(`Skipping merge: not enough active memories found (${existing.length}/${action.ids.length})`);
        return;
      }

      const maxVersion = Math.max(...existing.map((r) => r.version));

      // Insert merged memory
      const { rows: newRows } = await client.query(
        `INSERT INTO core_memory (category, key, value, version)
         VALUES ($1, $2, $3, $4)
         RETURNING id`,
        [action.category, action.key, action.value, maxVersion + 1]
      );

      // Mark all source memories as superseded by the merged one
      for (const row of existing) {
        await client.query(
          `UPDATE core_memory SET superseded_by = $1 WHERE id = $2`,
          [newRows[0].id, row.id]
        );
      }
    } else {
      await client.query('ROLLBACK');
      logger.logMinimal(`Skipping unknown action: ${action.action}`);
      return;
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Split an array into chunks of a given size.
 */
function chunkArray<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

/**
 * Main handler: fetch memories, analyze in chunks of 50, apply improvements, log results.
 */
export async function handleMemoryMaintenance(job: TempoJob): Promise<Record<string, unknown>> {
  logger.log(`Starting memory maintenance job ${job.id}`);

  // Support force mode via payload
  const force = !!(job.payload as Record<string, unknown>)?.force;
  if (force) logger.log(`Force mode enabled — bypassing cooldown`);

  // 1. Fetch all active memories (oldest first, skipping recently touched)
  const memories = await fetchActiveMemories(force);
  logger.log(`Found ${memories.length} active memories eligible for maintenance`);

  if (memories.length === 0) {
    return { analyzed: 0, updated: 0, merged: 0, skipped: 0, actions: [] };
  }

  // 2. Chunk memories into groups of 50 and analyze each chunk separately
  const CHUNK_SIZE = 50;
  const chunks = chunkArray(memories, CHUNK_SIZE);
  const allActions: MemoryAction[] = [];
  const allInsights: MemoryInsight[] = [];

  for (let i = 0; i < chunks.length; i++) {
    logger.log(`Analyzing chunk ${i + 1}/${chunks.length} (${chunks[i].length} memories)`);
    const { actions: chunkActions, insights: chunkInsights } = await analyzeMemories(chunks[i]);
    allActions.push(...chunkActions);
    allInsights.push(...chunkInsights);
  }

  const actions = allActions;
  const insights = allInsights;
  logger.log(`Total: ${actions.length} action(s), ${insights.length} insight(s) across ${chunks.length} chunk(s)`);

  // 3. Apply each action
  const result: MaintenanceResult = {
    analyzed: memories.length,
    updated: 0,
    merged: 0,
    skipped: 0,
    insights_logged: 0,
    pkg_promoted: 0,
    actions: [],
  };

  if (actions.length === 0 && insights.length === 0) {
    return result as unknown as Record<string, unknown>;
  }

  for (const action of actions) {
    try {
      // Basic validation
      if (!action.category || !action.key || !action.value || !action.ids?.length) {
        logger.logMinimal(`Skipping invalid action:`, JSON.stringify(action).slice(0, 200));
        result.skipped++;
        continue;
      }

      // Value length check
      if (action.value.length > 2000) {
        logger.logMinimal(`Skipping action: value too long (${action.value.length} chars)`);
        result.skipped++;
        continue;
      }

      await applyAction(action);

      if (action.action === 'update') result.updated++;
      else if (action.action === 'merge') result.merged++;

      result.actions.push({
        action: action.action,
        key: action.key,
        reason: action.reason || 'No reason provided',
      });

      logger.log(`Applied ${action.action}: ${action.key} -- ${action.reason}`);
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal(`Failed to apply action for "${action.key}": ${msg}`);
      result.skipped++;
    }
  }

  // 4. Promote confirmed/consolidated facts to the Personal Knowledge Graph
  const DOMAIN_MAP: Record<string, string> = {
    people: 'people',
    preferences: 'preferences',
    context: 'events',
    recurring: 'lifestyle',
    goals: 'interests',
    other: 'interests',
  };

  const appliedActions = actions.filter(
    (a) => a.category && a.key && a.value && a.ids?.length
  );

  // Only promote actions that were actually applied (not skipped)
  const successfulActions = appliedActions.filter((a) =>
    result.actions.some((r) => r.key === a.key && r.action === a.action)
  );

  if (successfulActions.length > 0) {
    const pkgFacts: FactInput[] = successfulActions.map((a) => ({
      domain: DOMAIN_MAP[a.category] ?? 'interests',
      category: a.category,
      key: a.key,
      value: a.value,
      confidence: 0.7,
      source: 'memory_maintenance',
    }));

    try {
      const { written } = await ingestFacts(pkgFacts);
      result.pkg_promoted = written;
      if (written > 0) {
        logger.log(`Promoted ${written} consolidated fact(s) to PKG`);
      }
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal(`PKG promotion failed: ${msg}`);
    }
  }

  // 5a. Log memory insights to aria_self_improvement
  if (insights.length > 0) {
    const pool2 = getPool();
    const CONFIDENCE_MAP: Record<string, number> = {
      stale: 0.8,
      contradictory: 0.8,
      low_value: 0.7,
      enrichable: 0.6,
    };
    for (const insight of insights) {
      try {
        if (!insight.key || !insight.issue || !insight.details) continue;

        const title = `Memory ${insight.issue}: ${insight.key}`;
        const confidence = CONFIDENCE_MAP[insight.issue] ?? 0.6;

        await pool2.query(
          `INSERT INTO aria_self_improvement (category, status, title, description, source, confidence)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          ['memory_insight', 'idea', title, insight.details, 'memory_maintenance', confidence]
        );
        result.insights_logged++;
      } catch (err) {
        const msg = (err as Error).message;
        logger.logMinimal(`Failed to log insight for "${insight.key}": ${msg}`);
      }
    }
    if (result.insights_logged > 0) {
      logger.log(`Logged ${result.insights_logged} memory insight(s) to self-improvement journal`);
    }
  }

  // 6. Log to actions_log
  const pool = getPool();
  await pool.query(
    `INSERT INTO actions_log (action_type, description, metadata)
     VALUES ($1, $2, $3)`,
    [
      'memory_maintenance',
      `Daily memory maintenance: analyzed ${result.analyzed} memories, updated ${result.updated}, merged ${result.merged}, skipped ${result.skipped}`,
      JSON.stringify(result),
    ]
  );

  logger.log(
    `Complete: ${result.updated} updated, ${result.merged} merged, ${result.skipped} skipped`
  );

  logEvent({
    action: `Memory maintenance: analyzed ${result.analyzed}, updated ${result.updated}, merged ${result.merged}`,
    component: 'memory',
    category: 'self_maintenance',
    metadata: { analyzed: result.analyzed, updated: result.updated, merged: result.merged, skipped: result.skipped },
  });

  return result as unknown as Record<string, unknown>;
}
