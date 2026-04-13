/**
 * memory-summarize handler
 *
 * Summarizes ARIA's 131K core_memory entries into concise agent_memory records.
 * Processes one category at a time, batching ~50 entries per LLM call.
 * Self-chains through categories until all are processed.
 *
 * Model: Qwen3.5-35B-A3B via Forge (standard port for bulk)
 * Source: core_memory WHERE superseded_by IS NULL
 * Target: agent_memory (agent_id='aria', source='summarized_from_core_memory')
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import OpenAI from 'openai';

const logger = createLogger('memory-summarize');

const BATCH_SIZE = 50;      // core_memory entries per LLM call
const CHUNK_SIZE = 200;     // entries per job execution (4 LLM calls)
const AGENT_ID = 'aria';

const CATEGORIES = ['context', 'people', 'preferences', 'recurring', 'communication',
  'career', 'events', 'interests', 'lifestyle', 'health', 'places'];

const SYSTEM_PROMPT = `You are summarizing a personal AI assistant's memory database into concise, useful memories.

Given a batch of key-value facts from one category, produce 3-8 concise memory entries that capture the most important information. Each memory should be a standalone fact that would be useful in future conversations.

Rules:
- Merge related facts into single memories (e.g., multiple facts about the same person → one memory)
- Drop trivial, outdated, or redundant information
- Keep names, dates, and specific details when they matter
- Each memory should be 1-2 sentences max
- Focus on what would actually be useful to know in a conversation

Reply with ONLY a JSON array of objects:
[{"content": "...", "category": "..."}]

The category should match the input category.`;

function getLLMClient(): OpenAI {
  // Use priority port (8088) to avoid being starved by bulk backfill jobs on 8080
  return new OpenAI({
    apiKey: 'not-needed',
    baseURL: process.env.FORGE_PRIORITY_URL || 'http://localhost:8088/v1',
    timeout: 60_000,
  });
}

interface Payload {
  category?: string;
  offset?: number;
}

export async function handleMemorySummarize(job: TempoJob): Promise<void> {
  const pool = getPool();
  const payload = (job.payload ?? {}) as Payload;
  const category = payload.category ?? CATEGORIES[0];
  const offset = payload.offset ?? 0;

  logger.log(`Starting: category=${category}, offset=${offset}`);

  // Check how many already summarized for this category
  const { rows: [{ count: alreadySummarized }] } = await pool.query<{ count: string }>(
    `SELECT count(*)::text as count FROM agent_memory
     WHERE agent_id = $1 AND source = 'summarized_from_core_memory' AND category = $2`,
    [AGENT_ID, category],
  );

  // Load batch of core_memory entries
  const { rows: entries } = await pool.query<{ key: string; value: string }>(
    `SELECT key, value FROM core_memory
     WHERE superseded_by IS NULL AND category = $1
     ORDER BY updated_at DESC
     OFFSET $2 LIMIT $3`,
    [category, offset, CHUNK_SIZE],
  );

  if (entries.length === 0) {
    // Move to next category
    const catIdx = CATEGORIES.indexOf(category);
    if (catIdx < CATEGORIES.length - 1) {
      const nextCategory = CATEGORIES[catIdx + 1];
      logger.log(`Category "${category}" complete (${alreadySummarized} memories). Moving to "${nextCategory}"`);
      await enqueueNext(pool, { category: nextCategory, offset: 0 });
    } else {
      logger.log(`All categories complete. Final category "${category}" has ${alreadySummarized} memories.`);

      // Log final stats
      const { rows: [{ total }] } = await pool.query<{ total: string }>(
        `SELECT count(*)::text as total FROM agent_memory
         WHERE agent_id = $1 AND source = 'summarized_from_core_memory'`,
        [AGENT_ID],
      );
      logger.log(`Total summarized memories: ${total}`);
    }
    return;
  }

  logger.log(`Processing ${entries.length} entries from "${category}" (offset ${offset})`);

  const llm = getLLMClient();
  let totalWritten = 0;
  let totalErrors = 0;

  // Process in batches of BATCH_SIZE
  for (let i = 0; i < entries.length; i += BATCH_SIZE) {
    const batch = entries.slice(i, i + BATCH_SIZE);
    const batchText = batch.map((e) => `- ${e.key}: ${e.value}`).join('\n');

    try {
      const response = await llm.chat.completions.create({
        model: 'qwen3.5-35b-a3b',
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: `Category: ${category}\n\n${batchText}` },
        ],
        temperature: 0.1,
        max_tokens: 2000,
        // @ts-expect-error — Forge-specific parameter to disable Qwen3 thinking
        chat_template_kwargs: { enable_thinking: false },
      });

      const text = response.choices[0]?.message?.content?.trim() ?? '';

      // Parse JSON array from response
      const jsonMatch = text.match(/\[[\s\S]*\]/);
      if (!jsonMatch) {
        logger.logVerbose(`No JSON array in response for batch ${i}/${entries.length}`);
        totalErrors++;
        continue;
      }

      const memories = JSON.parse(jsonMatch[0]) as Array<{ content: string; category: string }>;

      for (const mem of memories) {
        if (!mem.content || mem.content.length < 5) continue;

        await pool.query(
          `INSERT INTO agent_memory (agent_id, memory_type, category, content, confidence, source)
           VALUES ($1, 'summarized', $2, $3, 0.9, 'summarized_from_core_memory')`,
          [AGENT_ID, mem.category || category, mem.content],
        );
        totalWritten++;
      }

      logger.logVerbose(`Batch ${i}-${i + batch.length}: ${memories.length} memories extracted`);
    } catch (err) {
      logger.logMinimal(`LLM error at batch ${i}:`, (err as Error).message);
      totalErrors++;
    }
  }

  logger.log(`Chunk complete: ${totalWritten} written, ${totalErrors} errors, offset ${offset}→${offset + entries.length}`);

  // Self-chain for next chunk
  await enqueueNext(pool, { category, offset: offset + CHUNK_SIZE });
}

async function enqueueNext(pool: ReturnType<typeof getPool>, payload: Payload): Promise<void> {
  await pool.query(
    `INSERT INTO tempo_jobs (job_type, payload, executor) VALUES ('memory-summarize', $1, 'nexus')`,
    [JSON.stringify(payload)],
  );
}
