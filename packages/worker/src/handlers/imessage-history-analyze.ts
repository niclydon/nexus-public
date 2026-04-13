/**
 * iMessage History Analyze handler.
 *
 * Processes incoming iMessages and extracts meaningful personal information
 * for ARIA's core memory system. Uses a watermark to track progress through
 * the imessage_incoming table, processing messages in batches.
 *
 * Extracted facts are written to core_memory as category/key/value triples.
 * If more messages remain, auto-enqueues another job to continue.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { jobLog } from '../lib/job-log.js';
import { routeRequest } from '../lib/llm/index.js';
import { ingestFacts, type FactInput } from '../lib/knowledge.js';

const logger = createLogger('imessage-analyze');

const WATERMARK_SOURCE = 'imessage_analysis';
const DEFAULT_BATCH_SIZE = 50;

interface IncomingMessage {
  id: number;
  sender: string;
  sender_name: string | null;
  contact_name: string | null;
  message: string;
  chat_id: string | null;
  is_group: boolean;
  is_from_me: boolean;
  group_name: string | null;
  received_at: Date;
}

interface MemoryExtraction {
  category: 'people' | 'preferences' | 'context' | 'recurring';
  key: string;
  value: string;
  when?: string;
}

interface AnalysisResult {
  extractions: MemoryExtraction[];
  skipped_reason?: string;
}

const SYSTEM_PROMPT = `You are ARIA, a personal AI assistant analyzing iMessages from your owner the owner's phone. Your task is to extract meaningful personal information that will help you be a better assistant.

You are analyzing real text messages to and from the owner. The messages span many years (some from 10+ years ago, some from recently). Pay close attention to the dates — they provide critical context about WHEN things happened.

Extract information that falls into these categories:

1. **people** -- Facts about specific people the owner communicates with (relationships, interests, jobs, family, important details)
2. **preferences** -- the owner's preferences, interests, opinions, likes, dislikes
3. **context** -- Life events, plans, history, situations
4. **recurring** -- Recurring events, habits, routines, patterns

CRITICAL — IDENTITY DISAMBIGUATION:
- the owner is the OWNER of this phone. Messages are to/from the owner.
- Carefully distinguish WHO a fact is about. If someone else describes THEIR medical issue, job change, or experience, that is a fact about THEM, not about the owner.
- When the owner is telling someone else about a third party's situation, attribute it to the correct person.
- Example: If the owner texts "my mom is in the hospital for a JP drain" → the medical fact is about the owner's MOTHER, not the owner.
- Example: If the owner texts [Person A] "I need to keep an eye on my blood pressure" → the health concern is about the owner, not [Person A].
- Example: If [Person A] texts "I just got a new job at Google" → this is about [Person A], not the owner.
- Always make the subject of each fact explicit in the value (e.g. "the owner's mother had a JP drain procedure" NOT "Had a JP drain procedure").

RULES:
- Focus on SUBSTANTIVE information. Skip trivial logistics ("ok", "sounds good", "on my way", "lol", "thanks").
- For people facts, use the person's actual name as part of the key (e.g. "alex_job", "jordan_birthday"). If no name is available, use the phone number.
- Keys should be descriptive and specific (e.g. "alex_job" not just "job").
- Values should be concise but complete statements of fact.
- ALWAYS include temporal context in your values when relevant. Instead of "[Person] works at Google", write "As of March 2024, [Person] works at Google". For past events, write "In June 2016, the owner went to Italy with [Person]".
- Include the optional "when" field with an approximate date or date range (e.g. "2024-03", "2016-06", "2015-2017") so ARIA knows how recent the information is.
- Understand that older facts may no longer be current — a job someone had in 2015 may not be their current job. Frame older facts accordingly (e.g. "As of 2015, [Person] worked at [Company]").
- Do NOT fabricate or infer information that isn't clearly stated or strongly implied.
- Do NOT extract information about the owner's location from timestamps alone.
- Do NOT attribute other people's medical conditions, jobs, experiences, or situations to the owner.
- If the messages are purely logistical with no extractable personal info, return an empty extractions array.
- Prefer fewer, high-quality extractions over many low-quality ones.

Respond with a JSON object (no markdown fences):
{
  "extractions": [
    { "category": "people", "key": "alex_job_2024", "value": "As of March 2024, Alex works as a software engineer at Google", "when": "2024-03" },
    { "category": "context", "key": "italy_trip_2016", "value": "In June 2016, the owner traveled to Italy with Jordan", "when": "2016-06" },
    { "category": "preferences", "key": "coffee_preference", "value": "Prefers oat milk lattes" }
  ],
  "skipped_reason": "Optional: why no extractions were made if array is empty"
}`;

/**
 * Get the current watermark (last analyzed message ID).
 */
async function getWatermark(): Promise<number> {
  const pool = getPool();
  try {
    const { rows } = await pool.query<{ metadata: Record<string, unknown> }>(
      `SELECT metadata FROM context_watermarks WHERE source = $1`,
      [WATERMARK_SOURCE]
    );
    if (rows.length > 0 && rows[0].metadata?.last_id) {
      return Number(rows[0].metadata.last_id) || 0;
    }
    return 0;
  } catch {
    return 0;
  }
}

/**
 * Update the watermark to track progress.
 */
async function updateWatermark(lastId: number, messagesProcessed: number): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO context_watermarks (source, last_checked_at, last_change_detected_at, metadata)
     VALUES ($1, NOW(), NOW(), $2)
     ON CONFLICT (source) DO UPDATE SET
       last_checked_at = NOW(),
       last_change_detected_at = NOW(),
       metadata = $2`,
    [WATERMARK_SOURCE, JSON.stringify({ last_id: lastId, messages_processed: messagesProcessed })]
  );
}

/**
 * Fetch unprocessed messages from imessage_incoming.
 */
async function fetchMessages(afterId: number, batchSize: number, senderFilter?: string): Promise<IncomingMessage[]> {
  const pool = getPool();
  let query = `
    SELECT id, sender, sender_name, contact_name, message, chat_id, is_group, is_from_me, group_name, received_at
    FROM imessage_incoming
    WHERE id > $1
  `;
  const params: unknown[] = [afterId];

  if (senderFilter) {
    query += ` AND sender = $2`;
    params.push(senderFilter);
  }

  query += ` ORDER BY id ASC LIMIT $${params.length + 1}`;
  params.push(batchSize);

  const { rows } = await pool.query<IncomingMessage>(query, params);
  return rows;
}

/**
 * Group messages by conversation (sender for 1:1, chat_id for groups).
 */
function groupMessages(messages: IncomingMessage[]): Map<string, IncomingMessage[]> {
  const groups = new Map<string, IncomingMessage[]>();

  for (const msg of messages) {
    const key = msg.is_group && msg.chat_id
      ? `group:${msg.chat_id}`
      : `dm:${msg.sender}`;

    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key)!.push(msg);
  }

  return groups;
}

/**
 * Format a conversation group for analysis.
 */
function formatConversation(groupKey: string, messages: IncomingMessage[]): string {
  const isGroup = groupKey.startsWith('group:');
  const resolvedName = (msg: IncomingMessage) => msg.contact_name || msg.sender_name || msg.sender;

  // Include date range so LLM knows temporal context
  const firstDate = new Date(messages[0].received_at);
  const lastDate = new Date(messages[messages.length - 1].received_at);
  const formatDate = (d: Date) => d.toLocaleDateString('en-US', { timeZone: 'America/New_York', year: 'numeric', month: 'long', day: 'numeric' });
  const dateRange = formatDate(firstDate) === formatDate(lastDate)
    ? formatDate(firstDate)
    : `${formatDate(firstDate)} to ${formatDate(lastDate)}`;

  const contactLabel = isGroup
    ? `Group chat: ${messages[0].group_name || groupKey}`
    : `Conversation with: ${resolvedName(messages[0])}`;
  const header = `${contactLabel}\nDate range: ${dateRange}`;

  const lines = [header, '---'];
  for (const msg of messages) {
    const name = msg.is_from_me ? 'the owner' : resolvedName(msg);
    const time = new Date(msg.received_at).toLocaleString('en-US', { timeZone: 'America/New_York' });
    lines.push(`[${time}] ${name}: ${msg.message}`);
  }

  return lines.join('\n');
}

/**
 * Analyze a conversation group and extract memory facts.
 */
async function analyzeConversation(groupKey: string, messages: IncomingMessage[]): Promise<MemoryExtraction[]> {
  const formatted = formatConversation(groupKey, messages);

  // Skip very short conversations that are likely trivial
  const totalChars = messages.reduce((sum, m) => sum + m.message.length, 0);
  if (totalChars < 20) {
    logger.log(`Skipping ${groupKey}: too short (${totalChars} chars)`);
    return [];
  }

  try {
    const result = await routeRequest({
      handler: 'imessage-history-analyze',
      taskTier: 'generation',
      systemPrompt: SYSTEM_PROMPT,
      userMessage: `Analyze these iMessages and extract any meaningful personal information:\n\n${formatted}`,
      maxTokens: 4096,
    });

    logger.log(`Analyzed ${groupKey} via ${result.model} (${result.provider}, ${result.estimatedCostCents}c)`);

    // Strip markdown fences if present
    let text = result.text.trim();
    if (text.startsWith('```')) {
      text = text.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
    }

    const parsed: AnalysisResult = JSON.parse(text);

    if (parsed.skipped_reason) {
      logger.log(`Skipped ${groupKey}: ${parsed.skipped_reason}`);
    }

    return Array.isArray(parsed.extractions) ? parsed.extractions : [];
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal(`Failed to analyze ${groupKey}: ${msg}`);
    return [];
  }
}

/**
 * Write extracted facts to core_memory with upsert semantics.
 */
async function writeMemoryFacts(extractions: MemoryExtraction[]): Promise<number> {
  const pool = getPool();
  let written = 0;

  for (const ext of extractions) {
    // Validate category
    if (!['people', 'preferences', 'context', 'recurring'].includes(ext.category)) {
      logger.logMinimal(`Skipping invalid category: ${ext.category}`);
      continue;
    }
    if (!ext.key || !ext.value || ext.key.length > 100 || ext.value.length > 2000) {
      logger.logMinimal(`Skipping invalid extraction: key=${ext.key?.slice(0, 30)}`);
      continue;
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Check for existing active memory with same category+key
      const { rows: existing } = await client.query<{ id: string; version: number }>(
        `SELECT id, version FROM core_memory
         WHERE category = $1 AND key = $2 AND superseded_by IS NULL
         LIMIT 1`,
        [ext.category, ext.key]
      );

      const nextVersion = existing.length > 0 ? existing[0].version + 1 : 1;

      // Insert new memory fact
      const { rows: newRows } = await client.query<{ id: string }>(
        `INSERT INTO core_memory (category, key, value, version)
         VALUES ($1, $2, $3, $4)
         RETURNING id`,
        [ext.category, ext.key, ext.value, nextVersion]
      );

      // Supersede the old one if it existed
      if (existing.length > 0) {
        await client.query(
          `UPDATE core_memory SET superseded_by = $1 WHERE id = $2`,
          [newRows[0].id, existing[0].id]
        );
      }

      await client.query('COMMIT');
      written++;
      logger.log(`Wrote memory: [${ext.category}] ${ext.key}`);
    } catch (err) {
      await client.query('ROLLBACK');
      const msg = (err as Error).message;
      logger.logMinimal(`Failed to write memory [${ext.category}] ${ext.key}: ${msg}`);
    } finally {
      client.release();
    }
  }

  return written;
}

/**
 * Main handler.
 */
export async function handleImessageHistoryAnalyze(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = (job.payload || {}) as {
    batch_size?: number;
    sender_filter?: string;
  };
  const batchSize = payload.batch_size ?? DEFAULT_BATCH_SIZE;
  const senderFilter = payload.sender_filter;

  logger.log(`Starting job ${job.id} (batch_size=${batchSize}${senderFilter ? `, sender=${senderFilter}` : ''})`);
  await jobLog(job.id, `Starting iMessage analysis (batch_size=${batchSize})`);

  // 1. Get current watermark
  const lastId = await getWatermark();
  logger.log(`Watermark: last_id=${lastId}`);
  await jobLog(job.id, `Watermark: last analyzed message ID = ${lastId}`);

  // 2. Fetch unprocessed messages
  const messages = await fetchMessages(lastId, batchSize, senderFilter);

  if (messages.length === 0) {
    logger.log('No new messages to analyze');
    return { analyzed: 0, extractions: 0, written: 0 };
  }

  logger.log(`Fetched ${messages.length} message(s) (ids ${messages[0].id}-${messages[messages.length - 1].id})`);
  await jobLog(job.id, `Fetched ${messages.length} messages (IDs ${messages[0].id}–${messages[messages.length - 1].id})`);

  // 3. Group messages by conversation
  const groups = groupMessages(messages);
  logger.log(`${groups.size} conversation group(s)`);
  await jobLog(job.id, `Grouped into ${groups.size} conversation(s)`);

  // 4. Analyze each conversation group
  const allExtractions: MemoryExtraction[] = [];

  for (const [groupKey, groupMessages] of groups) {
    const extractions = await analyzeConversation(groupKey, groupMessages);
    allExtractions.push(...extractions);
  }

  logger.log(`Total extractions: ${allExtractions.length}`);

  // 5. Write facts to core_memory
  const written = allExtractions.length > 0 ? await writeMemoryFacts(allExtractions) : 0;

  // 5b. Also write to owner_profile for the structured knowledge graph
  let profileWritten = 0;
  if (allExtractions.length > 0) {
    const CATEGORY_TO_DOMAIN: Record<string, string> = {
      people: 'people',
      preferences: 'preferences',
      context: 'events',
      recurring: 'lifestyle',
    };

    const profileFacts: FactInput[] = allExtractions.map((ext) => ({
      domain: CATEGORY_TO_DOMAIN[ext.category] || ext.category,
      category: ext.category,
      key: ext.key,
      value: ext.value,
      confidence: 0.6,
      source: 'imessage',
      validFrom: ext.when ?? undefined,
    }));

    const ingestResult = await ingestFacts(profileFacts);
    profileWritten = ingestResult.written;
    if (profileWritten > 0) {
      logger.log(`Wrote ${profileWritten} facts to knowledge graph`);
    }
  }

  // 6. Update watermark and mark messages as analyzed
  const maxId = messages[messages.length - 1].id;
  await updateWatermark(maxId, messages.length);

  // 7. Check if there are more messages to process and mark as analyzed
  const pool = getPool();

  // Mark individual messages as analyzed for per-message tracking
  const messageIds = messages.map(m => m.id);
  await pool.query(
    `UPDATE imessage_incoming SET analyzed_at = NOW() WHERE id = ANY($1) AND analyzed_at IS NULL`,
    [messageIds]
  );
  const { rows: remaining } = await pool.query<{ count: string }>(
    `SELECT COUNT(*) as count FROM imessage_incoming WHERE id > $1`,
    [maxId]
  );
  const remainingCount = parseInt(remaining[0]?.count ?? '0', 10);

  if (remainingCount > 0) {
    logger.log(`${remainingCount} more message(s) to process, enqueuing continuation job`);
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, max_attempts)
       VALUES ($1, $2, 1)`,
      ['imessage-history-analyze', JSON.stringify({})]
    );
  }

  // 8. Log event
  logEvent({
    action: `iMessage analysis: processed ${messages.length} messages, extracted ${allExtractions.length} facts, wrote ${written} to memory, ${profileWritten} to profile`,
    component: 'imessage',
    category: 'self_maintenance',
    metadata: {
      messages_analyzed: messages.length,
      conversation_groups: groups.size,
      extractions: allExtractions.length,
      written,
      profile_written: profileWritten,
      remaining: remainingCount,
      watermark_id: maxId,
    },
  });

  logger.log(`Complete: ${messages.length} messages, ${allExtractions.length} extractions, ${written} written, ${profileWritten} profile, ${remainingCount} remaining`);
  await jobLog(job.id, `Complete: ${messages.length} msgs → ${allExtractions.length} extractions → ${written} memory, ${profileWritten} PKG. ${remainingCount} remaining`);

  return {
    messages_analyzed: messages.length,
    conversation_groups: groups.size,
    extractions: allExtractions.length,
    written,
    profile_written: profileWritten,
    remaining: remainingCount,
    watermark_id: maxId,
  };
}
