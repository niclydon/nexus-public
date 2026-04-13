/**
 * Conversation Embed Handler
 *
 * Processes conversation history (iMessage, email, Google Voice) into
 * chunked segments with high-quality embeddings for semantic search.
 *
 * For each conversation partner:
 * 1. Pulls all messages chronologically
 * 2. Chunks into overlapping windows (default 50 messages, 10 overlap)
 * 3. Resolves the conversation partner to a knowledge_entity
 * 4. Stores chunks with entity links
 * 5. Generates embeddings via the embedding router (conversation tier)
 *
 * Self-chaining: processes one conversation partner per job, then chains
 * the next partner. This keeps individual jobs small and resumable.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import {
  ingestConversationChunk,
  type ConversationChunkInput,
  type EntityHint,
} from '../lib/knowledge.js';

const logger = createLogger('conversation-embed');

// ---------------------------------------------------------------------------
// Message chunking
// ---------------------------------------------------------------------------

interface Message {
  sender: string;
  sender_name: string | null;
  contact_name: string | null;
  message: string;
  is_from_me: boolean;
  received_at: Date;
}

/**
 * Format messages into a readable conversation chunk.
 */
function formatChunk(messages: Message[], ownerName: string = 'Owner'): string {
  return messages.map(m => {
    const name = m.is_from_me
      ? ownerName
      : (m.contact_name || m.sender_name || m.sender);
    const time = m.received_at.toISOString().slice(0, 16).replace('T', ' ');
    return `[${time}] ${name}: ${m.message}`;
  }).join('\n');
}

/**
 * Split messages into overlapping chunks.
 */
function chunkMessages(
  messages: Message[],
  chunkSize: number,
  overlap: number
): Array<{ messages: Message[]; startIdx: number }> {
  const chunks: Array<{ messages: Message[]; startIdx: number }> = [];
  const step = Math.max(1, chunkSize - overlap);

  for (let i = 0; i < messages.length; i += step) {
    const end = Math.min(i + chunkSize, messages.length);
    chunks.push({ messages: messages.slice(i, end), startIdx: i });
    if (end >= messages.length) break;
  }

  return chunks;
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

export async function handleConversationEmbed(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as {
    source_type: string;
    source_identifier?: string;
    chunk_size?: number;
    chunk_overlap?: number;
    contact_offset?: number;
  };
  const sourceType = payload.source_type;
  const chunkSize = payload.chunk_size || 50;
  const chunkOverlap = payload.chunk_overlap || 10;
  const contactOffset = payload.contact_offset || 0;

  const pool = getPool();

  if (sourceType === 'imessage') {
    return handleIMessageEmbed(pool, job.id, payload, chunkSize, chunkOverlap, contactOffset);
  }

  // Future: email, google_voice handlers
  logger.log(`Source type "${sourceType}" not yet implemented`);
  return { status: 'skipped', reason: `Source type "${sourceType}" not yet implemented` };
}

async function handleIMessageEmbed(
  pool: ReturnType<typeof getPool>,
  jobId: string,
  payload: {
    source_type: string;
    source_identifier?: string;
    chunk_size?: number;
    chunk_overlap?: number;
    contact_offset?: number;
  },
  chunkSize: number,
  chunkOverlap: number,
  contactOffset: number,
): Promise<Record<string, unknown>> {
  // If a specific source_identifier is provided, process just that one
  if (payload.source_identifier) {
    await jobLog(jobId, `Processing specific sender: ${payload.source_identifier}`);
    return processIMessageConversation(pool, jobId, payload.source_identifier, chunkSize, chunkOverlap);
  }

  // Otherwise, get distinct senders and process one at a time (self-chaining)
  const { rows: senders } = await pool.query<{ sender: string; msg_count: string }>(
    `SELECT sender, COUNT(*) as msg_count
     FROM imessage_incoming
     WHERE is_group = FALSE AND sender IS NOT NULL
     GROUP BY sender
     HAVING COUNT(*) >= 10
     ORDER BY COUNT(*) DESC
     OFFSET $1 LIMIT 1`,
    [contactOffset]
  );

  if (senders.length === 0) {
    logger.log('All conversation partners processed — no more senders');
    await jobLog(jobId, `All conversation partners processed (${contactOffset} total)`);
    return { status: 'complete', contacts_processed: contactOffset };
  }

  const sender = senders[0];
  logger.log(`Processing sender ${sender.sender} (${sender.msg_count} messages, contact #${contactOffset + 1})`);
  await jobLog(jobId, `Processing contact #${contactOffset + 1}: ${sender.sender} (${sender.msg_count} messages)`);

  // Check if already processed
  const { rows: existing } = await pool.query<{ cnt: string }>(
    `SELECT COUNT(*) as cnt FROM knowledge_conversation_chunks
     WHERE source_type = 'imessage' AND source_identifier = $1`,
    [sender.sender]
  );

  if (parseInt(existing[0].cnt) > 0) {
    logger.log(`Sender ${sender.sender} already has ${existing[0].cnt} chunks — skipping`);
    await jobLog(jobId, `Skipped: ${sender.sender} already has ${existing[0].cnt} chunks`);
  } else {
    await processIMessageConversation(pool, jobId, sender.sender, chunkSize, chunkOverlap);
  }

  // Chain next sender
  const nextOffset = contactOffset + 1;

  // Check if there are more
  const { rows: hasMore } = await pool.query<{ cnt: string }>(
    `SELECT COUNT(*) as cnt FROM (
       SELECT sender FROM imessage_incoming
       WHERE is_group = FALSE AND sender IS NOT NULL
       GROUP BY sender HAVING COUNT(*) >= 10
       OFFSET $1 LIMIT 1
     ) sub`,
    [nextOffset]
  );

  if (parseInt(hasMore[0].cnt) > 0) {
    logger.log(`Chaining next sender at offset ${nextOffset}`);
    await jobLog(jobId, `Chaining next contact at offset ${nextOffset}`);
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
       VALUES ('conversation-embed', $1, 'pending', 0, 3)`,
      [JSON.stringify({
        source_type: 'imessage',
        chunk_size: chunkSize,
        chunk_overlap: chunkOverlap,
        contact_offset: nextOffset,
      })]
    );
  } else {
    logger.log('This was the last sender — iMessage conversation embedding complete');
    await jobLog(jobId, 'Last sender processed — iMessage conversation embedding complete');
  }

  return {
    status: 'processed',
    sender: sender.sender,
    message_count: parseInt(sender.msg_count),
    contact_index: contactOffset,
  };
}

async function processIMessageConversation(
  pool: ReturnType<typeof getPool>,
  jobId: string,
  sender: string,
  chunkSize: number,
  chunkOverlap: number,
): Promise<Record<string, unknown>> {
  // Pull all messages for this sender, chronologically
  const { rows: messages } = await pool.query<Message>(
    `SELECT sender, sender_name, contact_name, message, is_from_me, received_at
     FROM imessage_incoming
     WHERE sender = $1 AND is_group = FALSE AND message IS NOT NULL AND message != ''
     ORDER BY received_at ASC`,
    [sender]
  );

  if (messages.length === 0) {
    logger.log(`No messages found for sender ${sender}`);
    return { status: 'empty', sender };
  }

  logger.log(`Found ${messages.length} messages for ${sender}`);
  await jobLog(jobId, `Found ${messages.length} messages for ${sender}`);

  // Determine entity hint from the sender info
  const sampleMsg = messages.find(m => !m.is_from_me) || messages[0];
  const displayName = sampleMsg.contact_name || sampleMsg.sender_name || sender;

  // Look up contact by phone number
  const { rows: contactMatch } = await pool.query<{ id: string; display_name: string }>(
    `SELECT id, display_name FROM contacts
     WHERE $1 = ANY(phone_numbers)
     LIMIT 1`,
    [sender]
  );

  const entityHint: EntityHint = {
    name: contactMatch[0]?.display_name || displayName,
    type: 'person',
    aliases: [sender],
    contactId: contactMatch[0]?.id,
  };

  // Chunk the conversation
  const chunks = chunkMessages(messages, chunkSize, chunkOverlap);
  logger.log(`Created ${chunks.length} chunks (size=${chunkSize}, overlap=${chunkOverlap})`);
  await jobLog(jobId, `Chunked ${displayName} conversation: ${chunks.length} chunks (${messages.length} messages)`);

  let written = 0;
  let errors = 0;

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    const chunkText = formatChunk(chunk.messages);
    const firstMsg = chunk.messages[0];
    const lastMsg = chunk.messages[chunk.messages.length - 1];

    try {
      const input: ConversationChunkInput = {
        sourceType: 'imessage',
        sourceIdentifier: sender,
        entityHint: i === 0 ? entityHint : undefined, // only resolve entity on first chunk
        chunkText,
        chunkIndex: i,
        messageCount: chunk.messages.length,
        timeStart: firstMsg.received_at.toISOString(),
        timeEnd: lastMsg.received_at.toISOString(),
        metadata: {
          contact_name: displayName,
          contact_id: contactMatch[0]?.id,
        },
      };

      // For subsequent chunks, reuse the entity from the first one
      if (i > 0) {
        // Find the entity we created/resolved on the first chunk
        const { rows: entityRows } = await pool.query<{ entity_id: string }>(
          `SELECT entity_id FROM knowledge_conversation_chunks
           WHERE source_type = 'imessage' AND source_identifier = $1 AND chunk_index = 0
           LIMIT 1`,
          [sender]
        );
        if (entityRows[0]) {
          input.entityId = entityRows[0].entity_id;
        }
      }

      await ingestConversationChunk(input);
      written++;
      logger.logVerbose(`Chunk ${i}/${chunks.length} written (${chunk.messages.length} messages, ${firstMsg.received_at.toISOString().slice(0, 10)} to ${lastMsg.received_at.toISOString().slice(0, 10)})`);
    } catch (err) {
      errors++;
      const msg = err instanceof Error ? err.message : String(err);
      logger.logMinimal(`Failed chunk ${i} for ${sender}: ${msg}`);
    }
  }

  const result = {
    status: 'processed',
    sender,
    display_name: displayName,
    total_messages: messages.length,
    chunks_created: written,
    chunks_failed: errors,
    time_range: {
      start: messages[0].received_at.toISOString(),
      end: messages[messages.length - 1].received_at.toISOString(),
    },
  };

  logger.log(`Complete: ${JSON.stringify(result)}`);
  await jobLog(jobId, `Complete: ${written} chunks embedded for ${displayName}, ${errors} errors`);
  return result;
}
