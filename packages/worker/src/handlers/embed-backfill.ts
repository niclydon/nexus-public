/**
 * Embed Backfill Handler
 *
 * Generates embeddings for rows missing them in any supported table.
 * Uses the local MLX embedding server (qwen3-embed-8b, 768 dims)
 * with fallback to Google Gemini.
 *
 * Resumable: uses cursor-based pagination (last_id) and self-chains
 * the next batch so a crash or timeout loses at most one batch.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, platformMode } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const LOCAL_EMBED_URL = process.env.FORGE_BASE_URL
  ? process.env.FORGE_BASE_URL.replace('/v1', '')  // Use Forge (strips /v1 since /embed is at root)
  : process.env.LOCAL_EMBED_URL || 'http://localhost:8787';
const EMBED_API_KEY = process.env.FORGE_API_KEY || '';
const EMBED_BATCH_SIZE = 256; // max texts per embedding request

// ---------------------------------------------------------------------------
// Table configuration — defines how to build embedding text for each table
// ---------------------------------------------------------------------------

interface TableConfig {
  /** SQL expression that builds the text to embed. Columns must exist on the table. */
  textExpr: string;
  /** Column name of the primary key. */
  idColumn: string;
  /** SQL type of the id column (for cursor cast). */
  idType: 'uuid' | 'text' | 'integer';
  /** Extra WHERE clause (e.g. filter out rows with no text). */
  extraWhere?: string;
  /** Content class hint for logging. */
  contentClass: string;
}

const TABLE_CONFIGS: Record<string, TableConfig> = {
  gmail_archive: {
    textExpr: `COALESCE(from_name, from_address, '') || ': ' || COALESCE(subject, '') || ' — ' || COALESCE(LEFT(body_text, 1000), snippet, '')`,
    idColumn: 'id',
    idType: 'text',
    extraWhere: `AND (subject IS NOT NULL OR body_text IS NOT NULL)`,
    contentClass: 'email',
  },
  imessage_incoming: {
    textExpr: `COALESCE(sender_name, contact_name, sender, '') || ': ' || COALESCE(message, '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND message IS NOT NULL AND message != ''`,
    contentClass: 'conversation',
  },
  photo_metadata: {
    textExpr: `COALESCE(description, '')`,
    idColumn: 'id',
    idType: 'uuid',
    extraWhere: `AND description IS NOT NULL AND description != ''`,
    contentClass: 'fact',
  },
  looki_moments: {
    textExpr: `COALESCE(title, '') || ' — ' || COALESCE(transcript, description, summary, '')`,
    idColumn: 'id',
    idType: 'text',
    extraWhere: `AND (transcript IS NOT NULL OR description IS NOT NULL OR summary IS NOT NULL)`,
    contentClass: 'transcript',
  },
  life_narration: {
    textExpr: `COALESCE(narrative, '')`,
    idColumn: 'id',
    idType: 'uuid',
    extraWhere: `AND narrative IS NOT NULL AND narrative != ''`,
    contentClass: 'narrative',
  },
  aria_journal: {
    textExpr: `COALESCE(content, '')`,
    idColumn: 'id',
    idType: 'uuid',
    extraWhere: `AND content IS NOT NULL AND content != ''`,
    contentClass: 'journal',
  },
  conversations: {
    textExpr: `COALESCE(title, '') || COALESCE(' — ' || summary, '')`,
    idColumn: 'id',
    idType: 'uuid',
    extraWhere: `AND (title IS NOT NULL OR summary IS NOT NULL)`,
    contentClass: 'fact',
  },
  contacts: {
    textExpr: `COALESCE(display_name, '') || COALESCE(' — ' || job_title, '') || COALESCE(' — ' || relationship, '') || COALESCE(' — ' || notes, '')`,
    idColumn: 'id',
    idType: 'uuid',
    contentClass: 'fact',
  },
  proactive_insights: {
    textExpr: `COALESCE(title, '') || ' — ' || COALESCE(body, '') || COALESCE(' [reasoning: ' || reasoning || ']', '')`,
    idColumn: 'id',
    idType: 'uuid',
    contentClass: 'fact',
  },
  data_imports: {
    textExpr: `COALESCE(filename, '') || ' — ' || COALESCE(LEFT(transcript, 1000), '')`,
    idColumn: 'id',
    idType: 'uuid',
    extraWhere: `AND transcript IS NOT NULL AND transcript != ''`,
    contentClass: 'transcript',
  },
  // --- Knowledge Tables ---
  knowledge_facts: {
    textExpr: `COALESCE(domain, '') || '/' || COALESCE(category, '') || ': ' || COALESCE(key, '') || ' = ' || COALESCE(LEFT(value, 1000), '')`,
    idColumn: 'id',
    idType: 'uuid',
    extraWhere: `AND value IS NOT NULL AND value != ''`,
    contentClass: 'fact',
  },
  knowledge_entities: {
    textExpr: `COALESCE(canonical_name, '') || COALESCE(' (' || entity_type || ')', '') || COALESCE(' — ' || LEFT(summary, 500), '')`,
    idColumn: 'id',
    idType: 'uuid',
    extraWhere: `AND canonical_name IS NOT NULL AND canonical_name != ''`,
    contentClass: 'fact',
  },
  // --- AURORA Raw Tables ---
  aurora_raw_gmail: {
    textExpr: `COALESCE(from_name, from_address, '') || ': ' || COALESCE(subject, '') || ' — ' || COALESCE(LEFT(body_text, 500), snippet, '')`,
    idColumn: 'id',
    idType: 'text',
    extraWhere: `AND (subject IS NOT NULL OR body_text IS NOT NULL)`,
    contentClass: 'email',
  },
  aurora_raw_imessage: {
    textExpr: `COALESCE(handle_id, '') || ': ' || COALESCE(text, '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND text IS NOT NULL AND LENGTH(text) > 5`,
    contentClass: 'fact',
  },
  aurora_raw_siri_interactions: {
    textExpr: `COALESCE(domain, '') || '/' || COALESCE(type, '') || ' via ' || COALESCE(bundle_id, '') || COALESCE(' — ' || LEFT(fields::text, 500), '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND domain IS NOT NULL`,
    contentClass: 'fact',
  },
  // aurora_raw_photos deprecated 2026-04-11 (migration 054) — embedding column
  // + HNSW index dropped. Photo data lives in photo_metadata instead.
  aurora_raw_chatgpt: {
    textExpr: `COALESCE(conversation_title, '') || ' — ' || COALESCE(LEFT(content_text, 1000), '')`,
    idColumn: 'id',
    idType: 'text',
    extraWhere: `AND content_text IS NOT NULL AND LENGTH(content_text) > 10 AND message_role IN ('user', 'assistant')`,
    contentClass: 'conversation',
  },
  aurora_raw_claude: {
    textExpr: `COALESCE(conversation_name, '') || ' — ' || COALESCE(LEFT(content_text, 1000), '')`,
    idColumn: 'id',
    idType: 'text',
    extraWhere: `AND content_text IS NOT NULL AND LENGTH(content_text) > 10`,
    contentClass: 'conversation',
  },
  // --- Ingested Archive Tables ---
  blogger_posts: {
    textExpr: `COALESCE(title, '') || ' — ' || COALESCE(LEFT(content_text, 1000), '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND content_text IS NOT NULL AND LENGTH(content_text) > 10`,
    contentClass: 'journal',
  },
  archived_sent_emails: {
    textExpr: `COALESCE(sender_name, '') || ': ' || COALESCE(subject, '') || ' — ' || COALESCE(LEFT(body_text, 1000), '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND body_text IS NOT NULL AND LENGTH(body_text) > 10`,
    contentClass: 'email',
  },
  bb_sms_messages: {
    textExpr: `COALESCE(phone_number, '') || ': ' || COALESCE(message, '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND message IS NOT NULL AND LENGTH(message) > 5`,
    contentClass: 'conversation',
  },
  site_guestbook: {
    textExpr: `COALESCE(poster_name, '') || ' to ' || COALESCE(recipient_name, '') || ': ' || COALESCE(message_text, '')`,
    idColumn: 'id',
    idType: 'uuid',
    extraWhere: `AND message_text IS NOT NULL AND LENGTH(message_text) > 5`,
    contentClass: 'conversation',
  },
  strava_activities: {
    textExpr: `COALESCE(activity_name, '') || ' (' || COALESCE(activity_type, '') || ')' || COALESCE(' — ' || description, '') || ' ' || COALESCE(ROUND(distance_meters::numeric / 1000, 1)::text || 'km', '') || ' ' || COALESCE(ROUND(moving_time_seconds::numeric / 60)::text || 'min', '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND activity_name IS NOT NULL`,
    contentClass: 'activity',
  },
  life_transactions: {
    textExpr: `COALESCE(merchant, '') || ': ' || COALESCE(description, '') || COALESCE(' $' || amount, '') || COALESCE(' [' || category || ']', '')`,
    idColumn: 'id',
    idType: 'uuid',
    extraWhere: `AND description IS NOT NULL AND LENGTH(description) > 5`,
    contentClass: 'transaction',
  },
  music_library: {
    textExpr: `COALESCE(track_name, '') || ' by ' || COALESCE(artist_name, '') || COALESCE(' — ' || album_name, '') || COALESCE(' [' || genre || ']', '')`,
    idColumn: 'id',
    idType: 'uuid',
    extraWhere: `AND track_name IS NOT NULL`,
    contentClass: 'music',
  },
  // --- Social / Messaging Archive Tables ---
  aurora_raw_google_voice: {
    textExpr: `COALESCE(contact_name, phone_number, '') || ' (' || record_type || '): ' || COALESCE(message_text, '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND message_text IS NOT NULL AND LENGTH(message_text) > 5`,
    contentClass: 'conversation',
  },
  aurora_raw_facebook: {
    textExpr: `COALESCE(sender, '') || ' (' || data_type || '): ' || COALESCE(content, '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND content IS NOT NULL AND LENGTH(content) > 5`,
    contentClass: 'conversation',
  },
  aurora_raw_instagram: {
    textExpr: `COALESCE(sender, '') || ' (' || data_type || '): ' || COALESCE(content, '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND content IS NOT NULL AND LENGTH(content) > 5`,
    contentClass: 'conversation',
  },
  aurora_raw_google_chat: {
    textExpr: `COALESCE(sender_name, sender_email, '') || ': ' || COALESCE(message_text, '')`,
    idColumn: 'id',
    idType: 'integer',
    extraWhere: `AND message_text IS NOT NULL AND LENGTH(message_text) > 5`,
    contentClass: 'conversation',
  },
};

// ---------------------------------------------------------------------------
// Embedding helpers
// ---------------------------------------------------------------------------

async function embedBatch(texts: string[]): Promise<number[][] | null> {
  try {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (EMBED_API_KEY) headers['Authorization'] = `Bearer ${EMBED_API_KEY}`;
    const response = await fetch(`${LOCAL_EMBED_URL}/embed`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ texts }),
      signal: AbortSignal.timeout(120_000),
    });
    if (!response.ok) {
      console.error(`[embed-backfill] MLX error ${response.status}`);
      return null;
    }
    const data = await response.json() as { embeddings?: number[][] };
    return data.embeddings ?? null;
  } catch (err) {
    console.error(`[embed-backfill] MLX request failed:`, (err as Error).message);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

export async function handleEmbedBackfill(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as {
    table: string;
    batch_size?: number;
    total_embedded?: number;
    last_id?: string | number;
  };
  const tableName = payload.table;
  const batchSize = payload.batch_size ?? 500;
  let totalEmbedded = payload.total_embedded ?? 0;

  const config = TABLE_CONFIGS[tableName];
  if (!config) {
    throw new Error(`[embed-backfill] Unknown table: ${tableName}`);
  }

  const pool = getPool();

  // Creative Mode short-circuit: Forge /embed is intentionally offline.
  // Skip the claim-process-fail cycle entirely — self-chain with a 30 min
  // backoff so we don't claim the same job every 30s just to skip it.
  // Knowledge-backfill has a Google fallback and can keep progressing
  // during creative mode, but embed-backfill is Forge-only and should
  // genuinely pause.
  if (await platformMode.isCreativeMode()) {
    await jobLog(job.id, `Skipped (creative_mode) — will retry in 30 min when mode returns to normal`);
    try {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
         SELECT 'embed-backfill', $1::jsonb, 'nexus', 0, 3, NOW() + INTERVAL '30 minutes'
         WHERE NOT EXISTS (
           SELECT 1 FROM tempo_jobs WHERE job_type = 'embed-backfill' AND status = 'pending'
         )`,
        [JSON.stringify({ table: tableName, batch_size: batchSize, total_embedded: totalEmbedded, last_id: payload.last_id })],
      );
    } catch {
      // non-fatal
    }
    return { table: tableName, status: 'skipped', reason: 'creative_mode', total_embedded: totalEmbedded };
  }

  // Build cursor WHERE based on id type — skip entirely on first batch
  const hasCursor = payload.last_id != null;
  const cursorWhere = hasCursor
    ? `AND ${config.idColumn} > $1::${config.idType}`
    : '';

  // Count remaining (only on first batch for logging)
  if (!payload.last_id) {
    const countResult = await pool.query(
      `SELECT COUNT(*) FROM ${tableName} WHERE embedding IS NULL ${config.extraWhere || ''}`
    );
    const remaining = parseInt(countResult.rows[0].count, 10);
    console.log(`[embed-backfill] ${tableName}: ${remaining.toLocaleString()} rows need embeddings`);
    await jobLog(job.id, `Starting ${tableName}: ${remaining.toLocaleString()} rows to embed`);

    if (remaining === 0) {
      return { table: tableName, status: 'nothing_to_do', total_embedded: totalEmbedded };
    }
  }

  // Fetch batch using cursor pagination
  const limitParam = hasCursor ? '$2' : '$1';
  const query = `
    SELECT ${config.idColumn}::text AS id, ${config.textExpr} AS embed_text
    FROM ${tableName}
    WHERE embedding IS NULL ${config.extraWhere || ''} ${cursorWhere}
    ORDER BY ${config.idColumn}
    LIMIT ${limitParam}
  `;
  const queryParams = hasCursor ? [payload.last_id, batchSize] : [batchSize];
  const { rows } = await pool.query(query, queryParams);

  if (rows.length === 0) {
    console.log(`[embed-backfill] ${tableName}: complete — ${totalEmbedded.toLocaleString()} total embedded`);
    await jobLog(job.id, `Complete: ${totalEmbedded.toLocaleString()} rows embedded`);
    return { table: tableName, status: 'complete', total_embedded: totalEmbedded };
  }

  // Generate embeddings in sub-batches
  let batchEmbedded = 0;
  let batchErrors = 0;

  for (let i = 0; i < rows.length; i += EMBED_BATCH_SIZE) {
    const subRows = rows.slice(i, i + EMBED_BATCH_SIZE);
    const texts = subRows.map((r: { embed_text: string }) => r.embed_text || '');

    const vectors = await embedBatch(texts);
    if (!vectors) {
      batchErrors += subRows.length;
      continue;
    }

    // Batch UPDATE using unnest
    const ids = subRows.map((r: { id: string }) => r.id);
    const vecStrs = vectors.map((v: number[]) => `[${v.join(',')}]`);

    // Build the unnest cast based on id type
    const idCast = config.idType === 'integer' ? 'int[]' : config.idType === 'uuid' ? 'uuid[]' : 'text[]';

    await pool.query(
      `UPDATE ${tableName} AS t
       SET embedding = batch.vec::vector
       FROM unnest($1::${idCast}, $2::text[]) AS batch(id, vec)
       WHERE t.${config.idColumn} = batch.id`,
      [ids, vecStrs],
    );

    batchEmbedded += ids.length;
  }

  totalEmbedded += batchEmbedded;
  const lastId = rows[rows.length - 1].id;

  console.log(
    `[embed-backfill] ${tableName}: batch done — ${batchEmbedded} embedded, ${batchErrors} errors, ` +
    `${totalEmbedded.toLocaleString()} total, cursor=${lastId}`
  );

  // Self-chain: enqueue next batch — only if no pending job exists
  const hasMore = rows.length === batchSize;
  if (hasMore) {
    const { rows: existingEb } = await pool.query(
      `SELECT id FROM tempo_jobs WHERE job_type = 'embed-backfill' AND status = 'pending' LIMIT 1`,
    );
    if (existingEb.length === 0) {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
         VALUES ('embed-backfill', $1, 'pending', 0, 3)`,
        [JSON.stringify({
          table: tableName,
          last_id: lastId,
          batch_size: batchSize,
          total_embedded: totalEmbedded,
        })],
      );
      console.log(`[embed-backfill] ${tableName}: chained next batch after id=${lastId}`);
    } else {
      console.log(`[embed-backfill] ${tableName}: skipped chain — pending job exists`);
    }
  } else {
    console.log(`[embed-backfill] ${tableName}: final batch — ${totalEmbedded.toLocaleString()} total embedded`);
    await jobLog(job.id, `Complete: ${totalEmbedded.toLocaleString()} rows embedded`);
  }

  return {
    table: tableName,
    status: hasMore ? 'chained' : 'complete',
    batch_embedded: batchEmbedded,
    batch_errors: batchErrors,
    total_embedded: totalEmbedded,
    last_id: lastId,
  };
}
