/**
 * sentiment-backfill handler
 *
 * Chunked, watermarked sentiment analysis across 10 communication channels.
 * Processes CHUNK_SIZE records per job, then self-chains if more remain.
 *
 * Model: Qwen3.5-35B-A3B via Forge /v1/chat/completions
 * Target: aurora_sentiment table
 *
 * Channels: imessage, gmail, google_chat, google_voice, instagram,
 * facebook_message, facebook_post, facebook_comment, voicemail, aria_chat,
 * chatgpt, claude, siri
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { jobLog } from '../lib/job-log.js';
import OpenAI from 'openai';
import { routeRequest } from '../lib/llm/index.js';

const logger = createLogger('sentiment-backfill');

const DEFAULT_CHUNK_SIZE = 50;
const LLM_BATCH_SIZE = 10; // Records per LLM call
const INNER_CONCURRENCY = 2; // Parallel LLM batches per worker. Conservative to avoid Anthropic 429s when 4 Secondary-Server workers each fan out.
const MAX_CONSECUTIVE_ERRORS = 10;
const ERROR_RATE_THRESHOLD = 0.3;

const SYSTEM_PROMPT = `Classify the sentiment of each message. Reply with ONLY a JSON array, one object per message:
[{"i": 0, "label": "positive|negative|neutral", "score": 0.0-1.0, "emotion": "joy|sadness|anger|fear|surprise|disgust|neutral"}]

Rules:
- score: 0.0 = most negative, 0.5 = neutral, 1.0 = most positive
- emotion: the single dominant emotion (use "neutral" if no clear emotion)
- Keep responses minimal — no explanation, just the JSON array`;

function getLLMClient(): OpenAI {
  // Route through Forge gateway so this works from any host (Primary-Server OR Secondary-Server).
  // Forge routes the requested model name to the right backend; we use qwen3-next-chat-80b
  // which lands on the bulk slot (port 8080) and stays off the VIP priority slot.
  return new OpenAI({
    apiKey: process.env.FORGE_API_KEY || 'not-needed',
    baseURL: process.env.FORGE_BASE_URL || 'http://localhost:8642/v1',
    timeout: 240_000,
  });
}

interface TextRecord {
  source: string;
  source_id: string;
  contact_identifier: string | null;
  text: string;
  date: string;
}

interface SentimentResult {
  i: number;
  label: string;
  score: number;
  emotion: string;
}

// ---------- Query unanalyzed records ----------

async function getChunk(chunkSize: number, channel?: string): Promise<TextRecord[]> {
  const pool = getPool();
  const channelFilter = channel ? `AND sub.source = '${channel}'` : '';

  const { rows } = await pool.query(`
    SELECT source, source_id, contact_identifier, text, date FROM (
      SELECT 'imessage' as source, guid as source_id,
        handle_id as contact_identifier,
        text, date::date as date
      FROM aurora_raw_imessage
      WHERE text IS NOT NULL AND LENGTH(text) > 5

      UNION ALL
      SELECT 'gmail', id, from_address,
        LEFT(body_text, 500), date::date
      FROM aurora_raw_gmail
      WHERE body_text IS NOT NULL AND LENGTH(body_text) > 10

      UNION ALL
      SELECT 'google_chat', id::text, COALESCE(sender_email, sender_name),
        message_text, timestamp::date
      FROM aurora_raw_google_chat
      WHERE message_text IS NOT NULL AND LENGTH(message_text) > 5

      UNION ALL
      SELECT 'google_voice', id::text, phone_number,
        message_text, timestamp::date
      FROM aurora_raw_google_voice
      WHERE record_type = 'text' AND message_text IS NOT NULL AND LENGTH(message_text) > 5

      UNION ALL
      SELECT 'instagram', id::text, sender,
        content, timestamp::date
      FROM aurora_raw_instagram
      WHERE data_type = 'message' AND content IS NOT NULL AND LENGTH(content) > 5

      UNION ALL
      SELECT 'facebook_message', id::text, sender,
        content, timestamp::date
      FROM aurora_raw_facebook
      WHERE data_type = 'message' AND content IS NOT NULL AND LENGTH(content) > 5

      UNION ALL
      SELECT 'facebook_post', id::text, sender,
        content, timestamp::date
      FROM aurora_raw_facebook
      WHERE data_type = 'post' AND content IS NOT NULL AND LENGTH(content) > 10

      UNION ALL
      SELECT 'facebook_comment', id::text, sender,
        content, timestamp::date
      FROM aurora_raw_facebook
      WHERE data_type = 'comment' AND content IS NOT NULL AND LENGTH(content) > 5

      UNION ALL
      SELECT 'voicemail', id::text, phone_number,
        message_text, timestamp::date
      FROM aurora_raw_google_voice
      WHERE record_type = 'voicemail' AND message_text IS NOT NULL AND LENGTH(message_text) > 5

      UNION ALL
      SELECT 'aria_chat', id::text, NULL,
        LEFT(content, 500), created_at::date
      FROM messages
      WHERE role = 'user' AND content IS NOT NULL AND LENGTH(content) > 10

      UNION ALL
      SELECT 'chatgpt', id, NULL,
        LEFT(content_text, 500), created_at::date
      FROM aurora_raw_chatgpt
      WHERE message_role = 'user' AND content_text IS NOT NULL AND LENGTH(content_text) > 10

      UNION ALL
      SELECT 'claude', id, NULL,
        LEFT(content_text, 500), created_at::date
      FROM aurora_raw_claude
      WHERE content_text IS NOT NULL AND LENGTH(content_text) > 10

      UNION ALL
      SELECT 'siri', id::text, bundle_id,
        LEFT(fields::text, 500), start_date::date
      FROM aurora_raw_siri_interactions
      WHERE domain IS NOT NULL AND fields IS NOT NULL AND LENGTH(fields::text) > 10

      UNION ALL
      SELECT 'blogger', id::text, NULL,
        LEFT(content_text, 500), published_at::date
      FROM blogger_posts
      WHERE content_text IS NOT NULL AND LENGTH(content_text) > 10

      UNION ALL
      SELECT 'archived_email', id::text, to_recipients,
        COALESCE(subject, '') || ' — ' || LEFT(body_text, 400), sent_at::date
      FROM archived_sent_emails
      WHERE body_text IS NOT NULL AND LENGTH(body_text) > 10

      UNION ALL
      SELECT 'bb_sms', id::text, phone_number,
        message, timestamp::date
      FROM bb_sms_messages
      WHERE message IS NOT NULL AND LENGTH(message) > 5

      UNION ALL
      SELECT 'guestbook', id::text, poster_name,
        message_text, posted_at::date
      FROM site_guestbook
      WHERE message_text IS NOT NULL AND LENGTH(message_text) > 5
    ) sub
    WHERE NOT EXISTS (
      SELECT 1 FROM aurora_sentiment s
      WHERE s.source = sub.source AND s.source_id = sub.source_id
    )
    ${channelFilter}
    ORDER BY sub.date DESC
    LIMIT $1
  `, [chunkSize]);

  return rows;
}

// ---------- Sanitize text for LLM ----------

/** Strip unpaired Unicode surrogates from in-memory strings */
function sanitize(text: string): string {
  // eslint-disable-next-line no-control-regex
  return text.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, '');
}

/**
 * Fix broken surrogate escape sequences in JSON text from llama-server.
 * llama-server's nlohmann/json sometimes produces \uD83D without the trailing
 * \uDC00-\uDFFF when emoji bytes split across tokens. This removes only the
 * broken escapes while preserving valid surrogate pairs.
 */
function sanitizeJsonSurrogates(raw: string): string {
  return raw
    // Remove unpaired high surrogates (not followed by low surrogate)
    .replace(/\\ud[89a-b][0-9a-f]{2}(?!\\ud[c-f][0-9a-f]{2})/gi, '')
    // Remove unpaired low surrogates (not preceded by high surrogate)
    .replace(/(?<!\\ud[89a-b][0-9a-f]{2})\\ud[c-f][0-9a-f]{2}/gi, '');
}

// ---------- Classify a small batch via LLM ----------

async function classifyBatch(_unused: OpenAI | null, texts: TextRecord[]): Promise<SentimentResult[]> {
  const userMessage = texts.map((t, i) => {
    let text = sanitize(t.text);
    text = text.replace(/[\u{10000}-\u{10FFFF}]/gu, '[emoji]');
    return `[${i}] ${text.slice(0, 200).replace(/\n/g, ' ')}`;
  }).join('\n');

  // Route via the LLM router. The classification tier currently goes to:
  //   primary: qwen3-next-chat-80b on Forge bulk slot (port 8080, RPC-split)
  //   fallback: gemini-2.5-flash-lite, claude-haiku, gpt-4o-mini
  // Sentiment classification is the textbook use case for the classification
  // tier. We use it instead of generation because classification has more
  // forgiving timeouts and better handles structured output.
  let raw: string;
  try {
    const result = await routeRequest({
      handler: 'sentiment-backfill',
      taskTier: 'classification',
      systemPrompt: SYSTEM_PROMPT,
      userMessage,
      maxTokens: texts.length * 60,
    });
    raw = result.text.trim();
  } catch (err) {
    throw new Error(`LLM error: ${(err as Error).message}`);
  }

  const jsonStr = sanitize(raw.replace(/^```json?\s*/i, '').replace(/\s*```$/i, ''));

  try {
    return JSON.parse(jsonStr) as SentimentResult[];
  } catch {
    const results: SentimentResult[] = [];
    const regex = /\{"i":\s*(\d+),\s*"label":\s*"(\w+)",\s*"score":\s*([\d.]+),\s*"emotion":\s*"(\w+)"\}/g;
    let match;
    while ((match = regex.exec(jsonStr)) !== null) {
      results.push({ i: parseInt(match[1]), label: match[2], score: parseFloat(match[3]), emotion: match[4] });
    }
    if (results.length === 0) {
      logger.logMinimal(`LLM parse error: ${jsonStr.slice(0, 100)}`);
    }
    return results;
  }
}

// ---------- Insert results ----------

async function insertResults(records: TextRecord[], results: SentimentResult[]): Promise<number> {
  const pool = getPool();
  let inserted = 0;

  for (const r of results) {
    if (r.i < 0 || r.i >= records.length) continue;
    const rec = records[r.i];
    const label = ['positive', 'negative', 'neutral'].includes(r.label) ? r.label : 'neutral';
    const score = Math.max(0, Math.min(1, r.score || 0.5));

    try {
      await pool.query(`
        INSERT INTO aurora_sentiment (source, source_id, contact_identifier, sentiment_score, sentiment_label, dominant_emotion, date)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (source, source_id) DO NOTHING
      `, [rec.source, rec.source_id, rec.contact_identifier, score, label, r.emotion || null, rec.date]);
      inserted++;
    } catch {
      // skip duplicates
    }
  }

  return inserted;
}

// ---------- Handler ----------

export async function handleSentimentBackfill(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = (job.payload ?? {}) as {
    chunk_size?: number;
    channel?: string;
    triggered_by?: string;
  };
  const chunkSize = payload.chunk_size ?? DEFAULT_CHUNK_SIZE;
  const channel = payload.channel;

  logger.log(`Starting chunk: size=${chunkSize}, channel=${channel || 'all'}, triggered_by=${payload.triggered_by || 'manual'}`);

  // 1. Get unanalyzed records for this chunk
  const records = await getChunk(chunkSize, channel);

  if (records.length === 0) {
    await jobLog(job.id, 'No unanalyzed records remaining');
    logger.log('No unanalyzed records — sentiment backfill complete');
    return { status: 'complete', processed: 0, remaining: 0 };
  }

  await jobLog(job.id, `Processing ${records.length} records`);

  // 2. Process in LLM batches — parallelized at INNER_CONCURRENCY
  const llm = getLLMClient();
  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();
  let abort = false;

  // Pre-slice the records into batches
  const batches: TextRecord[][] = [];
  for (let i = 0; i < records.length; i += LLM_BATCH_SIZE) {
    batches.push(records.slice(i, i + LLM_BATCH_SIZE));
  }

  // Process batches in waves of INNER_CONCURRENCY
  for (let waveStart = 0; waveStart < batches.length && !abort; waveStart += INNER_CONCURRENCY) {
    const wave = batches.slice(waveStart, waveStart + INNER_CONCURRENCY);

    const waveResults = await Promise.allSettled(
      wave.map(async (batch) => {
        const results = await classifyBatch(llm, batch);
        const inserted = await insertResults(batch, results);
        return { batchSize: batch.length, results, inserted };
      }),
    );

    let waveErrors = 0;
    for (let j = 0; j < waveResults.length; j++) {
      const r = waveResults[j];
      if (r.status === 'fulfilled') {
        totalInserted += r.value.inserted;

        // First-batch validation: very first batch returning no results aborts
        if (waveStart === 0 && j === 0 && r.value.inserted === 0 && r.value.results.length === 0) {
          logger.logMinimal('First batch returned no results — aborting chunk');
          await jobLog(job.id, 'Aborted: first batch returned no results');
          return { status: 'aborted', reason: 'first_batch_empty', processed: 0 };
        }
      } else {
        totalErrors++;
        waveErrors++;
        const msg = (r.reason as Error)?.message ?? 'unknown error';
        logger.logMinimal(`Batch error at wave ${waveStart}+${j}: ${msg}`);
      }
    }

    // Stop chunk if a whole wave failed
    if (waveErrors >= INNER_CONCURRENCY) {
      logger.logMinimal(`Entire wave failed (${INNER_CONCURRENCY} consecutive errors) — stopping chunk`);
      await jobLog(job.id, `Stopped: wave-level failure. Processed ${totalInserted} before halt.`);
      abort = true;
      break;
    }

    // Error rate check after warm-up
    const wavesDone = Math.floor(waveStart / INNER_CONCURRENCY) + 1;
    if (wavesDone >= 3 && totalErrors / (wavesDone * INNER_CONCURRENCY) > ERROR_RATE_THRESHOLD) {
      logger.logMinimal(`Error rate ${(totalErrors / (wavesDone * INNER_CONCURRENCY) * 100).toFixed(0)}% exceeds threshold — stopping chunk`);
      abort = true;
      break;
    }
  }

  const durationS = Math.round((Date.now() - startTime) / 1000);
  const rate = totalInserted > 0 ? (totalInserted / durationS).toFixed(1) : '0';

  // 3. Check if more records remain (re-run the same query with LIMIT 1)
  const pool = getPool();
  const remainingRecords = await getChunk(1, channel);
  const hasMore = remainingRecords.length > 0;

  logger.log(`Chunk complete: ${totalInserted} inserted, ${totalErrors} errors, ${durationS}s (${rate}/s), more=${hasMore}`);
  await jobLog(job.id, `Chunk: ${totalInserted} inserted, ${totalErrors} errors, ${durationS}s`);

  logEvent({
    action: `Sentiment: ${totalInserted} classified in ${durationS}s`,
    component: 'sentiment-backfill',
    category: 'background',
    metadata: { inserted: totalInserted, errors: totalErrors, chunk_size: chunkSize, duration_s: durationS },
  });

  // 4. Self-chain if more records remain — but only if no pending job already exists
  if (hasMore && !abort) {
    const { rows: existing } = await pool.query(
      `SELECT id FROM tempo_jobs WHERE job_type = 'sentiment-backfill' AND status = 'pending' LIMIT 1`,
    );
    if (existing.length === 0) {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
         VALUES ('sentiment-backfill', $1, 'pending', 0, 3)`,
        [JSON.stringify({
          channel,
          chunk_size: chunkSize,
          triggered_by: 'self-chain',
        })],
      );
      logger.logVerbose('Self-chained next chunk');
    } else {
      logger.logVerbose('Skipped self-chain — pending job already exists');
    }
  }

  return {
    status: hasMore ? 'chunk_complete' : 'complete',
    processed: totalInserted,
    errors: totalErrors,
    duration_s: durationS,
    rate_per_sec: parseFloat(rate),
    has_more: hasMore,
  };
}
