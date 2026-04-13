/**
 * Gmail Transaction Extraction — LLM-powered structured data extraction
 * from transactional emails (Amazon, Uber, DoorDash, Airbnb, etc.)
 *
 * Two modes:
 * - Backfill: processes existing life_transactions with gmail_id but no enriched data
 * - Ongoing: triggered by enrichment pipeline for new transactional emails
 *
 * Uses classification tier LLM to extract: amount, items, location, order_id.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import { routeRequest } from '../lib/llm/router.js';

const logger = createLogger('gmail-tx-extract');

const BATCH_SIZE = 10;

// Known transactional senders for the ongoing mode
const TRANSACTIONAL_SENDERS = [
  'amazon', 'uber', 'doordash', 'airbnb', 'cash.app', 'venmo', 'paypal',
  'hotels.com', 'booking.com', 'expedia', 'jetblue', 'lyft', 'grubhub',
  'instacart', 'marriott', 'hilton', 'target', 'starbucks', 'klook',
  'cathay', 'icelandair', 'finnair', 'spirit', 'delta', 'aa.com',
  'southwest', 'british-airways', 'turkish', 'walmart',
];

interface TransactionExtraction {
  is_transaction: boolean;
  transaction_type: string;
  merchant: string;
  amount: number | null;
  currency: string;
  items: string[];
  location: string | null;
  order_id: string | null;
  confidence: number;
}

export async function handleGmailTransactionExtract(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { mode?: string; batch_size?: number; offset?: number };
  const batchSize = payload.batch_size ?? BATCH_SIZE;

  logger.log(`Starting Gmail transaction extraction (mode: ${payload.mode ?? 'backfill'})`);

  // Backfill mode: process existing unenriched life_transactions
  const { rows } = await pool.query<{
    id: string;
    gmail_id: string;
    source: string;
    transaction_type: string;
    subject: string;
    body_text: string | null;
    from_address: string;
    snippet: string | null;
  }>(`
    SELECT lt.id, lt.gmail_id, lt.source, lt.transaction_type, lt.subject,
           g.body_text, g.from_address, g.snippet
    FROM life_transactions lt
    JOIN aurora_raw_gmail g ON g.id = lt.gmail_id
    WHERE lt.enriched_at IS NULL AND lt.gmail_id IS NOT NULL
    ORDER BY lt.transaction_date DESC
    LIMIT $1
  `, [batchSize]);

  if (rows.length === 0) {
    logger.log('No unenriched transactions remaining');
    await jobLog(job.id, 'Backfill complete — all transactions enriched');
    return { status: 'complete', remaining: 0 };
  }

  logger.log(`Processing ${rows.length} transactions`);
  await jobLog(job.id, `Extracting from ${rows.length} emails`);

  let enriched = 0;
  let skipped = 0;
  let errors = 0;

  for (const row of rows) {
    const bodyText = row.body_text?.slice(0, 2000) ?? row.snippet ?? '';

    if (!bodyText || bodyText.length < 20) {
      // Mark as enriched with no data to avoid re-processing
      await pool.query(
        `UPDATE life_transactions SET enriched_at = NOW(), enrichment_confidence = 0 WHERE id = $1`,
        [row.id],
      );
      skipped++;
      continue;
    }

    try {
      const response = await routeRequest({
        handler: 'gmail-transaction-extract',
        taskTier: 'classification',
        systemPrompt: 'Extract transaction details from emails. Respond with ONLY valid JSON, no markdown.',
        userMessage: `Extract transaction details from this email. Respond with JSON.

From: ${row.from_address}
Subject: ${row.subject}
Body (first 2000 chars):
${bodyText}

{
  "is_transaction": true,
  "transaction_type": "${row.transaction_type || 'unknown'}",
  "merchant": "store/restaurant/service name",
  "amount": 29.99,
  "currency": "USD",
  "items": ["item 1", "item 2"],
  "location": "city, state or address if mentioned",
  "order_id": "order/confirmation number if visible",
  "confidence": 0.9
}`,
        maxTokens: 300,
      });

      let extraction: TransactionExtraction;
      try {
        const jsonMatch = response.text.match(/\{[\s\S]*\}/);
        if (!jsonMatch) throw new Error('No JSON');
        extraction = JSON.parse(jsonMatch[0]);
      } catch {
        logger.logVerbose(`Failed to parse LLM response for ${row.id}`);
        await pool.query(
          `UPDATE life_transactions SET enriched_at = NOW(), enrichment_confidence = 0 WHERE id = $1`,
          [row.id],
        );
        skipped++;
        continue;
      }

      // Update the transaction record
      await pool.query(
        `UPDATE life_transactions SET
          amount = $1,
          merchant = COALESCE($2, merchant),
          items = $3,
          location = COALESCE($4, location),
          order_id = $5,
          address = $6,
          enrichment_confidence = $7,
          enriched_at = NOW(),
          metadata = metadata || $8
        WHERE id = $9`,
        [
          extraction.amount ? String(extraction.amount) : null,
          extraction.merchant || null,
          extraction.items?.length > 0 ? JSON.stringify(extraction.items) : null,
          extraction.location || null,
          extraction.order_id || null,
          extraction.location || null, // use location as address too
          extraction.confidence,
          JSON.stringify({ currency: extraction.currency, llm_extracted: true }),
          row.id,
        ],
      );
      enriched++;

    } catch (err) {
      logger.logVerbose(`LLM error for ${row.id}:`, (err as Error).message);
      errors++;
      // Don't mark as enriched so it gets retried
      if (errors > batchSize * 0.3) {
        logger.logMinimal('Error rate too high, stopping batch');
        break;
      }
    }
  }

  // Count remaining
  const { rows: [countRow] } = await pool.query<{ remaining: string }>(
    `SELECT COUNT(*)::text AS remaining FROM life_transactions WHERE enriched_at IS NULL AND gmail_id IS NOT NULL`,
  );
  const remaining = parseInt(countRow.remaining);

  // Self-chain if more to process
  if (remaining > 0 && errors < batchSize * 0.3) {
    // Check no existing pending job
    const { rows: existing } = await pool.query(
      `SELECT id FROM tempo_jobs WHERE job_type = 'gmail-transaction-extract' AND status = 'pending' LIMIT 1`,
    );
    if (existing.length === 0) {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, executor, priority)
         VALUES ('gmail-transaction-extract', $1, 'nexus', 0)`,
        [JSON.stringify({ mode: 'backfill', batch_size: batchSize })],
      );
    }
  }

  const result = { enriched, skipped, errors, remaining };
  logger.log(`Batch complete: ${enriched} enriched, ${skipped} skipped, ${errors} errors, ${remaining} remaining`);
  await jobLog(job.id, `Enriched ${enriched}, skipped ${skipped}, ${remaining} remaining`);
  return result;
}
