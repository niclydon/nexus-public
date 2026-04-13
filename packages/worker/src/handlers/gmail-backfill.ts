/**
 * Gmail Backfill Handler
 *
 * Archives every email from Gmail into the gmail_archive table by calling
 * the ARIA API in paginated batches. Self-chains to process the entire
 * mailbox without a single long-running job.
 *
 * After each batch, queues a knowledge-backfill job with source
 * 'gmail_archive' so the newly archived emails flow through the KG pipeline.
 *
 * Payload:
 *   page_token?:     string  -- Gmail pagination token (omit for first page)
 *   batch_size?:     number  -- Messages per batch (default 50)
 *   total_archived?: number  -- Running total for progress logging
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('gmail-backfill');

/** Delay helper for rate limiting. */
function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function handleGmailBackfill(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = (job.payload ?? {}) as {
    page_token?: string;
    batch_size?: number;
    total_archived?: number;
  };
  const pageToken = payload.page_token;
  const batchSize = payload.batch_size ?? 50;
  const totalArchived = payload.total_archived ?? 0;

  const ariaBaseUrl = process.env.ARIA_BASE_URL || process.env.NEXTAUTH_URL || 'https://aria.example.io';
  const apiToken = process.env.ARIA_API_TOKEN;

  if (!apiToken) {
    logger.logMinimal('ARIA_API_TOKEN not set -- cannot call backfill API');
    return { error: 'ARIA_API_TOKEN not configured' };
  }

  logger.log(`Starting batch: page_token=${pageToken || '(first)'}, batch_size=${batchSize}, total_so_far=${totalArchived}`);
  await jobLog(job.id, `Starting batch: page=${pageToken || '(first)'}, batch_size=${batchSize}, total_so_far=${totalArchived}`);

  // Rate limit: wait 2 seconds before hitting the Gmail API
  await delay(2000);

  const stopTimer = logger.time('backfill-api-call');
  let response: Response;
  try {
    response = await fetch(`${ariaBaseUrl}/api/gmail/backfill`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiToken}`,
      },
      body: JSON.stringify({ page_token: pageToken, batch_size: batchSize }),
    });
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('Network error calling backfill API:', msg);
    await jobLog(job.id, `Network error: ${msg}`, 'error');
    throw new Error(`Network error calling backfill API: ${msg}`);
  }
  stopTimer();

  if (!response.ok) {
    const errorText = await response.text().catch(() => response.statusText);
    logger.logMinimal(`Backfill API returned ${response.status}:`, errorText.slice(0, 500));
    await jobLog(job.id, `API error ${response.status}: ${errorText.slice(0, 200)}`, 'error');
    throw new Error(`Backfill API error: ${response.status}`);
  }

  const result = await response.json() as {
    archived: number;
    errors: number;
    next_page_token?: string;
    total_estimate: number;
  };

  const newTotal = totalArchived + result.archived;

  logger.log(
    `Batch complete: archived=${result.archived}, errors=${result.errors}, ` +
    `running_total=${newTotal}, total_estimate=${result.total_estimate}, ` +
    `next_page=${result.next_page_token ? 'yes' : 'done'}`
  );
  await jobLog(
    job.id,
    `Archived ${result.archived} emails (${result.errors} errors). ` +
    `Running total: ${newTotal} of ~${result.total_estimate}`
  );

  // Skip KG processing during bulk backfill — queue it only on the final batch
  // to avoid flooding the job queue with hundreds of knowledge-backfill jobs.
  // Once the full archive is complete, a single knowledge-backfill with
  // source='gmail_archive' can process everything.
  if (result.archived > 0 && !result.next_page_token) {
    try {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
         VALUES ($1, $2, 'pending', 0, 3)`,
        [
          'knowledge-backfill',
          JSON.stringify({ sources: ['gmail_archive'], batch_size: 200 }),
        ]
      );
      logger.log('Backfill complete — queued knowledge-backfill for gmail_archive');
    } catch (err) {
      logger.logMinimal('Failed to queue knowledge-backfill:', (err as Error).message);
    }
  }

  // Self-chain if there are more pages
  if (result.next_page_token) {
    try {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
         VALUES ($1, $2, 'pending', 0, 3)`,
        [
          'gmail-backfill',
          JSON.stringify({
            page_token: result.next_page_token,
            batch_size: batchSize,
            total_archived: newTotal,
          }),
        ]
      );
      logger.log(`Self-chained: next page queued (total_archived=${newTotal})`);
      await jobLog(job.id, `Queued next page (running total: ${newTotal})`);
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal('Failed to self-chain:', msg);
      await jobLog(job.id, `Failed to queue next page: ${msg}`, 'error');
      // Don't throw -- the current batch succeeded
    }
  } else {
    logger.log(`Backfill complete! Total archived: ${newTotal}`);
    await jobLog(job.id, `Backfill complete! Total archived: ${newTotal}`);
  }

  return {
    archived: result.archived,
    errors: result.errors,
    total_archived: newTotal,
    total_estimate: result.total_estimate,
    has_more: !!result.next_page_token,
  };
}
