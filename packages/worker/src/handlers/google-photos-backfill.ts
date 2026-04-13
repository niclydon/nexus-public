/**
 * Google Photos Backfill Handler
 *
 * Archives every photo's metadata from Google Photos into the
 * google_photos_archive table by calling the ARIA API in paginated
 * batches. Self-chains to process the entire library without a single
 * long-running job.
 *
 * Does NOT queue knowledge-backfill — this is metadata-only for AURORA.
 *
 * Payload:
 *   page_token?:     string  -- Google Photos pagination token (omit for first page)
 *   page_size?:      number  -- Items per batch (default 100)
 *   total_archived?: number  -- Running total for progress logging
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('google-photos-backfill');

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function handleGooglePhotosBackfill(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = (job.payload ?? {}) as {
    page_token?: string;
    page_size?: number;
    total_archived?: number;
  };
  const pageToken = payload.page_token;
  const pageSize = payload.page_size ?? 100;
  const totalArchived = payload.total_archived ?? 0;

  const ariaBaseUrl = process.env.ARIA_BASE_URL || process.env.NEXTAUTH_URL || 'https://aria.example.io';
  const apiToken = process.env.ARIA_API_TOKEN;

  if (!apiToken) {
    logger.logMinimal('ARIA_API_TOKEN not set — cannot call archive API');
    return { error: 'ARIA_API_TOKEN not configured' };
  }

  logger.log(`Starting batch: page_token=${pageToken || '(first)'}, page_size=${pageSize}, total_so_far=${totalArchived}`);
  await jobLog(job.id, `Starting batch: page=${pageToken || '(first)'}, page_size=${pageSize}, total_so_far=${totalArchived}`);

  // Rate limit: wait 2 seconds before hitting the Google Photos API
  await delay(2000);

  const stopTimer = logger.time('archive-api-call');
  let response: Response;
  try {
    response = await fetch(`${ariaBaseUrl}/api/photos/google-archive`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiToken}`,
      },
      body: JSON.stringify({ page_token: pageToken, page_size: pageSize }),
    });
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('Network error calling archive API:', msg);
    await jobLog(job.id, `Network error: ${msg}`, 'error');
    throw new Error(`Network error calling archive API: ${msg}`);
  }
  stopTimer();

  if (!response.ok) {
    const errorText = await response.text().catch(() => response.statusText);
    logger.logMinimal(`Archive API returned ${response.status}:`, errorText.slice(0, 500));
    await jobLog(job.id, `API error ${response.status}: ${errorText.slice(0, 200)}`, 'error');
    throw new Error(`Archive API error: ${response.status}`);
  }

  const result = await response.json() as {
    archived: number;
    errors: number;
    next_page_token?: string;
    total_processed: number;
  };

  const newTotal = totalArchived + result.archived;

  logger.log(
    `Batch complete: archived=${result.archived}, errors=${result.errors}, ` +
    `running_total=${newTotal}, processed=${result.total_processed}, ` +
    `next_page=${result.next_page_token ? 'yes' : 'done'}`
  );
  await jobLog(
    job.id,
    `Archived ${result.archived} photos (${result.errors} errors). Running total: ${newTotal}`
  );

  // Self-chain if there are more pages
  if (result.next_page_token) {
    try {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
         VALUES ($1, $2, 'pending', 0, 3)`,
        [
          'google-photos-backfill',
          JSON.stringify({
            page_token: result.next_page_token,
            page_size: pageSize,
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
    }
  } else {
    logger.log(`Backfill complete! Total archived: ${newTotal}`);
    await jobLog(job.id, `Backfill complete! Total archived: ${newTotal}`);
  }

  return {
    archived: result.archived,
    errors: result.errors,
    total_archived: newTotal,
    total_processed: result.total_processed,
    has_more: !!result.next_page_token,
  };
}
