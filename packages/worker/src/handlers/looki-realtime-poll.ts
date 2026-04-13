/**
 * Looki Realtime Poll handler.
 *
 * Polls GET /realtime/latest-event every 60 seconds (scheduled via recurring job).
 * Deduplicates by event ID and stores new events in looki_realtime_events.
 * The context accumulator reads from this table every 5 minutes to feed PIE,
 * picking up ALL events in the window rather than just the latest.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { getAllFastTrackPatterns } from '../lib/proactive/adaptive-fast-track.js';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('looki-realtime-poll');

const LOOKI_FETCH_TIMEOUT_MS = 10_000;

export async function handleLookiRealtimePoll(job: TempoJob): Promise<Record<string, unknown>> {
  const t0 = Date.now();

  const apiKey = process.env.LOOKI_API_KEY;
  const baseUrl = process.env.LOOKI_BASE_URL || 'https://open.looki.ai/api/v1';

  if (!apiKey) {
    return { status: 'skipped', reason: 'LOOKI_API_KEY not configured' };
  }

  // Check how long this job waited in the queue before being claimed
  const queueWaitMs = job.claimed_at ? new Date(job.claimed_at).getTime() - new Date(job.created_at).getTime() : 0;
  if (queueWaitMs > 60_000) {
    await jobLog(job.id, `Warning: waited ${Math.round(queueWaitMs / 1000)}s in queue before being claimed`, 'warn');
  }

  await jobLog(job.id, 'Fetching latest event from Looki API...');

  // Fetch latest event
  let data: {
    code?: number;
    data?: {
      id?: string;
      description?: string;
      start_time?: string;
      end_time?: string;
      tz?: string;
      location?: string;
    };
  };

  try {
    const resp = await fetch(`${baseUrl}/realtime/latest-event`, {
      headers: {
        'X-API-Key': apiKey,
        'Accept': 'application/json',
      },
      signal: AbortSignal.timeout(LOOKI_FETCH_TIMEOUT_MS),
    });

    if (!resp.ok) {
      const body = await resp.text().catch(() => '');
      logger.logMinimal(`API HTTP ${resp.status}: ${body.substring(0, 300)}`);
      return { status: 'error', reason: `HTTP ${resp.status}`, body: body.substring(0, 300) };
    }
    data = await resp.json() as typeof data;
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal(`Fetch failed: ${msg}`);
    return { status: 'error', reason: msg };
  }

  // Check if proactive mode returned data
  if (data.code !== 0 || !data.data?.id) {
    return { status: 'no_event', code: data.code };
  }

  const event = data.data;
  const pool = getPool();

  // Deduplicate: check if this event ID already exists.
  // Also fetch the most recent event's description to detect a stuck Looki description.
  const { rows: existing } = await pool.query<{ description: string }>(
    `SELECT id, description FROM looki_realtime_events WHERE looki_event_id = $1 LIMIT 1`,
    [event.id]
  );

  if (existing.length > 0) {
    await pool.query(
      `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active', consecutive_failures = 0 WHERE source_key = 'looki'`,
    );
    return { status: 'duplicate', looki_event_id: event.id };
  }

  // Warn if Looki keeps generating new event IDs but the description hasn't changed.
  // This usually means the wearable isn't detecting activity changes (offline, uncharged, etc.)
  const { rows: prevEvents } = await pool.query<{ description: string; created_at: Date }>(
    `SELECT description, created_at FROM looki_realtime_events ORDER BY created_at DESC LIMIT 1`
  );
  if (prevEvents.length > 0 && prevEvents[0].description === (event.description ?? '')) {
    const prevAgeMs = Date.now() - prevEvents[0].created_at.getTime();
    const prevAgeMin = Math.round(prevAgeMs / 60_000);
    logger.logMinimal(
      `New event ID ${event.id} but description unchanged for ~${prevAgeMin} min: "${(event.description ?? '').substring(0, 80)}"`
    );
  }

  // Parse location JSON string
  let location: Record<string, unknown> | null = null;
  if (event.location) {
    try {
      location = JSON.parse(event.location);
    } catch {
      location = { raw: event.location };
    }
  }

  // Store the new event
  await pool.query(
    `INSERT INTO looki_realtime_events (looki_event_id, description, start_time, end_time, timezone, location)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [
      event.id,
      event.description ?? '',
      event.start_time ?? null,
      event.end_time ?? null,
      event.tz ?? null,
      location ? JSON.stringify(location) : null,
    ]
  );

  await pool.query(
    `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active', consecutive_failures = 0 WHERE source_key = 'looki'`,
  );

  const fetchMs = Date.now() - t0;
  logger.log(`Stored event ${event.id} in ${fetchMs}ms: ${(event.description ?? '').substring(0, 80)}...`);
  await jobLog(job.id, `Stored event ${event.id} (fetch+store: ${fetchMs}ms)`);

  // Fast-track pre-screen: if the event description matches adaptive patterns,
  // create a context snapshot immediately and chain directly to Gate 2 (skip 5-min batch).
  const description = event.description ?? '';
  const fastTrackPatterns = await getAllFastTrackPatterns(pool);
  const isFastTrack = fastTrackPatterns.some(p => p.test(description));

  if (isFastTrack) {
    logger.log(`Fast-tracking event ${event.id} — safety/location signal detected`);

    try {
      // Create a fast-tracked context snapshot
      const { rows: snapRows } = await pool.query<{ id: string }>(
        `INSERT INTO context_snapshots (source, change_summary, change_data, fast_tracked)
         VALUES ('looki_realtime', $1, $2, true)
         RETURNING id`,
        [
          `Looki realtime: ${description.substring(0, 200)}`,
          JSON.stringify({
            event_id: event.id,
            description,
            start_time: event.start_time,
            end_time: event.end_time,
            location,
            fast_tracked_reason: 'safety_location_signal',
          }),
        ]
      );

      // Chain directly to Gate 2 with fast_tracked flag and elevated priority
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
         VALUES ($1, $2, $3, $4)`,
        [
          'significance-check',
          JSON.stringify({
            snapshot_ids: [snapRows[0].id],
            fast_tracked: true,
          }),
          3, // High priority
          2,
        ]
      );

      return {
        status: 'fast_tracked',
        looki_event_id: event.id,
        snapshot_id: snapRows[0].id,
        description_preview: description.substring(0, 100),
      };
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal(`Fast-track failed, falling back to standard pipeline: ${msg}`);
      // Fall through to standard return — the accumulator will pick it up
    }
  }

  // Standard path: context accumulator reads from looki_realtime_events every 5 minutes.
  return {
    status: 'stored',
    looki_event_id: event.id,
    description_preview: description.substring(0, 100),
  };
}

// Self-chain: enqueue next poll after handler completes (called by job-worker wrapper)
// We add this via a finally-like pattern in the handler registration
import { getPool as getPoolForChain } from '@nexus/core';
const LOOKI_INTERVAL_SECONDS = 60;

// Wrap the original handler to add self-chaining
const _originalHandler = handleLookiRealtimePoll;
export { _originalHandler };

// Note: self-chaining is handled by overriding the export below
export async function handleLookiRealtimePollWithChain(job: TempoJob): Promise<Record<string, unknown>> {
  try {
    return await _originalHandler(job);
  } finally {
    // Self-chain with pile-up guard — skip if one is already pending.
    try {
      const pool = getPoolForChain();
      const existing = await pool.query(
        `SELECT id FROM tempo_jobs WHERE job_type = 'looki-realtime-poll' AND status = 'pending' LIMIT 1`,
      );
      if (existing.rows.length === 0) {
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
           VALUES ('looki-realtime-poll', '{}', 'nexus', 3, 3, NOW() + INTERVAL '${LOOKI_INTERVAL_SECONDS} seconds')`,
        );
      }
    } catch {
      // Silently fail — stale reaper or next schedule will pick it up
    }
  }
}
