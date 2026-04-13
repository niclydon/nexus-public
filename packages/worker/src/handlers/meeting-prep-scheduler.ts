/**
 * Meeting prep scheduler handler.
 *
 * Self-chains every 15 minutes. Scans device_calendar_events for meetings
 * starting in the next 60–90 minutes, resolves attendees via aurora_social_identities,
 * gathers relationship/sentiment context, and queues calendar-prep jobs
 * with enriched payloads. Deduplicates against recently queued prep jobs.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('meeting-prep-scheduler');

const CHAIN_INTERVAL_SECONDS = 900; // 15 minutes
const WINDOW_MIN_MINUTES = 60;
const WINDOW_MAX_MINUTES = 90;

interface CalendarEvent {
  id: string;
  title: string;
  start_date: Date;
  end_date: Date | null;
  location: string | null;
  attendees: string[] | null;
  notes: string | null;
}

interface AttendeeContext {
  name: string;
  email: string | null;
  person_id: string | null;
  last_interaction: string | null;
  relationship_health: number | null;
  recent_sentiment: string | null;
  sentiment_score: number | null;
}

/**
 * Resolve an attendee string to a person_id via aurora_social_identities.
 * Attendees can be "Name <email>" or just "email" or just "Name".
 */
async function resolveAttendee(
  pool: ReturnType<typeof getPool>,
  attendee: string
): Promise<{ name: string; email: string | null; person_id: string | null }> {
  const emailMatch = attendee.match(/<([^>]+)>/);
  const email = emailMatch ? emailMatch[1] : attendee.includes('@') ? attendee.trim() : null;
  const name = attendee.split(/[<@]/)[0].trim() || attendee;

  if (!email && !name) {
    return { name: attendee, email: null, person_id: null };
  }

  try {
    // Try email first (most reliable), then name
    if (email) {
      const { rows } = await pool.query<{ person_id: string }>(
        `SELECT person_id FROM aurora_social_identities
         WHERE identity_value ILIKE $1 AND person_id IS NOT NULL
         LIMIT 1`,
        [email]
      );
      if (rows.length > 0) {
        return { name, email, person_id: rows[0].person_id };
      }
    }

    if (name && name.length > 1) {
      const { rows } = await pool.query<{ person_id: string }>(
        `SELECT person_id FROM aurora_social_identities
         WHERE identity_value ILIKE $1 AND person_id IS NOT NULL
         LIMIT 1`,
        [`%${name}%`]
      );
      if (rows.length > 0) {
        return { name, email, person_id: rows[0].person_id };
      }
    }
  } catch (err) {
    logger.logVerbose(`Failed to resolve attendee "${attendee}":`, (err as Error).message);
  }

  return { name, email, person_id: null };
}

/**
 * Gather relationship and sentiment context for a resolved person_id.
 */
async function gatherAttendeeContext(
  pool: ReturnType<typeof getPool>,
  personId: string
): Promise<{
  last_interaction: string | null;
  relationship_health: number | null;
  recent_sentiment: string | null;
  sentiment_score: number | null;
}> {
  const result = {
    last_interaction: null as string | null,
    relationship_health: null as number | null,
    recent_sentiment: null as string | null,
    sentiment_score: null as number | null,
  };

  try {
    // Last interaction from aurora_unified_communication
    const { rows: commRows } = await pool.query<{ timestamp: Date }>(
      `SELECT timestamp FROM aurora_unified_communication
       WHERE identity_id = $1
       ORDER BY timestamp DESC LIMIT 1`,
      [personId]
    );
    if (commRows.length > 0) {
      result.last_interaction = commRows[0].timestamp.toISOString();
    }
  } catch {
    logger.logDebug(`No unified communication data for person ${personId}`);
  }

  try {
    // Relationship health from aurora_relationships
    const { rows: relRows } = await pool.query<{ health_score: number }>(
      `SELECT health_score FROM aurora_relationships
       WHERE person_id = $1
       LIMIT 1`,
      [personId]
    );
    if (relRows.length > 0) {
      result.relationship_health = relRows[0].health_score;
    }
  } catch {
    logger.logDebug(`No relationship data for person ${personId}`);
  }

  try {
    // Recent sentiment from aurora_sentiment
    const { rows: sentRows } = await pool.query<{ sentiment: string; score: number }>(
      `SELECT sentiment, score FROM aurora_sentiment
       WHERE person_id = $1
       ORDER BY created_at DESC LIMIT 1`,
      [personId]
    );
    if (sentRows.length > 0) {
      result.recent_sentiment = sentRows[0].sentiment;
      result.sentiment_score = sentRows[0].score;
    }
  } catch {
    logger.logDebug(`No sentiment data for person ${personId}`);
  }

  return result;
}

/**
 * Check if a calendar-prep job was already queued for this event recently.
 */
async function isDuplicatePrep(pool: ReturnType<typeof getPool>, eventId: string): Promise<boolean> {
  try {
    const { rows } = await pool.query<{ id: string }>(
      `SELECT id FROM tempo_jobs
       WHERE job_type = 'calendar-prep'
         AND payload::text ILIKE $1
         AND created_at > NOW() - INTERVAL '2 hours'
       LIMIT 1`,
      [`%${eventId}%`]
    );
    return rows.length > 0;
  } catch (err) {
    logger.logMinimal('Dedup check failed:', (err as Error).message);
    return false; // Err on the side of queuing
  }
}

async function handleMeetingPrepSchedulerCore(job: TempoJob): Promise<Record<string, unknown>> {
  const t0 = Date.now();
  const pool = getPool();

  logger.log('Scanning for upcoming meetings...');
  await jobLog(job.id, 'Checking for meetings in the next 60-90 minutes');

  // Find events starting in the 60-90 minute window
  const { rows: events } = await pool.query<CalendarEvent>(
    `SELECT id, title, start_date, end_date, location, attendees, notes
     FROM device_calendar_events
     WHERE start_date > NOW() + INTERVAL '${WINDOW_MIN_MINUTES} minutes'
       AND start_date <= NOW() + INTERVAL '${WINDOW_MAX_MINUTES} minutes'
       AND start_date > NOW()
     ORDER BY start_date ASC`,
  );

  if (events.length === 0) {
    const ms = Date.now() - t0;
    logger.logVerbose(`No upcoming meetings found (${ms}ms)`);
    return { status: 'no_events', scanned_in_ms: ms };
  }

  logger.log(`Found ${events.length} upcoming event(s) in the prep window`);

  let queued = 0;
  let skippedDuplicate = 0;
  let skippedNoAttendees = 0;

  for (const event of events) {
    const attendees = Array.isArray(event.attendees) ? event.attendees : [];

    // Skip events with no attendees (solo blocks, focus time, etc.)
    if (attendees.length === 0) {
      logger.logVerbose(`Skipping "${event.title}" — no attendees`);
      skippedNoAttendees++;
      continue;
    }

    // Check for duplicate prep jobs
    const alreadyQueued = await isDuplicatePrep(pool, event.id);
    if (alreadyQueued) {
      logger.logVerbose(`Skipping "${event.title}" — prep already queued`);
      skippedDuplicate++;
      continue;
    }

    // Resolve attendees and gather context
    const attendeeContexts: AttendeeContext[] = [];

    for (const attendee of attendees.slice(0, 10)) { // Cap at 10 attendees
      const resolved = await resolveAttendee(pool, attendee);

      let context: AttendeeContext = {
        name: resolved.name,
        email: resolved.email,
        person_id: resolved.person_id,
        last_interaction: null,
        relationship_health: null,
        recent_sentiment: null,
        sentiment_score: null,
      };

      if (resolved.person_id) {
        const enriched = await gatherAttendeeContext(pool, resolved.person_id);
        context = { ...context, ...enriched };
      }

      attendeeContexts.push(context);
    }

    const resolvedCount = attendeeContexts.filter(a => a.person_id !== null).length;
    logger.log(
      `"${event.title}" — ${attendees.length} attendees, ${resolvedCount} resolved to person_id`
    );

    // Queue the calendar-prep job with enriched payload
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
       VALUES ('calendar-prep', $1, 'nexus', 5, 2, NOW())`,
      [
        JSON.stringify({
          event_id: event.id,
          event_title: event.title,
          start_date: event.start_date,
          end_date: event.end_date,
          location: event.location,
          notes: event.notes,
          attendees: attendees,
          attendee_context: attendeeContexts,
          recipient: 'owner',
          queued_by: 'meeting-prep-scheduler',
        }),
      ]
    );

    await jobLog(
      job.id,
      `Queued calendar-prep for "${event.title}" (${resolvedCount}/${attendees.length} attendees resolved)`
    );
    queued++;
  }

  const ms = Date.now() - t0;
  logger.log(
    `Done in ${ms}ms — queued: ${queued}, duplicate: ${skippedDuplicate}, no-attendees: ${skippedNoAttendees}`
  );

  return {
    status: 'ok',
    events_found: events.length,
    prep_jobs_queued: queued,
    skipped_duplicate: skippedDuplicate,
    skipped_no_attendees: skippedNoAttendees,
    duration_ms: ms,
  };
}

/**
 * Exported handler with self-chaining.
 * After completion (success or failure), queues the next run 15 minutes out.
 */
export async function handleMeetingPrepScheduler(job: TempoJob): Promise<Record<string, unknown>> {
  try {
    return await handleMeetingPrepSchedulerCore(job);
  } finally {
    // Self-chain with pile-up guard — skip if one is already pending.
    try {
      const pool = getPool();
      const existing = await pool.query(
        `SELECT id FROM tempo_jobs WHERE job_type = 'meeting-prep-scheduler' AND status = 'pending' LIMIT 1`,
      );
      if (existing.rows.length === 0) {
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
           VALUES ('meeting-prep-scheduler', '{}', 'nexus', 3, 3, NOW() + INTERVAL '${CHAIN_INTERVAL_SECONDS} seconds')`,
        );
        logger.logVerbose(`Chained next run in ${CHAIN_INTERVAL_SECONDS}s`);
      }
    } catch (err) {
      logger.logMinimal('Failed to chain next run:', (err as Error).message);
    }
  }
}
