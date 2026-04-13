/**
 * Calendar Sync — poller that runs every 5 minutes.
 * Pulls events via Google Drive MCP, pushes through ingestion pipeline.
 * MCP returns plain text (not JSON) — parser extracts structured events.
 * Self-chains after completion.
 */
import { getPool, createLogger } from '@nexus/core';
import { callMcpTool } from '../lib/mcp-client-manager.js';
import { ingest } from '../lib/ingestion-pipeline.js';
import { triageUrgency, type TriageItem } from '../lib/urgency-triage.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/calendar-sync');

const INTERVAL_SECONDS = 300;            // 5 minutes (healthy)
const BACKOFF_INTERVAL_SECONDS = 1800;   // 30 minutes (after failures / disabled)
const MAX_CONSECUTIVE_FAILURES_BEFORE_DISABLE = 5;

export async function handleCalendarSync(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  // When true, self-chain uses the long backoff instead of the healthy 5-min cadence.
  let useBackoff = false;

  try {
    // Short-circuit if the data source has been disabled (e.g. OAuth expired,
    // user paused it). Self-chain on backoff cadence so the handler resumes
    // automatically when someone flips enabled=true.
    const sourceCheck = await pool.query<{ enabled: boolean; status: string; consecutive_failures: number }>(
      `SELECT enabled, status, COALESCE(consecutive_failures, 0) AS consecutive_failures
       FROM data_source_registry WHERE source_key = 'google_calendar'`,
    );
    if (sourceCheck.rows.length > 0 && !sourceCheck.rows[0].enabled) {
      logger.logVerbose('google_calendar disabled in data_source_registry, skipping');
      useBackoff = true;
      return { skipped: true, reason: 'data_source_disabled' };
    }

    const now = new Date();
    const timeMin = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const timeMax = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000).toISOString();

    logger.logVerbose(`Checking calendar: ${timeMin} → ${timeMax}`);

    let eventsResult: string;
    try {
      eventsResult = await callMcpTool('google-drive', 'getCalendarEvents', {
        timeMin,
        timeMax,
        maxResults: 50,
        singleEvents: true,
        orderBy: 'startTime',
      });
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal('Calendar MCP error:', msg);
      // Record the failure so we can back off exponentially + auto-disable.
      const updated = await pool.query<{ consecutive_failures: number }>(
        `UPDATE data_source_registry
         SET consecutive_failures = COALESCE(consecutive_failures, 0) + 1,
             last_error = $1,
             last_error_at = NOW(),
             status = 'error'
         WHERE source_key = 'google_calendar'
         RETURNING consecutive_failures`,
        [msg.slice(0, 500)],
      );
      const failureCount = updated.rows[0]?.consecutive_failures ?? 0;
      useBackoff = true;
      if (failureCount >= MAX_CONSECUTIVE_FAILURES_BEFORE_DISABLE) {
        await pool.query(
          `UPDATE data_source_registry SET enabled = false WHERE source_key = 'google_calendar'`,
        );
        logger.logMinimal(
          `google_calendar auto-disabled after ${failureCount} consecutive failures. ` +
          `Complete OAuth re-auth and flip enabled=true in data_source_registry to resume.`,
        );
      }
      return { error: msg, checked: false, consecutive_failures: failureCount };
    }

    // Parse events from text or JSON response
    let events: Record<string, unknown>[];
    try {
      const parsed = JSON.parse(eventsResult);
      const items = Array.isArray(parsed) ? parsed : (parsed.items ?? parsed.events ?? []);
      events = items as Record<string, unknown>[];
    } catch {
      // MCP returns plain text — parse it
      events = parseCalendarText(eventsResult);
    }

    if (events.length === 0) {
      logger.logVerbose('No events in window');
      await pool.query(
        `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active' WHERE source_key = 'google_calendar'`,
      );
      return { checked: true, new_events: 0 };
    }

    logger.logVerbose(`Parsed ${events.length} calendar events`);

    // Push through pipeline
    const result = await ingest('google_calendar', events);

    // Urgency triage — flag events starting within 2 hours
    let urgentCount = 0;
    if (result.ingested > 0) {
      const soonEvents = events.filter(e => {
        const start = e.start as Record<string, string> | undefined;
        const dateStr = start?.dateTime ?? start?.date ?? String(e._start_date ?? '');
        const startTime = new Date(dateStr).getTime();
        return startTime > now.getTime() && startTime < now.getTime() + 2 * 60 * 60 * 1000;
      });

      if (soonEvents.length > 0) {
        const triageItems: TriageItem[] = soonEvents.map(e => ({
          from: String(e._attendees ?? 'No attendees'),
          subject: String(e.summary ?? e.title ?? e._title ?? '(no title)'),
          preview: `Starts: ${e._start_date ?? 'unknown'}\nLocation: ${String(e.location ?? 'none')}`,
        }));
        urgentCount = await triageUrgency(triageItems, { insightCategory: 'calendar_urgent' });
      }
    }

    // Success: reset failure counter.
    await pool.query(
      `UPDATE data_source_registry
       SET consecutive_failures = 0, last_error = NULL, status = 'active'
       WHERE source_key = 'google_calendar'`,
    );

    logger.log(`Calendar sync: ${result.ingested} new, ${result.skipped} dupes, ${urgentCount} urgent`);

    return {
      checked: true,
      new_events: result.ingested,
      duplicates: result.skipped,
      proactive_actions: result.proactive,
      enrichments_queued: result.enrichments,
      urgent_flagged: urgentCount,
    };
  } finally {
    // Self-chain, but honor the backoff flag set by failure paths. Also
    // guard against piling on: if there's already a pending future-scheduled
    // calendar-sync, don't add another.
    try {
      const existing = await pool.query(
        `SELECT id FROM tempo_jobs
         WHERE job_type = 'calendar-sync' AND status = 'pending' LIMIT 1`,
      );
      if (existing.rows.length === 0) {
        const intervalSec = useBackoff ? BACKOFF_INTERVAL_SECONDS : INTERVAL_SECONDS;
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
           VALUES ('calendar-sync', '{}', 'nexus', 3, 3, NOW() + ($1 || ' seconds')::interval)`,
          [intervalSec],
        );
      }
    } catch (err) {
      logger.logMinimal('Failed to self-chain calendar-sync:', (err as Error).message);
    }
  }
}

/**
 * Parse calendar events from Google Drive MCP plain text response.
 *
 * Format:
 * Found N event(s):
 *
 * **Event Title**
 * Date: 2026-03-30 - 2026-03-31
 * Time: 10:00 AM - 11:00 AM (optional)
 * Location: Some Place (optional)
 * Description: ... (optional)
 * Link: https://...
 * Event ID: abc123
 *
 * ---
 */
function parseCalendarText(text: string): Record<string, unknown>[] {
  const events: Record<string, unknown>[] = [];
  const blocks = text.split('---').filter(b => b.trim());

  for (const block of blocks) {
    const lines = block.trim().split('\n').map(l => l.trim()).filter(Boolean);

    // Skip the "Found N event(s):" header
    if (lines.length < 2) continue;

    let title = '';
    let startDate = '';
    let endDate = '';
    let location = '';
    let description = '';
    let eventId = '';
    let link = '';

    for (const line of lines) {
      if (line.startsWith('**') && line.endsWith('**')) {
        title = line.replace(/\*\*/g, '').trim();
      } else if (line.startsWith('Date:')) {
        const datePart = line.replace('Date:', '').trim();
        const parts = datePart.split(' - ');
        startDate = parts[0]?.trim() ?? '';
        endDate = parts[1]?.trim() ?? startDate;
      } else if (line.startsWith('Time:')) {
        // If there's a time, append it to the date
        const timePart = line.replace('Time:', '').trim();
        const parts = timePart.split(' - ');
        if (startDate && parts[0]) {
          startDate = `${startDate}T${convertTo24h(parts[0].trim())}`;
          if (endDate && parts[1]) {
            endDate = `${endDate}T${convertTo24h(parts[1].trim())}`;
          }
        }
      } else if (line.startsWith('Location:')) {
        location = line.replace('Location:', '').trim();
      } else if (line.startsWith('Description:')) {
        description = line.replace('Description:', '').trim();
      } else if (line.startsWith('Event ID:')) {
        eventId = line.replace('Event ID:', '').trim();
      } else if (line.startsWith('Link:')) {
        link = line.replace('Link:', '').trim();
      } else if (line.match(/^Found \d+ event/)) {
        // Skip header line
        continue;
      }
    }

    if (!eventId && !title) continue;

    // Build a structure that matches what the google_calendar normalizer expects
    events.push({
      id: eventId || title.replace(/\s+/g, '_').toLowerCase(),
      summary: title,
      title,
      description,
      location,
      status: 'confirmed',
      start: { date: startDate, dateTime: startDate.includes('T') ? startDate : undefined },
      end: { date: endDate, dateTime: endDate.includes('T') ? endDate : undefined },
      htmlLink: link,
      attendees: [],
      // Extra fields for triage
      _title: title,
      _start_date: startDate,
    });
  }

  return events;
}

function convertTo24h(time12h: string): string {
  const match = time12h.match(/(\d{1,2}):(\d{2})\s*(AM|PM)/i);
  if (!match) return '00:00:00';
  let hours = parseInt(match[1], 10);
  const minutes = match[2];
  const period = match[3].toUpperCase();
  if (period === 'PM' && hours < 12) hours += 12;
  if (period === 'AM' && hours === 12) hours = 0;
  return `${String(hours).padStart(2, '0')}:${minutes}:00`;
}
