/**
 * Calendar prep handler.
 *
 * Sends a pre-meeting briefing via push notification ~15 minutes before an event.
 * Includes: who you're meeting, what you last discussed with them,
 * relevant email threads, and any memory context.
 *
 * Requires: ANTHROPIC_API_KEY, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { deliverReport } from '../lib/report-delivery.js';
import { routeRequest } from '../lib/llm/index.js';

const logger = createLogger('calendar-prep');

/**
 * Fetch event details from device_calendar_events or ingestion data.
 * Falls back to basic info if full details unavailable.
 */
async function fetchEventDetails(eventId: string): Promise<{
  summary: string;
  description: string;
  location: string;
  start: string;
  end: string;
  attendees: string[];
  organizer: string;
} | null> {
  const pool = getPool();

  try {
    // Try device_calendar_events first (synced via calendar-sync)
    const { rows } = await pool.query(`
      SELECT title, location, start_date, end_date, notes, organizer, attendees
      FROM device_calendar_events
      WHERE id = $1 OR title ILIKE '%' || $1 || '%'
      LIMIT 1
    `, [eventId]);

    if (rows.length === 0) return null;

    const event = rows[0];
    const attendeeList = Array.isArray(event.attendees) ? event.attendees : [];

    return {
      summary: event.title ?? '(No title)',
      description: event.notes ?? '',
      location: event.location ?? '',
      start: event.start_date?.toISOString() ?? '',
      end: event.end_date?.toISOString() ?? '',
      attendees: attendeeList.filter((a: string) => a && a.length > 0),
      organizer: event.organizer ?? '',
    };
  } catch (err) {
    logger.logMinimal(`Failed to fetch event ${eventId}:`, (err as Error).message);
    return null;
  }
}

/**
 * Search memory for any facts about the attendees.
 */
async function fetchAttendeeMemory(attendees: string[]): Promise<string> {
  if (attendees.length === 0) return '';

  const pool = getPool();

  // Search core memory for mentions of any attendee name
  const conditions = attendees.map((_, i) => `(key ILIKE $${i + 1} OR value ILIKE $${i + 1})`).join(' OR ');
  const params = attendees.map(a => `%${a.split(/[<@]/)[0].trim()}%`);

  try {
    const { rows } = await pool.query(
      `SELECT category, key, value FROM core_memory
       WHERE superseded_by IS NULL AND (${conditions})
       LIMIT 10`,
      params
    );

    if (rows.length === 0) return '';

    const lines = rows.map(r => `  - [${r.category}] ${r.key}: ${r.value}`);
    return `Memory about attendees:\n${lines.join('\n')}`;
  } catch {
    return '';
  }
}

/**
 * Search recent emails for threads involving attendees.
 */
async function fetchRecentEmailContext(attendees: string[]): Promise<string> {
  if (attendees.length === 0) return '';

  // Extract email addresses from attendees
  const emailAddresses = attendees
    .map(a => {
      const match = a.match(/<([^>]+)>/);
      return match ? match[1] : a;
    })
    .filter(a => a.includes('@'));

  if (emailAddresses.length === 0) return '';

  const pool = getPool();

  try {
    // Query aurora_raw_gmail for recent emails from/to attendees (last 14 days)
    const addrs = emailAddresses.slice(0, 3);
    const result = await pool.query(
      `SELECT from_address, from_name, subject, date FROM aurora_raw_gmail
       WHERE date > NOW() - INTERVAL '14 days'
         AND (from_address = ANY($1) OR to_addresses && $1::text[])
       ORDER BY date DESC LIMIT 5`,
      [addrs]
    );

    if (result.rows.length === 0) return '';

    const threads: string[] = [];
    for (const row of result.rows) {
      const from = row.from_name ? `${row.from_name} <${row.from_address}>` : row.from_address;
      const subject = row.subject || '(No subject)';
      const date = new Date(row.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      threads.push(`  - ${date}: ${from} — "${subject}"`);
    }

    return `Recent email threads with attendees:\n${threads.join('\n')}`;
  } catch {
    return '';
  }
}

/**
 * Search knowledge graph for entity summaries about attendees.
 */
async function fetchKGContext(attendees: string[]): Promise<string> {
  if (attendees.length === 0) return '';

  const pool = getPool();

  try {
    const names = attendees.map(a => a.split(/[<@]/)[0].trim()).filter(Boolean);
    if (names.length === 0) return '';

    const conditions = names.map((_, i) => `canonical_name ILIKE $${i + 1}`).join(' OR ');
    const params = names.map(n => `%${n}%`);

    const { rows } = await pool.query(
      `SELECT canonical_name, entity_type, summary FROM knowledge_entities
       WHERE (${conditions}) AND summary IS NOT NULL
       LIMIT 5`,
      params
    );

    if (rows.length === 0) return '';

    const lines = rows.map(r => `  - ${r.canonical_name} (${r.entity_type}): ${r.summary.slice(0, 300)}`);
    return `Knowledge graph context:\n${lines.join('\n')}`;
  } catch {
    return '';
  }
}

export async function handleCalendarPrep(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { event_id: string; recipient: string };
  logger.log(`Preparing briefing for event ${payload.event_id}`);

  const eventDetails = await fetchEventDetails(payload.event_id);
  if (!eventDetails) {
    throw new Error(`Could not fetch event details for ${payload.event_id}`);
  }

  // Gather context in parallel
  const [attendeeMemory, emailContext, kgContext] = await Promise.all([
    fetchAttendeeMemory(eventDetails.attendees),
    fetchRecentEmailContext(eventDetails.attendees),
    fetchKGContext(eventDetails.attendees),
  ]);

  const result = await routeRequest({
    handler: 'calendar-prep',
    taskTier: 'generation',
    systemPrompt: `You are ARIA, a personal AI assistant. Compose a concise pre-meeting briefing for your owner. Include: what the meeting is about, who's attending, any relevant context you have about them, and recent related email threads. Be direct and scannable. No emojis. Max 1400 characters.`,
    userMessage: `Prepare me for this meeting:\n\nEvent: ${eventDetails.summary}\nTime: ${eventDetails.start} - ${eventDetails.end}\nLocation: ${eventDetails.location || 'Not specified'}\nDescription: ${eventDetails.description || 'None'}\nOrganizer: ${eventDetails.organizer}\nAttendees: ${eventDetails.attendees.join(', ') || 'Just you'}\n\n${attendeeMemory}\n\n${kgContext}\n\n${emailContext}`,
    maxTokens: 1500,
  });

  const briefingText = result.text;
  logger.log(`Generated via ${result.model} (${result.provider}, ${result.estimatedCostCents}¢)`);

  const deliveredVia = await deliverReport({
    reportType: 'calendar-prep',
    title: `Meeting Prep: ${eventDetails.summary}`,
    body: briefingText,
    category: 'general',
    metadata: { event_id: payload.event_id, attendees: eventDetails.attendees },
  });

  logger.log(`Delivered via: ${deliveredVia.join(', ') || 'none'}`);

  const pool = getPool();
  await pool.query(
    `INSERT INTO actions_log (action_type, description, metadata)
     VALUES ($1, $2, $3)`,
    ['calendar_prep', `Pre-meeting briefing for "${eventDetails.summary}" delivered via ${deliveredVia.join(', ')}`, JSON.stringify({ deliveredVia, event_id: payload.event_id })]
  );

  // --- Write calendar-derived facts to Knowledge Graph ---
  /*
  try {
    const pkgFacts: FactInput[] = [];

    // Record people the owner meets with
    for (const attendee of eventDetails.attendees) {
      const name = attendee.split(/[<@]/)[0].trim();
      if (!name || name === 'Unknown') continue;

      pkgFacts.push({
        domain: 'people',
        category: 'meetings',
        key: `meets_with_${name.toLowerCase().replace(/\s+/g, '_')}`,
        value: `Meets with ${name} — last scheduled: "${eventDetails.summary}" on ${eventDetails.start}`,
        confidence: 0.5,
        source: 'calendar',
      });
    }

    // Record the meeting as a schedule pattern if it has attendees (likely recurring/important)
    if (eventDetails.attendees.length > 0) {
      pkgFacts.push({
        domain: 'lifestyle',
        category: 'schedule',
        key: `meeting_pattern_${eventDetails.summary.toLowerCase().replace(/[^a-z0-9]+/g, '_').slice(0, 60)}`,
        value: `Attends "${eventDetails.summary}" with ${eventDetails.attendees.join(', ')}${eventDetails.location ? ` at ${eventDetails.location}` : ''}`,
        confidence: 0.5,
        source: 'calendar',
      });
    }

    // Record important upcoming event
    pkgFacts.push({
      domain: 'events',
      category: 'calendar',
      key: `upcoming_${payload.event_id}`,
      value: `"${eventDetails.summary}" on ${eventDetails.start}${eventDetails.location ? ` at ${eventDetails.location}` : ''} — ${eventDetails.attendees.length > 0 ? `with ${eventDetails.attendees.join(', ')}` : 'solo'}`,
      confidence: 0.5,
      source: 'calendar',
      validFrom: eventDetails.start || undefined,
      validUntil: eventDetails.end || undefined,
    });

    if (pkgFacts.length > 0) {
      const { written } = await ingestFacts(pkgFacts);
      logger.log(`Wrote ${written}/${pkgFacts.length} facts to knowledge graph`);
    }
  } catch (err) {
    logger.logMinimal('PKG write failed (non-fatal):', err);
  }
  */

  return {
    delivered_via: deliveredVia,
    recipient: payload.recipient,
    event_summary: eventDetails.summary,
    attendee_count: eventDetails.attendees.length,
  };
}
