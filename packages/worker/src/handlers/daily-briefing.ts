/**
 * Daily briefing handler.
 *
 * Generates a morning summary covering calendar, email, and active reminders,
 * then delivers it via email and inbox. Uses Claude to compose a concise, scannable briefing.
 *
 * Requires: ANTHROPIC_API_KEY, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { deliverReport } from '../lib/report-delivery.js';
import { routeRequest } from '../lib/llm/index.js';
import { formatDateET, formatTimeET, formatDateTimeET, hourET, todayBoundsET } from '../lib/timezone.js';

const logger = createLogger('daily-briefing');

/**
 * Fetch today's calendar events.
 */
async function fetchTodayEvents(): Promise<string> {
  // const auth = await getAuthenticatedClient();
  // if (!auth) return 'Calendar not connected.';
  return 'Calendar not connected (Google auth not yet ported).';

  /*
  const calendar = google.calendar({ version: 'v3', auth });
  const now = new Date();
  const { end: endOfDay } = todayBoundsET(now);

  // Also grab tomorrow for lookahead
  const tomorrow = new Date(endOfDay.getTime() + 1);
  const { end: endOfTomorrow } = todayBoundsET(tomorrow);

  const response = await calendar.events.list({
    calendarId: 'primary',
    timeMin: now.toISOString(),
    timeMax: endOfTomorrow.toISOString(),
    maxResults: 20,
    singleEvents: true,
    orderBy: 'startTime',
  });

  const events = response.data.items ?? [];
  if (events.length === 0) return 'No events today or tomorrow.';

  const lines: string[] = [];
  let currentDate = '';

  for (const event of events) {
    const start = event.start?.dateTime ?? event.start?.date ?? '';
    const eventDate = new Date(start);
    const dateLabel = formatDateET(eventDate, { weekday: 'short', month: 'short', day: 'numeric' });

    if (dateLabel !== currentDate) {
      currentDate = dateLabel;
      lines.push(`\n${dateLabel}:`);
    }

    if (event.start?.date) {
      lines.push(`  - ${event.summary ?? '(No title)'} (all day)`);
    } else {
      const timeStr = formatTimeET(eventDate, { hour: 'numeric', minute: '2-digit', hour12: true });
      const endDate = new Date(event.end?.dateTime ?? '');
      const endStr = formatTimeET(endDate, { hour: 'numeric', minute: '2-digit', hour12: true });
      const attendeeCount = (event.attendees ?? []).length;
      const attendeeNote = attendeeCount > 0 ? ` (${attendeeCount} attendees)` : '';
      lines.push(`  - ${timeStr}-${endStr}: ${event.summary ?? '(No title)'}${attendeeNote}`);
    }

    if (event.location) lines.push(`    @ ${event.location}`);
  }

  return lines.join('\n').trim();
  */
}

/**
 * Fetch Gmail inbox summary.
 */
async function fetchMailSummary(): Promise<string> {
  const pool = getPool();

  // Read from gmail_archive instead of hitting Gmail API
  const [countResult, recentResult] = await Promise.all([
    pool.query(
      `SELECT COUNT(*) AS unread_count FROM gmail_archive
       WHERE 'INBOX' = ANY(labels) AND 'UNREAD' = ANY(labels)`
    ),
    pool.query(
      `SELECT from_address, from_name, subject FROM gmail_archive
       WHERE 'INBOX' = ANY(labels) AND 'UNREAD' = ANY(labels)
       ORDER BY date DESC LIMIT 5`
    ),
  ]);

  const unreadCount = parseInt(countResult.rows[0]?.unread_count ?? '0', 10);
  if (recentResult.rows.length === 0) return `${unreadCount} unread emails. Inbox is clear.`;

  const lines: string[] = [`${unreadCount} unread emails. Recent:`];
  for (const row of recentResult.rows) {
    const sender = row.from_name || row.from_address;
    const subject = row.subject || '(No subject)';
    lines.push(`  - ${sender}: "${subject}"`);
  }

  return lines.join('\n');
}

/**
 * Fetch pending reminders scheduled for today.
 */
async function fetchTodayReminders(): Promise<string> {
  const pool = getPool();
  const { end: endOfDay } = todayBoundsET();

  const { rows } = await pool.query(
    `SELECT payload, next_run_at FROM tempo_jobs
     WHERE job_type = 'send-reminder'
       AND status = 'pending'
       AND next_run_at IS NOT NULL
       AND next_run_at <= $1
     ORDER BY next_run_at ASC
     LIMIT 10`,
    [endOfDay.toISOString()]
  );

  if (rows.length === 0) return 'No pending reminders today.';

  const lines = rows.map(r => {
    const time = formatTimeET(new Date(r.next_run_at), { hour: 'numeric', minute: '2-digit', hour12: true });
    const msg = (r.payload as Record<string, unknown>).message as string;
    return `  - ${time}: ${msg}`;
  });

  return `Pending reminders:\n${lines.join('\n')}`;
}

/**
 * Fetch recent core memory facts for context.
 */
async function fetchRecentMemoryContext(): Promise<string> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT category, key, value FROM core_memory
     WHERE superseded_by IS NULL
       AND category IN ('goals', 'recurring')
     ORDER BY created_at DESC
     LIMIT 10`
  );

  if (rows.length === 0) return '';

  const lines = rows.map(r => `  - [${r.category}] ${r.key}: ${r.value}`);
  return `Active goals & routines:\n${lines.join('\n')}`;
}

export async function handleDailyBriefing(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { recipient: string };

  // Check if subscription is enabled
  const pool = getPool();
  const { rows: subRows } = await pool.query(
    `SELECT is_enabled FROM report_subscriptions WHERE report_type = $1 LIMIT 1`,
    ['daily-briefing']
  );
  if (subRows.length > 0 && !subRows[0].is_enabled) {
    logger.log('Subscription disabled, skipping');
    return { skipped: true, reason: 'subscription_disabled' };
  }

  logger.log(`Generating briefing for ${payload.recipient}`);

  // Gather all context in parallel
  const [calendarContext, mailContext, reminderContext, memoryContext] = await Promise.all([
    fetchTodayEvents(),
    fetchMailSummary(),
    fetchTodayReminders(),
    fetchRecentMemoryContext(),
  ]);

  const now = new Date();
  const hour = hourET(now);
  const timeOfDay = hour < 12 ? 'morning' : hour < 17 ? 'afternoon' : 'evening';

  const result = await routeRequest({
    handler: 'daily-briefing',
    taskTier: 'generation',
    systemPrompt: `You are ARIA, a personal AI assistant. Compose a concise ${timeOfDay} briefing for your owner. Keep it scannable and warm but direct. Use line breaks for readability. No emojis. Max 1500 characters total. Include only what's relevant -- skip sections with nothing to report.`,
    userMessage: `It's ${formatDateTimeET(now, { weekday: 'long', month: 'long', day: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true })}. Generate my briefing from this data:\n\nCALENDAR:\n${calendarContext}\n\nEMAIL:\n${mailContext}\n\nREMINDERS:\n${reminderContext}\n\n${memoryContext}`,
    maxTokens: 2000,
  });

  const briefingText = result.text;
  logger.log(`Generated via ${result.model} (${result.provider}, ${result.estimatedCostCents}¢)`);

  // Deliver via configured methods
  const deliveredVia = await deliverReport({
    reportType: 'daily-briefing',
    title: `${timeOfDay.charAt(0).toUpperCase() + timeOfDay.slice(1)} Briefing`,
    body: briefingText,
    category: 'general',
  });

  logger.log(`Delivered via: ${deliveredVia.join(', ') || 'none'}`);

  // Log to actions_log
  await pool.query(
    `INSERT INTO actions_log (action_type, description, metadata)
     VALUES ($1, $2, $3)`,
    ['daily_briefing', `Daily briefing delivered via ${deliveredVia.join(', ')}`, JSON.stringify({ deliveredVia })]
  );

  return { delivered_via: deliveredVia, briefing_length: briefingText.length };
}
