/**
 * Context Accumulator — Gate 1 of the Proactive Intelligence pipeline.
 *
 * Monitors all 16 data sources by comparing current state against stored
 * watermarks. When changes are detected, creates context_snapshots with
 * human-readable change summaries for the significance classifier (Gate 2).
 *
 * Cost: $0 — pure SQL queries against the shared database + Google API polls.
 */
import { google } from 'googleapis';
import { getPool, createLogger } from '@nexus/core';
import { getAuthenticatedClient } from '../google-auth.js';
import { logEvent } from '../event-log.js';
import { routeRequest } from '../llm/index.js';
import { todayET, hourET } from '../timezone.js';
import { reverseGeocode } from '../geocode.js';

const logger = createLogger('proactive-accumulator');

export interface ChangeDetection {
  source: string;
  changed: boolean;
  summary: string;
  data: Record<string, unknown>;
}

interface WatermarkRow {
  source: string;
  last_checked_at: Date;
  last_change_detected_at: Date | null;
  metadata: Record<string, unknown>;
}

// ─── Watermark Management ────────────────────────────────────────

async function getWatermark(source: string): Promise<WatermarkRow | null> {
  const pool = getPool();
  const { rows } = await pool.query<WatermarkRow>(
    `SELECT * FROM context_watermarks WHERE source = $1`,
    [source]
  );
  return rows[0] ?? null;
}

async function ensureWatermark(source: string): Promise<WatermarkRow> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO context_watermarks (source) VALUES ($1) ON CONFLICT (source) DO NOTHING`,
    [source]
  );
  const { rows } = await pool.query<WatermarkRow>(
    `SELECT * FROM context_watermarks WHERE source = $1`,
    [source]
  );
  return rows[0];
}

async function updateWatermark(
  source: string,
  changed: boolean,
  metadata?: Record<string, unknown>
): Promise<void> {
  const pool = getPool();
  const sets = ['last_checked_at = NOW()'];
  const params: unknown[] = [source];
  let idx = 2;

  if (changed) {
    sets.push('last_change_detected_at = NOW()');
  }
  if (metadata) {
    sets.push(`metadata = $${idx++}`);
    params.push(JSON.stringify(metadata));
  }

  await pool.query(
    `UPDATE context_watermarks SET ${sets.join(', ')} WHERE source = $1`,
    params
  );
}

async function saveSnapshot(
  source: string,
  summary: string,
  data: Record<string, unknown>
): Promise<string> {
  const pool = getPool();
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO context_snapshots (source, change_summary, change_data)
     VALUES ($1, $2, $3) RETURNING id`,
    [source, summary, JSON.stringify(data)]
  );
  return rows[0].id;
}

// ─── Delivery Window Detection ────────────────────────────────────

/**
 * Detect same-day delivery time windows in message text.
 * Matches patterns like "between 2 and 5 PM", "arriving by 3:00 PM",
 * "delivery window: 1-4pm", "expected 12:00 PM - 3:00 PM", etc.
 */
function detectDeliveryWindow(text: string): string | null {
  if (!text) return null;
  const lower = text.toLowerCase();

  // Must contain delivery-related keywords
  const deliveryKeywords = ['deliver', 'arrival', 'arriving', 'shipped', 'package', 'order', 'drop-off', 'dropoff', 'eta', 'estimated time', 'window', 'scheduled for'];
  const hasDeliveryContext = deliveryKeywords.some(k => lower.includes(k));
  if (!hasDeliveryContext) return null;

  // Must contain time-related keywords suggesting today
  const todayKeywords = ['today', 'tonight', 'this evening', 'this afternoon', 'this morning'];
  const hasToday = todayKeywords.some(k => lower.includes(k));

  // Match time patterns: "between X and Y PM", "X:XX PM - Y:XX PM", "by X PM", "X-Y pm"
  const timePatterns = [
    /between\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\s+(?:and|&|-)\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))/i,
    /(\d{1,2}(?::\d{2})?\s*(?:am|pm))\s*[-–—to]+\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm))/i,
    /(?:by|before|arriving|expected|eta)\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))/i,
    /window[:\s]+(\d{1,2}(?::\d{2})?\s*(?:am|pm))\s*[-–—to]+\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm))/i,
  ];

  for (const pattern of timePatterns) {
    const match = text.match(pattern);
    if (match) {
      // If we have today context or the time pattern is strong enough on its own
      if (hasToday || lower.includes('pm') || lower.includes('am')) {
        return match[0].trim();
      }
    }
  }

  return null;
}

/**
 * Create an urgent proactive insight for a detected delivery window.
 * Bypasses the normal 3-gate pipeline since this is time-critical.
 */
async function createDeliveryInsight(params: {
  sender: string;
  subject: string;
  deliveryWindow: string;
  source: 'gmail' | 'sms' | 'aria_email';
}): Promise<void> {
  const pool = getPool();
  const title = `Delivery arriving: ${params.deliveryWindow}`;
  const body = `You have a delivery expected ${params.deliveryWindow}.\n\nFrom: ${params.sender}\nSubject: ${params.subject}\nDetected in: ${params.source}`;

  try {
    // Create urgent insight directly
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO proactive_insights
       (title, body, reasoning, category, urgency, confidence, risk_level_required,
        source_snapshots, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW() + INTERVAL '12 hours')
       RETURNING id`,
      [
        title,
        body,
        `Detected a same-day delivery time window "${params.deliveryWindow}" in a ${params.source === 'gmail' ? 'Gmail' : params.source === 'sms' ? 'SMS' : 'email'} message from ${params.sender}.`,
        'calendar_prep',
        'urgent',
        0.95,
        'low',
        '{}',
      ]
    );

    // Enqueue immediate delivery via urgent methods
    const { rows: settingsRows } = await pool.query<{ value: unknown }>(
      `SELECT value FROM proactive_settings WHERE key = 'urgent_delivery_methods'`
    );
    const urgentMethodIds = (settingsRows[0]?.value as string[]) ?? [];

    if (urgentMethodIds.length > 0) {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
         VALUES ($1, $2, $3, $4)`,
        [
          'insight-deliver',
          JSON.stringify({ insight_id: rows[0].id, delivery_method_ids: urgentMethodIds }),
          3, // high priority
          2,
        ]
      );
    }

    logEvent({
      action: `Detected same-day delivery: "${params.deliveryWindow}" from ${params.sender}`,
      component: 'proactive',
      category: 'background',
      metadata: { sender: params.sender, window: params.deliveryWindow, source: params.source },
    });

    logger.log(`Created urgent delivery insight: ${title}`);
  } catch (err) {
    logger.logMinimal('Failed to create delivery insight:', err instanceof Error ? err.message : err);
  }
}

// ─── Data Source Monitors ────────────────────────────────────────

async function checkCoreMemory(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  const { rows } = await pool.query(
    `SELECT category, key, value FROM core_memory
     WHERE superseded_by IS NULL AND updated_at > $1
     ORDER BY updated_at DESC LIMIT 20`,
    [since]
  );
  if (rows.length === 0) return { source: 'core_memory', changed: false, summary: '', data: {} };
  const items = rows.map((r: { category: string; key: string; value: string }) => `[${r.category}] ${r.key}: ${r.value}`);
  return {
    source: 'core_memory',
    changed: true,
    summary: `${rows.length} memory fact(s) updated: ${items.slice(0, 3).join('; ')}`,
    data: { count: rows.length, facts: rows },
  };
}

async function checkConversations(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  const { rows } = await pool.query(
    `SELECT m.content, c.title FROM messages m
     JOIN conversations c ON c.id = m.conversation_id
     WHERE m.created_at > $1 AND m.role = 'user'
     ORDER BY m.created_at DESC LIMIT 10`,
    [since]
  );
  if (rows.length === 0) return { source: 'conversations', changed: false, summary: '', data: {} };
  const previews = rows.map((r: { content: string; title: string }) =>
    `"${r.content.slice(0, 80)}${r.content.length > 80 ? '...' : ''}" in "${r.title || 'Untitled'}"`
  );
  return {
    source: 'conversations',
    changed: true,
    summary: `${rows.length} new user message(s): ${previews.slice(0, 2).join('; ')}`,
    data: { count: rows.length, messages: rows },
  };
}

async function checkDeviceReminders(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    const { rows } = await pool.query(
      `SELECT title, due_date, is_completed FROM device_reminders
       WHERE updated_at > $1 OR created_at > $1
       ORDER BY due_date ASC NULLS LAST LIMIT 20`,
      [since]
    );
    if (rows.length === 0) return { source: 'device_reminders', changed: false, summary: '', data: {} };
    const upcoming = rows.filter((r: { is_completed: boolean }) => !r.is_completed);
    return {
      source: 'device_reminders',
      changed: true,
      summary: `${rows.length} reminder(s) changed, ${upcoming.length} still pending`,
      data: { count: rows.length, pending: upcoming.length, reminders: rows },
    };
  } catch { return { source: 'device_reminders', changed: false, summary: '', data: {} }; }
}

async function checkContacts(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    const { rows } = await pool.query(
      `SELECT display_name, relationship FROM contacts
       WHERE updated_at > $1 OR created_at > $1
       ORDER BY updated_at DESC LIMIT 20`,
      [since]
    );
    if (rows.length === 0) return { source: 'contacts', changed: false, summary: '', data: {} };
    const names = rows.map((r: { display_name: string }) => r.display_name).slice(0, 5);
    return {
      source: 'contacts',
      changed: true,
      summary: `${rows.length} contact(s) updated: ${names.join(', ')}`,
      data: { count: rows.length, contacts: rows },
    };
  } catch { return { source: 'contacts', changed: false, summary: '', data: {} }; }
}

async function checkHealthData(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    const { rows } = await pool.query(
      `SELECT date, data_type, value FROM health_data
       WHERE created_at > $1
       ORDER BY date DESC LIMIT 20`,
      [since]
    );
    if (rows.length === 0) return { source: 'health_data', changed: false, summary: '', data: {} };
    const types = [...new Set(rows.map((r: { data_type: string }) => r.data_type))];
    return {
      source: 'health_data',
      changed: true,
      summary: `${rows.length} new health record(s) for: ${types.join(', ')}`,
      data: { count: rows.length, types, records: rows },
    };
  } catch { return { source: 'health_data', changed: false, summary: '', data: {} }; }
}

async function checkDeviceLocation(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  try {
    const { rows } = await pool.query(
      `SELECT place_name, locality, latitude, longitude, timestamp FROM device_location
       WHERE timestamp > $1
       ORDER BY timestamp DESC LIMIT 1`,
      [wm.last_checked_at]
    );
    if (rows.length === 0) return { source: 'device_location', changed: false, summary: '', data: {} };
    const loc = rows[0];
    const lastKnown = (wm.metadata as Record<string, unknown>)?.last_place as string | undefined;
    let currentPlace = loc.place_name || loc.locality;
    if (!currentPlace) {
      currentPlace = await reverseGeocode(loc.latitude, loc.longitude) || `${loc.latitude},${loc.longitude}`;
    }
    if (currentPlace === lastKnown) return { source: 'device_location', changed: false, summary: '', data: {} };
    return {
      source: 'device_location',
      changed: true,
      summary: `Location changed to: ${currentPlace}`,
      data: { place: currentPlace, ...loc },
    };
  } catch { return { source: 'device_location', changed: false, summary: '', data: {} }; }
}

async function checkTwilioMessages(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    const { rows } = await pool.query(
      `SELECT from_number, body, direction FROM twilio_messages
       WHERE created_at > $1 AND direction = 'inbound'
       ORDER BY created_at DESC LIMIT 10`,
      [since]
    );
    if (rows.length === 0) return { source: 'twilio_messages', changed: false, summary: '', data: {} };

    // Check each inbound SMS for delivery time windows
    for (const msg of rows) {
      const deliveryWindow = detectDeliveryWindow(msg.body ?? '');
      if (deliveryWindow) {
        await createDeliveryInsight({
          sender: msg.from_number,
          subject: (msg.body ?? '').slice(0, 60),
          deliveryWindow,
          source: 'sms',
        });
      }
    }

    return {
      source: 'twilio_messages',
      changed: true,
      summary: `${rows.length} new inbound SMS message(s)`,
      data: { count: rows.length, messages: rows },
    };
  } catch { return { source: 'twilio_messages', changed: false, summary: '', data: {} }; }
}

async function checkTwilioCalls(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    const { rows } = await pool.query(
      `SELECT from_number, direction, status, duration FROM twilio_calls
       WHERE created_at > $1
       ORDER BY created_at DESC LIMIT 10`,
      [since]
    );
    if (rows.length === 0) return { source: 'twilio_calls', changed: false, summary: '', data: {} };
    const missed = rows.filter((r: { status: string }) => r.status === 'no-answer' || r.status === 'busy');
    return {
      source: 'twilio_calls',
      changed: true,
      summary: `${rows.length} new call(s)${missed.length > 0 ? `, ${missed.length} missed` : ''}`,
      data: { count: rows.length, missed: missed.length, calls: rows },
    };
  } catch { return { source: 'twilio_calls', changed: false, summary: '', data: {} }; }
}

async function checkAriaInboundEmails(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    const { rows } = await pool.query(
      `SELECT from_address, subject FROM aria_inbound_emails
       WHERE stored_at > $1
       ORDER BY stored_at DESC LIMIT 10`,
      [since]
    );
    if (rows.length === 0) return { source: 'aria_inbound_emails', changed: false, summary: '', data: {} };
    const senders = rows.map((r: { from_address: string; subject: string }) => `${r.from_address}: "${r.subject}"`).slice(0, 3);
    return {
      source: 'aria_inbound_emails',
      changed: true,
      summary: `${rows.length} new email(s) to ARIA: ${senders.join('; ')}`,
      data: { count: rows.length, emails: rows },
    };
  } catch { return { source: 'aria_inbound_emails', changed: false, summary: '', data: {} }; }
}

async function checkSocialInteractions(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    // Only monitor inbound social interactions (notifications, received DMs)
    // Exclude ARIA's own posts, comments, and votes
    const { rows } = await pool.query(
      `SELECT interaction_type, content_summary FROM social_interactions
       WHERE created_at > $1
         AND interaction_type NOT IN ('post_create', 'comment_create', 'vote', 'post_read')
       ORDER BY created_at DESC LIMIT 10`,
      [since]
    );
    if (rows.length === 0) return { source: 'social_interactions', changed: false, summary: '', data: {} };
    const types = [...new Set(rows.map((r: { interaction_type: string }) => r.interaction_type))];
    return {
      source: 'social_interactions',
      changed: true,
      summary: `${rows.length} new social interaction(s): ${types.join(', ')}`,
      data: { count: rows.length, types, interactions: rows },
    };
  } catch { return { source: 'social_interactions', changed: false, summary: '', data: {} }; }
}

// aria_journal is excluded from monitoring — ARIA writes her own journal entries,
// so monitoring them would create a feedback loop. Journal content is already
// informed by conversations and memory, which ARE monitored.

async function checkTempoJobs(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    // Exclude proactive pipeline jobs and ARIA's own delivery/maintenance jobs
    const { rows } = await pool.query(
      `SELECT job_type, status, last_error FROM tempo_jobs
       WHERE updated_at > $1 AND status IN ('failed', 'completed')
         AND job_type NOT IN (
           'context-accumulate', 'significance-check', 'anticipation-analyze',
           'insight-deliver', 'digest-compile', 'pattern-maintenance',
           'aria-email-send', 'push-notification', 'sensor-data-hygiene',
           'journal', 'social-engagement', 'memory-maintenance',
           'self-improvement-report', 'music-analysis'
         )
       ORDER BY updated_at DESC LIMIT 10`,
      [since]
    );
    if (rows.length === 0) return { source: 'tempo_jobs', changed: false, summary: '', data: {} };
    const failed = rows.filter((r: { status: string }) => r.status === 'failed');
    return {
      source: 'tempo_jobs',
      changed: true,
      summary: `${rows.length} job(s) completed${failed.length > 0 ? `, ${failed.length} failed` : ''}`,
      data: { count: rows.length, failed: failed.length, jobs: rows },
    };
  } catch { return { source: 'tempo_jobs', changed: false, summary: '', data: {} }; }
}

async function checkGoogleCalendar(wm: WatermarkRow): Promise<ChangeDetection> {
  // Query device_calendar_events (synced from iOS EventKit, includes Google Calendar)
  // instead of hitting the Google Calendar API directly.
  try {
    const pool = getPool();
    const now = new Date();
    const nextWeek = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);

    const { rows: events } = await pool.query<{
      device_id: string;
      title: string;
      start_date: Date;
      end_date: Date;
      location: string | null;
      is_all_day: boolean;
      calendar_name: string | null;
    }>(
      `SELECT device_id, title, start_date, end_date, location, is_all_day, calendar_name
       FROM device_calendar_events
       WHERE start_date >= $1 AND start_date <= $2
       ORDER BY start_date ASC
       LIMIT 20`,
      [now, nextWeek]
    );

    const lastEventIds = ((wm.metadata as Record<string, unknown>)?.event_ids as string[]) ?? [];
    const currentEventIds = events.map(e => e.device_id);

    const newEvents = currentEventIds.filter(id => !lastEventIds.includes(id));
    const removedEvents = lastEventIds.filter(id => !currentEventIds.includes(id));

    if (newEvents.length === 0 && removedEvents.length === 0) {
      return { source: 'google_calendar', changed: false, summary: '', data: { _watermark_meta: { event_ids: currentEventIds } } };
    }

    const upcomingToday = events.filter(e => e.start_date.toDateString() === now.toDateString());

    return {
      source: 'google_calendar',
      changed: true,
      summary: `Calendar: ${newEvents.length} new, ${removedEvents.length} removed event(s). ${upcomingToday.length} today.`,
      data: {
        new_count: newEvents.length,
        removed_count: removedEvents.length,
        today_count: upcomingToday.length,
        events: events.slice(0, 10).map(e => ({
          summary: e.title,
          start: e.start_date.toISOString(),
          end: e.end_date.toISOString(),
          location: e.location,
          calendar: e.calendar_name,
        })),
        _watermark_meta: { event_ids: currentEventIds },
      },
    };
  } catch (err) {
    logger.logMinimal('Calendar check failed:', err instanceof Error ? err.message : err);
    return { source: 'google_calendar', changed: false, summary: '', data: {} };
  }
}

// ─── Gmail Email Classifier (Gemini Flash-Lite) ──────────────────

interface EmailClassification {
  /** Category: spam, commercial, subscription, personal, work, transactional, financial, urgent */
  category: string;
  /** Brief one-line summary of the email's content */
  summary: string;
  /** Whether ARIA should process this email further via the PIE */
  send_to_pie: boolean;
  /** Suggested action: ignore, archive, flag, process, respond */
  action: string;
  /** Brief reason for the classification decision */
  reason: string;
  /** Confidence that this email is spam (0.0–1.0), only meaningful when category is "spam" */
  spam_confidence: number;
}

const EMAIL_CLASSIFIER_PROMPT = `You are an email triage classifier for a personal AI assistant named ARIA. Your job is to quickly classify incoming emails and decide which ones deserve ARIA's attention for proactive intelligence analysis.

For each email, classify it and decide whether to send it to the Proactive Intelligence Engine (PIE) for deeper analysis.

Categories:
- "spam": Junk, phishing, unsolicited marketing → NEVER send to PIE
- "commercial": Promotional offers, sales, marketing from known brands → Usually skip PIE unless it's a great deal or time-sensitive offer the user might care about
- "subscription": Newsletters, digests, automated updates → Usually skip PIE unless it contains actionable/time-sensitive info
- "transactional": Receipts, shipping confirmations, order updates, delivery notifications → Send to PIE (may need action)
- "financial": Banking, bills, payments, investment alerts → Send to PIE (often actionable)
- "personal": From a real person, personal communication → Always send to PIE
- "work": Work-related from colleagues, clients, business contacts → Always send to PIE
- "urgent": Anything time-sensitive requiring immediate attention → Always send to PIE
- "automated": System alerts, security notifications, password resets, 2FA → Send to PIE only if it looks like something requiring action

Actions:
- "ignore": No action needed, ARIA should skip this
- "archive": Low priority, could be auto-archived
- "flag": Worth noting but no immediate action
- "process": Send through PIE for analysis and potential action
- "respond": May need a reply or follow-up

For the "spam" category, also include a confidence score (0.0–1.0) indicating how confident you are that the email is spam.

Respond with a JSON array (no markdown fences):
[
  {
    "email_index": 0,
    "category": "...",
    "summary": "Brief one-line summary",
    "send_to_pie": true/false,
    "action": "...",
    "reason": "Brief reason",
    "spam_confidence": 0.0
  }
]`;

/**
 * Classify a batch of emails using Gemini Flash-Lite (essentially free).
 * Returns classification for each email to decide PIE routing.
 */
async function classifyEmails(
  emails: Array<{ from: string; subject: string; snippet: string; body: string }>
): Promise<EmailClassification[]> {
  try {
    const emailDescriptions = emails.map((e, i) =>
      `[${i}] From: ${e.from}\nSubject: ${e.subject}\nSnippet: ${e.snippet}\nBody preview: ${e.body.slice(0, 1000)}`
    ).join('\n\n---\n\n');

    logger.logVerbose(`Classifying ${emails.length} email(s) via LLM router`);
    const stopClassify = logger.time('email-classification');
    const result = await routeRequest({
      handler: 'email-classifier',
      taskTier: 'classification',
      systemPrompt: EMAIL_CLASSIFIER_PROMPT,
      userMessage: `Classify these ${emails.length} email(s):\n\n${emailDescriptions}`,
      maxTokens: 1024,
    });

    stopClassify();
    const text = result.text;
    logger.logDebug('Email classification raw response:', text.slice(0, 500));
    const cleaned = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    const results: Array<EmailClassification & { email_index: number }> = JSON.parse(cleaned);

    // Map results back by index, defaulting to send_to_pie=true for any missing
    return emails.map((_, i) => {
      const r = results.find(r => r.email_index === i);
      if (r) return { category: r.category, summary: r.summary, send_to_pie: r.send_to_pie, action: r.action, reason: r.reason, spam_confidence: r.spam_confidence ?? 0 };
      return { category: 'unknown', summary: '', send_to_pie: true, action: 'process', reason: 'Not classified', spam_confidence: 0 };
    });
  } catch (err) {
    logger.logMinimal('Email classification failed:', err instanceof Error ? err.message : err);
    // On failure, default to sending everything to PIE
    return emails.map(() => ({
      category: 'unknown',
      summary: '',
      send_to_pie: true,
      action: 'process',
      reason: 'Classification failed',
      spam_confidence: 0,
    }));
  }
}

async function checkGmail(wm: WatermarkRow): Promise<ChangeDetection> {
  try {
    const auth = await getAuthenticatedClient();
    if (!auth) return { source: 'gmail', changed: false, summary: '', data: {} };

    const gmail = google.gmail({ version: 'v1', auth });

    // Try incremental sync via History API if we have a stored historyId
    const storedHistoryId = (wm.metadata as Record<string, unknown>)?.history_id as string | undefined;
    let newMessageIds: string[];
    let messageIds: string[];
    let latestHistoryId: string | undefined;

    if (storedHistoryId) {
      // Incremental: use History API for efficient change detection
      try {
        let pageToken: string | undefined;
        const historyMessageIds: string[] = [];

        do {
          const historyResponse = await gmail.users.history.list({
            userId: 'me',
            startHistoryId: storedHistoryId,
            historyTypes: ['messageAdded'],
            labelId: 'INBOX',
            pageToken,
          });

          const history = historyResponse.data.history ?? [];
          for (const entry of history) {
            const added = entry.messagesAdded ?? [];
            for (const m of added) {
              if (m.message?.id) historyMessageIds.push(m.message.id);
            }
          }

          latestHistoryId = historyResponse.data.historyId ?? undefined;
          pageToken = historyResponse.data.nextPageToken ?? undefined;
        } while (pageToken);

        newMessageIds = [...new Set(historyMessageIds)];
        // Also fetch current inbox for watermark update
        const recent = await gmail.users.messages.list({
          userId: 'me', labelIds: ['INBOX'], maxResults: 20,
        });
        messageIds = (recent.data.messages ?? []).map(m => m.id!);
        logger.logVerbose(`Gmail: History API found ${newMessageIds.length} new message(s) since historyId ${storedHistoryId}`);
      } catch (historyErr) {
        // History API can fail if historyId is too old (404). Fall back to full list.
        const errMsg = historyErr instanceof Error ? historyErr.message : String(historyErr);
        if (errMsg.includes('404') || errMsg.includes('notFound') || errMsg.includes('Start history id is too old')) {
          logger.logVerbose('Gmail: History API historyId expired, falling back to full list');
        } else {
          logger.logMinimal('Gmail: History API error, falling back to full list:', errMsg);
        }
        // Fall through to full list approach
        const recent = await gmail.users.messages.list({
          userId: 'me', labelIds: ['INBOX'], maxResults: 20,
        });
        messageIds = (recent.data.messages ?? []).map(m => m.id!);
        const lastMessageIds = ((wm.metadata as Record<string, unknown>)?.message_ids as string[]) ?? [];
        newMessageIds = messageIds.filter(id => !lastMessageIds.includes(id));
        // Reset historyId — will be re-bootstrapped below
        latestHistoryId = undefined;
      }
    } else {
      // No historyId yet — use full list approach and bootstrap the historyId
      const recent = await gmail.users.messages.list({
        userId: 'me', labelIds: ['INBOX'], maxResults: 20,
      });
      messageIds = (recent.data.messages ?? []).map(m => m.id!);
      const lastMessageIds = ((wm.metadata as Record<string, unknown>)?.message_ids as string[]) ?? [];
      newMessageIds = messageIds.filter(id => !lastMessageIds.includes(id));

      // Bootstrap historyId for future incremental syncs
      try {
        const profile = await gmail.users.getProfile({ userId: 'me' });
        latestHistoryId = profile.data.historyId ?? undefined;
        logger.logVerbose(`Gmail: Bootstrapped historyId ${latestHistoryId} for future incremental sync`);
      } catch {
        // Non-fatal — we'll try again next cycle
      }
    }

    if (newMessageIds.length === 0) {
      return { source: 'gmail', changed: false, summary: '', data: { _watermark_meta: { message_ids: messageIds, history_id: latestHistoryId ?? storedHistoryId } } };
    }

    // Archive new messages to gmail_archive for permanent storage + KG processing.
    // Fire-and-forget via the ARIA API — don't block the PIE pipeline on this.
    const ariaBaseUrl = process.env.ARIA_BASE_URL || process.env.NEXTAUTH_URL || 'https://aria.example.io';
    const apiToken = process.env.ARIA_API_TOKEN;
    if (apiToken && newMessageIds.length > 0) {
      try {
        const archiveResp = await fetch(`${ariaBaseUrl}/api/gmail/archive`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiToken}`,
          },
          body: JSON.stringify({ message_ids: newMessageIds }),
        });
        if (archiveResp.ok) {
          const archiveResult = await archiveResp.json() as { archived: number; errors: number };
          logger.logVerbose(`Gmail: archived ${archiveResult.archived} new message(s) to gmail_archive (${archiveResult.errors} errors)`);
        } else {
          logger.logMinimal(`Gmail: archive API returned ${archiveResp.status}`);
        }
      } catch (archiveErr) {
        logger.logMinimal('Gmail: failed to archive new messages:', archiveErr instanceof Error ? archiveErr.message : archiveErr);
        // Non-fatal — don't block PIE on archive failures
      }
    }

    // Two-pass approach: fetch metadata first (fast), classify, then fetch full body only for PIE-worthy emails
    const ARIA_EMAIL = process.env.ARIA_EMAIL_ADDRESS || 'aria@example.io';
    const MAX_BODY_LENGTH = 4000;
    const CONCURRENCY = 5;

    // Pass 1: Fetch metadata for all new messages in parallel (max 5 concurrent)
    const metadataMessages: Array<{ id: string; from: string; subject: string; snippet: string }> = [];
    for (let i = 0; i < newMessageIds.length; i += CONCURRENCY) {
      const batch = newMessageIds.slice(i, i + CONCURRENCY);
      const results = await Promise.allSettled(
        batch.map(id => gmail.users.messages.get({ userId: 'me', id, format: 'metadata' }))
      );
      for (const result of results) {
        if (result.status === 'fulfilled') {
          const msg = result.value;
          const headers = msg.data.payload?.headers ?? [];
          const from = headers.find(h => h.name?.toLowerCase() === 'from')?.value ?? '';
          const to = headers.find(h => h.name?.toLowerCase() === 'to')?.value ?? '';
          const subject = headers.find(h => h.name?.toLowerCase() === 'subject')?.value ?? '';
          const snippet = msg.data.snippet ?? '';

          // Skip emails from/to ARIA herself, forwarded ARIA emails, and SES delivery notifications
          if (from.includes(ARIA_EMAIL) || subject.startsWith('[ARIA]')) continue;
          if (to.includes(ARIA_EMAIL)) continue;
          // Skip AWS SES delivery status notifications triggered by ARIA's own sends
          if (from.includes('no-reply@ses.amazonaws.com') || from.includes('postmaster@') || from.includes('mailer-daemon@')) continue;
          if (subject.startsWith('Delivery Status Notification') || subject.startsWith('Mail delivery failed')) continue;

          metadataMessages.push({ id: msg.data.id!, from, subject, snippet });
        }
      }
    }

    // If all new emails were from ARIA, no meaningful change
    if (metadataMessages.length === 0) {
      return { source: 'gmail', changed: false, summary: '', data: { _watermark_meta: { message_ids: messageIds, history_id: latestHistoryId ?? storedHistoryId } } };
    }

    // Classify emails using metadata only (subject + from + snippet is usually enough)
    const classifications = await classifyEmails(
      metadataMessages.map(m => ({ from: m.from, subject: m.subject, snippet: m.snippet, body: m.snippet }))
    );

    // Pass 2: Fetch full body only for emails that pass classification (send_to_pie: true)
    const pieIds: string[] = [];
    const skippedMessagesEarly: Array<{ from: string; subject: string; category: string; action: string }> = [];
    for (let i = 0; i < metadataMessages.length; i++) {
      const cls = classifications[i];
      if (cls.send_to_pie) {
        pieIds.push(metadataMessages[i].id);
      } else {
        const m = metadataMessages[i];
        skippedMessagesEarly.push({ from: m.from, subject: m.subject, category: cls.category, action: cls.action });

        // Auto-mark as spam in Gmail if classification is spam with >=90% confidence
        if (cls.category === 'spam' && cls.spam_confidence >= 0.9 && m.id) {
          try {
            await gmail.users.messages.modify({
              userId: 'me',
              id: m.id,
              requestBody: {
                addLabelIds: ['SPAM'],
                removeLabelIds: ['INBOX'],
              },
            });
            logger.log(`Gmail: auto-marked as spam: "${m.subject}" (confidence: ${cls.spam_confidence})`);
            logEvent({
              action: `Auto-marked email as spam: "${m.subject}" from ${m.from} (confidence: ${cls.spam_confidence})`,
              component: 'proactive',
              category: 'background',
              metadata: { subject: m.subject, from: m.from, spam_confidence: cls.spam_confidence },
            });
          } catch (err) {
            logger.logMinimal(`Failed to mark as spam: "${m.subject}"`, err instanceof Error ? err.message : err);
          }
        }
      }
    }

    if (skippedMessagesEarly.length > 0) {
      logger.logVerbose(`Gmail: skipped ${skippedMessagesEarly.length} email(s) at classification (${skippedMessagesEarly.map(m => m.category).join(', ')})`);
    }

    // Fetch full content only for PIE-worthy emails, in parallel with concurrency limit
    const newMessages: Array<{ id: string; from: string; subject: string; snippet: string; body: string; classification: EmailClassification }> = [];
    for (let i = 0; i < pieIds.length; i += CONCURRENCY) {
      const batch = pieIds.slice(i, i + CONCURRENCY);
      const results = await Promise.allSettled(
        batch.map(id => gmail.users.messages.get({ userId: 'me', id, format: 'full' }))
      );
      for (const result of results) {
        if (result.status === 'fulfilled') {
          const msg = result.value;
          const headers = msg.data.payload?.headers ?? [];
          const from = headers.find(h => h.name?.toLowerCase() === 'from')?.value ?? '';
          const subject = headers.find(h => h.name?.toLowerCase() === 'subject')?.value ?? '';
          const snippet = msg.data.snippet ?? '';

          const body = extractPlainTextBody(msg.data.payload) ?? snippet;
          const truncatedBody = body.length > MAX_BODY_LENGTH
            ? body.slice(0, MAX_BODY_LENGTH) + `\n[truncated — original ${body.length} chars]`
            : body;

          // Look up classification for this message
          const metaIdx = metadataMessages.findIndex(m => m.id === msg.data.id);
          const cls = metaIdx >= 0 ? classifications[metaIdx] : { category: 'unknown', summary: '', send_to_pie: true, action: 'process', reason: 'Unknown', spam_confidence: 0 };

          logger.logVerbose(`Gmail classified: "${subject}" → ${cls.category} (${cls.action}, PIE: ${cls.send_to_pie}, spam_confidence: ${cls.spam_confidence}) — ${cls.reason}`);

          newMessages.push({ id: msg.data.id!, from, subject, snippet, body: truncatedBody, classification: cls });

          // Check for same-day delivery time windows in subject + snippet + body
          const deliveryWindow = detectDeliveryWindow(`${subject} ${snippet} ${body}`);
          if (deliveryWindow) {
            await createDeliveryInsight({
              sender: from.split('<')[0].trim() || from,
              subject,
              deliveryWindow,
              source: 'gmail',
            });
          }
        }
      }
    }
    // Only create a snapshot (and trigger PIE) for emails that passed classification
    if (newMessages.length === 0) {
      return { source: 'gmail', changed: false, summary: '', data: { _watermark_meta: { message_ids: messageIds, history_id: latestHistoryId ?? storedHistoryId } } };
    }

    const previews = newMessages.map(m => `${m.from.split('<')[0].trim()}: "${m.subject}" [${m.classification.category}]`).slice(0, 5);
    return {
      source: 'gmail',
      changed: true,
      summary: `${newMessages.length} new email(s) for review (${skippedMessagesEarly.length} filtered): ${previews.join('; ')}`,
      data: {
        count: newMessages.length,
        filtered_count: skippedMessagesEarly.length,
        messages: newMessages,
        filtered: skippedMessagesEarly,
        _watermark_meta: { message_ids: messageIds, history_id: latestHistoryId ?? storedHistoryId },
      },
    };
  } catch (err) {
    logger.logMinimal('Gmail check failed:', err instanceof Error ? err.message : err);
    return { source: 'gmail', changed: false, summary: '', data: {} };
  }
}

/**
 * Extract plain text body from a Gmail message payload.
 * Walks the MIME tree looking for text/plain parts.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function extractPlainTextBody(payload: any): string | null {
  if (!payload) return null;

  // Direct text/plain body
  if (payload.mimeType === 'text/plain' && payload.body?.data) {
    return Buffer.from(payload.body.data, 'base64url').toString('utf-8');
  }

  // Walk multipart parts
  if (payload.parts) {
    for (const part of payload.parts as typeof payload[]) {
      const text = extractPlainTextBody(part);
      if (text) return text;
    }
  }

  // Fallback: if it's text/html and no plain text found
  if (payload.mimeType === 'text/html' && payload.body?.data) {
    const html = Buffer.from(payload.body.data, 'base64url').toString('utf-8');
    return html.replace(/<[^>]*>/g, '').replace(/\s+/g, ' ').trim();
  }

  return null;
}

async function checkLooki(wm: WatermarkRow): Promise<ChangeDetection> {
  const apiKey = process.env.LOOKI_API_KEY;
  const baseUrl = process.env.LOOKI_BASE_URL;
  if (!apiKey || !baseUrl) return { source: 'looki', changed: false, summary: '', data: {} };

  try {
    const today = todayET();
    const res = await fetch(`${baseUrl}/moments?on_date=${today}`, {
      headers: { 'X-API-Key': apiKey },
    });
    if (!res.ok) return { source: 'looki', changed: false, summary: '', data: {} };
    const raw = (await res.json()) as Record<string, unknown>;
    // Unwrap Looki envelope: { code, detail, data: [...] }
    const moments = (Array.isArray(raw.data) ? raw.data : []) as Array<Record<string, unknown>>;
    const lastCount = ((wm.metadata as Record<string, unknown>)?.moment_count as number) ?? 0;

    // Persist moments to looki_moments table
    if (moments.length > 0) {
      const pool = getPool();
      for (const m of moments) {
        try {
          await pool.query(
            `INSERT INTO looki_moments (id, title, description, summary, transcript, tags, location, thumbnail_url, start_time, duration_seconds)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (id) DO UPDATE SET
               title = EXCLUDED.title,
               description = EXCLUDED.description,
               summary = EXCLUDED.summary,
               transcript = EXCLUDED.transcript,
               tags = EXCLUDED.tags,
               location = EXCLUDED.location,
               synced_at = NOW()`,
            [
              String(m.id ?? m.moment_id ?? ''),
              (m.title as string) ?? null,
              (m.description as string) ?? null,
              (m.summary as string) ?? null,
              (m.transcript as string) ?? null,
              Array.isArray(m.tags) ? m.tags : null,
              m.location ? JSON.stringify(m.location) : null,
              (m.thumbnail_url as string) ?? (m.thumbnail as string) ?? null,
              m.start_time ? new Date(m.start_time as string) : null,
              typeof m.duration_seconds === 'number' ? m.duration_seconds : (typeof m.duration === 'number' ? m.duration : null),
            ]
          );
        } catch (err) {
          logger.logVerbose(`Failed to upsert looki moment ${m.id}:`, err instanceof Error ? err.message : err);
        }
      }
      logger.logVerbose(`Upserted ${moments.length} Looki moment(s) to looki_moments`);
    }

    if (moments.length <= lastCount) return { source: 'looki', changed: false, summary: '', data: {} };
    return {
      source: 'looki',
      changed: true,
      summary: `${moments.length - lastCount} new Looki moment(s) captured today`,
      data: {
        new_count: moments.length - lastCount,
        total: moments.length,
        _watermark_meta: { moment_count: moments.length },
      },
    };
  } catch {
    return { source: 'looki', changed: false, summary: '', data: {} };
  }
}

async function checkLookiRealtime(wm: WatermarkRow): Promise<ChangeDetection> {
  // Read from the looki_realtime_events table (populated by looki-realtime-poll every 60s)
  // instead of calling the Looki API directly. This way we see ALL events since the last
  // check, not just whichever one happens to be "latest" at the moment we poll.
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    const { rows } = await pool.query(
      `SELECT looki_event_id, description, start_time, end_time, timezone, location, created_at
       FROM looki_realtime_events
       WHERE created_at > $1
       ORDER BY created_at ASC`,
      [since]
    );

    if (rows.length === 0) {
      return { source: 'looki_realtime', changed: false, summary: '', data: {} };
    }

    // Build a summary covering all events in this window
    const descriptions = rows
      .map((r: { description: string }) => r.description)
      .filter((d: string) => d && d.length > 0);

    const summary = rows.length === 1
      ? `Looki realtime: ${descriptions[0] || 'new event detected'}`
      : `${rows.length} Looki realtime events: ${descriptions.slice(0, 3).join(' → ')}${descriptions.length > 3 ? ` (+${descriptions.length - 3} more)` : ''}`;

    logger.logVerbose(`${rows.length} new Looki realtime event(s) since ${since}`);

    return {
      source: 'looki_realtime',
      changed: true,
      summary,
      data: {
        event_count: rows.length,
        events: rows.map((r: { looki_event_id: string; description: string; start_time: string | null; end_time: string | null; timezone: string | null; location: unknown; created_at: string }) => ({
          looki_event_id: r.looki_event_id,
          description: r.description,
          start_time: r.start_time,
          end_time: r.end_time,
          timezone: r.timezone,
          location: r.location,
          created_at: r.created_at,
        })),
      },
    };
  } catch (err) {
    logger.logMinimal('Looki realtime check failed:', err instanceof Error ? err.message : err);
    return { source: 'looki_realtime', changed: false, summary: '', data: {} };
  }
}

async function checkMusic(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    const { rows } = await pool.query<{
      title: string;
      artist_name: string;
      genre: string | null;
      played_at: string;
    }>(
      `SELECT title, artist_name, genre, played_at
       FROM device_music
       WHERE played_at IS NOT NULL AND played_at > $1
       ORDER BY played_at DESC LIMIT 20`,
      [since]
    );
    if (rows.length === 0) return { source: 'music', changed: false, summary: '', data: {} };

    const artists = [...new Set(rows.map(r => r.artist_name))];
    const genres = [...new Set(rows.map(r => r.genre).filter(Boolean))];
    const trackDescriptions = rows.slice(0, 5).map(r => `${r.artist_name} - ${r.title}`);

    return {
      source: 'music',
      changed: true,
      summary: `Listened to ${rows.length} track(s): ${trackDescriptions.join(', ')}`,
      data: {
        count: rows.length,
        tracks: rows,
        artists,
        genres,
      },
    };
  } catch { return { source: 'music', changed: false, summary: '', data: {} }; }
}

async function checkDeviceActivity(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  try {
    // Skip "unknown" entries where all activity fields are false
    const { rows } = await pool.query(
      `SELECT stationary, walking, running, cycling, automotive, confidence, timestamp
       FROM device_activity
       WHERE created_at > $1
         AND (stationary = true OR walking = true OR running = true OR cycling = true OR automotive = true)
       ORDER BY timestamp DESC LIMIT 10`,
      [wm.last_checked_at]
    );
    if (rows.length === 0) return { source: 'device_activity', changed: false, summary: '', data: {} };

    const latest = rows[0];
    const activity = latest.automotive ? 'driving' :
      latest.running ? 'running' :
      latest.cycling ? 'cycling' :
      latest.walking ? 'walking' : 'stationary';

    const lastActivity = (wm.metadata as Record<string, unknown>)?.last_activity as string | undefined;
    if (activity === lastActivity) return { source: 'device_activity', changed: false, summary: '', data: {} };

    return {
      source: 'device_activity',
      changed: true,
      summary: `Activity changed to: ${activity} (confidence: ${latest.confidence})`,
      data: {
        activity,
        confidence: latest.confidence,
        timestamp: latest.timestamp,
        _watermark_meta: { last_activity: activity },
      },
    };
  } catch { return { source: 'device_activity', changed: false, summary: '', data: {} }; }
}

async function checkPhotoMemories(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  try {
    const meta = wm.metadata as Record<string, unknown> | null;
    const parts: string[] = [];
    const data: Record<string, unknown> = {};

    // 1. Recently synced photos — rate-limited to once per hour
    const lastSyncCheck = meta?.last_sync_check as string | undefined;
    const currentHour = `${todayET()}-${hourET()}`;
    if (lastSyncCheck !== currentHour) {
      // Find photos synced since the last hourly check
      const sinceTime = wm.last_checked_at;
      const { rows: recentRows } = await pool.query<{ count: string }>(
        `SELECT COUNT(*) as count FROM photo_metadata WHERE synced_at > $1`,
        [sinceTime]
      );
      const recentCount = parseInt(recentRows[0]?.count ?? '0', 10);
      if (recentCount > 0) {
        const { rows: recentSummary } = await pool.query<{ media_type: string; cnt: string }>(
          `SELECT media_type, COUNT(*) as cnt
           FROM photo_metadata WHERE synced_at > $1
           GROUP BY media_type ORDER BY cnt DESC`,
          [sinceTime]
        );
        const breakdown = recentSummary.map(r => `${r.cnt} ${r.media_type}(s)`).join(', ');
        parts.push(`${recentCount} new photos synced: ${breakdown}`);

        // Include AI descriptions of recently described photos for richer context
        const { rows: describedRows } = await pool.query<{ description: string; taken_at: string | null }>(
          `SELECT description, taken_at FROM photo_metadata
           WHERE synced_at > $1 AND description IS NOT NULL
           ORDER BY described_at DESC LIMIT 10`,
          [sinceTime]
        );
        const descriptions = describedRows.map(r => r.description);

        data.recent_sync = {
          count: recentCount,
          breakdown: recentSummary.map(r => ({ type: r.media_type, count: parseInt(r.cnt, 10) })),
          ...(descriptions.length > 0 ? { descriptions } : {}),
        };
        if (descriptions.length > 0) {
          parts.push(`Recent photo descriptions: ${descriptions.slice(0, 5).join('; ')}`);
        }
      }
      data._watermark_meta = { ...(data._watermark_meta as Record<string, unknown> ?? {}), last_sync_check: currentHour };
    }

    // 2. "On this day" memories — once per day
    const lastMemoryDate = meta?.last_memory_date as string | undefined;
    const today = todayET();
    if (lastMemoryDate !== today) {
      const { rows } = await pool.query<{ photo_count: string; year: number }>(
        `SELECT COUNT(*) as photo_count, EXTRACT(YEAR FROM taken_at)::int as year
         FROM photo_metadata
         WHERE EXTRACT(MONTH FROM taken_at) = EXTRACT(MONTH FROM CURRENT_DATE)
           AND EXTRACT(DAY FROM taken_at) = EXTRACT(DAY FROM CURRENT_DATE)
           AND EXTRACT(YEAR FROM taken_at) < EXTRACT(YEAR FROM CURRENT_DATE)
           AND taken_at IS NOT NULL
         GROUP BY EXTRACT(YEAR FROM taken_at)
         ORDER BY year DESC
         LIMIT 5`
      );
      if (rows.length > 0) {
        const total = rows.reduce((sum, r) => sum + parseInt(r.photo_count, 10), 0);
        const years = rows.map(r => `${r.year} (${r.photo_count})`).join(', ');
        parts.push(`On this day in previous years: ${total} photos/videos from: ${years}`);

        // Include descriptions from "On this day" photos
        const { rows: memoryDescriptions } = await pool.query<{ description: string; year: number }>(
          `SELECT description, EXTRACT(YEAR FROM taken_at)::int as year
           FROM photo_metadata
           WHERE EXTRACT(MONTH FROM taken_at) = EXTRACT(MONTH FROM CURRENT_DATE)
             AND EXTRACT(DAY FROM taken_at) = EXTRACT(DAY FROM CURRENT_DATE)
             AND EXTRACT(YEAR FROM taken_at) < EXTRACT(YEAR FROM CURRENT_DATE)
             AND taken_at IS NOT NULL AND description IS NOT NULL
           ORDER BY taken_at DESC LIMIT 5`
        );
        const memDescs = memoryDescriptions.map(r => `${r.year}: ${r.description}`);
        data.memories = {
          total,
          years: rows.map(r => ({ year: r.year, count: parseInt(r.photo_count, 10) })),
          ...(memDescs.length > 0 ? { descriptions: memDescs } : {}),
        };
        if (memDescs.length > 0) {
          parts.push(`Memory photo descriptions: ${memDescs.join('; ')}`);
        }
      }
      data._watermark_meta = { ...(data._watermark_meta as Record<string, unknown> ?? {}), last_memory_date: today };
    }

    if (parts.length === 0) return { source: 'photo_metadata', changed: false, summary: '', data: {} };

    return {
      source: 'photo_metadata',
      changed: true,
      summary: parts.join('. '),
      data,
    };
  } catch { return { source: 'photo_metadata', changed: false, summary: '', data: {} }; }
}

// ─── Monitor Registry ────────────────────────────────────────────

// ─── Haversine Distance ──────────────────────────────────────────

function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ─── Travel Detection ────────────────────────────────────────────

async function checkTravelStatus(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  try {
    // 1. Compute home anchor — location where user spends most nights (23:00–06:00) over 30 days
    const { rows: homeRows } = await pool.query<{ latitude: number; longitude: number; nights: string }>(
      `SELECT ROUND(latitude::numeric, 2)::float AS latitude,
              ROUND(longitude::numeric, 2)::float AS longitude,
              COUNT(*) AS nights
       FROM device_location
       WHERE (EXTRACT(HOUR FROM timestamp) >= 23 OR EXTRACT(HOUR FROM timestamp) < 6)
         AND timestamp >= NOW() - INTERVAL '30 days'
       GROUP BY ROUND(latitude::numeric, 2), ROUND(longitude::numeric, 2)
       ORDER BY nights DESC
       LIMIT 1`
    );
    if (homeRows.length === 0) {
      return { source: 'travel_episodes', changed: false, summary: '', data: {} };
    }
    const homeLat = homeRows[0].latitude;
    const homeLon = homeRows[0].longitude;

    // 2. Get most recent device location
    const { rows: locRows } = await pool.query<{ latitude: number; longitude: number; timestamp: Date }>(
      `SELECT latitude, longitude, timestamp FROM device_location ORDER BY timestamp DESC LIMIT 1`
    );
    if (locRows.length === 0) {
      return { source: 'travel_episodes', changed: false, summary: '', data: {} };
    }
    const currentLat = locRows[0].latitude;
    const currentLon = locRows[0].longitude;
    const currentTimestamp = locRows[0].timestamp;

    // 3. Calculate distance from home
    const distanceFromHome = haversineKm(homeLat, homeLon, currentLat, currentLon);

    // 4. Check for active travel episode
    const { rows: activeEpisodes } = await pool.query<{
      id: string;
      origin_lat: number;
      origin_lon: number;
      max_distance_km: number;
    }>(
      `SELECT id, origin_lat, origin_lon, max_distance_km FROM travel_episodes WHERE status = 'active' LIMIT 1`
    );
    const activeEpisode = activeEpisodes[0] ?? null;

    // 5. If far from home (>80km) and away for >6 hours, start travel episode
    if (distanceFromHome > 80 && !activeEpisode) {
      // Check how long user has been >80km from home
      const { rows: awayRows } = await pool.query<{ earliest: Date }>(
        `SELECT MIN(timestamp) as earliest FROM device_location
         WHERE timestamp >= NOW() - INTERVAL '24 hours'
         AND (
           SELECT ROUND(latitude::numeric, 2)::float != $1
             OR ROUND(longitude::numeric, 2)::float != $2
             FROM (SELECT 1) t
         )
         ORDER BY timestamp ASC LIMIT 1`,
        [homeLat, homeLon]
      );

      // Simpler check: see if the oldest non-home location is >6 hours ago
      const { rows: firstAway } = await pool.query<{ first_away: Date }>(
        `SELECT MIN(timestamp) as first_away FROM device_location
         WHERE timestamp >= NOW() - INTERVAL '24 hours'`,
      );

      const hoursAway = firstAway[0]?.first_away
        ? (Date.now() - new Date(firstAway[0].first_away).getTime()) / (1000 * 60 * 60)
        : 0;

      if (hoursAway >= 6) {
        // Resolve destination coordinates to a location name
        const destinationName = await reverseGeocode(currentLat, currentLon);

        // Create a new travel episode
        await pool.query(
          `INSERT INTO travel_episodes (start_at, origin_lat, origin_lon, origin_name, destination_lat, destination_lon, destination_name, max_distance_km, status)
           VALUES ($1, $2, $3, 'Home', $4, $5, $6, $7, 'active')`,
          [currentTimestamp, homeLat, homeLon, currentLat, currentLon, destinationName, Math.round(distanceFromHome)]
        );

        await updateWatermark('travel_episodes', true);

        const locationLabel = destinationName || `${currentLat.toFixed(4)}, ${currentLon.toFixed(4)}`;
        return {
          source: 'travel_episodes',
          changed: true,
          summary: `Travel detected: ${Math.round(distanceFromHome)}km from home, currently near ${locationLabel}`,
          data: {
            distance_km: Math.round(distanceFromHome),
            destination: locationLabel,
          },
        };
      }
    }

    // 6. If active episode exists and user is back near home (<20km), complete it
    if (activeEpisode && distanceFromHome < 20) {
      const maxDist = Math.max(activeEpisode.max_distance_km ?? 0, distanceFromHome);
      await pool.query(
        `UPDATE travel_episodes SET end_at = NOW(), status = 'completed', max_distance_km = $1
         WHERE id = $2`,
        [Math.round(maxDist), activeEpisode.id]
      );

      await updateWatermark('travel_episodes', true);

      return {
        source: 'travel_episodes',
        changed: true,
        summary: `Travel completed: returned home after ${Math.round(maxDist)}km max distance`,
        data: { episode_id: activeEpisode.id, max_distance_km: Math.round(maxDist) },
      };
    }

    // 7. If active episode, update max distance if needed
    if (activeEpisode && distanceFromHome > (activeEpisode.max_distance_km ?? 0)) {
      const destName = await reverseGeocode(currentLat, currentLon);
      await pool.query(
        `UPDATE travel_episodes SET max_distance_km = $1, destination_lat = $2, destination_lon = $3, destination_name = COALESCE($4, destination_name)
         WHERE id = $5`,
        [Math.round(distanceFromHome), currentLat, currentLon, destName, activeEpisode.id]
      );
    }

    return { source: 'travel_episodes', changed: false, summary: '', data: {} };
  } catch (err) {
    logger.logMinimal('Travel status check failed:', err instanceof Error ? err.message : err);
    return { source: 'travel_episodes', changed: false, summary: '', data: {} };
  }
}

// ─── Geofence Events Monitor ─────────────────────────────────────

async function checkGeofenceEvents(wm: WatermarkRow): Promise<ChangeDetection> {
  const pool = getPool();
  const since = wm.last_checked_at;
  try {
    const { rows } = await pool.query<{
      event_type: string;
      name: string;
      recorded_at: Date;
    }>(
      `SELECT ge.event_type, gl.name, ge.recorded_at
       FROM geofence_events ge
       JOIN geofence_locations gl ON gl.id = ge.geofence_id
       WHERE ge.created_at > $1
       ORDER BY ge.recorded_at DESC LIMIT 10`,
      [since]
    );
    if (rows.length === 0) return { source: 'geofence_events', changed: false, summary: '', data: {} };

    const descriptions = rows.map(r =>
      `${r.event_type === 'arrival' ? 'Arrived at' : 'Left'} ${r.name}`
    );

    return {
      source: 'geofence_events',
      changed: true,
      summary: `${rows.length} geofence event(s): ${descriptions[0]}`,
      data: { count: rows.length, events: rows },
    };
  } catch { return { source: 'geofence_events', changed: false, summary: '', data: {} }; }
}

// ─── Monitor Registry ────────────────────────────────────────────

const MONITORS: Record<string, (wm: WatermarkRow) => Promise<ChangeDetection>> = {
  google_calendar: checkGoogleCalendar,
  gmail: checkGmail,
  core_memory: checkCoreMemory,
  device_reminders: checkDeviceReminders,
  contacts: checkContacts,
  health_data: checkHealthData,
  device_location: checkDeviceLocation,
  // conversations excluded — user's own chat messages should not trigger proactive alerts
  // conversations: checkConversations,
  twilio_messages: checkTwilioMessages,
  twilio_calls: checkTwilioCalls,
  aria_inbound_emails: checkAriaInboundEmails,
  looki: checkLooki,
  looki_realtime: checkLookiRealtime,
  social_interactions: checkSocialInteractions,
  // aria_journal excluded — ARIA's own output, monitoring it is self-referential
  // tempo_jobs excluded — infrastructure noise, not personal data. Monitoring belongs in log-monitor.
  // tempo_jobs: checkTempoJobs,
  device_activity: checkDeviceActivity,
  music: checkMusic,
  photo_metadata: checkPhotoMemories,
  travel_episodes: checkTravelStatus,
  geofence_events: checkGeofenceEvents,
};

// ─── Main Accumulator ────────────────────────────────────────────

/**
 * Run the context accumulator across all (or specified) data sources.
 * Returns IDs of newly created context_snapshots for sources that changed.
 */
export async function accumulateContext(sources?: string[]): Promise<{
  checked: number;
  changed: number;
  snapshot_ids: string[];
}> {
  logger.logVerbose(`accumulateContext() entry, sources=${sources ? sources.join(',') : 'all'}`);
  const stopTotal = logger.time('accumulateContext');
  const pool = getPool();
  const sourcesToCheck = sources ?? Object.keys(MONITORS);
  const snapshotIds: string[] = [];
  let changed = 0;

  // Ensure all watermarks exist and run all monitors in parallel
  const monitorEntries = sourcesToCheck
    .map(source => {
      const monitor = MONITORS[source];
      if (!monitor) {
        logger.logMinimal(`Unknown source: ${source}`);
        return null;
      }
      return { source, monitor };
    })
    .filter((e): e is { source: string; monitor: (wm: WatermarkRow) => Promise<ChangeDetection> } => e !== null);

  const results = await Promise.allSettled(
    monitorEntries.map(async ({ source, monitor }) => {
      const monitorStart = Date.now();
      let wm = await getWatermark(source);
      if (!wm) {
        logger.logVerbose(`Auto-creating watermark for source: ${source}`);
        wm = await ensureWatermark(source);
      }
      const detection = await monitor(wm);
      const monitorMs = Date.now() - monitorStart;
      if (monitorMs > 5_000) {
        logger.logMinimal(`Slow monitor: ${source} took ${Math.round(monitorMs / 1000)}s`);
      }
      return { source, detection, wm, monitorMs };
    })
  );

  // Log timing summary
  const timings: string[] = [];
  for (const result of results) {
    if (result.status === 'fulfilled') {
      const { source, monitorMs } = result.value;
      if (monitorMs > 2_000) {
        timings.push(`${source}:${Math.round(monitorMs / 1000)}s`);
      }
    }
  }
  if (timings.length > 0) {
    logger.logVerbose(`Slow monitors: ${timings.join(', ')}`);
  }

  // Process results sequentially (snapshot creation + watermark updates are cheap DB writes)
  for (const result of results) {
    if (result.status === 'rejected') {
      logger.logMinimal(`Monitor failed:`, result.reason instanceof Error ? result.reason.message : result.reason);
      continue;
    }

    const { source, detection, wm } = result.value;
    try {
      if (detection.changed) {
        // Extract watermark metadata from data if present
        const watermarkMeta = (detection.data._watermark_meta as Record<string, unknown>) ?? undefined;
        const snapshotData = { ...detection.data };
        delete snapshotData._watermark_meta;

        const snapshotId = await saveSnapshot(source, detection.summary, snapshotData);
        snapshotIds.push(snapshotId);
        changed++;

        // Update watermark with change detected + any source-specific metadata
        const newMeta = watermarkMeta ? { ...wm.metadata, ...watermarkMeta } : undefined;
        await updateWatermark(source, true, newMeta);

        logger.logVerbose(`${source}: ${detection.summary}`);
      } else {
        // Still update watermark for unchanged sources that returned watermark metadata
        const watermarkMeta = (detection.data._watermark_meta as Record<string, unknown>) ?? undefined;
        if (watermarkMeta) {
          await updateWatermark(source, false, { ...wm.metadata, ...watermarkMeta });
        } else {
          await updateWatermark(source, false);
        }
      }
    } catch (err) {
      logger.logMinimal(`Error processing ${source}:`, err instanceof Error ? err.message : err);
    }
  }

  if (changed > 0) {
    logEvent({
      action: `Context accumulator: checked ${sourcesToCheck.length} sources, ${changed} changed`,
      component: 'proactive',
      category: 'background',
      metadata: { checked: sourcesToCheck.length, changed, snapshot_ids: snapshotIds },
    });
  }

  logger.log(`Checked ${sourcesToCheck.length} sources, ${changed} changed, ${snapshotIds.length} snapshot(s)`);
  stopTotal();
  return { checked: sourcesToCheck.length, changed, snapshot_ids: snapshotIds };
}
