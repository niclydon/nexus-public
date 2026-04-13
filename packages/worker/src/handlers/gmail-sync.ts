/**
 * Gmail Sync — lightweight poller that runs every 90 seconds.
 * Searches Gmail via MCP for new messages, reads full content,
 * pushes through ingestion pipeline + urgency triage.
 * Self-chains after completion.
 *
 * The Gmail MCP returns plain text from search (not JSON), so we parse
 * the text format to extract message IDs, then read_email for full content.
 */
import { getPool, createLogger } from '@nexus/core';
import { callMcpTool } from '../lib/mcp-client-manager.js';
import { ingest } from '../lib/ingestion-pipeline.js';
import { triageUrgency, type TriageItem } from '../lib/urgency-triage.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/gmail-sync');

const INTERVAL_SECONDS = 90;
const MAX_NEW_EMAILS_PER_CYCLE = 10;

export async function handleGmailSync(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  try {
    // Get watermark
    const { rows } = await pool.query<{ last_sync_at: Date }>(
      `SELECT last_sync_at FROM data_source_registry WHERE source_key = 'gmail'`,
    );

    const lastSync = rows[0]?.last_sync_at ?? new Date(Date.now() - 24 * 60 * 60 * 1000);
    // Use newer_than for tight window, fall back to after: for first run
    const msSince = Date.now() - lastSync.getTime();
    const hoursSince = Math.max(1, Math.ceil(msSince / (60 * 60 * 1000)));
    const query = hoursSince <= 48
      ? `newer_than:${hoursSince}h`
      : `after:${lastSync.toISOString().split('T')[0].replace(/-/g, '/')}`;

    logger.logVerbose(`Checking Gmail: ${query}`);

    // Step 1: Search for message IDs
    let searchResult: string;
    try {
      searchResult = await callMcpTool('gmail', 'search_emails', {
        query,
        maxResults: 50,
      });
    } catch (err) {
      logger.logMinimal('Gmail MCP search error:', (err as Error).message);
      return { error: (err as Error).message, checked: false };
    }

    // Step 2: Parse message IDs from text response
    const messageIds = parseMessageIds(searchResult);
    if (messageIds.length === 0) {
      logger.logVerbose('No new message IDs found');
      await pool.query(
        `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active' WHERE source_key = 'gmail'`,
      );
      return { checked: true, new_emails: 0 };
    }

    logger.logVerbose(`Found ${messageIds.length} message IDs, checking against archive`);

    // Step 3: Filter out messages we already have
    const { rows: existing } = await pool.query<{ id: string }>(
      `SELECT id FROM aurora_raw_gmail WHERE id = ANY($1)`,
      [messageIds],
    );
    const existingIds = new Set(existing.map(r => r.id));
    const newIds = messageIds.filter(id => !existingIds.has(id));

    if (newIds.length === 0) {
      logger.logVerbose('All messages already archived');
      await pool.query(
        `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active' WHERE source_key = 'gmail'`,
      );
      return { checked: true, new_emails: 0, already_archived: messageIds.length };
    }

    logger.log(`${newIds.length} new emails to fetch (${existingIds.size} already archived)`);

    // Step 4: Read full content for new messages (cap per cycle)
    const toFetch = newIds.slice(0, MAX_NEW_EMAILS_PER_CYCLE);
    const emails: Record<string, unknown>[] = [];

    for (const msgId of toFetch) {
      try {
        const content = await callMcpTool('gmail', 'read_email', { messageId: msgId });
        const email = parseEmailContent(msgId, content);
        if (email) emails.push(email);
      } catch (err) {
        logger.logVerbose(`Failed to read email ${msgId}:`, (err as Error).message);
      }
    }

    if (emails.length === 0) {
      logger.logVerbose('No emails parsed successfully');
      return { checked: true, new_emails: 0, fetch_failures: toFetch.length };
    }

    // Step 5: Push through ingestion pipeline
    const result = await ingest('gmail', emails);

    // Step 6: Urgency triage on newly ingested emails
    let urgentCount = 0;
    if (result.ingested > 0) {
      const triageItems: TriageItem[] = emails.map(e => ({
        from: String(e.from_address ?? e.from ?? 'unknown'),
        subject: String(e.subject ?? '(no subject)'),
        preview: String(e.snippet ?? e.body_text ?? '').slice(0, 200),
      }));
      urgentCount = await triageUrgency(triageItems, { insightCategory: 'email_urgent' });
    }

    logger.log(`Gmail sync: ${result.ingested} new, ${result.skipped} dupes, ${result.proactive} actions, ${urgentCount} urgent`);

    return {
      checked: true,
      new_emails: result.ingested,
      duplicates: result.skipped,
      proactive_actions: result.proactive,
      enrichments_queued: result.enrichments,
      urgent_flagged: urgentCount,
      remaining: newIds.length - toFetch.length,
    };
  } finally {
    // Self-chain: enqueue next run regardless of success/failure, but skip
    // if one is already pending. Prevents runaway pile-up if something
    // enqueues outside the normal path (stale reaper, manual inserts,
    // overlapping handler runs).
    try {
      const existing = await pool.query(
        `SELECT id FROM tempo_jobs WHERE job_type = 'gmail-sync' AND status = 'pending' LIMIT 1`,
      );
      if (existing.rows.length === 0) {
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
           VALUES ('gmail-sync', '{}', 'nexus', 3, 3, NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
        );
        logger.logVerbose(`Next gmail-sync scheduled in ${INTERVAL_SECONDS}s`);
      } else {
        logger.logVerbose('Skipped self-chain: gmail-sync already pending');
      }
    } catch (err) {
      logger.logMinimal('Failed to self-chain gmail-sync:', (err as Error).message);
    }
  }
}

// ── Parsers ────────────────────────────────────────────────────

/**
 * Parse message IDs from Gmail MCP search_emails text response.
 * Format: "ID: <hex>\nSubject: ...\nFrom: ...\nDate: ...\n\n"
 */
function parseMessageIds(text: string): string[] {
  const ids: string[] = [];
  const regex = /^ID:\s*(\S+)/gm;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(text)) !== null) {
    ids.push(match[1]);
  }
  return ids;
}

/**
 * Parse full email content from Gmail MCP read_email response.
 * The MCP returns human-readable text — we extract structured fields.
 */
function parseEmailContent(messageId: string, text: string): Record<string, unknown> | null {
  if (!text || text.length < 10) return null;

  const getField = (name: string): string => {
    const regex = new RegExp(`^${name}:\\s*(.+)$`, 'mi');
    const match = text.match(regex);
    return match?.[1]?.trim() ?? '';
  };

  // Extract from address from "From: Name <email@domain.com>" format
  const fromRaw = getField('From');
  const fromMatch = fromRaw.match(/<([^>]+)>/) ?? fromRaw.match(/(\S+@\S+)/);
  const fromAddress = fromMatch?.[1] ?? fromRaw;
  const fromName = fromRaw.replace(/<[^>]+>/, '').trim() || fromAddress;

  const subject = getField('Subject');
  const date = getField('Date');
  const to = getField('To');

  // Body: everything after the first blank line (after headers)
  const bodyStart = text.indexOf('\n\n');
  const body = bodyStart > 0 ? text.slice(bodyStart + 2).trim() : '';

  return {
    id: messageId,
    messageId,
    from: fromRaw,
    from_address: fromAddress,
    from_name: fromName,
    to_addresses: to,
    subject,
    date,
    body_text: body.slice(0, 10000), // cap body size
    snippet: body.slice(0, 200),
    labels: [],
    thread_id: messageId, // MCP doesn't give us thread ID, use message ID
    has_attachments: false,
    is_sent: false,
  };
}
