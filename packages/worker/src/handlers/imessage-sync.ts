/**
 * iMessage Sync — reads chat.db directly via SSH to Dev-Server.
 *
 * Bypasses the iMessage MCP server which had multiple issues:
 * - Used wrong parameter names (hours vs date_from)
 * - WAL lock prevented reading fresh messages
 * - Returned rowid instead of guid, causing dedup failures
 *
 * This handler:
 * 1. SSH into Dev-Server
 * 2. Copy chat.db to break WAL lock
 * 3. Run sqlite3 query for messages since last sync
 * 4. Parse JSON output and push through ingestion pipeline
 *
 * Self-chains after completion.
 */
import { execSync } from 'node:child_process';
import { getPool, createLogger } from '@nexus/core';
import { ingest } from '../lib/ingestion-pipeline.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/imessage-sync');

const ANVIL_HOST = 'dev-server.example.io';
const CHAT_DB = '~/Library/Messages/chat.db';
const TEMP_DB = '/tmp/imessage-sync-chat.db';
const BATCH_LIMIT = 500;
const INTERVAL_SECONDS = 60;

export async function handleImessageSync(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  try {
    logger.log(`Syncing iMessages (newest ${BATCH_LIMIT}, dedup by GUID)`);

    // SSH to Dev-Server: copy chat.db (breaks WAL lock), query newest messages as JSON.
    // No date filter — dedup by GUID handles duplicates naturally.
    // Write query script once (idempotent), then call with limit param.
    const queryScript = `#!/bin/bash
cp ~/Library/Messages/chat.db /tmp/imessage-sync-chat.db 2>/dev/null
sqlite3 -json /tmp/imessage-sync-chat.db "
SELECT m.guid, COALESCE(m.text, '') as text,
  datetime(m.date/1000000000 + 978307200, 'unixepoch') as date,
  m.is_from_me, COALESCE(h.id, '') as handle_id,
  COALESCE(m.service, 'iMessage') as service,
  COALESCE(c.chat_identifier, '') as chat_id,
  m.cache_has_attachments as has_attachments,
  COALESCE(c.display_name, '') as group_title,
  m.associated_message_guid,
  m.associated_message_type
FROM message m
LEFT JOIN handle h ON m.handle_id = h.ROWID
LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
LEFT JOIN chat c ON c.ROWID = cmj.chat_id
ORDER BY m.date DESC LIMIT \$1;
"`;

    // Write script to Dev-Server (idempotent)
    try {
      execSync(
        `ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 ${ANVIL_HOST} 'cat > /tmp/imessage-sync.sh && chmod +x /tmp/imessage-sync.sh'`,
        { input: queryScript, timeout: 10_000 },
      );
    } catch (err) {
      logger.logMinimal('Failed to write sync script to Dev-Server:', (err as Error).message?.slice(0, 200));
      return { error: 'script write failed', checked: false };
    }

    let rawOutput: string;
    try {
      rawOutput = execSync(
        `ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 ${ANVIL_HOST} '/tmp/imessage-sync.sh ${BATCH_LIMIT}'`,
        { timeout: 30_000, encoding: 'utf-8' },
      ).trim();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.logMinimal('SSH/sqlite3 error:', msg.slice(0, 200));
      return { error: msg.slice(0, 200), checked: false };
    }

    if (!rawOutput || rawOutput === '[]' || rawOutput === 'null') {
      logger.logVerbose('No new messages');
      await pool.query(
        `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active' WHERE source_key = 'imessage'`,
      );
      return { checked: true, new_messages: 0 };
    }

    // Parse JSON array
    let messages: Array<{
      guid: string;
      text: string;
      date: string;
      is_from_me: number;
      handle_id: string;
      service: string;
      chat_id: string;
      has_attachments: number;
      group_title: string;
    }>;

    try {
      messages = JSON.parse(rawOutput);
    } catch {
      logger.logMinimal('Failed to parse sqlite3 JSON output:', rawOutput.slice(0, 300));
      return { checked: true, new_messages: 0, parse_error: true };
    }

    if (messages.length === 0) {
      logger.logVerbose('No new messages');
      await pool.query(
        `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active' WHERE source_key = 'imessage'`,
      );
      return { checked: true, new_messages: 0 };
    }

    logger.log(`Found ${messages.length} messages from Dev-Server`);

    // Normalize for ingestion pipeline — normalize phone numbers to E.164
    const { normalizePhone } = await import('../lib/phone-normalize.js');
    const normalized = messages.map((m: Record<string, unknown>) => ({
      guid: String(m.guid ?? ''),
      text: String(m.text ?? ''),
      date: m.date ? new Date(m.date + 'Z').toISOString() : new Date().toISOString(),
      is_from_me: !!m.is_from_me,
      handle_id: normalizePhone(String(m.handle_id ?? '')),
      service: String(m.service ?? 'iMessage'),
      chat_id: String(m.chat_id ?? ''),
      has_attachments: !!m.has_attachments,
      group_title: String(m.group_title ?? ''),
      associated_message_guid: m.associated_message_guid ? String(m.associated_message_guid) : null,
      associated_message_type: m.associated_message_type != null ? Number(m.associated_message_type) : null,
    }));

    // Push through ingestion pipeline
    const result = await ingest('imessage', normalized);

    // Update watermark
    await pool.query(
      `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active', consecutive_failures = 0 WHERE source_key = 'imessage'`,
    );

    logger.log(`iMessage sync: ${result.ingested} new, ${result.skipped} dupes`);

    // Self-chain
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, executor, priority, max_attempts, next_run_at)
       VALUES ('imessage-sync', 'nexus', 1, 3, NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
    );

    return {
      checked: true,
      new_messages: result.ingested,
      duplicates: result.skipped,
      proactive_actions: result.proactive,
      enrichments_queued: result.enrichments,
    };

  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logMinimal('iMessage sync error:', msg);

    // Update failure count
    await pool.query(
      `UPDATE data_source_registry SET consecutive_failures = consecutive_failures + 1, status = 'error' WHERE source_key = 'imessage'`,
    );

    // Still self-chain on error
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, executor, priority, max_attempts, next_run_at)
       VALUES ('imessage-sync', 'nexus', 1, 3, NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
    );

    return { error: msg, checked: false };
  }
}
