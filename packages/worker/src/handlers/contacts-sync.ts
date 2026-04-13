/**
 * Contacts Sync — pulls contacts from two sources:
 * 1. iMessage MCP on Dev-Server (message-based contact list with handles)
 * 2. Apple Contacts iCloud DB on Dev-Server (birthdays, relations, nicknames, maiden names)
 *
 * Syncs into the contacts table AND enriches aurora_social_identities with
 * birthday, birth_year, and nickname data.
 *
 * Runs every 6 hours. Self-chains after completion.
 */
import { getPool, createLogger } from '@nexus/core';
import { callMcpTool } from '../lib/mcp-client-manager.js';
import type { TempoJob } from '../job-worker.js';
import { execSync } from 'child_process';

const logger = createLogger('handler/contacts-sync');

const INTERVAL_SECONDS = 6 * 60 * 60; // 6 hours

// iCloud source DB path on Dev-Server (contains full contact cards with birthdays)
const CONTACTS_DB_PATH = '~/Library/Application\\ Support/AddressBook/Sources/*/AddressBook-v22.abcddb';

interface AppleContact {
  first_name: string | null;
  last_name: string | null;
  birth_year: number | null;
  birthday_yearless: number | null;
  maiden_name: string | null;
  nickname: string | null;
}

interface AppleRelation {
  label: string;
  name: string;
  owner_first: string;
  owner_last: string;
}

export async function handleContactsSync(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const result: Record<string, unknown> = { checked: true };

  // Phase 1: iMessage MCP contacts (handles + message counts)
  try {
    const mcpResult = await syncFromImessageMcp(pool);
    Object.assign(result, mcpResult);
  } catch (err) {
    logger.logMinimal('iMessage MCP sync error:', (err as Error).message);
    result.mcp_error = (err as Error).message;
  }

  // Phase 2: Apple Contacts DB enrichment (birthdays, relations, nicknames)
  try {
    const enrichResult = await enrichFromAppleContacts(pool);
    Object.assign(result, enrichResult);
  } catch (err) {
    logger.logMinimal('Apple Contacts enrichment error:', (err as Error).message);
    result.enrich_error = (err as Error).message;
  }

  // Update data source registry
  await pool.query(
    `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active'
     WHERE source_key = 'contacts'`,
  ).catch(() => {});

  // Self-chain for next run
  try {
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
       VALUES ('contacts-sync', '{}', 'nexus', 1, 3, NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
    );
  } catch (err) {
    logger.logMinimal('Failed to self-chain contacts-sync:', (err as Error).message);
  }

  return result;
}

// ── Phase 1: iMessage MCP ──────────────────────────────────

async function syncFromImessageMcp(pool: ReturnType<typeof getPool>): Promise<Record<string, unknown>> {
  let contactsResult: string;
  try {
    contactsResult = await callMcpTool('imessage', 'list_contacts', {
      limit: 500,
      sort_by: 'messages',
    });
  } catch (err) {
    logger.logMinimal('Contacts MCP error:', (err as Error).message);
    return { mcp_error: (err as Error).message };
  }

  let contacts: Record<string, unknown>[];
  try {
    const parsed = JSON.parse(contactsResult);
    contacts = Array.isArray(parsed) ? parsed : (parsed.contacts ?? parsed.data ?? []);
  } catch {
    const match = contactsResult.match(/\[[\s\S]*\]/);
    if (match) {
      try { contacts = JSON.parse(match[0]); } catch { contacts = []; }
    } else {
      logger.log('Contacts returned unparseable:', contactsResult.slice(0, 300));
      return { mcp_synced: 0 };
    }
  }

  if (contacts.length === 0) {
    logger.logVerbose('No contacts returned from MCP');
    return { mcp_synced: 0 };
  }

  logger.log(`Processing ${contacts.length} contacts from iMessage MCP`);
  let synced = 0;

  for (const c of contacts) {
    const name = String(c.contact_name ?? c.display_name ?? c.name ?? '').trim();
    const handle = String(c.handle ?? c.phone ?? c.chat_id ?? '').trim();
    if (!name && !handle) continue;

    const displayName = name || handle;
    const messageCount = Number(c.message_count ?? c.total_messages ?? 0);
    const isEmail = handle.includes('@');
    const phones = !isEmail && handle ? [handle] : [];
    const emails = isEmail ? [handle] : [];

    try {
      await pool.query(
        `INSERT INTO contacts (display_name, phone_numbers, email_addresses, notes, updated_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (display_name) DO UPDATE SET
           phone_numbers = CASE
             WHEN contacts.phone_numbers IS NULL OR contacts.phone_numbers = '[]'::jsonb
             THEN EXCLUDED.phone_numbers ELSE contacts.phone_numbers END,
           email_addresses = CASE
             WHEN contacts.email_addresses IS NULL OR contacts.email_addresses = '[]'::jsonb
             THEN EXCLUDED.email_addresses ELSE contacts.email_addresses END,
           updated_at = NOW()
         RETURNING id`,
        [displayName, JSON.stringify(phones), JSON.stringify(emails),
         messageCount > 0 ? `iMessage: ${messageCount} messages` : null],
      );
      synced++;
    } catch (err) {
      logger.logVerbose(`Contact upsert failed for ${displayName}:`, (err as Error).message);
    }
  }

  logger.log(`iMessage MCP: ${synced} contacts synced`);
  return { mcp_synced: synced, mcp_total: contacts.length };
}

// ── Phase 2: Apple Contacts DB ─────────────────────────────

async function enrichFromAppleContacts(pool: ReturnType<typeof getPool>): Promise<Record<string, unknown>> {
  logger.log('Enriching from Apple Contacts DB on Dev-Server');

  // Pull contacts with birthdays
  let contactRows: AppleContact[];
  try {
    const raw = execSync(
      `ssh dev-server "sqlite3 -separator '|' ${CONTACTS_DB_PATH} \\"SELECT ZFIRSTNAME, ZLASTNAME, ZBIRTHDAYYEAR, ZBIRTHDAYYEARLESS, ZMAIDENNAME, ZNICKNAME FROM ZABCDRECORD WHERE ZFIRSTNAME IS NOT NULL\\""`,
      { timeout: 30000, encoding: 'utf-8' },
    );
    contactRows = raw.trim().split('\n').filter(Boolean).map(line => {
      const [first, last, year, yearless, maiden, nick] = line.split('|');
      return {
        first_name: first || null,
        last_name: last || null,
        birth_year: year && parseInt(year) > 1900 ? parseInt(year) : null,
        birthday_yearless: yearless ? parseFloat(yearless) : null,
        maiden_name: maiden || null,
        nickname: nick || null,
      };
    });
  } catch (err) {
    logger.logMinimal('Failed to query Apple Contacts DB:', (err as Error).message);
    return { enrich_error: 'SSH/sqlite3 failed' };
  }

  logger.log(`Found ${contactRows.length} Apple Contacts, ${contactRows.filter(c => c.birth_year).length} with birth years`);

  // Pull relations
  let relations: AppleRelation[];
  try {
    const raw = execSync(
      `ssh dev-server "sqlite3 -separator '|' ${CONTACTS_DB_PATH} \\"SELECT r.ZLABEL, r.ZNAME, rec.ZFIRSTNAME, rec.ZLASTNAME FROM ZABCDRELATEDNAME r JOIN ZABCDRECORD rec ON rec.Z_PK = r.ZOWNER\\""`,
      { timeout: 30000, encoding: 'utf-8' },
    );
    relations = raw.trim().split('\n').filter(Boolean).map(line => {
      const [label, name, ownerFirst, ownerLast] = line.split('|');
      return {
        label: (label || '').replace(/_\$!<|>!\$_/g, '').toLowerCase(),
        name: name || '',
        owner_first: ownerFirst || '',
        owner_last: ownerLast || '',
      };
    });
    logger.log(`Found ${relations.length} Apple Contact relations`);
  } catch {
    relations = [];
    logger.logVerbose('No relations data from Apple Contacts');
  }

  // Enrich aurora_social_identities with birthday + nickname
  let enriched = 0;
  for (const c of contactRows) {
    if (!c.first_name) continue;
    const fullName = [c.first_name, c.last_name].filter(Boolean).join(' ');

    // Try to match to aurora_social_identities
    const { rows } = await pool.query<{ id: number; display_name: string }>(
      `SELECT id, display_name FROM aurora_social_identities
       WHERE is_person = TRUE AND (
         LOWER(display_name) = LOWER($1)
         OR LOWER(display_name) = LOWER($2)
       ) LIMIT 1`,
      [fullName, c.first_name],
    );

    if (rows.length === 0) continue;
    const person = rows[0];

    // Build update
    const updates: string[] = [];
    const params: unknown[] = [person.id];
    let idx = 2;

    if (c.birth_year) {
      updates.push(`birth_year = COALESCE(birth_year, $${idx})`);
      params.push(c.birth_year);
      idx++;
    }

    if (c.nickname && c.nickname !== c.first_name) {
      updates.push(`nickname = COALESCE(nickname, $${idx})`);
      params.push(c.nickname);
      idx++;
    }

    if (updates.length > 0) {
      updates.push('updated_at = NOW()');
      await pool.query(
        `UPDATE aurora_social_identities SET ${updates.join(', ')} WHERE id = $1`,
        params,
      );
      enriched++;
    }

    // Store maiden name as alias
    if (c.maiden_name) {
      const maidenFull = [c.maiden_name, c.last_name].filter(Boolean).join(' ');
      await pool.query(
        `INSERT INTO person_aliases (person_id, alias_name, alias_type)
         VALUES ($1, $2, 'maiden_name') ON CONFLICT DO NOTHING`,
        [person.id, maidenFull],
      ).catch(() => {});
    }
  }

  // Store relations in person_connections
  let relationsStored = 0;
  for (const r of relations) {
    const ownerName = [r.owner_first, r.owner_last].filter(Boolean).join(' ');
    const { rows: owners } = await pool.query<{ id: number }>(
      `SELECT id FROM aurora_social_identities WHERE is_person = TRUE AND LOWER(display_name) = LOWER($1) LIMIT 1`,
      [ownerName],
    );
    if (owners.length === 0) continue;

    const { rows: related } = await pool.query<{ id: number }>(
      `SELECT id FROM aurora_social_identities WHERE is_person = TRUE AND LOWER(display_name) = LOWER($1) LIMIT 1`,
      [r.name],
    );

    if (related.length > 0) {
      await pool.query(
        `INSERT INTO person_connections (person_id, related_person_id, related_name, relationship)
         VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
        [owners[0].id, related[0].id, r.name, r.label],
      ).catch(() => {});
      relationsStored++;
    }
  }

  logger.log(`Apple Contacts: ${enriched} persons enriched, ${relationsStored} relations stored`);
  return { enriched, relations_stored: relationsStored, apple_contacts_total: contactRows.length };
}
