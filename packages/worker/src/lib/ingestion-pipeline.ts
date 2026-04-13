/**
 * Unified Ingestion Pipeline
 *
 * Every piece of data enters through ingest() → normalize → immediate check → store → queue enrichment.
 * Source-specific normalizers convert raw data to IngestRecord format.
 * Proactive rules check if immediate action is needed.
 * Enrichment config determines which jobs to queue per category.
 */
import { getPool, createLogger } from '@nexus/core';

const logger = createLogger('ingestion');

// ── Types ───────────────────────────────────────────────────

export type Category = 'email' | 'message' | 'event' | 'photo' | 'health' | 'location' | 'social' | 'document' | 'contact' | 'activity';

export interface IngestRecord {
  source_key: string;
  source_id: string;
  timestamp: Date;
  category: Category;
  summary: string;
  content: string | null;
  entities: Array<{ name: string; type: string }>;
  metadata: Record<string, unknown>;
  raw: unknown;
}

export type Normalizer = (rawData: unknown) => IngestRecord[];

// ── Normalizer Registry ─────────────────────────────────────

const normalizers = new Map<string, Normalizer>();

export function registerNormalizer(sourceKey: string, fn: Normalizer): void {
  normalizers.set(sourceKey, fn);
  logger.logVerbose(`Registered normalizer: ${sourceKey}`);
}

// ── Core Pipeline ───────────────────────────────────────────

/**
 * Ingest raw data from a source. This is the single entry point.
 */
export async function ingest(sourceKey: string, rawData: unknown[]): Promise<{
  ingested: number;
  proactive: number;
  enrichments: number;
  skipped: number;
}> {
  const normalizer = normalizers.get(sourceKey);
  if (!normalizer) {
    throw new Error(`No normalizer registered for source: ${sourceKey}`);
  }

  const pool = getPool();
  let ingested = 0;
  let proactive = 0;
  let enrichments = 0;
  let skipped = 0;

  // Collect PIE snapshot IDs for batched significance-check (instead of 1 job per message)
  const pieSnapshotIds: string[] = [];

  for (const raw of rawData) {
    const records = normalizer(raw);

    for (const record of records) {
      try {
        // 1+2: Atomic: ingestion_log + source-specific store in a single transaction.
        // If either fails, neither commits — prevents phantom ingestion_log entries.
        const client = await pool.connect();
        let logId: string;
        try {
          await client.query('BEGIN');

          // Dedup check
          const { rows } = await client.query(
            `INSERT INTO ingestion_log (source_key, source_id, timestamp, category, summary, entities, metadata)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (source_key, source_id) DO NOTHING
             RETURNING id`,
            [record.source_key, record.source_id, record.timestamp, record.category,
             record.summary, JSON.stringify(record.entities), JSON.stringify(record.metadata)],
          );

          if (rows.length === 0) {
            await client.query('ROLLBACK');
            skipped++;
            continue; // already ingested
          }

          logId = rows[0].id;

          // Store in source-specific table (same transaction)
          await storeSourceSpecific(record);

          await client.query('COMMIT');
        } catch (err) {
          await client.query('ROLLBACK');
          logger.logMinimal(`Ingest+store error for ${record.source_key}/${record.source_id}:`, (err as Error).message);
          continue; // Skip PIE and enrichment — data didn't persist
        } finally {
          client.release();
        }

        ingested++;

        // 2b. Resolve person identity (deterministic match at ingest time)
        if (record.entities?.length > 0 || record.metadata?.handle_id || record.metadata?.from_address) {
          try {
            const identifier = String(record.metadata?.handle_id || record.metadata?.from_address || record.entities?.[0]?.name || '');
            const platform = record.source_key;
            const displayName = String(record.metadata?.from_name || record.entities?.[0]?.name || '');
            if (identifier.length > 1) {
              await pool.query(
                `SELECT resolve_person_identity($1, $2, $3)`,
                [identifier, platform, displayName || null],
              );
            }
          } catch (err) {
            logger.logDebug('Person identity resolution error:', (err as Error).message);
          }
        }

        // 3. Immediate action check (only runs if store succeeded)
        try {
          const actionTaken = await checkProactiveRules(record);
          if (actionTaken) proactive++;
        } catch (err) {
          logger.logDebug('Proactive rules error:', (err as Error).message);
        }

        // 3b. Feed context_snapshots for PIE (batched — one significance-check per sync, not per message)
        const PIE_CATEGORIES: Category[] = ['email', 'message', 'event'];
        // Newsletter / bulk-mail filter — don't let SCMP, marketing blasts, or
        // automated notifications seed proactive_insights. They were causing
        // ARIA to fire repeated Pushover alerts for "Breaking News" headlines.
        const isNewsletter = record.category === 'email' && (() => {
          const from = String(record.metadata?.from ?? '').toLowerCase();
          if (!from) return false;
          return /(?:^|<|@)(?:no[-_.]?reply|do[-_.]?not[-_.]?reply|news|newsletter|notifications?|digest|updates?|alerts?|marketing|promo|info|hello|team|support|noreply|donotreply)@|@e\.|@em\.|@mail\./.test(from)
            || /unsubscribe/i.test(String(record.content ?? '').slice(0, 2000));
        })();
        if (PIE_CATEGORIES.includes(record.category) && !isNewsletter) {
          try {
            const { rows: snapRows } = await pool.query<{ id: string }>(
              `INSERT INTO context_snapshots (source, change_summary, change_data, fast_tracked)
               VALUES ($1, $2, $3, false)
               RETURNING id`,
              [
                record.source_key,
                record.summary,
                JSON.stringify({
                  source_key: record.source_key,
                  source_id: record.source_id,
                  category: record.category,
                  timestamp: record.timestamp,
                  entities: record.entities,
                  metadata: record.metadata,
                }),
              ],
            );

            if (snapRows.length > 0) {
              pieSnapshotIds.push(snapRows[0].id);
            }
          } catch (err) {
            logger.logDebug('Failed to create context snapshot:', (err as Error).message);
          }
        }

        // Mark as checked
        await pool.query(
          `UPDATE ingestion_log SET proactive_checked = true WHERE id = $1`,
          [logId],
        );

        // 4. Queue enrichments
        const enrichCount = await queueEnrichments(record, logId);
        enrichments += enrichCount;

        // Mark as queued
        await pool.query(
          `UPDATE ingestion_log SET enrichments_queued = true WHERE id = $1`,
          [logId],
        );

      } catch (err) {
        logger.logMinimal(`Ingest error for ${record.source_key}/${record.source_id}:`, (err as Error).message);
      }
    }
  }

  // Batch PIE: one significance-check job per sync batch, not per message.
  // Groups up to 10 snapshot IDs per job to avoid flooding the queue.
  if (pieSnapshotIds.length > 0) {
    const PIE_BATCH_SIZE = 10;
    for (let i = 0; i < pieSnapshotIds.length; i += PIE_BATCH_SIZE) {
      const batch = pieSnapshotIds.slice(i, i + PIE_BATCH_SIZE);
      try {
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts)
           VALUES ('significance-check', $1, 'nexus', 1, 2)`,
          [JSON.stringify({ snapshot_ids: batch })],
        );
      } catch (err) {
        logger.logDebug('Failed to queue significance-check batch:', (err as Error).message);
      }
    }
    logger.logVerbose(`Queued ${Math.ceil(pieSnapshotIds.length / PIE_BATCH_SIZE)} significance-check job(s) for ${pieSnapshotIds.length} snapshot(s)`);
  }

  // Update data source registry
  await pool.query(
    `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active', consecutive_failures = 0
     WHERE source_key = $1`,
    [sourceKey],
  );

  logger.log(`Ingested ${sourceKey}: ${ingested} new, ${skipped} dupes, ${proactive} actions, ${enrichments} enrichments queued`);
  return { ingested, proactive, enrichments, skipped };
}

// ── Source-Specific Storage ─────────────────────────────────

async function storeSourceSpecific(record: IngestRecord): Promise<void> {
  const pool = getPool();
  const m = record.metadata;

  switch (record.source_key) {
    case 'gmail': {
      // to_addresses, cc_addresses, labels are PostgreSQL arrays — ensure they're arrays
      const toArr = Array.isArray(m.to_addresses) ? m.to_addresses : (m.to_addresses ? [String(m.to_addresses)] : []);
      const ccArr = Array.isArray(m.cc_addresses) ? m.cc_addresses : (m.cc_addresses ? [String(m.cc_addresses)] : []);
      const labelsArr = Array.isArray(m.labels) ? m.labels : (m.labels ? [String(m.labels)] : []);
      await pool.query(
        `INSERT INTO aurora_raw_gmail (id, thread_id, date, from_address, from_name, to_addresses, cc_addresses, subject, body_text, labels, snippet, has_attachments, is_sent, ingested_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
         ON CONFLICT (id) DO NOTHING`,
        [record.source_id, m.thread_id, record.timestamp, m.from, m.from_name,
         toArr, ccArr, m.subject, record.content,
         labelsArr, m.snippet, m.has_attachments ?? false, m.is_sent ?? false],
      );
      break;
    }

    case 'google_calendar': {
      const startTime = m.start_time ? new Date(String(m.start_time)) : record.timestamp;
      const endTime = m.end_time ? new Date(String(m.end_time)) : startTime;
      const isAllDay = !String(m.start_time ?? '').includes('T');
      await pool.query(
        `INSERT INTO device_calendar_events (device_id, title, notes, start_date, end_date, is_all_day, location, attendees, status, source, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'google', NOW())
         ON CONFLICT (device_id) DO UPDATE SET title=$2, notes=$3, start_date=$4, end_date=$5, is_all_day=$6, location=$7, attendees=$8, status=$9, updated_at=NOW()`,
        [record.source_id, m.title, m.description, startTime, endTime,
         isAllDay, m.location, JSON.stringify(m.attendees ?? []), m.status ?? 'confirmed'],
      );
      break;
    }

    case 'imessage': {
      // Sanitize null bytes — iMessages can contain \x00 from reactions/tapbacks
      const sanitize = (s: unknown) => typeof s === 'string' ? s.replace(/\x00/g, '') : s;
      await pool.query(
        `INSERT INTO aurora_raw_imessage (guid, text, date, is_from_me, handle_id, service, chat_id, has_attachments, group_title, associated_message_guid, associated_message_type, ingested_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
         ON CONFLICT (guid) DO NOTHING`,
        [record.source_id, sanitize(record.content), record.timestamp, m.is_from_me,
         sanitize(m.handle_id), m.service, sanitize(m.chat_id), m.has_attachments, sanitize(m.group_title),
         m.associated_message_guid ?? null, m.associated_message_type ?? null],
      );
      break;
    }

    case 'instagram':
      await pool.query(
        `INSERT INTO aurora_raw_instagram (data_type, timestamp, content, media_url, sender, raw_data, harvested_at)
         VALUES ($1, $2, $3, $4, $5, $6, NOW())`,
        [m.data_type ?? 'post', record.timestamp, record.content,
         m.media_url, m.sender, JSON.stringify(record.raw)],
      );
      break;

    case 'apple_photos': {
      const persons = Array.isArray(m.persons) ? (m.persons as string[]).filter((p: string) => p !== '_UNKNOWN_') : [];
      const albums = Array.isArray(m.albums) ? m.albums as string[] : [];
      await pool.query(
        `INSERT INTO photo_metadata (local_identifier, taken_at, latitude, longitude, location_name,
         pixel_width, pixel_height, media_type, is_favorite, people, albums, source, synced_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'apple_photos', NOW())
         ON CONFLICT (local_identifier) DO UPDATE SET
           taken_at = COALESCE(EXCLUDED.taken_at, photo_metadata.taken_at),
           latitude = COALESCE(EXCLUDED.latitude, photo_metadata.latitude),
           longitude = COALESCE(EXCLUDED.longitude, photo_metadata.longitude),
           location_name = COALESCE(EXCLUDED.location_name, photo_metadata.location_name),
           people = EXCLUDED.people,
           albums = EXCLUDED.albums,
           is_favorite = EXCLUDED.is_favorite,
           synced_at = NOW()`,
        [record.source_id, record.timestamp,
         m.latitude ?? null, m.longitude ?? null, m.place ?? null,
         m.width ?? null, m.height ?? null,
         m.ismovie ? 'video' : 'photo', m.favorite ?? false,
         persons, albums],
      );
      break;
    }

    default:
      // Generic: store raw in metadata for sources without dedicated tables
      logger.logVerbose(`No source-specific table for ${record.source_key}, stored in ingestion_log only`);
  }
}

// ── Proactive Rules ─────────────────────────────────────────

async function checkProactiveRules(record: IngestRecord): Promise<boolean> {
  const pool = getPool();

  // Simple rules first — can be made database-driven later
  const rules = [
    {
      name: 'upcoming-event',
      categories: ['event'] as Category[],
      check: () => {
        const eventTime = record.timestamp.getTime();
        const now = Date.now();
        return eventTime > now && eventTime < now + 2 * 60 * 60 * 1000; // within 2 hours
      },
      urgency: 'urgent',
      insightCategory: 'calendar_prep',
      titleFn: () => `Event soon: ${record.summary}`,
    },
  ];

  for (const rule of rules) {
    if (!rule.categories.includes(record.category)) continue;
    if (!rule.check()) continue;

    try {
      await pool.query(
        `INSERT INTO proactive_insights (title, body, category, urgency, confidence, status)
         VALUES ($1, $2, $3, $4, 0.8, 'pending')`,
        [rule.titleFn(), record.summary, rule.insightCategory, rule.urgency],
      );
      logger.log(`Proactive insight: ${rule.name} → ${record.summary}`);
      return true;
    } catch (err) {
      logger.logDebug(`Proactive rule ${rule.name} failed:`, (err as Error).message);
    }
  }

  return false;
}

// ── Enrichment Queuing ──────────────────────────────────────

async function queueEnrichments(record: IngestRecord, logId: string): Promise<number> {
  const pool = getPool();

  const { rows: configs } = await pool.query<{ enrichment_type: string; job_type: string }>(
    `SELECT enrichment_type, job_type FROM enrichment_config
     WHERE category = $1 AND is_enabled = true`,
    [record.category],
  );

  // Map source_key to the table that embed-backfill needs
  const SOURCE_TO_TABLE: Record<string, string> = {
    gmail: 'aurora_raw_gmail',
    google_calendar: 'device_calendar_events',
    imessage: 'aurora_raw_imessage',
    apple_photos: 'photo_metadata',
    instagram: 'aurora_raw_instagram',
  };

  let queued = 0;
  for (const config of configs) {
    try {
      const payload: Record<string, unknown> = {
        source_key: record.source_key,
        source_id: record.source_id,
        ingestion_log_id: logId,
        category: record.category,
        triggered_by: 'ingestion-pipeline',
      };

      // embed-backfill needs a table name to know which table to process
      if (config.job_type === 'embed-backfill') {
        payload.table = SOURCE_TO_TABLE[record.source_key] ?? record.source_key;
      }

      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
         VALUES ($1, $2, 0, 3)`,
        [config.job_type, JSON.stringify(payload)],
      );
      queued++;
    } catch (err) {
      logger.logDebug(`Failed to queue ${config.enrichment_type}:`, (err as Error).message);
    }
  }

  return queued;
}

// ── Built-in Normalizers ────────────────────────────────────

registerNormalizer('gmail', (raw: unknown) => {
  const email = raw as Record<string, unknown>;
  return [{
    source_key: 'gmail',
    source_id: String(email.id ?? email.messageId ?? ''),
    timestamp: new Date(String(email.date ?? email.internalDate ?? Date.now())),
    category: 'email' as Category,
    summary: `${email.from ?? email.from_address ?? 'unknown'} — ${email.subject ?? '(no subject)'}`,
    content: String(email.body_text ?? email.body ?? email.snippet ?? ''),
    entities: [
      ...(email.from ? [{ name: String(email.from_address ?? email.from), type: 'person' }] : []),
    ],
    metadata: {
      from: email.from_address ?? email.from,
      from_name: email.from_name,
      to_addresses: email.to_addresses ?? email.to,
      cc_addresses: email.cc_addresses ?? email.cc,
      subject: email.subject,
      labels: email.labels ?? email.labelIds,
      thread_id: email.thread_id ?? email.threadId,
      snippet: email.snippet,
      has_attachments: email.has_attachments ?? false,
      is_sent: email.is_sent ?? false,
    },
    raw: email,
  }];
});

registerNormalizer('google_calendar', (raw: unknown) => {
  const event = raw as Record<string, unknown>;
  const start = event.start as Record<string, string> | undefined;
  const end = event.end as Record<string, string> | undefined;
  const attendees = (event.attendees as Array<{ email?: string; displayName?: string }>) ?? [];

  return [{
    source_key: 'google_calendar',
    source_id: String(event.id ?? ''),
    timestamp: new Date(start?.dateTime ?? start?.date ?? String(Date.now())),
    category: 'event' as Category,
    summary: String(event.summary ?? event.title ?? ''),
    content: String(event.description ?? ''),
    entities: attendees.map(a => ({ name: a.displayName ?? a.email ?? '', type: 'person' })),
    metadata: {
      title: event.summary ?? event.title,
      description: event.description,
      start_time: start?.dateTime ?? start?.date,
      end_time: end?.dateTime ?? end?.date,
      location: event.location,
      attendees,
      status: event.status,
    },
    raw: event,
  }];
});

registerNormalizer('instagram', (raw: unknown) => {
  const item = raw as Record<string, unknown>;

  // Comments have _data_type='comment' tagged by the sync handler
  if (item._data_type === 'comment') {
    const user = item.user as Record<string, unknown> | undefined;
    const username = String(user?.username ?? item.username ?? '');
    const commentId = String(item.pk ?? item.id ?? '');
    return [{
      source_key: 'instagram',
      source_id: `comment_${commentId}`,
      timestamp: new Date(
        item.created_at ? Number(item.created_at) * 1000 : String(item.timestamp ?? Date.now()),
      ),
      category: 'social' as Category,
      summary: `@${username} commented: ${String(item.text ?? '').slice(0, 80)}`,
      content: String(item.text ?? ''),
      entities: username ? [{ name: username, type: 'person' }] : [],
      metadata: {
        data_type: 'comment',
        sender: username,
        parent_post_id: item._parent_post_id,
        like_count: item.comment_like_count ?? item.like_count,
      },
      raw: item,
    }];
  }

  // Posts (own posts or timeline feed)
  const caption = item.caption as Record<string, unknown> | undefined;
  const captionText = String(caption?.text ?? item.caption_text ?? item.text ?? '');
  const user = item.user as Record<string, unknown> | undefined;
  const username = String(user?.username ?? item.username ?? '');
  const mediaUrl = String(item.image_url ?? item.thumbnail_url ?? item.media_url ?? '');
  const postId = String(item.pk ?? item.id ?? item.media_id ?? '');

  return [{
    source_key: 'instagram',
    source_id: postId,
    timestamp: new Date(
      item.taken_at
        ? Number(item.taken_at) * 1000
        : String(item.timestamp ?? Date.now()),
    ),
    category: 'social' as Category,
    summary: `@${username}: ${captionText.slice(0, 80)}`,
    content: captionText,
    entities: username ? [{ name: username, type: 'person' }] : [],
    metadata: {
      data_type: 'post',
      sender: username,
      media_url: mediaUrl,
      like_count: item.like_count,
      comment_count: item.comment_count,
      media_type: item.media_type,
    },
    raw: item,
  }];
});

registerNormalizer('apple_photos', (raw: unknown) => {
  const photo = raw as Record<string, unknown>;
  const uuid = String(photo.uuid ?? '');
  const persons = (photo.persons as string[] ?? []).filter(p => p !== '_UNKNOWN_');
  const place = String(photo.place ?? '');
  const isMovie = !!photo.ismovie;

  const summaryParts = [isMovie ? 'Video' : 'Photo'];
  if (persons.length > 0) summaryParts.push(`with ${persons.join(', ')}`);
  if (place) summaryParts.push(`at ${place}`);

  return [{
    source_key: 'apple_photos',
    source_id: uuid,
    timestamp: new Date(String(photo.date ?? Date.now())),
    category: 'photo' as Category,
    summary: summaryParts.join(' '),
    content: String(photo.description ?? photo.title ?? ''),
    entities: persons.map(p => ({ name: p, type: 'person' })),
    metadata: {
      filename: photo.filename,
      latitude: photo.latitude,
      longitude: photo.longitude,
      place,
      width: photo.width,
      height: photo.height,
      favorite: photo.favorite,
      ismovie: isMovie,
      persons,
      albums: photo.albums ?? [],
      keywords: photo.keywords ?? [],
    },
    raw: photo,
  }];
});

registerNormalizer('imessage', (raw: unknown) => {
  const msg = raw as Record<string, unknown>;
  // Sanitize null bytes that iMessages can contain (reactions, tapbacks)
  const clean = (s: unknown) => typeof s === 'string' ? s.replace(/\x00/g, '') : String(s ?? '');
  const text = clean(msg.text);
  return [{
    source_key: 'imessage',
    source_id: String(msg.guid ?? msg.id ?? ''),
    timestamp: new Date(String(msg.date ?? Date.now())),
    category: 'message' as Category,
    summary: `${msg.is_from_me ? 'Sent' : 'Received'}: ${text.slice(0, 80)}`,
    content: text,
    entities: msg.handle_id ? [{ name: clean(msg.handle_id), type: 'person' }] : [],
    metadata: {
      is_from_me: msg.is_from_me,
      handle_id: msg.handle_id,
      service: msg.service,
      chat_id: msg.chat_id,
      has_attachments: msg.has_attachments ?? false,
      group_title: msg.group_title,
    },
    raw: msg,
  }];
});
