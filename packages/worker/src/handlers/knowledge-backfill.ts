/**
 * Knowledge Backfill Handler
 *
 * Universal sync process that migrates data into the Knowledge Graph from
 * multiple source stores. Runs both as a one-time backfill and as a
 * recurring sync (every 30 min) to catch new data.
 *
 * Source modes:
 *   - owner_profile: Migrate legacy PKG facts (one-time backfill with offset)
 *   - core_memory:   Sync chat-sourced facts not yet in KG (recurring)
 *   - photo_metadata: Ingest described photos not yet in KG (recurring)
 *   - gmail:          Extract facts from emails — people, topics, context (recurring)
 *
 * Processes in batches with self-chaining for resumability.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import { ingestFact, generateEmbedding, type FactInput, type EntityHint } from '../lib/knowledge.js';
import { reverseGeocode, batchReverseGeocode, geocodeCacheKey } from '../lib/geocode.js';

const logger = createLogger('knowledge-backfill');

/** Process items with bounded concurrency. */
async function parallelMap<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let idx = 0;

  async function worker() {
    while (idx < items.length) {
      const i = idx++;
      results[i] = await fn(items[i]);
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()));
  return results;
}

// Concurrency for fact ingestion — each ingestFact acquires its own DB connection,
// so we can safely run multiple in parallel. 10 keeps DB pool pressure reasonable.
const INGEST_CONCURRENCY = 10;

// ---------------------------------------------------------------------------
// Entity hint extraction from fact text
// ---------------------------------------------------------------------------

/**
 * Extract entity hints from a fact's domain, category, key, and value.
 * Uses pattern matching to identify people, places, and topics.
 */
function extractEntityHints(
  domain: string,
  category: string,
  key: string,
  value: string,
  contactMap: Map<string, { id: string; name: string; phones: string[] }>
): EntityHint[] {
  const hints: EntityHint[] = [];

  // People domain — the key often IS the person name
  if (domain === 'people' || domain === 'communication') {
    // Extract person name from the key
    // Keys like "person_relationship" or "person_name" or "person_career"
    const nameFromKey = extractPersonNameFromKey(key);
    if (nameFromKey) {
      // Try to find a matching contact
      const contact = findContactByName(nameFromKey, contactMap);
      hints.push({
        name: contact?.name || nameFromKey,
        type: 'person',
        role: 'subject',
        contactId: contact?.id,
        aliases: contact?.phones || [],
      });
    }

    // Also look for names in the value text
    const namesInValue = extractNamesFromValue(value, contactMap);
    for (const nm of namesInValue) {
      // Avoid duplicating the key-derived name
      if (nameFromKey && normalizeName(nm.name) === normalizeName(nameFromKey)) continue;
      hints.push(nm);
    }
  }

  // Places domain
  if (domain === 'places' || domain === 'events') {
    const placeFromKey = extractPlaceFromKey(key);
    if (placeFromKey) {
      hints.push({ name: placeFromKey, type: 'place', role: 'location' });
    }
  }

  // Topic inference from domain
  const topicName = domainToTopic(domain, category);
  if (topicName) {
    hints.push({ name: topicName, type: 'topic', role: 'topic' });
  }

  return hints;
}

function normalizeName(name: string): string {
  return name.toLowerCase().trim().replace(/\s+/g, ' ');
}

/**
 * Convert snake_case keys like "person_relationship" → "Jane Smith"
 */
function extractPersonNameFromKey(key: string): string | null {
  // Remove common suffixes
  const suffixes = [
    '_relationship', '_career', '_preferences', '_family',
    '_health', '_personality', '_interests', '_communication',
    '_background', '_lifestyle', '_hobbies', '_music', '_food',
    '_travel', '_education', '_pets', '_vehicles', '_home',
    '_workplace', '_birthday', '_age', '_gender', '_location',
    '_occupation', '_role', '_contact', '_phone', '_email',
    '_nickname', '_status', '_notes', '_about', '_context',
    '_dynamic', '_history', '_frequency', '_pattern', '_topics',
    '_sentiment', '_morning', '_evening', '_weekend', '_weekday',
  ];

  let cleaned = key;
  for (const suffix of suffixes) {
    if (cleaned.endsWith(suffix)) {
      cleaned = cleaned.slice(0, -suffix.length);
      break;
    }
  }

  // Remove numeric suffixes
  cleaned = cleaned.replace(/_\d+$/, '');

  // Convert to title case
  const parts = cleaned.split('_').filter(p => p.length > 0);
  if (parts.length === 0 || parts.length > 4) return null;

  // Heuristic: if all parts look like name components (2+ chars, alpha only)
  const looksLikeName = parts.every(p => /^[a-zA-Z]{2,}$/.test(p));
  if (!looksLikeName) return null;

  return parts.map(p => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase()).join(' ');
}

function extractPlaceFromKey(key: string): string | null {
  // Keys like "city_state" or "city_trips" or "country_travel"
  const suffixes = ['_trips', '_travel', '_visits', '_home', '_work', '_office'];
  let cleaned = key;
  for (const suffix of suffixes) {
    if (cleaned.endsWith(suffix)) {
      cleaned = cleaned.slice(0, -suffix.length);
      break;
    }
  }

  const parts = cleaned.split('_').filter(p => p.length > 0);
  if (parts.length === 0 || parts.length > 3) return null;

  return parts.map(p => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase()).join(' ');
}

function extractNamesFromValue(
  value: string,
  contactMap: Map<string, { id: string; name: string; phones: string[] }>
): EntityHint[] {
  const hints: EntityHint[] = [];

  // Try to match known contact names in the value
  for (const [, contact] of contactMap) {
    if (value.toLowerCase().includes(contact.name.toLowerCase())) {
      hints.push({
        name: contact.name,
        type: 'person',
        role: 'object',
        contactId: contact.id,
        aliases: contact.phones,
      });
    }
  }

  return hints;
}

function findContactByName(
  name: string,
  contactMap: Map<string, { id: string; name: string; phones: string[] }>
): { id: string; name: string; phones: string[] } | undefined {
  const normalized = normalizeName(name);
  for (const [, contact] of contactMap) {
    if (normalizeName(contact.name) === normalized) return contact;
    // Also check if the extracted name is a partial match (first name only)
    if (normalizeName(contact.name).startsWith(normalized + ' ')) return contact;
  }
  return undefined;
}

function domainToTopic(domain: string, category: string): string | null {
  const topicMap: Record<string, string> = {
    'career': 'Career & Work',
    'health': 'Health & Fitness',
    'interests': 'Interests & Hobbies',
    'lifestyle': 'Lifestyle',
    'preferences': 'Preferences',
    'events': 'Life Events',
    'travel': 'Travel',
  };
  return topicMap[domain] || null;
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------
export async function handleKnowledgeBackfill(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as {
    sources?: string[];
    batch_size?: number;
    offset?: number;
    generate_embeddings?: boolean;
  };
  const sources = payload.sources || ['owner_profile'];
  const batchSize = payload.batch_size || 200;
  const offset = payload.offset || 0;
  const generateEmbeddings = payload.generate_embeddings !== false;

  logger.log(`Starting backfill: sources=${sources.join(',')}, batch=${batchSize}, offset=${offset}, embeddings=${generateEmbeddings}`);
  await jobLog(job.id, `Starting backfill: sources=${sources.join(',')}, offset=${offset}, batch=${batchSize}`);

  const pool = getPool();

  // Load contacts for entity resolution
  const { rows: contacts } = await pool.query<{
    id: string;
    display_name: string;
    phone_numbers: string[];
  }>(
    `SELECT id, display_name, phone_numbers FROM contacts WHERE display_name IS NOT NULL`
  );

  const contactMap = new Map<string, { id: string; name: string; phones: string[] }>();
  for (const c of contacts) {
    contactMap.set(c.id, { id: c.id, name: c.display_name, phones: c.phone_numbers || [] });
  }
  logger.log(`Loaded ${contactMap.size} contacts for entity resolution`);
  await jobLog(job.id, `Loaded ${contactMap.size} contacts for entity resolution`);

  let totalProcessed = 0;
  let totalWritten = 0;
  let totalErrors = 0;
  const sampleErrors: string[] = []; // capture first few error messages for diagnostics

  for (const source of sources) {
    if (source === 'owner_profile') {
      // Read batch from owner_profile
      const { rows: facts } = await pool.query<{
        domain: string;
        category: string;
        key: string;
        value: string;
        confidence: number;
        evidence_count: number;
        sources: string[];
        valid_from: string | null;
        valid_until: string | null;
      }>(
        `SELECT domain, category, key, value, confidence, evidence_count, sources, valid_from, valid_until
         FROM owner_profile
         WHERE superseded_by IS NULL
         ORDER BY id
         OFFSET $1 LIMIT $2`,
        [offset, batchSize]
      );

      logger.log(`Read ${facts.length} facts from owner_profile (offset ${offset})`);
      await jobLog(job.id, `Read ${facts.length} facts from owner_profile (offset ${offset})`);

      await parallelMap(facts, INGEST_CONCURRENCY, async (fact) => {
        try {
          const entityHints = extractEntityHints(fact.domain, fact.category, fact.key, fact.value, contactMap);

          const input: FactInput = {
            domain: fact.domain,
            category: fact.category,
            key: fact.key,
            value: fact.value,
            confidence: fact.confidence,
            source: fact.sources?.[0] || 'owner_profile_backfill',
            validFrom: fact.valid_from || undefined,
            validUntil: fact.valid_until || undefined,
            entityHints: entityHints.length > 0 ? entityHints : undefined,
            skipEmbedding: !generateEmbeddings,
          };

          const id = await ingestFact(input);
          if (id) totalWritten++;
          totalProcessed++;
        } catch (err) {
          totalErrors++;
          const msg = err instanceof Error ? err.message : String(err);
          if (sampleErrors.length < 5) sampleErrors.push(`${fact.domain}/${fact.key}: ${msg.slice(0, 200)}`);
          logger.logVerbose(`Error ingesting ${fact.domain}/${fact.category}/${fact.key}: ${msg}`);
        }
      });

      await jobLog(job.id, `Batch done: ${totalWritten} written, ${totalErrors} errors out of ${totalProcessed} processed`);

      // If we got a full batch, chain the next one
      if (facts.length === batchSize) {
        const nextOffset = offset + batchSize;

        // Only chain if no pending job already exists
        const { rows: existingKb } = await pool.query(
          `SELECT id FROM tempo_jobs WHERE job_type = 'knowledge-backfill' AND status = 'pending' LIMIT 1`,
        );
        if (existingKb.length === 0) {
          logger.log(`Chaining next batch at offset ${nextOffset}`);
          await jobLog(job.id, `Chaining next batch at offset ${nextOffset}`);

          await pool.query(
            `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
             VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
            [JSON.stringify({
              sources: ['owner_profile'],
              batch_size: batchSize,
              offset: nextOffset,
              generate_embeddings: generateEmbeddings,
            })]
          );
        } else {
          logger.log(`Skipped chain at offset ${nextOffset} — pending job already exists`);
        }
      } else {
        logger.log(`Owner profile backfill complete — no more rows after offset ${offset}`);
        await jobLog(job.id, `Owner profile backfill complete — processed all rows through offset ${offset}`);

        // If core_memory is also in sources and we just finished owner_profile,
        // chain core_memory backfill
        if (sources.includes('core_memory')) {
          logger.log('Chaining core_memory backfill');
          await pool.query(
            `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
             VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
            [JSON.stringify({
              sources: ['core_memory'],
              batch_size: batchSize,
              offset: 0,
              generate_embeddings: generateEmbeddings,
            })]
          );
        }
      }
    }

    if (source === 'core_memory') {
      // Find core_memory facts not yet synced to knowledge_facts.
      // Uses kg_synced_at watermark — facts are marked after successful ingestion.
      // Also picks up facts updated after their last sync.
      const { rows: facts } = await pool.query<{
        id: string;
        category: string;
        key: string;
        value: string;
      }>(
        `SELECT cm.id, cm.category, cm.key, cm.value
         FROM core_memory cm
         WHERE cm.superseded_by IS NULL
         AND (cm.kg_synced_at IS NULL OR cm.updated_at > cm.kg_synced_at)
         ORDER BY cm.updated_at ASC
         LIMIT $1`,
        [batchSize]
      );

      // Deduplicate within the batch — core_memory can have multiple rows with the
      // same category+key but different values, which cause unique constraint violations
      // when ingested concurrently. Keep only the most recent (first in DESC order).
      const seen = new Set<string>();
      const dedupedFacts = facts.filter(f => {
        const k = `${f.category}::${f.key}`;
        if (seen.has(k)) return false;
        seen.add(k);
        return true;
      });

      logger.log(`Read ${facts.length} facts from core_memory, ${dedupedFacts.length} after dedup`);
      await jobLog(job.id, `Read ${facts.length} facts from core_memory, ${dedupedFacts.length} after dedup`);

      const syncedIds: string[] = [];

      await parallelMap(dedupedFacts, INGEST_CONCURRENCY, async (fact) => {
        try {
          // Map core_memory categories to knowledge graph domains
          const domain = fact.category; // people, preferences, goals, recurring, context, other
          const entityHints = extractEntityHints(domain, fact.category, fact.key, fact.value, contactMap);

          const input: FactInput = {
            domain,
            category: fact.category,
            key: fact.key,
            value: fact.value,
            confidence: 0.6, // core_memory has no confidence — use moderate default
            source: 'core_memory_backfill',
            entityHints: entityHints.length > 0 ? entityHints : undefined,
            skipEmbedding: !generateEmbeddings,
          };

          const id = await ingestFact(input);
          if (id) totalWritten++;
          totalProcessed++;
          syncedIds.push(fact.id);
        } catch (err) {
          totalErrors++;
          const msg = err instanceof Error ? err.message : String(err);
          if (sampleErrors.length < 5) sampleErrors.push(`${fact.category}/${fact.key}: ${msg.slice(0, 200)}`);
          logger.logVerbose(`Error ingesting core_memory ${fact.category}/${fact.key}: ${msg}`);
          // Still mark as synced to avoid infinite retry on permanently failing facts
          syncedIds.push(fact.id);
        }
      });

      // Mark processed facts as synced so they don't reappear
      if (syncedIds.length > 0) {
        await pool.query(
          `UPDATE core_memory SET kg_synced_at = NOW() WHERE id = ANY($1::uuid[])`,
          [syncedIds]
        );
      }

      await jobLog(job.id, `Core memory batch done: ${totalWritten} written, ${totalErrors} errors, ${syncedIds.length} marked synced`);

      // Chain next batch if we got a full batch (more may remain)
      if (facts.length === batchSize) {
        logger.log('Chaining next core_memory batch');
        await jobLog(job.id, 'Chaining next core_memory batch');
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
           VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
          [JSON.stringify({
            sources: ['core_memory'],
            batch_size: batchSize,
            generate_embeddings: generateEmbeddings,
          })]
        );
      } else {
        logger.log(`Core memory sync complete — ${totalWritten} new/updated facts`);
        await jobLog(job.id, `Core memory sync complete — ${totalWritten} new/updated facts`);
      }
    }

    if (source === 'photo_metadata') {
      // Find photos that are described but not yet KG-ingested,
      // or whose metadata was updated after KG ingestion
      const { rows: photos } = await pool.query<{
        id: string;
        local_identifier: string;
        description: string;
        taken_at: string | null;
        latitude: number | null;
        longitude: number | null;
        is_favorite: boolean;
        people: string[] | null;
        media_type: string;
        kg_ingested_at: string | null;
      }>(
        `SELECT id, local_identifier, description, taken_at, latitude, longitude,
                is_favorite, people, media_type, kg_ingested_at
         FROM photo_metadata
         WHERE processed_at IS NOT NULL
           AND description IS NOT NULL
           AND (kg_ingested_at IS NULL OR described_at > kg_ingested_at)
         ORDER BY processed_at DESC
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${photos.length} photos needing KG sync`);
      await jobLog(job.id, `Found ${photos.length} photos needing KG sync`);

      // Batch reverse-geocode all photos with GPS coordinates
      const coordsToResolve = photos
        .filter(p => p.latitude != null && p.longitude != null)
        .map(p => ({ lat: Number(p.latitude), lon: Number(p.longitude) }));

      const locationMap = coordsToResolve.length > 0
        ? await batchReverseGeocode(coordsToResolve)
        : new Map<string, string>();

      if (locationMap.size > 0) {
        logger.log(`Resolved ${locationMap.size} unique locations for ${coordsToResolve.length} GPS-tagged photos`);
      }

      const photoIds: string[] = [];
      const locationUpdates: Array<{ id: string; locationName: string }> = [];

      await parallelMap(photos, INGEST_CONCURRENCY, async (photo) => {
        try {
          // Look up resolved location name for this photo
          let locationName: string | undefined;
          if (photo.latitude != null && photo.longitude != null) {
            const key = geocodeCacheKey(Number(photo.latitude), Number(photo.longitude));
            locationName = locationMap.get(key);
            if (locationName) {
              locationUpdates.push({ id: photo.id, locationName });
            }
          }

          const facts = buildPhotoFacts(photo, contactMap, locationName);

          for (const fact of facts) {
            const id = await ingestFact({
              ...fact,
              skipEmbedding: !generateEmbeddings,
            });
            if (id) totalWritten++;
            totalProcessed++;
          }

          photoIds.push(photo.id);
        } catch (err) {
          totalErrors++;
          const msg = err instanceof Error ? err.message : String(err);
          if (sampleErrors.length < 5) sampleErrors.push(`photo/${photo.local_identifier.slice(0, 30)}: ${msg.slice(0, 200)}`);
          logger.logVerbose(`Error ingesting photo ${photo.local_identifier}: ${msg}`);
        }
      });

      // Mark photos as KG-ingested and persist resolved location names
      if (photoIds.length > 0) {
        await pool.query(
          `UPDATE photo_metadata SET kg_ingested_at = NOW() WHERE id = ANY($1::uuid[])`,
          [photoIds]
        );
        logger.log(`Marked ${photoIds.length} photos as KG-ingested`);
      }

      // Batch update location_name on photo_metadata
      if (locationUpdates.length > 0) {
        for (const { id, locationName } of locationUpdates) {
          await pool.query(
            `UPDATE photo_metadata SET location_name = $1 WHERE id = $2 AND location_name IS NULL`,
            [locationName, id]
          );
        }
        logger.log(`Set location_name on ${locationUpdates.length} photos`);
      }

      await jobLog(job.id, `Photo sync done: ${photoIds.length} photos → ${totalWritten} facts written, ${totalErrors} errors`);

      // Chain next batch if we got a full batch (more may remain)
      if (photos.length === batchSize) {
        logger.log('Chaining next photo_metadata batch');
        await jobLog(job.id, 'Chaining next photo_metadata batch');
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
           VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
          [JSON.stringify({
            sources: ['photo_metadata'],
            batch_size: batchSize,
            generate_embeddings: generateEmbeddings,
          })]
        );
      }
    }

    if (source === 'gmail' || source === 'gmail_archive' || source === 'aurora_raw_gmail') {
      // Extract knowledge facts from emails — people, topics, commitments, events
      logger.log(`[gmail] Querying aurora_raw_gmail with batch_size=${batchSize}`);
      const { rows: emails } = await pool.query<{
        id: string;
        from_address: string;
        from_name: string;
        to_addresses: string[];
        subject: string;
        body_text: string;
        date: Date;
        labels: string[];
        is_sent: boolean;
        kg_ingested_at: string | null;
      }>(
        `SELECT id, from_address, from_name, to_addresses, subject,
                LEFT(body_text, 2000) as body_text, date, labels, is_sent, kg_ingested_at
         FROM aurora_raw_gmail
         WHERE (subject IS NOT NULL OR body_text IS NOT NULL)
           AND kg_ingested_at IS NULL
         ORDER BY date DESC
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${emails.length} emails needing KG extraction`);
      await jobLog(job.id, `Found ${emails.length} emails needing KG extraction`);

      if (emails.length === 0) {
        await jobLog(job.id, 'No emails need KG extraction');
      } else {
        const emailIds: string[] = [];

        await parallelMap(emails, INGEST_CONCURRENCY, async (email) => {
          try {
            const facts = buildEmailFacts(email);
            for (const fact of facts) {
              const id = await ingestFact(fact);
              if (id) totalWritten++;
              totalProcessed++;
            }
            emailIds.push(email.id);
          } catch (err) {
            totalErrors++;
            const msg = err instanceof Error ? err.message : String(err);
            if (sampleErrors.length < 5) sampleErrors.push(`gmail/${email.id.slice(0, 20)}: ${msg.slice(0, 200)}`);
            logger.logVerbose(`Error ingesting email ${email.id}: ${msg}`);
          }
        });

        // Mark as KG-ingested
        if (emailIds.length > 0) {
          await pool.query(
            `UPDATE aurora_raw_gmail SET kg_ingested_at = NOW() WHERE id = ANY($1::text[])`,
            [emailIds]
          );
          logger.log(`Marked ${emailIds.length} emails as KG-ingested`);
        }

        await jobLog(job.id, `Gmail KG: ${emailIds.length} emails → ${totalWritten} facts, ${totalErrors} errors`);

        // Self-chain if full batch
        if (emails.length === batchSize) {
          await pool.query(
            `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
             VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
            [JSON.stringify({ sources: ['gmail'], batch_size: batchSize })]
          );
        }
      }
    }

    if (source === 'blogger') {
      // Extract knowledge facts from personal blog posts (2002-2012)
      const { rows: posts } = await pool.query<{
        id: number;
        title: string | null;
        content_text: string | null;
        published_at: Date | null;
        tags: string[] | null;
      }>(
        `SELECT id, title, LEFT(content_text, 2000) as content_text, published_at, tags
         FROM blogger_posts
         WHERE content_text IS NOT NULL AND LENGTH(content_text) > 10
           AND kg_ingested_at IS NULL
         ORDER BY published_at ASC
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${posts.length} blog posts needing KG extraction`);
      await jobLog(job.id, `Found ${posts.length} blog posts needing KG extraction`);

      if (posts.length === 0) {
        await jobLog(job.id, 'No blog posts need KG extraction');
      } else {
        const postIds: number[] = [];

        await parallelMap(posts, INGEST_CONCURRENCY, async (post) => {
          try {
            // Build facts from blog content — personal journal entries about life, events, people
            const dateStr = post.published_at ? post.published_at.toISOString().slice(0, 10) : 'unknown';
            const entityHints = extractEntityHints('journal', 'blog', post.title || '', post.content_text || '', contactMap);

            // Main journal fact
            const input: FactInput = {
              domain: 'journal',
              category: 'blog',
              key: `blog_${dateStr}_${post.id}`,
              value: `${post.title || '(untitled)'}: ${post.content_text || ''}`.slice(0, 2000),
              confidence: 0.9,
              source: 'blogger_backfill',
              validFrom: post.published_at?.toISOString(),
              entityHints: entityHints.length > 0 ? entityHints : undefined,
              skipEmbedding: !generateEmbeddings,
            };

            const id = await ingestFact(input);
            if (id) totalWritten++;
            totalProcessed++;

            // Tag-based topic facts
            if (post.tags && post.tags.length > 0) {
              for (const tag of post.tags.slice(0, 5)) {
                try {
                  await ingestFact({
                    domain: 'interests',
                    category: 'blog_tags',
                    key: `tag_${tag.toLowerCase().replace(/\s+/g, '_')}`,
                    value: `Blog post tagged "${tag}" on ${dateStr}: ${post.title || '(untitled)'}`,
                    confidence: 0.8,
                    source: 'blogger_backfill',
                    validFrom: post.published_at?.toISOString(),
                    entityHints: [{ name: tag, type: 'topic', role: 'topic' }],
                    skipEmbedding: true,
                  });
                } catch {
                  // Duplicate tag fact — skip
                }
              }
            }

            postIds.push(post.id);
          } catch (err) {
            totalErrors++;
            const msg = err instanceof Error ? err.message : String(err);
            if (sampleErrors.length < 5) sampleErrors.push(`blogger/${post.id}: ${msg.slice(0, 200)}`);
            logger.logVerbose(`Error ingesting blog post ${post.id}: ${msg}`);
          }
        });

        // Mark as KG-ingested
        if (postIds.length > 0) {
          await pool.query(
            `UPDATE blogger_posts SET kg_ingested_at = NOW() WHERE id = ANY($1::int[])`,
            [postIds]
          );
          logger.log(`Marked ${postIds.length} blog posts as KG-ingested`);
        }

        await jobLog(job.id, `Blogger KG: ${postIds.length} posts → ${totalWritten} facts, ${totalErrors} errors`);

        // Self-chain if full batch
        if (posts.length === batchSize) {
          const { rows: existingKb } = await pool.query(
            `SELECT id FROM tempo_jobs WHERE job_type = 'knowledge-backfill' AND status = 'pending' LIMIT 1`,
          );
          if (existingKb.length === 0) {
            await pool.query(
              `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
               VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
              [JSON.stringify({ sources: ['blogger'], batch_size: batchSize, generate_embeddings: generateEmbeddings })]
            );
          }
        }
      }
    }

    if (source === 'google_voice') {
      // Extract knowledge facts from Google Voice texts and voicemails
      const { rows: records } = await pool.query<{
        id: number;
        record_type: string;
        phone_number: string | null;
        contact_name: string | null;
        timestamp: Date;
        message_text: string | null;
      }>(
        `SELECT id, record_type, phone_number, contact_name, timestamp,
                LEFT(message_text, 2000) as message_text
         FROM aurora_raw_google_voice
         WHERE message_text IS NOT NULL AND LENGTH(message_text) > 5
           AND kg_ingested_at IS NULL
         ORDER BY timestamp ASC
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${records.length} Google Voice records needing KG extraction`);
      await jobLog(job.id, `Found ${records.length} Google Voice records needing KG extraction`);

      if (records.length === 0) {
        await jobLog(job.id, 'No Google Voice records need KG extraction');
      } else {
        const recordIds: number[] = [];

        await parallelMap(records, INGEST_CONCURRENCY, async (rec) => {
          try {
            const dateStr = rec.timestamp ? new Date(rec.timestamp).toISOString().slice(0, 10) : 'unknown';
            const contactLabel = rec.contact_name || rec.phone_number || 'unknown';
            const entityHints: EntityHint[] = [];
            if (rec.contact_name) {
              entityHints.push({ name: rec.contact_name, type: 'person', role: 'subject' });
            }

            const input: FactInput = {
              domain: 'communication',
              category: rec.record_type === 'voicemail' ? 'voicemail' : 'sms',
              key: `gv_${rec.record_type}_${rec.id}`,
              value: `${contactLabel} (${rec.record_type}) on ${dateStr}: ${rec.message_text || ''}`.slice(0, 2000),
              confidence: 0.7,
              source: 'google_voice_backfill',
              validFrom: rec.timestamp?.toISOString(),
              entityHints: entityHints.length > 0 ? entityHints : undefined,
              skipEmbedding: !generateEmbeddings,
            };

            const id = await ingestFact(input);
            if (id) totalWritten++;
            totalProcessed++;
            recordIds.push(rec.id);
          } catch (err) {
            totalErrors++;
            const msg = err instanceof Error ? err.message : String(err);
            if (sampleErrors.length < 5) sampleErrors.push(`google_voice/${rec.id}: ${msg.slice(0, 200)}`);
            logger.logVerbose(`Error ingesting Google Voice record ${rec.id}: ${msg}`);
          }
        });

        if (recordIds.length > 0) {
          await pool.query(
            `UPDATE aurora_raw_google_voice SET kg_ingested_at = NOW() WHERE id = ANY($1::int[])`,
            [recordIds]
          );
          logger.log(`Marked ${recordIds.length} Google Voice records as KG-ingested`);
        }

        await jobLog(job.id, `Google Voice KG: ${recordIds.length} records → ${totalWritten} facts, ${totalErrors} errors`);

        if (records.length === batchSize) {
          const { rows: existingKb } = await pool.query(
            `SELECT id FROM tempo_jobs WHERE job_type = 'knowledge-backfill' AND status = 'pending' LIMIT 1`,
          );
          if (existingKb.length === 0) {
            await pool.query(
              `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
               VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
              [JSON.stringify({ sources: ['google_voice'], batch_size: batchSize, generate_embeddings: generateEmbeddings })]
            );
          }
        }
      }
    }

    if (source === 'facebook') {
      // Extract knowledge facts from Facebook messages, posts, and comments
      const { rows: records } = await pool.query<{
        id: number;
        data_type: string;
        timestamp: Date | null;
        content: string | null;
        sender: string | null;
        conversation_title: string | null;
      }>(
        `SELECT id, data_type, timestamp, LEFT(content, 2000) as content, sender, conversation_title
         FROM aurora_raw_facebook
         WHERE content IS NOT NULL AND LENGTH(content) > 5
           AND kg_ingested_at IS NULL
         ORDER BY timestamp ASC NULLS LAST
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${records.length} Facebook records needing KG extraction`);
      await jobLog(job.id, `Found ${records.length} Facebook records needing KG extraction`);

      if (records.length === 0) {
        await jobLog(job.id, 'No Facebook records need KG extraction');
      } else {
        const recordIds: number[] = [];

        await parallelMap(records, INGEST_CONCURRENCY, async (rec) => {
          try {
            const dateStr = rec.timestamp ? new Date(rec.timestamp).toISOString().slice(0, 10) : 'unknown';
            const entityHints: EntityHint[] = [];
            if (rec.sender) {
              entityHints.push({ name: rec.sender, type: 'person', role: 'subject' });
            }

            const input: FactInput = {
              domain: rec.data_type === 'message' ? 'communication' : 'social',
              category: `facebook_${rec.data_type}`,
              key: `fb_${rec.data_type}_${rec.id}`,
              value: `${rec.sender || 'unknown'} (${rec.data_type}${rec.conversation_title ? ' in ' + rec.conversation_title : ''}) on ${dateStr}: ${rec.content || ''}`.slice(0, 2000),
              confidence: 0.7,
              source: 'facebook_backfill',
              validFrom: rec.timestamp?.toISOString(),
              entityHints: entityHints.length > 0 ? entityHints : undefined,
              skipEmbedding: !generateEmbeddings,
            };

            const id = await ingestFact(input);
            if (id) totalWritten++;
            totalProcessed++;
            recordIds.push(rec.id);
          } catch (err) {
            totalErrors++;
            const msg = err instanceof Error ? err.message : String(err);
            if (sampleErrors.length < 5) sampleErrors.push(`facebook/${rec.id}: ${msg.slice(0, 200)}`);
            logger.logVerbose(`Error ingesting Facebook record ${rec.id}: ${msg}`);
          }
        });

        if (recordIds.length > 0) {
          await pool.query(
            `UPDATE aurora_raw_facebook SET kg_ingested_at = NOW() WHERE id = ANY($1::int[])`,
            [recordIds]
          );
          logger.log(`Marked ${recordIds.length} Facebook records as KG-ingested`);
        }

        await jobLog(job.id, `Facebook KG: ${recordIds.length} records → ${totalWritten} facts, ${totalErrors} errors`);

        if (records.length === batchSize) {
          const { rows: existingKb } = await pool.query(
            `SELECT id FROM tempo_jobs WHERE job_type = 'knowledge-backfill' AND status = 'pending' LIMIT 1`,
          );
          if (existingKb.length === 0) {
            await pool.query(
              `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
               VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
              [JSON.stringify({ sources: ['facebook'], batch_size: batchSize, generate_embeddings: generateEmbeddings })]
            );
          }
        }
      }
    }

    if (source === 'instagram') {
      // Extract knowledge facts from Instagram messages and posts
      const { rows: records } = await pool.query<{
        id: number;
        data_type: string;
        timestamp: Date | null;
        content: string | null;
        sender: string | null;
      }>(
        `SELECT id, data_type, timestamp, LEFT(content, 2000) as content, sender
         FROM aurora_raw_instagram
         WHERE content IS NOT NULL AND LENGTH(content) > 5
           AND kg_ingested_at IS NULL
         ORDER BY timestamp ASC NULLS LAST
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${records.length} Instagram records needing KG extraction`);
      await jobLog(job.id, `Found ${records.length} Instagram records needing KG extraction`);

      if (records.length === 0) {
        await jobLog(job.id, 'No Instagram records need KG extraction');
      } else {
        const recordIds: number[] = [];

        await parallelMap(records, INGEST_CONCURRENCY, async (rec) => {
          try {
            const dateStr = rec.timestamp ? new Date(rec.timestamp).toISOString().slice(0, 10) : 'unknown';
            const entityHints: EntityHint[] = [];
            if (rec.sender) {
              entityHints.push({ name: rec.sender, type: 'person', role: 'subject' });
            }

            const input: FactInput = {
              domain: rec.data_type === 'message' ? 'communication' : 'social',
              category: `instagram_${rec.data_type}`,
              key: `ig_${rec.data_type}_${rec.id}`,
              value: `${rec.sender || 'unknown'} (${rec.data_type}) on ${dateStr}: ${rec.content || ''}`.slice(0, 2000),
              confidence: 0.7,
              source: 'instagram_backfill',
              validFrom: rec.timestamp?.toISOString(),
              entityHints: entityHints.length > 0 ? entityHints : undefined,
              skipEmbedding: !generateEmbeddings,
            };

            const id = await ingestFact(input);
            if (id) totalWritten++;
            totalProcessed++;
            recordIds.push(rec.id);
          } catch (err) {
            totalErrors++;
            const msg = err instanceof Error ? err.message : String(err);
            if (sampleErrors.length < 5) sampleErrors.push(`instagram/${rec.id}: ${msg.slice(0, 200)}`);
            logger.logVerbose(`Error ingesting Instagram record ${rec.id}: ${msg}`);
          }
        });

        if (recordIds.length > 0) {
          await pool.query(
            `UPDATE aurora_raw_instagram SET kg_ingested_at = NOW() WHERE id = ANY($1::int[])`,
            [recordIds]
          );
          logger.log(`Marked ${recordIds.length} Instagram records as KG-ingested`);
        }

        await jobLog(job.id, `Instagram KG: ${recordIds.length} records → ${totalWritten} facts, ${totalErrors} errors`);

        if (records.length === batchSize) {
          const { rows: existingKb } = await pool.query(
            `SELECT id FROM tempo_jobs WHERE job_type = 'knowledge-backfill' AND status = 'pending' LIMIT 1`,
          );
          if (existingKb.length === 0) {
            await pool.query(
              `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
               VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
              [JSON.stringify({ sources: ['instagram'], batch_size: batchSize, generate_embeddings: generateEmbeddings })]
            );
          }
        }
      }
    }

    if (source === 'google_chat') {
      // Extract knowledge facts from Google Chat messages
      const { rows: records } = await pool.query<{
        id: number;
        group_name: string | null;
        sender_name: string | null;
        sender_email: string | null;
        message_text: string | null;
        timestamp: Date;
      }>(
        `SELECT id, group_name, sender_name, sender_email,
                LEFT(message_text, 2000) as message_text, timestamp
         FROM aurora_raw_google_chat
         WHERE message_text IS NOT NULL AND LENGTH(message_text) > 5
           AND kg_ingested_at IS NULL
         ORDER BY timestamp ASC
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${records.length} Google Chat records needing KG extraction`);
      await jobLog(job.id, `Found ${records.length} Google Chat records needing KG extraction`);

      if (records.length === 0) {
        await jobLog(job.id, 'No Google Chat records need KG extraction');
      } else {
        const recordIds: number[] = [];

        await parallelMap(records, INGEST_CONCURRENCY, async (rec) => {
          try {
            const dateStr = rec.timestamp ? new Date(rec.timestamp).toISOString().slice(0, 10) : 'unknown';
            const senderLabel = rec.sender_name || rec.sender_email || 'unknown';
            const entityHints: EntityHint[] = [];
            if (rec.sender_name) {
              entityHints.push({ name: rec.sender_name, type: 'person', role: 'subject' });
            }

            const input: FactInput = {
              domain: 'communication',
              category: 'google_chat',
              key: `gchat_${rec.id}`,
              value: `${senderLabel}${rec.group_name ? ' in ' + rec.group_name : ''} on ${dateStr}: ${rec.message_text || ''}`.slice(0, 2000),
              confidence: 0.7,
              source: 'google_chat_backfill',
              validFrom: rec.timestamp?.toISOString(),
              entityHints: entityHints.length > 0 ? entityHints : undefined,
              skipEmbedding: !generateEmbeddings,
            };

            const id = await ingestFact(input);
            if (id) totalWritten++;
            totalProcessed++;
            recordIds.push(rec.id);
          } catch (err) {
            totalErrors++;
            const msg = err instanceof Error ? err.message : String(err);
            if (sampleErrors.length < 5) sampleErrors.push(`google_chat/${rec.id}: ${msg.slice(0, 200)}`);
            logger.logVerbose(`Error ingesting Google Chat record ${rec.id}: ${msg}`);
          }
        });

        if (recordIds.length > 0) {
          await pool.query(
            `UPDATE aurora_raw_google_chat SET kg_ingested_at = NOW() WHERE id = ANY($1::int[])`,
            [recordIds]
          );
          logger.log(`Marked ${recordIds.length} Google Chat records as KG-ingested`);
        }

        await jobLog(job.id, `Google Chat KG: ${recordIds.length} records → ${totalWritten} facts, ${totalErrors} errors`);

        if (records.length === batchSize) {
          const { rows: existingKb } = await pool.query(
            `SELECT id FROM tempo_jobs WHERE job_type = 'knowledge-backfill' AND status = 'pending' LIMIT 1`,
          );
          if (existingKb.length === 0) {
            await pool.query(
              `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
               VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
              [JSON.stringify({ sources: ['google_chat'], batch_size: batchSize, generate_embeddings: generateEmbeddings })]
            );
          }
        }
      }
    }

    if (source === 'bb_sms') {
      // Extract knowledge facts from BlackBerry SMS messages (2009-2010)
      const { rows: messages } = await pool.query<{
        id: number;
        phone_number: string | null;
        message: string | null;
        timestamp: Date | null;
        direction: string | null;
        person_id: number | null;
      }>(
        `SELECT id, phone_number, message, timestamp, direction, person_id
         FROM bb_sms_messages
         WHERE message IS NOT NULL AND LENGTH(message) > 5
           AND kg_ingested_at IS NULL
         ORDER BY timestamp ASC
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${messages.length} BB SMS messages needing KG extraction`);
      await jobLog(job.id, `Found ${messages.length} BB SMS messages needing KG extraction`);

      if (messages.length === 0) {
        await jobLog(job.id, 'No BB SMS messages need KG extraction');
      } else {
        const msgIds: number[] = [];

        await parallelMap(messages, INGEST_CONCURRENCY, async (msg) => {
          try {
            const dateStr = msg.timestamp ? msg.timestamp.toISOString().slice(0, 10) : 'unknown';
            const dir = msg.direction === 'sent' ? 'to' : 'from';
            const entityHints = extractEntityHints('communication', 'sms', msg.phone_number || '', msg.message || '', contactMap);

            const input: FactInput = {
              domain: 'communication',
              category: 'sms',
              key: `bb_sms_${msg.id}`,
              value: `BlackBerry SMS ${dir} ${msg.phone_number || 'unknown'} on ${dateStr}: ${msg.message || ''}`.slice(0, 2000),
              confidence: 0.85,
              source: 'bb_sms_backfill',
              validFrom: msg.timestamp?.toISOString(),
              entityHints: entityHints.length > 0 ? entityHints : undefined,
              skipEmbedding: !generateEmbeddings,
            };

            const id = await ingestFact(input);
            if (id) totalWritten++;
            totalProcessed++;
            msgIds.push(msg.id);
          } catch (err) {
            totalErrors++;
            const errMsg = err instanceof Error ? err.message : String(err);
            if (sampleErrors.length < 5) sampleErrors.push(`bb_sms/${msg.id}: ${errMsg.slice(0, 200)}`);
            logger.logVerbose(`Error ingesting BB SMS ${msg.id}: ${errMsg}`);
          }
        });

        if (msgIds.length > 0) {
          await pool.query(`UPDATE bb_sms_messages SET kg_ingested_at = NOW() WHERE id = ANY($1::int[])`, [msgIds]);
          logger.log(`Marked ${msgIds.length} BB SMS messages as KG-ingested`);
        }

        await jobLog(job.id, `BB SMS KG: ${msgIds.length} messages → ${totalWritten} facts, ${totalErrors} errors`);

        if (messages.length === batchSize) {
          const { rows: existingKb } = await pool.query(
            `SELECT id FROM tempo_jobs WHERE job_type = 'knowledge-backfill' AND status = 'pending' LIMIT 1`,
          );
          if (existingKb.length === 0) {
            await pool.query(
              `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
               VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
              [JSON.stringify({ sources: ['bb_sms'], batch_size: batchSize, generate_embeddings: generateEmbeddings })]
            );
          }
        }
      }
    }

    if (source === 'archived_email') {
      // Extract knowledge facts from [Employer] sent emails (2008-2010)
      const { rows: emails } = await pool.query<{
        id: number;
        sent_at: Date | null;
        subject: string | null;
        to_recipients: string | null;
        body_text: string | null;
      }>(
        `SELECT id, sent_at, subject, to_recipients, LEFT(body_text, 2000) as body_text
         FROM archived_sent_emails
         WHERE body_text IS NOT NULL AND LENGTH(body_text) > 10
           AND kg_ingested_at IS NULL
         ORDER BY sent_at ASC
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${emails.length} archived emails needing KG extraction`);
      await jobLog(job.id, `Found ${emails.length} archived emails needing KG extraction`);

      if (emails.length === 0) {
        await jobLog(job.id, 'No archived emails need KG extraction');
      } else {
        const emailIds: number[] = [];

        await parallelMap(emails, INGEST_CONCURRENCY, async (email) => {
          try {
            const dateStr = email.sent_at ? email.sent_at.toISOString().slice(0, 10) : 'unknown';
            const entityHints = extractEntityHints('communication', 'work_email', email.subject || '', email.body_text || '', contactMap);

            const input: FactInput = {
              domain: 'communication',
              category: 'work_email',
              key: `archived_email_${email.id}`,
              value: `[Employer] email to ${email.to_recipients || 'unknown'} on ${dateStr}: ${email.subject || '(no subject)'} — ${email.body_text || ''}`.slice(0, 2000),
              confidence: 0.85,
              source: 'archived_email_backfill',
              validFrom: email.sent_at?.toISOString(),
              entityHints: entityHints.length > 0 ? entityHints : undefined,
              skipEmbedding: !generateEmbeddings,
            };

            const id = await ingestFact(input);
            if (id) totalWritten++;
            totalProcessed++;
            emailIds.push(email.id);
          } catch (err) {
            totalErrors++;
            const errMsg = err instanceof Error ? err.message : String(err);
            if (sampleErrors.length < 5) sampleErrors.push(`archived_email/${email.id}: ${errMsg.slice(0, 200)}`);
            logger.logVerbose(`Error ingesting archived email ${email.id}: ${errMsg}`);
          }
        });

        if (emailIds.length > 0) {
          await pool.query(`UPDATE archived_sent_emails SET kg_ingested_at = NOW() WHERE id = ANY($1::int[])`, [emailIds]);
          logger.log(`Marked ${emailIds.length} archived emails as KG-ingested`);
        }

        await jobLog(job.id, `Archived Email KG: ${emailIds.length} emails → ${totalWritten} facts, ${totalErrors} errors`);

        if (emails.length === batchSize) {
          const { rows: existingKb } = await pool.query(
            `SELECT id FROM tempo_jobs WHERE job_type = 'knowledge-backfill' AND status = 'pending' LIMIT 1`,
          );
          if (existingKb.length === 0) {
            await pool.query(
              `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
               VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
              [JSON.stringify({ sources: ['archived_email'], batch_size: batchSize, generate_embeddings: generateEmbeddings })]
            );
          }
        }
      }
    }

    if (source === 'guestbook') {
      // Extract knowledge facts from example-personal.com guestbook messages (2004-2005)
      const { rows: messages } = await pool.query<{
        id: string;
        poster_name: string | null;
        recipient_name: string | null;
        recipient_page: string | null;
        message_text: string | null;
        posted_at: Date | null;
      }>(
        `SELECT id, poster_name, recipient_name, recipient_page, message_text, posted_at
         FROM site_guestbook
         WHERE message_text IS NOT NULL AND LENGTH(message_text) > 5
           AND kg_ingested_at IS NULL
         ORDER BY posted_at ASC
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${messages.length} guestbook messages needing KG extraction`);
      await jobLog(job.id, `Found ${messages.length} guestbook messages needing KG extraction`);

      if (messages.length === 0) {
        await jobLog(job.id, 'No guestbook messages need KG extraction');
      } else {
        const msgIds: string[] = [];

        await parallelMap(messages, INGEST_CONCURRENCY, async (msg) => {
          try {
            const dateStr = msg.posted_at ? msg.posted_at.toISOString().slice(0, 10) : 'unknown';
            const entityHints: EntityHint[] = [];
            if (msg.poster_name) entityHints.push({ name: msg.poster_name, type: 'person', role: 'subject' });
            if (msg.recipient_name) entityHints.push({ name: msg.recipient_name, type: 'person', role: 'object' });

            const input: FactInput = {
              domain: 'social',
              category: 'guestbook',
              key: `guestbook_${msg.id}`,
              value: `${msg.poster_name || 'Someone'} wrote to ${msg.recipient_name || 'unknown'} on ${dateStr} (${msg.recipient_page || 'unknown page'}): ${msg.message_text || ''}`.slice(0, 2000),
              confidence: 0.9,
              source: 'guestbook_backfill',
              validFrom: msg.posted_at?.toISOString(),
              entityHints: entityHints.length > 0 ? entityHints : undefined,
              skipEmbedding: !generateEmbeddings,
            };

            const id = await ingestFact(input);
            if (id) totalWritten++;
            totalProcessed++;
            msgIds.push(msg.id);
          } catch (err) {
            totalErrors++;
            const errMsg = err instanceof Error ? err.message : String(err);
            if (sampleErrors.length < 5) sampleErrors.push(`guestbook/${msg.id}: ${errMsg.slice(0, 200)}`);
            logger.logVerbose(`Error ingesting guestbook ${msg.id}: ${errMsg}`);
          }
        });

        if (msgIds.length > 0) {
          await pool.query(`UPDATE site_guestbook SET kg_ingested_at = NOW() WHERE id = ANY($1::uuid[])`, [msgIds]);
          logger.log(`Marked ${msgIds.length} guestbook messages as KG-ingested`);
        }

        await jobLog(job.id, `Guestbook KG: ${msgIds.length} messages → ${totalWritten} facts, ${totalErrors} errors`);

        if (messages.length === batchSize) {
          const { rows: existingKb } = await pool.query(
            `SELECT id FROM tempo_jobs WHERE job_type = 'knowledge-backfill' AND status = 'pending' LIMIT 1`,
          );
          if (existingKb.length === 0) {
            await pool.query(
              `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
               VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
              [JSON.stringify({ sources: ['guestbook'], batch_size: batchSize, generate_embeddings: generateEmbeddings })]
            );
          }
        }
      }
    }

    if (source === 'embed_backfill') {
      // Generate embeddings for facts that are missing them
      const { rows: facts } = await pool.query<{
        id: string;
        domain: string;
        category: string;
        key: string;
        value: string;
      }>(
        `SELECT id, domain, category, key, value
         FROM knowledge_facts
         WHERE embedding IS NULL AND superseded_by IS NULL
         ORDER BY created_at DESC
         LIMIT $1`,
        [batchSize]
      );

      logger.log(`Found ${facts.length} facts missing embeddings`);
      await jobLog(job.id, `Found ${facts.length} facts missing embeddings`);

      await parallelMap(facts, INGEST_CONCURRENCY, async (fact) => {
        try {
          const text = `${fact.domain} ${fact.category}: ${fact.key} — ${fact.value}`;
          const embedding = await generateEmbedding(text, 'fact');
          if (embedding) {
            await pool.query(
              `UPDATE knowledge_facts SET embedding = $2::vector WHERE id = $1`,
              [fact.id, `[${embedding.join(',')}]`]
            );
            totalWritten++;
          }
          totalProcessed++;
        } catch (err) {
          totalErrors++;
          const msg = err instanceof Error ? err.message : String(err);
          if (sampleErrors.length < 5) sampleErrors.push(`embed/${fact.key.slice(0, 30)}: ${msg.slice(0, 200)}`);
          logger.logVerbose(`Error embedding ${fact.domain}/${fact.key}: ${msg}`);
        }
      });

      await jobLog(job.id, `Embedding backfill: ${totalWritten} embedded, ${totalErrors} errors out of ${totalProcessed}`);

      // Chain if full batch
      if (facts.length === batchSize) {
        logger.log('Chaining next embed_backfill batch');
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
           VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
          [JSON.stringify({ sources: ['embed_backfill'], batch_size: batchSize })]
        );
      } else {
        logger.log(`Embedding backfill complete — no more facts without embeddings`);
        await jobLog(job.id, `Embedding backfill complete — all facts embedded`);
      }
    }

    if (source === 'entity_geocode') {
      // Process coordinate-named place entities in small batches (5 per job)
      // to avoid the 20-minute stale job threshold. Self-chains if more remain.
      const GEOCODE_BATCH_SIZE = 5;
      const coordPattern = /^-?\d+\.\d{1,6},\s*-?\d+\.\d{1,6}$/;

      const { rows: coordEntities } = await pool.query<{
        id: string;
        canonical_name: string;
        metadata: Record<string, unknown>;
        fact_count: number;
      }>(
        `SELECT id, canonical_name, metadata, fact_count
         FROM knowledge_entities
         WHERE entity_type = 'place'
           AND canonical_name ~ '^-?[0-9]+\\.[0-9]+,\\s*-?[0-9]+\\.[0-9]+'
         ORDER BY fact_count DESC
         LIMIT $1`,
        [GEOCODE_BATCH_SIZE]
      );

      logger.log(`Geocode batch: ${coordEntities.length} coordinate entities to resolve`);
      await jobLog(job.id, `Geocode batch: ${coordEntities.length} coordinate entities`);

      if (coordEntities.length === 0) {
        logger.log('No more coordinate entities to geocode');
        await jobLog(job.id, 'Entity geocode complete — no coordinate entities remaining');
      } else {
        for (const entity of coordEntities) {
          try {
            const parts = entity.canonical_name.split(',').map(s => s.trim());
            const lat = parseFloat(parts[0]);
            const lon = parseFloat(parts[1]);
            if (isNaN(lat) || isNaN(lon)) continue;

            const locationName = await reverseGeocode(lat, lon);
            if (!locationName) {
              logger.logVerbose(`Could not resolve ${entity.canonical_name}`);
              continue;
            }

            // Check if a named entity already exists for this location
            const { rows: existingNamed } = await pool.query<{ id: string; fact_count: number }>(
              `SELECT id, fact_count FROM knowledge_entities
               WHERE entity_type = 'place' AND LOWER(canonical_name) = LOWER($1)
               AND id != $2
               LIMIT 1`,
              [locationName, entity.id]
            );

            if (existingNamed.length > 0) {
              // Merge this coordinate entity into the existing named entity
              const targetId = existingNamed[0].id;
              await pool.query(
                `UPDATE knowledge_fact_entities SET entity_id = $1 WHERE entity_id = $2`,
                [targetId, entity.id]
              );
              await pool.query(
                `UPDATE knowledge_entities SET fact_count = (
                   SELECT COUNT(DISTINCT fact_id) FROM knowledge_fact_entities WHERE entity_id = $1
                 ) WHERE id = $1`,
                [targetId]
              );
              // Re-point conversation chunks before deleting
              await pool.query(
                `UPDATE knowledge_conversation_chunks SET entity_id = $1 WHERE entity_id = $2`,
                [targetId, entity.id]
              );
              // Replace dangling entity_id in knowledge_summaries UUID arrays
              await pool.query(
                `UPDATE knowledge_summaries SET entity_ids = array_replace(entity_ids, $2::uuid, $1::uuid) WHERE $2::uuid = ANY(entity_ids)`,
                [targetId, entity.id]
              );
              await pool.query(`DELETE FROM knowledge_entities WHERE id = $1`, [entity.id]);
              totalWritten++;
              logger.logVerbose(`Merged "${entity.canonical_name}" into existing "${locationName}" (${targetId})`);
            } else {
              // Rename the coordinate entity to the resolved location name
              await pool.query(
                `UPDATE knowledge_entities SET canonical_name = $1 WHERE id = $2`,
                [locationName, entity.id]
              );
              totalWritten++;
              logger.logVerbose(`Renamed "${entity.canonical_name}" to "${locationName}"`);
            }

            totalProcessed++;
          } catch (err) {
            totalErrors++;
            const msg = err instanceof Error ? err.message : String(err);
            if (sampleErrors.length < 5) sampleErrors.push(`geocode/${entity.canonical_name}: ${msg.slice(0, 200)}`);
          }
        }

        logger.log(`Geocode batch done: ${totalProcessed} resolved, ${totalWritten} updated/merged, ${totalErrors} errors`);
        await jobLog(job.id, `Geocode batch: ${totalProcessed} resolved, ${totalWritten} written, ${totalErrors} errors`);

        // Self-chain if we processed a full batch (more likely remain)
        if (coordEntities.length === GEOCODE_BATCH_SIZE) {
          logger.log('Chaining next geocode batch');
          await pool.query(
            `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
             VALUES ('knowledge-backfill', $1, 'pending', 0, 3)`,
            [JSON.stringify({ sources: ['entity_geocode'] })]
          );
        }
      }
    }
  }

  const result: Record<string, unknown> = {
    source: sources.join(','),
    offset,
    batch_size: batchSize,
    processed: totalProcessed,
    written: totalWritten,
    errors: totalErrors,
    embeddings: generateEmbeddings,
  };
  if (sampleErrors.length > 0) {
    result.sample_errors = sampleErrors;
    logger.log(`Sample errors: ${sampleErrors.join(' | ')}`);
    await jobLog(job.id, `Sample errors: ${sampleErrors.join(' | ')}`);
  }

  logger.log(`Batch complete: ${JSON.stringify(result)}`);
  return result;
}

// ---------------------------------------------------------------------------
// Photo → KG fact extraction
// ---------------------------------------------------------------------------

interface PhotoRow {
  id: string;
  local_identifier: string;
  description: string;
  taken_at: string | null;
  latitude: number | null;
  longitude: number | null;
  is_favorite: boolean;
  people: string[] | null;
  media_type: string;
  kg_ingested_at: string | null;
}

/**
 * Build KG facts from a described photo's metadata.
 * Generates 1-3 facts per photo: description, location, and people.
 */
export function buildPhotoFacts(
  photo: PhotoRow,
  contactMap: Map<string, { id: string; name: string; phones: string[] }>,
  locationName?: string | null
): FactInput[] {
  const facts: FactInput[] = [];
  const dateStr = photo.taken_at ? new Date(photo.taken_at).toISOString().split('T')[0] : 'unknown';
  const photoKey = `photo_${photo.local_identifier.replace(/[^a-zA-Z0-9]/g, '_')}`;

  // Fact 1: The AI description (always present)
  const descEntityHints: EntityHint[] = [];

  // Add people as entity hints
  if (photo.people?.length) {
    for (const personName of photo.people) {
      const contact = findContactByName(personName, contactMap);
      descEntityHints.push({
        name: contact?.name || personName,
        type: 'person',
        role: 'subject',
        contactId: contact?.id,
        aliases: contact?.phones || [],
      });
    }
  }

  // Add location as entity hint if GPS available
  if (photo.latitude != null && photo.longitude != null) {
    descEntityHints.push({
      name: locationName || `${Number(photo.latitude).toFixed(2)}, ${Number(photo.longitude).toFixed(2)}`,
      type: 'place',
      role: 'location',
      metadata: { latitude: photo.latitude, longitude: photo.longitude },
    });
  }

  // Build a rich value that combines all photo context for better embeddings
  // and inference. The raw description alone misses who, when, and where.
  const parts: string[] = [];
  if (photo.people?.length) {
    parts.push(`People: ${photo.people.join(', ')}`);
  }
  if (photo.taken_at) {
    const d = new Date(photo.taken_at);
    parts.push(`Date: ${d.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}`);
  }
  if (locationName) {
    parts.push(`Location: ${locationName}`);
  } else if (photo.latitude != null && photo.longitude != null) {
    parts.push(`Location: ${Number(photo.latitude).toFixed(4)}, ${Number(photo.longitude).toFixed(4)}`);
  }
  parts.push(photo.description);

  facts.push({
    domain: 'photos',
    category: photo.media_type || 'photo',
    key: photoKey,
    value: parts.join(' | '),
    confidence: 0.7,
    source: 'photo_describe',
    validFrom: photo.taken_at || undefined,
    entityHints: descEntityHints.length > 0 ? descEntityHints : undefined,
  });

  return facts;
}

// ---------------------------------------------------------------------------
// Email Fact Builder
// ---------------------------------------------------------------------------

interface EmailRow {
  id: string;
  from_address: string;
  from_name: string;
  to_addresses: string[];
  subject: string;
  body_text: string;
  date: Date;
  labels: string[];
  is_sent: boolean;
}

function buildEmailFacts(email: EmailRow): FactInput[] {
  const facts: FactInput[] = [];
  const dateStr = email.date ? new Date(email.date).toISOString().split('T')[0] : 'unknown';
  const emailKey = `email_${email.id.replace(/[^a-zA-Z0-9]/g, '_')}`;

  // Extract entity hints
  const entityHints: EntityHint[] = [];

  // Sender
  const senderName = email.from_name || email.from_address?.split('@')[0] || '';
  if (senderName && senderName.length > 1) {
    entityHints.push({ name: senderName, type: 'person', role: 'subject' });
  }

  // Recipients (for sent emails)
  if (email.is_sent && email.to_addresses?.length) {
    for (const addr of email.to_addresses.slice(0, 3)) {
      const name = typeof addr === 'string' ? addr.split('@')[0] : String(addr);
      if (name && name.length > 1) {
        entityHints.push({ name, type: 'person', role: 'object' });
      }
    }
  }

  // Build rich value
  const parts: string[] = [];
  if (email.is_sent) {
    parts.push(`Sent to: ${(email.to_addresses || []).slice(0, 3).join(', ')}`);
  } else {
    parts.push(`From: ${senderName}`);
  }
  parts.push(`Date: ${dateStr}`);
  if (email.subject) parts.push(`Subject: ${email.subject}`);
  if (email.body_text) parts.push(email.body_text.slice(0, 500));

  facts.push({
    domain: 'communication',
    category: 'email',
    key: emailKey,
    value: parts.join(' | '),
    confidence: 0.7,
    source: 'gmail_sync',
    validFrom: email.date ? email.date.toISOString() : undefined,
    entityHints: entityHints.length > 0 ? entityHints : undefined,
  });

  return facts;
}
