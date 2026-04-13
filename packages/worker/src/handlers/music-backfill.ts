/**
 * Apple Music Library Backfill — pages through the entire user library.
 *
 * Fetches /v1/me/library/songs in pages of 100. Each job processes one page
 * and self-chains with the next offset until all songs are imported.
 *
 * Payload: { offset?: number }
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import crypto from 'crypto';
import { readFileSync } from 'fs';

const logger = createLogger('music-backfill');

const PAGE_SIZE = 100;
const APPLE_MUSIC_BASE = 'https://api.music.apple.com';

// ── Developer Token (shared with music-sync) ──────────────

let cachedDevToken: { token: string; expiresAt: number } | null = null;

function generateDeveloperToken(): string {
  if (cachedDevToken && cachedDevToken.expiresAt > Date.now() + 3600_000) {
    return cachedDevToken.token;
  }

  const keyId = process.env.MUSICKIT_KEY_ID ?? 'KEY_ID_HERE';
  const teamId = process.env.MUSICKIT_TEAM_ID ?? 'TEAM_ID_HERE';
  const keyFilePath = process.env.MUSICKIT_KEY_FILE ?? '/opt/nexus/Services/nexus/musickit.p8';

  let privateKey: string;
  try {
    privateKey = readFileSync(keyFilePath, 'utf8');
  } catch {
    throw new Error(`Cannot read MusicKit key file at ${keyFilePath}`);
  }

  const b64url = (obj: Record<string, unknown>) =>
    Buffer.from(JSON.stringify(obj))
      .toString('base64')
      .replace(/=/g, '')
      .replace(/\+/g, '-')
      .replace(/\//g, '_');

  const now = Math.floor(Date.now() / 1000);
  const exp = now + 15777000;
  const header = b64url({ alg: 'ES256', kid: keyId });
  const payload = b64url({ iss: teamId, iat: now, exp });
  const input = `${header}.${payload}`;

  const sign = crypto.createSign('SHA256');
  sign.update(input);
  const sig = sign
    .sign({ key: privateKey, dsaEncoding: 'ieee-p1363' })
    .toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');

  const token = `${input}.${sig}`;
  cachedDevToken = { token, expiresAt: exp * 1000 };
  return token;
}

// ── API Types ──────────────────────────────────────────────

interface AppleMusicResponse {
  data?: AppleMusicTrack[];
  next?: string;
  meta?: { total?: number };
  errors?: Array<{ title: string; detail: string; status: string }>;
}

interface AppleMusicTrack {
  id: string;
  type: string;
  attributes: {
    name: string;
    artistName: string;
    albumName?: string;
    genreNames?: string[];
    durationInMillis?: number;
    artwork?: { url: string; width: number; height: number };
    isrc?: string;
    dateAdded?: string;
    playParams?: { id: string; catalogId?: string };
    lastPlayedDate?: string;
    playCount?: number;
    contentRating?: string;
  };
}

// ── Main Handler ───────────────────────────────────────────

export async function handleMusicBackfill(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const userToken = process.env.MUSICKIT_USER_TOKEN;

  if (!userToken) {
    logger.logMinimal('MUSICKIT_USER_TOKEN not set — cannot backfill library');
    await jobLog(job.id, 'Skipped: MUSICKIT_USER_TOKEN not configured', 'warn');
    return { skipped: true, reason: 'no_user_token' };
  }

  let devToken: string;
  try {
    devToken = generateDeveloperToken();
  } catch (err) {
    logger.logMinimal('Developer token generation failed:', (err as Error).message);
    return { error: (err as Error).message };
  }

  const offset = Number(job.payload?.offset ?? 0);
  const path = `/v1/me/library/songs?limit=${PAGE_SIZE}&offset=${offset}`;

  logger.log(`Fetching library page at offset ${offset}`);
  await jobLog(job.id, `Backfilling library songs, offset=${offset}`);

  const stop = logger.time('apple-music-library-page');
  let resp: AppleMusicResponse;
  try {
    const response = await fetch(`${APPLE_MUSIC_BASE}${path}`, {
      headers: {
        Authorization: `Bearer ${devToken}`,
        'Music-User-Token': userToken,
      },
    });
    stop();

    if (!response.ok) {
      const body = await response.text().catch(() => '');
      logger.logDebug(`Apple Music API error (${response.status}):`, body.slice(0, 500));

      if (response.status === 401 || response.status === 403) {
        throw new Error(`Apple Music auth failed (${response.status}) — user token may be expired`);
      }
      throw new Error(`Apple Music API error: ${response.status}`);
    }

    resp = await response.json() as AppleMusicResponse;
  } catch (err) {
    stop();
    logger.logMinimal(`Library fetch failed at offset ${offset}:`, (err as Error).message);
    return { error: (err as Error).message, offset };
  }

  const tracks = resp.data ?? [];
  if (tracks.length === 0) {
    logger.log(`Backfill complete — no more tracks at offset ${offset}`);
    await jobLog(job.id, `Backfill complete. Total offset reached: ${offset}`);
    return { status: 'complete', total_offset: offset };
  }

  logger.log(`Processing ${tracks.length} library tracks at offset ${offset}`);
  let synced = 0;
  let skipped = 0;

  for (const item of tracks) {
    const attrs = item.attributes;
    if (!attrs?.name || !attrs?.artistName) {
      skipped++;
      continue;
    }

    const artworkUrl = attrs.artwork?.url
      ?.replace('{w}', '300')
      .replace('{h}', '300') ?? null;

    // Use the catalog ID if available for cross-referencing
    const trackId = attrs.playParams?.catalogId ?? item.id;

    try {
      await pool.query(
        `INSERT INTO music_library (
          track_id, track_name, artist_name, album_name, genre,
          duration_ms, added_at, play_count, last_played_at,
          loved, artwork_url, isrc, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false, $10, $11, $12)
        ON CONFLICT (track_id) DO UPDATE SET
          play_count = GREATEST(music_library.play_count, EXCLUDED.play_count),
          last_played_at = GREATEST(music_library.last_played_at, EXCLUDED.last_played_at),
          metadata = EXCLUDED.metadata`,
        [
          trackId,
          attrs.name,
          attrs.artistName,
          attrs.albumName ?? null,
          attrs.genreNames?.[0] ?? null,
          attrs.durationInMillis ?? null,
          attrs.dateAdded ?? null,
          attrs.playCount ?? 0,
          attrs.lastPlayedDate ?? null,
          artworkUrl,
          attrs.isrc ?? null,
          JSON.stringify({
            library_id: item.id,
            genres: attrs.genreNames ?? [],
            content_rating: attrs.contentRating ?? null,
          }),
        ],
      );
      synced++;
    } catch (err) {
      logger.logVerbose(`Failed to upsert track ${trackId}:`, (err as Error).message);
      skipped++;
    }
  }

  logger.log(`Page offset=${offset}: ${synced} synced, ${skipped} skipped`);

  // Self-chain for next page if there are more results
  const hasNext = !!resp.next || tracks.length === PAGE_SIZE;
  if (hasNext) {
    const nextOffset = offset + PAGE_SIZE;
    try {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts)
         VALUES ('music-backfill', $1, 'nexus', 0, 3)`,
        [JSON.stringify({ offset: nextOffset })],
      );
      logger.logVerbose(`Chained next page at offset ${nextOffset}`);
    } catch (err) {
      logger.logMinimal('Failed to self-chain music-backfill:', (err as Error).message);
    }
  } else {
    logger.log(`Backfill complete — processed all tracks through offset ${offset}`);
    await jobLog(job.id, `Backfill complete. Final offset: ${offset + tracks.length}`);

    // Queue embedding backfill for all library tracks
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, executor, priority)
       VALUES ('embed-backfill', $1, 'nexus', 0)`,
      [JSON.stringify({ table: 'music_library', batch_size: 50 })],
    ).catch(() => {});
  }

  // Log to ingestion pipeline
  if (synced > 0) {
    await pool.query(
      `INSERT INTO ingestion_log (source_key, source_id, timestamp, category, summary, metadata)
       VALUES ('apple_music', $1, NOW(), 'music', $2, $3)
       ON CONFLICT (source_key, source_id) DO NOTHING`,
      [
        `backfill-${offset}`,
        `Backfill page offset=${offset}: ${synced} tracks`,
        JSON.stringify({ offset, synced, skipped }),
      ],
    ).catch(() => {});
  }

  await jobLog(job.id, `Page ${offset}: ${synced} synced, ${skipped} skipped. ${hasNext ? 'Continuing...' : 'Done.'}`);

  return {
    offset,
    synced,
    skipped,
    has_next: hasNext,
    next_offset: hasNext ? offset + PAGE_SIZE : null,
  };
}
