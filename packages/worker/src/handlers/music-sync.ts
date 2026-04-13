/**
 * Apple Music Sync — pulls listening history and library from Apple Music API.
 *
 * Requires:
 * - Developer token (generated from MUSICKIT_KEY_ID, MUSICKIT_TEAM_ID, MUSICKIT_PRIVATE_KEY)
 * - User token (obtained via browser MusicKit JS auth, stored as MUSICKIT_USER_TOKEN)
 *
 * Endpoints:
 * - GET /v1/me/recent/played — recently played tracks
 * - GET /v1/me/library/recently-added — recently added library items
 *
 * Self-chains every 30 minutes.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import crypto from 'crypto';
import { readFileSync } from 'fs';

const logger = createLogger('music-sync');

const INTERVAL_SECONDS = 30 * 60; // 30 minutes
const APPLE_MUSIC_BASE = 'https://api.music.apple.com';

// ── Developer Token ────────────────────────────────────────

let cachedDevToken: { token: string; expiresAt: number } | null = null;

function generateDeveloperToken(): string {
  // Return cached token if still valid (with 1 hour buffer)
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
  const exp = now + 15777000; // ~6 months
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
  logger.logVerbose('Generated new developer token');
  return token;
}

// ── Apple Music API Client ─────────────────────────────────

interface AppleMusicResponse {
  data?: AppleMusicTrack[];
  next?: string;
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
    playParams?: { id: string };
    artwork?: { url: string; width: number; height: number };
    isrc?: string;
    previews?: Array<{ url: string }>;
    lastPlayedDate?: string;
    dateAdded?: string;
    playCount?: number;
    inLibrary?: boolean;
  };
}

async function appleMusicFetch(
  path: string,
  devToken: string,
  userToken: string,
): Promise<AppleMusicResponse> {
  const url = path.startsWith('http') ? path : `${APPLE_MUSIC_BASE}${path}`;

  const stop = logger.time(`apple-music-api ${path.split('?')[0]}`);
  const response = await fetch(url, {
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
    throw new Error(`Apple Music API error: ${response.status} ${response.statusText}`);
  }

  return response.json() as Promise<AppleMusicResponse>;
}

// ── Sync: Recently Played ──────────────────────────────────

async function syncRecentlyPlayed(
  pool: ReturnType<typeof getPool>,
  devToken: string,
  userToken: string,
): Promise<{ synced: number; skipped: number }> {
  logger.log('Fetching recently played tracks');

  // recent/played/tracks returns individual songs, not albums/playlists
  const resp = await appleMusicFetch('/v1/me/recent/played/tracks?limit=30', devToken, userToken);
  const tracks = resp.data ?? [];

  logger.log(`Got ${tracks.length} recently played items`);
  let synced = 0;
  let skipped = 0;

  for (const item of tracks) {
    // recent/played/tracks should return songs but check anyway
    if (item.type !== 'songs' && item.type !== 'library-songs' && item.type !== 'music-videos') {
      logger.logVerbose(`Skipping ${item.type}: ${item.attributes?.name}`);
      skipped++;
      continue;
    }

    const attrs = item.attributes;
    if (!attrs?.name || !attrs?.artistName) {
      skipped++;
      continue;
    }

    const artworkUrl = attrs.artwork?.url
      ?.replace('{w}', '300')
      .replace('{h}', '300') ?? null;

    try {
      await pool.query(
        `INSERT INTO music_listening_history (
          track_id, track_name, artist_name, album_name, genre,
          duration_ms, played_at, artwork_url, isrc, preview_url, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (track_id, played_at) DO NOTHING`,
        [
          item.id,
          attrs.name,
          attrs.artistName,
          attrs.albumName ?? null,
          attrs.genreNames?.[0] ?? null,
          attrs.durationInMillis ?? null,
          attrs.lastPlayedDate ?? new Date().toISOString(),
          artworkUrl,
          attrs.isrc ?? null,
          attrs.previews?.[0]?.url ?? null,
          JSON.stringify({
            genres: attrs.genreNames ?? [],
            type: item.type,
          }),
        ],
      );
      synced++;
    } catch (err) {
      logger.logVerbose(`Failed to insert track ${item.id}:`, (err as Error).message);
      skipped++;
    }
  }

  return { synced, skipped };
}

// ── Sync: Recently Added to Library ────────────────────────

async function syncRecentlyAdded(
  pool: ReturnType<typeof getPool>,
  devToken: string,
  userToken: string,
): Promise<{ synced: number; skipped: number }> {
  logger.log('Fetching recently added library items');

  const resp = await appleMusicFetch('/v1/me/library/recently-added?limit=25', devToken, userToken);
  const items = resp.data ?? [];

  logger.log(`Got ${items.length} recently added items`);
  let synced = 0;
  let skipped = 0;

  for (const item of items) {
    if (item.type === 'library-albums') {
      // For albums, fetch the tracks within
      try {
        const tracksResp = await appleMusicFetch(
          `/v1/me/library/albums/${item.id}/tracks?limit=50`,
          devToken, userToken,
        );
        const albumTracks = tracksResp.data ?? [];
        logger.logVerbose(`Album "${item.attributes?.name}": ${albumTracks.length} tracks`);
        // Process each track from the album
        for (const track of albumTracks) {
          const tAttrs = track.attributes;
          if (!tAttrs?.name || !tAttrs?.artistName) continue;
          const tArt = tAttrs.artwork?.url?.replace('{w}', '300').replace('{h}', '300') ?? null;
          try {
            await pool.query(
              `INSERT INTO music_library (
                track_id, track_name, artist_name, album_name, genre,
                duration_ms, added_at, play_count, loved, artwork_url, isrc, metadata
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, false, $9, $10, $11)
              ON CONFLICT (track_id) DO UPDATE SET
                play_count = COALESCE(EXCLUDED.play_count, music_library.play_count),
                metadata = EXCLUDED.metadata`,
              [
                track.id, tAttrs.name, tAttrs.artistName,
                tAttrs.albumName ?? item.attributes?.name ?? null,
                tAttrs.genreNames?.[0] ?? null,
                tAttrs.durationInMillis ?? null,
                tAttrs.dateAdded ?? item.attributes?.dateAdded ?? new Date().toISOString(),
                tAttrs.playCount ?? 0, tArt, tAttrs.isrc ?? null,
                JSON.stringify({ genres: tAttrs.genreNames ?? [], type: track.type, albumId: item.id }),
              ],
            );
            synced++;
          } catch {
            skipped++;
          }
        }
      } catch (err) {
        logger.logVerbose(`Failed to fetch tracks for album ${item.id}:`, (err as Error).message);
        skipped++;
      }
      continue;
    }
    if (item.type !== 'library-songs') {
      logger.logVerbose(`Skipping recently-added ${item.type}: ${item.attributes?.name}`);
      skipped++;
      continue;
    }

    const attrs = item.attributes;
    if (!attrs?.name || !attrs?.artistName) {
      skipped++;
      continue;
    }

    const artworkUrl = attrs.artwork?.url
      ?.replace('{w}', '300')
      .replace('{h}', '300') ?? null;

    try {
      await pool.query(
        `INSERT INTO music_library (
          track_id, track_name, artist_name, album_name, genre,
          duration_ms, added_at, play_count, loved, artwork_url, isrc, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, false, $9, $10, $11)
        ON CONFLICT (track_id) DO UPDATE SET
          play_count = COALESCE(EXCLUDED.play_count, music_library.play_count),
          metadata = EXCLUDED.metadata`,
        [
          item.id,
          attrs.name,
          attrs.artistName,
          attrs.albumName ?? null,
          attrs.genreNames?.[0] ?? null,
          attrs.durationInMillis ?? null,
          attrs.dateAdded ?? new Date().toISOString(),
          attrs.playCount ?? 0,
          artworkUrl,
          attrs.isrc ?? null,
          JSON.stringify({
            genres: attrs.genreNames ?? [],
            type: item.type,
          }),
        ],
      );
      synced++;
    } catch (err) {
      logger.logVerbose(`Failed to upsert library track ${item.id}:`, (err as Error).message);
      skipped++;
    }
  }

  return { synced, skipped };
}

// ── Ingestion Pipeline ─────────────────────────────────────

async function logToIngestionPipeline(
  pool: ReturnType<typeof getPool>,
  syncedHistory: number,
  syncedLibrary: number,
): Promise<void> {
  if (syncedHistory === 0 && syncedLibrary === 0) return;

  try {
    await pool.query(
      `INSERT INTO ingestion_log (source_key, source_id, timestamp, category, summary, metadata)
       VALUES ('apple_music', $1, NOW(), 'music', $2, $3)
       ON CONFLICT (source_key, source_id) DO NOTHING`,
      [
        `sync-${new Date().toISOString().slice(0, 16)}`,
        `Synced ${syncedHistory} plays, ${syncedLibrary} library items`,
        JSON.stringify({ history: syncedHistory, library: syncedLibrary }),
      ],
    );
  } catch (err) {
    logger.logVerbose('Ingestion log insert failed:', (err as Error).message);
  }
}

// ── Main Handler ───────────────────────────────────────────

export async function handleMusicSync(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Honor the data_source_registry.enabled flag. When the source is flipped
  // to disabled (e.g. user token expired, auth needs renewal), skip the work
  // and self-chain with a long backoff so we don't spam 401 failures in the
  // journal every 30 min. Resumes automatically when enabled flips true.
  try {
    const sourceCheck = await pool.query<{ enabled: boolean }>(
      `SELECT enabled FROM data_source_registry WHERE source_key = 'apple_music'`,
    );
    if (sourceCheck.rows.length > 0 && !sourceCheck.rows[0].enabled) {
      logger.logVerbose('apple_music disabled in data_source_registry, skipping');
      await jobLog(job.id, 'Skipped: apple_music source disabled', 'info');
      await selfChainBackoff(pool);
      return { skipped: true, reason: 'data_source_disabled' };
    }
  } catch {
    // Non-fatal — continue with normal sync
  }

  const userToken = process.env.MUSICKIT_USER_TOKEN;

  if (!userToken) {
    logger.logMinimal('MUSICKIT_USER_TOKEN not set — cannot sync personal library. Run the auth flow first.');
    await jobLog(job.id, 'Skipped: MUSICKIT_USER_TOKEN not configured', 'warn');

    // Still self-chain so it picks up once the token is set
    await selfChain(pool);
    return { skipped: true, reason: 'no_user_token' };
  }

  let devToken: string;
  try {
    devToken = generateDeveloperToken();
  } catch (err) {
    logger.logMinimal('Developer token generation failed:', (err as Error).message);
    await jobLog(job.id, `Error: ${(err as Error).message}`, 'error');
    await selfChain(pool);
    return { error: (err as Error).message };
  }

  logger.log('Starting Apple Music sync');
  await jobLog(job.id, 'Starting Apple Music sync');

  const result: Record<string, unknown> = { checked: true };

  // Phase 1: Recently played
  try {
    const history = await syncRecentlyPlayed(pool, devToken, userToken);
    result.history_synced = history.synced;
    result.history_skipped = history.skipped;
    logger.log(`Recently played: ${history.synced} synced, ${history.skipped} skipped`);
  } catch (err) {
    logger.logMinimal('Recently played sync failed:', (err as Error).message);
    result.history_error = (err as Error).message;
  }

  // Phase 2: Recently added to library
  try {
    const library = await syncRecentlyAdded(pool, devToken, userToken);
    result.library_synced = library.synced;
    result.library_skipped = library.skipped;
    logger.log(`Recently added: ${library.synced} synced, ${library.skipped} skipped`);
  } catch (err) {
    logger.logMinimal('Recently added sync failed:', (err as Error).message);
    result.library_error = (err as Error).message;
  }

  // Log to ingestion pipeline
  await logToIngestionPipeline(
    pool,
    (result.history_synced as number) ?? 0,
    (result.library_synced as number) ?? 0,
  );

  // Update data source registry
  await pool.query(
    `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active'
     WHERE source_key = 'apple_music'`,
  ).catch(() => {});

  const total = ((result.history_synced as number) ?? 0) + ((result.library_synced as number) ?? 0);
  await jobLog(job.id, `Music sync complete: ${total} items synced`);

  // Self-chain
  await selfChain(pool);

  return result;
}

async function selfChain(pool: ReturnType<typeof getPool>): Promise<void> {
  try {
    const existing = await pool.query(
      `SELECT id FROM tempo_jobs WHERE job_type = 'music-sync' AND status = 'pending' LIMIT 1`,
    );
    if (existing.rows.length === 0) {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
         VALUES ('music-sync', '{}', 'nexus', 1, 3, NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
      );
    }
  } catch (err) {
    logger.logMinimal('Failed to self-chain music-sync:', (err as Error).message);
  }
}

/**
 * Long-backoff self-chain used when the data source is disabled or auth
 * is broken — avoids hammering the 30-min cadence when there's nothing
 * we can do until something external changes.
 */
async function selfChainBackoff(pool: ReturnType<typeof getPool>): Promise<void> {
  try {
    const existing = await pool.query(
      `SELECT id FROM tempo_jobs WHERE job_type = 'music-sync' AND status = 'pending' LIMIT 1`,
    );
    if (existing.rows.length === 0) {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
         VALUES ('music-sync', '{}', 'nexus', 1, 3, NOW() + INTERVAL '6 hours')`,
      );
    }
  } catch (err) {
    logger.logMinimal('Failed to self-chain music-sync (backoff):', (err as Error).message);
  }
}
