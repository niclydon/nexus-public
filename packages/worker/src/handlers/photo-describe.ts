/**
 * photo-describe handler
 *
 * Two source modes:
 *   1. S3 full-res (primary): Mac relay uploads full-res photo to S3 (full_image_s3_key).
 *      Handler reads it, sends to AI for description, resizes to 512x512 thumbnail
 *      via Sharp, uploads thumbnail to S3, deletes the full-res original, sets all fields.
 *   2. Base64 thumbnail (legacy): iOS uploads base64 thumbnail inline with metadata.
 *      Handler sends to AI, uploads thumbnail to S3, NULLs the base64 column.
 *
 * AI model: Forge VLM (Qwen3-VL-32B) via /v1/describe endpoint
 *
 * Both paths set: description, described_at, thumbnail_s3_key, processed_at.
 * S3 full-res path also NULLs full_image_s3_key after processing.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { jobLog } from '../lib/job-log.js';
import { ingestFact, type FactInput, type EntityHint } from '../lib/knowledge.js';
import { reverseGeocode } from '../lib/geocode.js';
import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import sharp from 'sharp';

const logger = createLogger('photo-describe');

let s3Client: S3Client | null = null;

const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const S3_BUCKET = process.env.ARIA_THUMBNAILS_S3_BUCKET || 'thumbnails-bucket';
const S3_THUMBNAIL_PREFIX = process.env.ARIA_THUMBNAILS_S3_PREFIX || 'aria-thumbnails/';
const FORGE_URL = process.env.FORGE_URL || 'http://forge.example.io';
const FORGE_API_KEY = process.env.FORGE_API_KEY || '';

function getS3(): S3Client {
  if (!s3Client) {
    s3Client = new S3Client({
      region: AWS_REGION,
      forcePathStyle: true,
      requestHandler: { requestTimeout: 30_000 },
    });
  }
  return s3Client;
}

interface PhotoToDescribe {
  id: string;
  local_identifier: string;
  thumbnail: string | null;
  full_image_s3_key: string | null;
  media_type: string;
  taken_at: string | null;
  latitude: number | null;
  longitude: number | null;
  is_favorite: boolean;
  people: string[] | null;
}

const CONCURRENCY = 1; // Sequential — Forge VLM processes one image at a time on GPU
const DESCRIBE_MAX_DIMENSION = 2048; // Max width/height for images sent to vision models

export async function handlePhotoDescribe(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { photo_ids?: string[]; batch_size?: number };
  const batchSize = Math.min(payload.batch_size ?? 200, 500);

  let photos: PhotoToDescribe[];

  if (payload.photo_ids?.length) {
    const { rows } = await pool.query<PhotoToDescribe>(
      `SELECT id, local_identifier, thumbnail, full_image_s3_key, media_type,
              taken_at, latitude, longitude, is_favorite, people
       FROM photo_metadata
       WHERE local_identifier = ANY($1)
         AND processed_at IS NULL
         AND (thumbnail IS NOT NULL OR full_image_s3_key IS NOT NULL)
       ORDER BY synced_at DESC
       LIMIT $2`,
      [payload.photo_ids, batchSize]
    );
    photos = rows;
  } else {
    // Prefer S3 full-res photos first, then fall back to base64 thumbnails
    const { rows } = await pool.query<PhotoToDescribe>(
      `SELECT id, local_identifier, thumbnail, full_image_s3_key, media_type,
              taken_at, latitude, longitude, is_favorite, people
       FROM photo_metadata
       WHERE processed_at IS NULL
         AND (thumbnail IS NOT NULL OR full_image_s3_key IS NOT NULL)
       ORDER BY
         CASE WHEN full_image_s3_key IS NOT NULL THEN 0 ELSE 1 END,
         synced_at DESC
       LIMIT $1`,
      [batchSize]
    );
    photos = rows;
  }

  if (photos.length === 0) {
    await jobLog(job.id, 'No photos to describe');
    return { described: 0, message: 'No photos to describe' };
  }

  const s3Count = photos.filter(p => p.full_image_s3_key).length;
  const b64Count = photos.filter(p => !p.full_image_s3_key && p.thumbnail).length;
  await jobLog(job.id, `Found ${photos.length} photos (${s3Count} S3 full-res, ${b64Count} base64 thumbnail)`);
  logger.logVerbose(`Batch: ${photos.length} photos, concurrency=${CONCURRENCY}`);

  const s3 = getS3();
  let described = 0;
  let failed = 0;
  const batchStart = Date.now();

  // Process photos in parallel with bounded concurrency
  const results = await processWithConcurrency(photos, CONCURRENCY, async (photo) => {
    if (photo.full_image_s3_key) {
      return processS3Photo(pool, s3, photo);
    } else if (photo.thumbnail) {
      return processBase64Photo(pool, s3, photo);
    }
    return null;
  });

  for (let i = 0; i < results.length; i++) {
    const result = results[i];
    if (result.status === 'fulfilled' && result.value) {
      described++;
    } else if (result.status === 'rejected') {
      const msg = result.reason instanceof Error ? result.reason.message : String(result.reason);
      logger.logMinimal(`Failed ${photos[i].local_identifier}: ${msg}`);
      failed++;
    }
  }

  const thumbnailsUploaded = described;

  // Clean up stale base64 thumbnails that already have S3 keys
  try {
    await pool.query(
      `UPDATE photo_metadata SET thumbnail = NULL
       WHERE thumbnail IS NOT NULL AND thumbnail_s3_key IS NOT NULL`
    );
  } catch {
    // non-critical
  }

  const batchDuration = Math.round((Date.now() - batchStart) / 1000);
  const avgPerPhoto = photos.length > 0 ? Math.round(batchDuration / photos.length) : 0;
  logger.logVerbose(`Batch complete: ${described}/${photos.length} in ${batchDuration}s (avg ${avgPerPhoto}s/photo), failed=${failed}`);

  logEvent({
    action: `Described ${described} photo(s)${failed > 0 ? `, ${failed} failed` : ''}`,
    component: 'photo-describe',
    category: 'integration',
    metadata: { described, failed, s3_source: s3Count, base64_source: b64Count, batch_size: photos.length, duration_s: batchDuration },
  });

  await jobLog(job.id, `Complete: ${described} described, ${failed} failed`);
  return { described, failed, thumbnails_uploaded: thumbnailsUploaded, batch_size: photos.length };
}

// ---------------------------------------------------------------------------
// S3 full-res flow: download full image -> describe -> resize to thumbnail -> upload thumbnail -> delete full-res
// ---------------------------------------------------------------------------

async function processS3Photo(
  pool: ReturnType<typeof getPool>,
  s3: S3Client,
  photo: PhotoToDescribe
): Promise<string> {
  const photoStart = Date.now();
  logger.logVerbose(`Processing ${photo.local_identifier} (S3: ${photo.full_image_s3_key})`);

  // 1. Download full-res from S3
  const getCmd = new GetObjectCommand({ Bucket: S3_BUCKET, Key: photo.full_image_s3_key! });
  const response = await s3.send(getCmd);
  const rawBuffer = Buffer.from(await response.Body!.transformToByteArray());
  logger.logDebug(`Downloaded ${photo.local_identifier}: ${Math.round(rawBuffer.length / 1024)}KB`);

  // 2. Resize to 2048x2048 max for vision model (saves bandwidth, models resize internally anyway)
  const describeBuffer = await sharp(rawBuffer)
    .resize(DESCRIBE_MAX_DIMENSION, DESCRIBE_MAX_DIMENSION, { fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: 85 })
    .toBuffer();

  // 3. Describe with Nova Pro, fall back to Gemini
  const { description, model } = await describeImage(photo, describeBuffer, 'image/jpeg');

  // 4. Resize to 512x512 thumbnail via Sharp (from raw for best quality)
  const thumbnailBuffer = await sharp(rawBuffer)
    .resize(512, 512, { fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: 75 })
    .toBuffer();

  // 4. Upload thumbnail to S3
  const thumbnailKey = `${S3_THUMBNAIL_PREFIX}${photo.id}.jpg`;
  await s3.send(new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: thumbnailKey,
    Body: thumbnailBuffer,
    ContentType: 'image/jpeg',
    CacheControl: 'public, max-age=31536000, immutable',
  }));

  // 5. Update DB first (so description is saved even if cleanup fails)
  await pool.query(
    `UPDATE photo_metadata
     SET description = $1, described_at = NOW(), thumbnail_s3_key = $2,
         thumbnail = NULL, full_image_s3_key = NULL, processed_at = NOW()
     WHERE id = $3`,
    [description, thumbnailKey, photo.id]
  );

  // 5b. Ingest into Knowledge Graph (non-fatal — description already saved)
  try {
    await ingestPhotoToKG(pool, photo, description);
  } catch (kgErr) {
    logger.logMinimal(`KG ingestion failed for ${photo.local_identifier}: ${kgErr instanceof Error ? kgErr.message : String(kgErr)}`);
  }

  // 6. Delete full-res original from S3 (non-fatal — description already saved)
  try {
    await s3.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key: photo.full_image_s3_key! }));
  } catch (delErr) {
    logger.logMinimal(`Failed to delete full-res ${photo.full_image_s3_key}: ${delErr instanceof Error ? delErr.message : String(delErr)}`);
  }

  logger.logVerbose(`Completed ${photo.local_identifier} via ${model} in ${Math.round((Date.now() - photoStart) / 1000)}s`);
  return model;
}

// ---------------------------------------------------------------------------
// Legacy base64 flow: describe from base64 -> upload thumbnail to S3
// ---------------------------------------------------------------------------

async function processBase64Photo(
  pool: ReturnType<typeof getPool>,
  s3: S3Client,
  photo: PhotoToDescribe
): Promise<string> {
  // Strip data URL prefix if present
  let base64Data = photo.thumbnail!;
  let mimeType = 'image/jpeg';
  const dataUrlMatch = base64Data.match(/^data:(image\/\w+);base64,(.+)$/);
  if (dataUrlMatch) {
    mimeType = dataUrlMatch[1];
    base64Data = dataUrlMatch[2];
  }

  // 1. Resize for vision model and describe
  const rawBuffer = Buffer.from(base64Data, 'base64');
  const describeBuffer = await sharp(rawBuffer)
    .resize(DESCRIBE_MAX_DIMENSION, DESCRIBE_MAX_DIMENSION, { fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: 85 })
    .toBuffer();
  const { description, model } = await describeImage(photo, describeBuffer, 'image/jpeg');

  // 2. Upload thumbnail to S3
  const thumbnailKey = `${S3_THUMBNAIL_PREFIX}${photo.id}.jpg`;

  try {
    await s3.send(new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: thumbnailKey,
      Body: describeBuffer,
      ContentType: 'image/jpeg',
      CacheControl: 'public, max-age=31536000, immutable',
    }));

    // 3. Update DB with description + S3 key, clear base64
    await pool.query(
      `UPDATE photo_metadata
       SET description = $1, described_at = NOW(), thumbnail_s3_key = $2,
           thumbnail = NULL, processed_at = NOW()
       WHERE id = $3`,
      [description, thumbnailKey, photo.id]
    );

    // 3b. Ingest into Knowledge Graph (non-fatal)
    try {
      await ingestPhotoToKG(pool, photo, description);
    } catch (kgErr) {
      logger.logMinimal(`KG ingestion failed for ${photo.local_identifier}: ${kgErr instanceof Error ? kgErr.message : String(kgErr)}`);
    }
  } catch {
    // S3 failed — set description but leave processed_at NULL for retry
    await pool.query(
      `UPDATE photo_metadata SET description = $1, described_at = NOW() WHERE id = $2`,
      [description, photo.id]
    );
  }

  return model;
}

// ---------------------------------------------------------------------------
// Concurrency helper: process items with bounded parallelism
// ---------------------------------------------------------------------------

async function processWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R | null>
): Promise<PromiseSettledResult<R | null>[]> {
  const results: PromiseSettledResult<R | null>[] = new Array(items.length);
  let index = 0;

  async function worker(): Promise<void> {
    while (index < items.length) {
      const i = index++;
      try {
        const value = await fn(items[i]);
        results[i] = { status: 'fulfilled', value };
      } catch (reason) {
        results[i] = { status: 'rejected', reason };
      }
      // Brief pause between requests to avoid overloading GPU
      if (index < items.length) {
        await new Promise(r => setTimeout(r, 500));
      }
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

// ---------------------------------------------------------------------------
// AI description via Forge VLM (Qwen3-VL-32B)
// ---------------------------------------------------------------------------

const DESCRIBE_PROMPT = `Describe this photo in 1-3 concise sentences. Focus on: who/what is in the photo, the setting/location, the activity or mood, and any visible text (signs, menus, screens). Be specific about people, objects, landmarks, and scenes. If GPS coordinates are provided, incorporate the known location. Do not mention image quality or resolution.`;

async function buildContext(photo: PhotoToDescribe): Promise<string> {
  const parts: string[] = [];
  if (photo.taken_at) parts.push(`Taken: ${photo.taken_at}`);
  if (photo.latitude != null && photo.longitude != null) {
    const locationName = await reverseGeocode(Number(photo.latitude), Number(photo.longitude));
    if (locationName) {
      parts.push(`Location: ${locationName}`);
    } else {
      parts.push(`Location: ${Number(photo.latitude).toFixed(4)}, ${Number(photo.longitude).toFixed(4)}`);
    }
  }
  if (photo.is_favorite) parts.push('Marked as favorite');
  if (photo.people?.length) parts.push(`People identified: ${photo.people.join(', ')}`);
  parts.push(`Type: ${photo.media_type}`);
  return parts.length > 0 ? `\nContext: ${parts.join('. ')}` : '';
}

async function describeImage(
  photo: PhotoToDescribe,
  imageBuffer: Buffer,
  _mimeType: string
): Promise<{ description: string; model: string }> {
  const context = await buildContext(photo);
  const fullPrompt = `${DESCRIBE_PROMPT}${context}`;

  const base64Data = imageBuffer.toString('base64');
  const stopTimer = logger.time(`forge-describe-${photo.local_identifier}`);

  const response = await fetch(`${FORGE_URL}/v1/describe`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${FORGE_API_KEY}`,
    },
    body: JSON.stringify({
      image: base64Data,
      image_format: 'jpeg',
      prompt: fullPrompt,
      people: photo.people || undefined,
      max_tokens: 1024,
      temperature: 0.3,
    }),
    signal: AbortSignal.timeout(120_000),
  });

  stopTimer();

  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(`Forge /v1/describe failed (${response.status}): ${body.slice(0, 200)}`);
  }

  const result = await response.json() as { description: string; model: string; latency_ms: number };
  if (!result.description) throw new Error('Empty response from Forge VLM');

  logger.logVerbose(`Forge described ${photo.local_identifier} in ${result.latency_ms}ms via ${result.model}`);
  return { description: result.description, model: result.model };
}

// ---------------------------------------------------------------------------
// Knowledge Graph ingestion for described photos
// ---------------------------------------------------------------------------

async function ingestPhotoToKG(
  pool: ReturnType<typeof getPool>,
  photo: PhotoToDescribe,
  description: string
): Promise<void> {
  const photoKey = `photo_${photo.local_identifier.replace(/[^a-zA-Z0-9]/g, '_')}`;
  const entityHints: EntityHint[] = [];

  // Add people as entity hints
  if (photo.people?.length) {
    for (const personName of photo.people) {
      entityHints.push({
        name: personName,
        type: 'person',
        role: 'subject',
      });
    }
  }

  // Add location as entity hint if GPS available — resolve to human-readable name
  if (photo.latitude != null && photo.longitude != null) {
    const locationName = await reverseGeocode(Number(photo.latitude), Number(photo.longitude));
    entityHints.push({
      name: locationName || `${Number(photo.latitude).toFixed(2)}, ${Number(photo.longitude).toFixed(2)}`,
      type: 'place',
      role: 'location',
      metadata: { latitude: photo.latitude, longitude: photo.longitude },
    });
  }

  const fact: FactInput = {
    domain: 'photos',
    category: photo.media_type || 'photo',
    key: photoKey,
    value: description,
    confidence: 0.7,
    source: 'photo_describe',
    validFrom: photo.taken_at || undefined,
    entityHints: entityHints.length > 0 ? entityHints : undefined,
  };

  await ingestFact(fact);

  // Mark as KG-ingested
  await pool.query(
    `UPDATE photo_metadata SET kg_ingested_at = NOW() WHERE id = $1`,
    [photo.id]
  );
}
