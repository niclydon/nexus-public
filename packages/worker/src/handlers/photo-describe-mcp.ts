/**
 * photo-describe-mcp — Two-tier photo description via Apple Photos MCP.
 *
 * Tier 1 (metadata-rich, ~90% of photos):
 *   Uses Apple ML labels, persons, place, EXIF to construct a text description.
 *   Zero LLM cost, instant.
 *
 * Tier 2 (bare photos, ~10%):
 *   Reads the macOS derivative JPEG (~480x360, ~90KB) from the Photos library
 *   on Dev-Server via SSH, sends base64 to Forge VLM (Qwen3-VL-32B on port 8081)
 *   for visual description.
 *
 * Processes photos from photo_metadata that have no description.
 * Batch size: 20 per job, self-chains if more remain.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { callMcpTool } from '../lib/mcp-client-manager.js';

const logger = createLogger('photo-describe-mcp');

const BATCH_SIZE = 20;
const VLM_TIMEOUT_MS = 30_000;
const VLM_URL = process.env.FORGE_VLM_URL ?? 'http://localhost:8081/v1/chat/completions';
const PHOTOS_LIB = '~/Pictures/Photos Library.photoslibrary';

export async function handlePhotoDescribeMcp(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Get undescribed photos
  const { rows: photos } = await pool.query<{
    id: string;
    local_identifier: string;
    people: string[] | null;
    location_name: string | null;
    media_type: string | null;
    taken_at: Date | null;
  }>(
    `SELECT id, local_identifier, people, location_name, media_type, taken_at
     FROM photo_metadata
     WHERE description IS NULL AND local_identifier IS NOT NULL
     ORDER BY taken_at DESC NULLS LAST
     LIMIT $1`,
    [BATCH_SIZE],
  );

  if (photos.length === 0) {
    logger.logVerbose('No undescribed photos');
    return { processed: 0, remaining: 0 };
  }

  let tier1Count = 0;
  let tier2Count = 0;
  let failCount = 0;

  for (const photo of photos) {
    try {
      // Get full metadata from MCP (includes labels, exif, persons, place)
      let metadata: Record<string, unknown>;
      try {
        const result = await callMcpTool('apple-photos', 'get_photo', { uuid: photo.local_identifier });
        metadata = JSON.parse(result);
      } catch (err) {
        logger.logVerbose(`Failed to get metadata for ${photo.local_identifier}:`, (err as Error).message);
        failCount++;
        continue;
      }

      const labels = (metadata.labels as string[]) ?? [];
      const persons = (metadata.persons as string[])?.filter(p => p !== '_UNKNOWN_') ?? [];
      const place = String(metadata.place ?? '');
      const exif = metadata.exif_info as Record<string, unknown> | undefined;

      // Tier 1: metadata-rich — build description from Apple ML data
      if (labels.length > 0 || persons.length > 0) {
        const description = buildMetadataDescription(metadata, labels, persons, place, exif);
        await pool.query(
          `UPDATE photo_metadata SET description = $1, described_at = NOW(), processed_at = NOW() WHERE id = $2`,
          [description, photo.id],
        );
        tier1Count++;
        logger.logVerbose(`Tier 1 described: ${photo.local_identifier} — ${description.slice(0, 80)}`);
        continue;
      }

      // Tier 2: bare photo — need VLM
      const description = await describeViaVlm(photo.local_identifier);
      if (description) {
        await pool.query(
          `UPDATE photo_metadata SET description = $1, described_at = NOW(), processed_at = NOW() WHERE id = $2`,
          [description, photo.id],
        );
        tier2Count++;
        logger.logVerbose(`Tier 2 (VLM) described: ${photo.local_identifier} — ${description.slice(0, 80)}`);
      } else {
        // VLM failed — mark with a placeholder so we don't retry every cycle
        await pool.query(
          `UPDATE photo_metadata SET description = '[undescribed]', processed_at = NOW() WHERE id = $1`,
          [photo.id],
        );
        failCount++;
      }
    } catch (err) {
      logger.logVerbose(`Error processing ${photo.local_identifier}:`, (err as Error).message);
      failCount++;
    }
  }

  // Check if more remain
  const { rows: remaining } = await pool.query<{ count: string }>(
    `SELECT COUNT(*) as count FROM photo_metadata WHERE description IS NULL AND local_identifier IS NOT NULL`,
  );
  const remainCount = parseInt(remaining[0].count, 10);

  // Self-chain if more to process
  if (remainCount > 0) {
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
       VALUES ('photo-describe-mcp', '{}', 'nexus', 1, 3, NOW() + INTERVAL '10 seconds')`,
    );
    logger.logVerbose(`Self-chained: ${remainCount} photos remaining`);
  }

  logger.log(`Photo describe: ${tier1Count} metadata, ${tier2Count} VLM, ${failCount} failed, ${remainCount} remaining`);

  return {
    processed: tier1Count + tier2Count,
    tier1_metadata: tier1Count,
    tier2_vlm: tier2Count,
    failed: failCount,
    remaining: remainCount,
  };
}

/**
 * Build a natural language description from Apple ML metadata.
 */
function buildMetadataDescription(
  metadata: Record<string, unknown>,
  labels: string[],
  persons: string[],
  place: string,
  exif?: Record<string, unknown>,
): string {
  const isMovie = !!metadata.ismovie;
  const parts: string[] = [];

  // What is it?
  if (isMovie) {
    parts.push('Video');
  } else if (labels.length > 0) {
    // Use the most descriptive labels
    const sceneLabels = labels.filter(l => !['People', 'Selfie'].includes(l));
    if (sceneLabels.length > 0) {
      parts.push(`Photo featuring ${sceneLabels.slice(0, 4).join(', ').toLowerCase()}`);
    } else {
      parts.push('Photo');
    }
  } else {
    parts.push('Photo');
  }

  // Who is in it?
  if (persons.length > 0) {
    parts.push(`with ${persons.join(' and ')}`);
  }

  // Where?
  if (place) {
    parts.push(`in ${place}`);
  }

  // When?
  const date = metadata.date ? new Date(String(metadata.date)) : null;
  if (date && !isNaN(date.getTime())) {
    parts.push(`on ${date.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })}`);
  }

  // Camera?
  if (exif?.camera_model) {
    const model = String(exif.camera_model);
    if (labels.includes('Selfie') || model.includes('front camera')) {
      parts.push('(selfie)');
    }
  }

  return parts.join(' ') + '.';
}

/**
 * Describe a photo using the VLM by reading the macOS derivative JPEG.
 * SSH to Dev-Server, base64 encode the derivative, send to Forge VLM.
 */
async function describeViaVlm(uuid: string): Promise<string | null> {
  const firstChar = uuid.charAt(0);
  // macOS derivatives follow the pattern: {first_char}/{UUID}_4_5005_c.jpeg
  const derivativePath = `${PHOTOS_LIB}/resources/derivatives/masters/${firstChar}/${uuid}_4_5005_c.jpeg`;

  let base64Img: string;
  try {
    const { execSync } = await import('node:child_process');
    base64Img = execSync(
      `ssh dev-server.example.io "base64 -i '${derivativePath}' | tr -d '\\n'"`,
      { timeout: 10_000, encoding: 'utf-8' },
    ).trim();

    if (!base64Img || base64Img.length < 100) {
      logger.logVerbose(`No derivative found for ${uuid}`);
      return null;
    }
  } catch (err) {
    logger.logVerbose(`Failed to read derivative for ${uuid}:`, (err as Error).message);
    return null;
  }

  try {
    const response = await fetch(VLM_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'qwen3-vl-32b',
        messages: [{
          role: 'user',
          content: [
            { type: 'image_url', image_url: { url: `data:image/jpeg;base64,${base64Img}` } },
            { type: 'text', text: 'Describe this photo in 2-3 sentences. Include people, setting, activity, and mood.' },
          ],
        }],
        max_tokens: 200,
      }),
      signal: AbortSignal.timeout(VLM_TIMEOUT_MS),
    });

    if (!response.ok) {
      logger.logVerbose('VLM error:', response.status);
      return null;
    }

    const data = await response.json() as {
      choices: Array<{ message: { content: string } }>;
    };

    return data.choices?.[0]?.message?.content?.trim() ?? null;
  } catch (err) {
    logger.logVerbose('VLM call failed:', (err as Error).message);
    return null;
  }
}
