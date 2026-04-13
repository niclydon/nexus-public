/**
 * data-import handler
 *
 * Processes uploaded files for bulk data import. Supports three file categories:
 *
 * 1. TEXT (html, json, csv, txt, md) — Parse into records, LLM extraction → PKG + core_memory
 * 2. AUDIO (mp3, wav, m4a, ogg, webm) — Transcribe via Forge Whisper → LLM extraction → PKG
 * 3. ZIP — Decompress, create child imports for each file, enqueue child jobs
 *
 * After processing, saves schema for future recognition and cleans up S3.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';
import { S3Client, GetObjectCommand, DeleteObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { createHash } from 'crypto';
import { parseMbox, type ParsedEmail } from '../lib/mbox-parser.js';
import { ingestFacts, ingestConversationChunks, type FactInput, type ConversationChunkInput, type EntityHint } from '../lib/knowledge.js';
import { createReadStream, createWriteStream, mkdirSync, readdirSync, statSync, readFileSync, rmSync, existsSync } from 'fs';
import { join, extname, basename } from 'path';
import { tmpdir } from 'os';
import { randomUUID } from 'crypto';
import { execSync } from 'child_process';

const logger = createLogger('data-import');

const BUCKET = process.env.DATA_IMPORTS_S3_BUCKET || 'data-imports';
const s3 = new S3Client({ region: process.env.AWS_REGION || 'us-east-1', forcePathStyle: true });

interface DataImportPayload {
  import_id: string;
}

interface ImportRecord {
  id: string;
  filename: string;
  s3_key: string;
  size_bytes: number;
  mime_type: string | null;
  file_type: string | null;
  parent_import_id: string | null;
  analysis: Record<string, unknown>;
  processing_plan: Record<string, unknown>;
}

interface Extraction {
  category: string;
  key: string;
  value: string;
  domain?: string;
  when?: string;
  confidence?: number;
}

// ---------- File type detection ----------

const TEXT_EXTENSIONS = new Set(['.html', '.htm', '.json', '.csv', '.tsv', '.txt', '.md', '.xml', '.log']);
const AUDIO_EXTENSIONS = new Set(['.mp3', '.wav', '.m4a', '.ogg', '.webm', '.flac', '.aac', '.wma']);
const ARCHIVE_EXTENSIONS = new Set(['.zip', '.tar', '.tgz', '.gz', '.7z', '.rar']);
const EMAIL_EXTENSIONS = new Set(['.mbox', '.eml']);
const SKIP_EXTENSIONS = new Set(['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.mp4', '.mov', '.avi', '.pdf']);

function classifyFile(filename: string): 'text' | 'audio' | 'zip' | 'email' | 'skip' | 'unknown' {
  const ext = extname(filename).toLowerCase();
  const lowerName = filename.toLowerCase();
  // .tar.gz has two extensions — check the compound suffix
  if (lowerName.endsWith('.tar.gz') || lowerName.endsWith('.tar.bz2') || lowerName.endsWith('.tar.xz')) return 'zip';
  if (ARCHIVE_EXTENSIONS.has(ext)) return 'zip';
  if (EMAIL_EXTENSIONS.has(ext)) return 'email';
  if (AUDIO_EXTENSIONS.has(ext)) return 'audio';
  if (TEXT_EXTENSIONS.has(ext)) return 'text';
  if (SKIP_EXTENSIONS.has(ext)) return 'skip';
  return 'unknown';
}

// ---------- Helpers ----------

function hashRecord(content: string): string {
  return createHash('sha256').update(content).digest('hex');
}

async function isRecordImported(hash: string): Promise<boolean> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT 1 FROM import_record_hashes WHERE record_hash = $1 LIMIT 1`,
    [hash]
  );
  return rows.length > 0;
}

async function markRecordImported(importId: string, hash: string, key?: string): Promise<void> {
  const pool = getPool();
  await pool.query(
    `INSERT INTO import_record_hashes (import_id, record_hash, record_key)
     VALUES ($1, $2, $3) ON CONFLICT (record_hash) DO NOTHING`,
    [importId, hash, key || null]
  );
}

async function downloadFile(s3Key: string): Promise<string> {
  const response = await s3.send(
    new GetObjectCommand({ Bucket: BUCKET, Key: s3Key })
  );
  return (await response.Body?.transformToString('utf-8')) || '';
}

async function downloadFileBinary(s3Key: string): Promise<Buffer> {
  const response = await s3.send(
    new GetObjectCommand({ Bucket: BUCKET, Key: s3Key })
  );
  const chunks: Uint8Array[] = [];
  if (response.Body) {
    const reader = response.Body.transformToWebStream().getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
  }
  return Buffer.concat(chunks);
}

async function uploadFile(s3Key: string, body: Buffer | string, contentType: string): Promise<void> {
  await s3.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: s3Key,
    Body: body,
    ContentType: contentType,
  }));
}

async function deleteFile(s3Key: string): Promise<void> {
  await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: s3Key }));
}

function parseRecords(content: string, filename: string): string[] {
  // Try JSON array
  if (filename.endsWith('.json')) {
    try {
      const parsed = JSON.parse(content);
      if (Array.isArray(parsed)) {
        return parsed.map((r) => JSON.stringify(r));
      }
      for (const key of Object.keys(parsed)) {
        if (Array.isArray(parsed[key]) && parsed[key].length > 0) {
          return parsed[key].map((r: unknown) => JSON.stringify(r));
        }
      }
      return [content];
    } catch {
      return content.split('\n').filter((line) => line.trim().startsWith('{'));
    }
  }

  // CSV/TSV
  if (filename.endsWith('.csv') || filename.endsWith('.tsv')) {
    const lines = content.split('\n');
    if (lines.length < 2) return [];
    const header = lines[0];
    const sep = filename.endsWith('.tsv') ? '\t' : ',';
    const headers = header.split(sep).map((h) => h.trim().replace(/^"|"$/g, ''));
    return lines.slice(1).filter((l) => l.trim()).map((line) => {
      const vals = line.split(sep).map((v) => v.trim().replace(/^"|"$/g, ''));
      const obj: Record<string, string> = {};
      headers.forEach((h, i) => { obj[h] = vals[i] || ''; });
      return JSON.stringify(obj);
    });
  }

  // HTML — split by common record markers
  if (filename.endsWith('.html') || filename.endsWith('.htm')) {
    const parts = content.split(/<(?:div|tr|article|section)\s[^>]*class="[^"]*(?:message|entry|item|record|call|sms)[^"]*"/i);
    if (parts.length > 1) {
      return parts.slice(1).map((p, i) => `Record ${i + 1}:\n${p.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 2000)}`);
    }
    const textParts = content.split(/<\/(?:div|p|tr)>/i)
      .map((p) => p.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim())
      .filter((p) => p.length > 20);
    return textParts;
  }

  // Plain text — split by double newlines or other delimiters
  const parts = content.split(/\n{2,}/).filter((p) => p.trim().length > 10);
  return parts.length > 1 ? parts : [content];
}

async function getExistingFacts(domains: string[]): Promise<string[]> {
  const pool = getPool();
  const { rows } = await pool.query(
    `SELECT domain, category, key, value FROM owner_profile
     WHERE domain = ANY($1) AND confidence >= 0.3
     ORDER BY confidence DESC LIMIT 100`,
    [domains]
  );
  return rows.map((r) => `[${r.domain}/${r.category}] ${r.key}: ${r.value}`);
}

// ---------- ZIP Handler ----------

async function handleZipImport(job: TempoJob, imp: ImportRecord): Promise<Record<string, unknown>> {
  const pool = getPool();
  const tmpDir = join(tmpdir(), `aria-import-${imp.id}`);
  const zipPath = join(tmpDir, 'archive.zip');

  try {
    // 1. Download ZIP from S3
    mkdirSync(tmpDir, { recursive: true });
    const zipData = await downloadFileBinary(imp.s3_key);
    const { writeFileSync } = await import('fs');
    writeFileSync(zipPath, zipData);
    logger.log(`Downloaded ZIP: ${zipData.length} bytes`);

    // 2. Decompress based on archive type
    const extractDir = join(tmpDir, 'extracted');
    mkdirSync(extractDir, { recursive: true });

    const lowerName = imp.filename.toLowerCase();
    let decompressCmd: string;

    if (lowerName.endsWith('.tar.gz') || lowerName.endsWith('.tgz')) {
      decompressCmd = `tar -xzf "${zipPath}" -C "${extractDir}"`;
    } else if (lowerName.endsWith('.tar.bz2')) {
      decompressCmd = `tar -xjf "${zipPath}" -C "${extractDir}"`;
    } else if (lowerName.endsWith('.tar.xz')) {
      decompressCmd = `tar -xJf "${zipPath}" -C "${extractDir}"`;
    } else if (lowerName.endsWith('.tar')) {
      decompressCmd = `tar -xf "${zipPath}" -C "${extractDir}"`;
    } else if (lowerName.endsWith('.gz')) {
      // Single .gz file (not tar.gz) — gunzip in place
      decompressCmd = `gunzip -c "${zipPath}" > "${join(extractDir, basename(imp.filename, '.gz'))}"`;
    } else if (lowerName.endsWith('.7z')) {
      decompressCmd = `7z x "${zipPath}" -o"${extractDir}" -y`;
    } else if (lowerName.endsWith('.rar')) {
      decompressCmd = `unrar x -o+ "${zipPath}" "${extractDir}/"`;
    } else {
      // Default to unzip for .zip
      decompressCmd = `unzip -o -q "${zipPath}" -d "${extractDir}"`;
    }

    execSync(decompressCmd, { timeout: 120000 });
    logger.log(`Archive extracted to ${extractDir}`);

    // 3. Walk extracted files and classify
    const files = walkDir(extractDir);
    logger.log(`Found ${files.length} files in ZIP`);

    // Filter out junk files (macOS metadata, thumbs.db, etc.)
    const meaningfulFiles = files.filter(f => {
      const base = basename(f);
      if (base.startsWith('.') || base.startsWith('__MACOSX')) return false;
      if (base === 'Thumbs.db' || base === '.DS_Store') return false;
      const type = classifyFile(f);
      return type !== 'skip';
    });

    logger.log(`${meaningfulFiles.length} meaningful files to process`);

    // 4. Create child import records and upload each to S3
    let childrenCreated = 0;
    const childIds: string[] = [];

    for (const filePath of meaningfulFiles) {
      const relPath = filePath.replace(extractDir + '/', '');
      const fileType = classifyFile(filePath);
      const childId = randomUUID();
      const childS3Key = `imports/${imp.id}/extracted/${childId}/${basename(filePath)}`;
      const fileSize = statSync(filePath).size;

      // Skip empty files
      if (fileSize === 0) continue;

      // Skip files we can't handle yet
      if (fileType === 'unknown') {
        logger.log(`Skipping unknown file type: ${relPath}`);
        continue;
      }

      // Upload to S3
      const fileData = readFileSync(filePath);
      const mimeType = fileType === 'audio' ? 'audio/mpeg' : fileType === 'email' ? 'message/rfc822' : 'text/plain';
      await uploadFile(childS3Key, fileData, mimeType);

      // Create child import record
      await pool.query(
        `INSERT INTO data_imports (id, filename, s3_key, size_bytes, mime_type, file_type, status, parent_import_id, analysis, processing_plan)
         VALUES ($1, $2, $3, $4, $5, $6, 'approved', $7, $8, $9)`,
        [
          childId,
          relPath,
          childS3Key,
          fileSize,
          mimeType,
          fileType,
          imp.id,
          JSON.stringify({ source: 'zip', parent_filename: imp.filename, relative_path: relPath }),
          JSON.stringify({ batch_size: 20 }),
        ]
      );

      // Enqueue child job
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, max_attempts)
         VALUES ('data-import', $1, 2)`,
        [JSON.stringify({ import_id: childId })]
      );

      childIds.push(childId);
      childrenCreated++;
    }

    // 5. Update parent with child count
    await pool.query(
      `UPDATE data_imports
       SET status = 'processing', children_total = $2,
           result = $3
       WHERE id = $1`,
      [imp.id, childrenCreated, JSON.stringify({
        total_files: files.length,
        meaningful_files: meaningfulFiles.length,
        children_created: childrenCreated,
        child_ids: childIds,
        file_breakdown: Object.fromEntries(
          meaningfulFiles.reduce((acc, f) => {
            const t = classifyFile(f);
            acc.set(t, (acc.get(t) || 0) + 1);
            return acc;
          }, new Map<string, number>())
        ),
      })]
    );

    // 6. Clean up temp dir and S3 ZIP
    rmSync(tmpDir, { recursive: true, force: true });
    try {
      await deleteFile(imp.s3_key);
    } catch (err) {
      logger.logMinimal(`Failed to delete ZIP from S3:`, err);
    }

    logEvent({
      action: `ZIP import "${imp.filename}": extracted ${childrenCreated} files for processing`,
      component: 'data-import',
      category: 'integration',
      metadata: { import_id: imp.id, children: childrenCreated },
    });

    logger.log(`ZIP extracted: ${childrenCreated} child imports created`);

    return {
      zip_extracted: true,
      total_files: files.length,
      children_created: childrenCreated,
    };
  } catch (err) {
    // Clean up on error
    if (existsSync(tmpDir)) rmSync(tmpDir, { recursive: true, force: true });
    throw err;
  }
}

/** Recursively walk a directory and return all file paths */
function walkDir(dir: string): string[] {
  const results: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      // Skip __MACOSX directories
      if (entry.name === '__MACOSX') continue;
      results.push(...walkDir(fullPath));
    } else {
      results.push(fullPath);
    }
  }
  return results;
}

// ---------- Audio Handler ----------

const FORGE_URL = process.env.FORGE_URL || 'http://forge.example.io';
const FORGE_API_KEY = process.env.FORGE_API_KEY || '';

/**
 * Transcribe audio via Forge Whisper (local, zero cost).
 * Downloads from S3, base64-encodes, POSTs to Forge /v1/transcribe.
 */
async function transcribeWithForge(s3Key: string, filename: string, _importId: string): Promise<{ text: string; duration: number }> {
  const audioData = await downloadFileBinary(s3Key);
  const base64Audio = audioData.toString('base64');
  const ext = extname(filename).toLowerCase().replace('.', '') || 'wav';

  logger.log(`Forge transcribe: ${filename} (${audioData.length} bytes, format: ${ext})`);
  const stopTimer = logger.time(`forge-transcribe-${filename}`);

  const response = await fetch(`${FORGE_URL}/v1/transcribe`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${FORGE_API_KEY}`,
    },
    body: JSON.stringify({
      audio: base64Audio,
      audio_format: ext,
      model: 'whisper',
    }),
    signal: AbortSignal.timeout(300_000), // 5 min for long audio
  });
  stopTimer();

  if (!response.ok) {
    const errBody = await response.text().catch(() => '');
    throw new Error(`Forge /v1/transcribe failed (${response.status}): ${errBody.slice(0, 500)}`);
  }

  const result = await response.json() as {
    transcript: string;
    language?: string;
    duration_seconds?: number;
    model?: string;
    latency_ms?: number;
  };

  logger.logVerbose(`Forge transcribed ${filename}: ${result.transcript.length} chars, ${result.duration_seconds?.toFixed(1)}s audio, ${result.latency_ms}ms latency via ${result.model}`);

  return { text: result.transcript || '', duration: result.duration_seconds || 0 };
}

async function handleAudioImport(job: TempoJob, imp: ImportRecord): Promise<Record<string, unknown>> {
  const pool = getPool();
  const fileSize = imp.size_bytes || 0;

  logger.log(`Audio: ${imp.filename} (${fileSize} bytes) — transcribing via Forge Whisper`);

  const transcription = await transcribeWithForge(imp.s3_key, imp.filename, imp.id);
  const transcript = transcription.text;
  const duration = transcription.duration;

  logger.log(`Transcribed ${imp.filename}: ${transcript.length} chars, ${duration.toFixed(1)}s via Forge Whisper`);

  // Save transcript to DB
  await pool.query(
    `UPDATE data_imports SET transcript = $2 WHERE id = $1`,
    [imp.id, transcript]
  );

  if (!transcript || transcript.trim().length < 10) {
    // Empty or near-empty transcript — mark as done with no facts
    await pool.query(
      `UPDATE data_imports
       SET status = 'completed', completed_at = NOW(),
           records_total = 0, records_processed = 0,
           result = $2
       WHERE id = $1`,
      [imp.id, JSON.stringify({ transcript_empty: true, transcript_length: transcript.length })]
    );
    return { transcript_empty: true };
  }

  // 3. Process transcript through the same LLM extraction pipeline as text
  // Treat the transcript as a single record with context about its source
  const existingFacts = await getExistingFacts([
    'people', 'preferences', 'places', 'career', 'health', 'events',
    'communication', 'interests', 'lifestyle'
  ]);

  const parentInfo = imp.analysis as { parent_filename?: string; relative_path?: string } || {};
  const contextHint = parentInfo.parent_filename
    ? `This is an audio file from "${parentInfo.parent_filename}" (path: ${parentInfo.relative_path || imp.filename}).`
    : `This is an audio file: "${imp.filename}".`;

  const result = await routeRequest({
    handler: 'data-import',
    taskTier: 'generation',
    systemPrompt: `You are ARIA, analyzing a transcript from an audio file. ${contextHint}

The audio may be a voicemail, phone call recording, voice memo, or other audio content. Extract meaningful personal information about the owner the owner.

If the transcript contains a voicemail or message left by someone, identify:
- WHO left the message (name, phone number, relationship if inferable)
- WHAT the message is about
- WHEN it was likely recorded (if there are date/time references)

EXISTING KNOWLEDGE (do NOT create duplicates):
${existingFacts.slice(0, 50).join('\n') || '(none yet)'}

RULES:
- Map to PKG domains: people, preferences, places, career, health, events, communication, interests, lifestyle
- Include temporal context when dates are available
- Use people's actual names when available
- Try to match phone numbers or names to existing contacts
- Note communication patterns (who calls, frequency, topics)
- Prefer fewer, high-quality extractions over many low-quality ones
- DO NOT use category 'actionable_item' or 'actionable_information'. Audio files have no reliable recency — a voicemail in this archive could be from yesterday or from 2010, and we cannot tell the difference. Treating them as live action items has caused phantom reminders ("call X back") for 10-year-old spam voicemails. Record the FACT of the message ('communication' category), not a TASK derived from it.

Respond with JSON (no markdown fences):
{
  "extractions": [
    { "category": "...", "domain": "...", "key": "...", "value": "...", "when": "...", "confidence": 0.7 }
  ],
  "audio_summary": "Brief summary of the audio content",
  "speaker": "Who is speaking (if identifiable)",
  "audio_type": "voicemail | call | memo | conversation | other"
}`,
    userMessage: `Analyze this audio transcript and extract meaningful personal information:\n\nFILENAME: ${imp.filename}\n\nTRANSCRIPT:\n${transcript}`,
    maxTokens: 4096,
  });

  let text = result.text.trim();
  if (text.startsWith('```')) {
    text = text.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
  }

  const parsed = JSON.parse(text);
  const extractions: Extraction[] = Array.isArray(parsed.extractions) ? parsed.extractions : [];

  // Write extractions to core_memory and PKG (same pattern as text imports)
  let factsCreated = 0;
  let factsDeduped = 0;

  for (const ext of extractions) {
    if (!ext.key || !ext.value) continue;

    // Hard filter: audio extractions must never produce actionable items.
    // We can't establish recency from a transcript, so a 2015 spam voicemail
    // would otherwise seed perpetual "call X back" proactive insights.
    const rawCat = (ext.category || '').toLowerCase();
    if (rawCat === 'actionable_item' || rawCat === 'actionable_information') {
      logger.logVerbose(`Dropped actionable extraction from audio: ${ext.key}`);
      continue;
    }

    const category = ext.category || 'context';
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { rows: existing } = await client.query<{ id: string; version: number; value: string }>(
        `SELECT id, version, value FROM core_memory
         WHERE category = $1 AND key = $2 AND superseded_by IS NULL LIMIT 1`,
        [category, ext.key]
      );

      if (existing.length > 0 && existing[0].value === ext.value) {
        factsDeduped++;
        await client.query('COMMIT');
        continue;
      }

      const nextVersion = existing.length > 0 ? existing[0].version + 1 : 1;
      const { rows: newRows } = await client.query<{ id: string }>(
        `INSERT INTO core_memory (category, key, value, version) VALUES ($1, $2, $3, $4) RETURNING id`,
        [category, ext.key, ext.value, nextVersion]
      );

      if (existing.length > 0) {
        await client.query(`UPDATE core_memory SET superseded_by = $1 WHERE id = $2`, [newRows[0].id, existing[0].id]);
      }

      await client.query('COMMIT');
      factsCreated++;
    } catch {
      await client.query('ROLLBACK');
    } finally {
      client.release();
    }
  }

  // Write to PKG
  const CATEGORY_TO_DOMAIN: Record<string, string> = {
    people: 'people', preferences: 'preferences', context: 'events', recurring: 'lifestyle',
  };

  const profileFacts: FactInput[] = extractions
    .filter((ext) => ext.key && ext.value)
    .map((ext) => ({
      domain: ext.domain || CATEGORY_TO_DOMAIN[ext.category] || ext.category || 'events',
      category: ext.category || 'context',
      key: ext.key,
      value: ext.value,
      confidence: ext.confidence ?? 0.6,
      source: 'data_import',
      validFrom: ext.when,
    }));

  if (profileFacts.length > 0) {
    await ingestFacts(profileFacts);
  }

  // Mark record as imported
  const hash = hashRecord(transcript);
  await markRecordImported(imp.id, hash, imp.filename);

  // Clean up S3
  try {
    await deleteFile(imp.s3_key);
  } catch { /* non-fatal */ }

  // Update parent progress if this is a child
  if (imp.parent_import_id) {
    await pool.query(
      `UPDATE data_imports SET children_completed = children_completed + 1 WHERE id = $1`,
      [imp.parent_import_id]
    );
    // Check if all children are done
    const { rows: parentRows } = await pool.query(
      `SELECT children_total, children_completed + 1 as done FROM data_imports WHERE id = $1`,
      [imp.parent_import_id]
    );
    if (parentRows.length > 0 && parentRows[0].done >= parentRows[0].children_total) {
      await pool.query(
        `UPDATE data_imports SET status = 'completed', completed_at = NOW() WHERE id = $1`,
        [imp.parent_import_id]
      );
      logger.log(`Parent import ${imp.parent_import_id} completed (all children done)`);
    }
  }

  // Mark this import complete
  await pool.query(
    `UPDATE data_imports
     SET status = 'completed', completed_at = NOW(),
         records_total = 1, records_processed = 1,
         facts_created = $2, facts_deduplicated = $3,
         result = $4
     WHERE id = $1`,
    [imp.id, factsCreated, factsDeduped, JSON.stringify({
      audio_type: parsed.audio_type || 'unknown',
      speaker: parsed.speaker || 'unknown',
      summary: parsed.audio_summary || '',
      transcript_length: transcript.length,
      facts_created: factsCreated,
      facts_deduplicated: factsDeduped,
    })]
  );

  logger.log(`Audio import complete: ${imp.filename} — ${factsCreated} facts, ${factsDeduped} deduped`);

  return {
    audio_type: parsed.audio_type,
    transcript_length: transcript.length,
    facts_created: factsCreated,
    facts_deduplicated: factsDeduped,
  };
}

// ---------- Text Handler (original logic) ----------

async function handleTextImport(job: TempoJob, imp: ImportRecord): Promise<Record<string, unknown>> {
  const pool = getPool();

  // 1. Download full file from S3
  const content = await downloadFile(imp.s3_key);
  logger.log(`Downloaded ${content.length} bytes from S3`);

  // 2. Parse into records
  const records = parseRecords(content, imp.filename);
  logger.log(`Parsed ${records.length} records`);

  await pool.query(
    `UPDATE data_imports SET records_total = $2 WHERE id = $1`,
    [imp.id, records.length]
  );

  // 3. Dedup: filter out already-imported records
  const newRecords: { content: string; hash: string; index: number }[] = [];
  let dedupSkipped = 0;
  for (let i = 0; i < records.length; i++) {
    const hash = hashRecord(records[i]);
    if (await isRecordImported(hash)) {
      dedupSkipped++;
    } else {
      newRecords.push({ content: records[i], hash, index: i });
    }
  }

  logger.log(`After dedup: ${newRecords.length} new records (${dedupSkipped} already imported)`);

  // 4. Get existing facts for semantic dedup context
  const existingFacts = await getExistingFacts([
    'people', 'preferences', 'places', 'career', 'health', 'events',
    'communication', 'interests', 'lifestyle'
  ]);

  // 5. Process in batches
  const batchSize = (imp.processing_plan as { batch_size?: number })?.batch_size || 20;
  let totalFactsCreated = 0;
  let totalFactsDeduped = 0;
  let recordsProcessed = 0;

  for (let batchStart = 0; batchStart < newRecords.length; batchStart += batchSize) {
    const batch = newRecords.slice(batchStart, batchStart + batchSize);
    const batchContent = batch.map((r, i) => `--- Record ${batchStart + i + 1} ---\n${r.content}`).join('\n\n');

    const analysisInfo = imp.analysis || {};
    const fileType = (analysisInfo as { file_type?: string }).file_type || imp.filename;

    try {
      const result = await routeRequest({
        handler: 'data-import',
        taskTier: 'generation',
        systemPrompt: `You are ARIA, analyzing imported data records from a file called "${imp.filename}" (type: ${fileType}).

Extract meaningful personal information about the owner the owner from these records. The data may contain messages, calls, locations, activities, or other personal data.

EXISTING KNOWLEDGE (do NOT create duplicates of these — only add NEW information or UPDATE if the new data is more recent/detailed):
${existingFacts.slice(0, 50).join('\n') || '(none yet)'}

RULES:
- Extract facts into categories: people, preferences, context, recurring
- Map to PKG domains: people, preferences, places, career, health, events, communication, interests, lifestyle
- ALWAYS include temporal context when dates are available (e.g. "As of June 2018, ...")
- Include the "when" field with approximate dates
- Use people's actual names when available
- Skip trivial/logistical records with no substantive info
- Do NOT duplicate existing facts listed above — only extract NEW information
- If a fact updates an existing one (e.g. someone changed jobs), note both the old and new info with dates
- Prefer fewer, high-quality extractions over many low-quality ones

Respond with JSON (no markdown fences):
{
  "extractions": [
    {
      "category": "people",
      "domain": "people",
      "key": "lucas_job_2018",
      "value": "As of 2018, [Person] worked at Acme Corp",
      "when": "2018",
      "confidence": 0.7
    }
  ],
  "records_with_info": number,
  "records_trivial": number
}`,
        userMessage: `Analyze these ${batch.length} records and extract meaningful personal information:\n\n${batchContent}`,
        maxTokens: 4096,
      });

      // Parse extractions
      let text = result.text.trim();
      if (text.startsWith('```')) {
        text = text.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
      }

      const parsed = JSON.parse(text);
      const extractions: Extraction[] = Array.isArray(parsed.extractions) ? parsed.extractions : [];

      // Write to core_memory
      let batchFactsCreated = 0;
      let batchFactsDeduped = 0;

      for (const ext of extractions) {
        if (!ext.key || !ext.value) continue;

        const category = ext.category || 'context';
        const client = await pool.connect();
        try {
          await client.query('BEGIN');

          const { rows: existing } = await client.query<{ id: string; version: number; value: string }>(
            `SELECT id, version, value FROM core_memory
             WHERE category = $1 AND key = $2 AND superseded_by IS NULL LIMIT 1`,
            [category, ext.key]
          );

          if (existing.length > 0 && existing[0].value === ext.value) {
            batchFactsDeduped++;
            await client.query('COMMIT');
            continue;
          }

          const nextVersion = existing.length > 0 ? existing[0].version + 1 : 1;

          const { rows: newRows } = await client.query<{ id: string }>(
            `INSERT INTO core_memory (category, key, value, version)
             VALUES ($1, $2, $3, $4) RETURNING id`,
            [category, ext.key, ext.value, nextVersion]
          );

          if (existing.length > 0) {
            await client.query(
              `UPDATE core_memory SET superseded_by = $1 WHERE id = $2`,
              [newRows[0].id, existing[0].id]
            );
          }

          await client.query('COMMIT');
          batchFactsCreated++;
        } catch {
          await client.query('ROLLBACK');
        } finally {
          client.release();
        }
      }

      // Write to owner_profile (PKG)
      const CATEGORY_TO_DOMAIN: Record<string, string> = {
        people: 'people',
        preferences: 'preferences',
        context: 'events',
        recurring: 'lifestyle',
      };

      const profileFacts: FactInput[] = extractions
        .filter((ext) => ext.key && ext.value)
        .map((ext) => ({
          domain: ext.domain || CATEGORY_TO_DOMAIN[ext.category] || ext.category || 'events',
          category: ext.category || 'context',
          key: ext.key,
          value: ext.value,
          confidence: ext.confidence ?? 0.6,
          source: 'data_import',
          validFrom: ext.when,
        }));

      if (profileFacts.length > 0) {
        await ingestFacts(profileFacts);
      }

      totalFactsCreated += batchFactsCreated;
      totalFactsDeduped += batchFactsDeduped;
      recordsProcessed += batch.length;

      // Mark records as imported
      for (const record of batch) {
        await markRecordImported(imp.id, record.hash);
      }

      // Update progress
      await pool.query(
        `UPDATE data_imports
         SET records_processed = $2, facts_created = $3, facts_deduplicated = $4
         WHERE id = $1`,
        [imp.id, recordsProcessed, totalFactsCreated, totalFactsDeduped + dedupSkipped]
      );

      logger.log(`Batch ${Math.floor(batchStart / batchSize) + 1}: ${extractions.length} extractions, ${batchFactsCreated} new, ${batchFactsDeduped} deduped`);
    } catch (err) {
      logger.logMinimal(`Batch error:`, err instanceof Error ? err.message : err);
    }
  }

  // 6. Delete file from S3
  try {
    await deleteFile(imp.s3_key);
    logger.log(`Deleted S3 file: ${imp.s3_key}`);
  } catch (err) {
    logger.logMinimal(`Failed to delete S3 file:`, err);
  }

  // Update parent progress if this is a child
  if (imp.parent_import_id) {
    await pool.query(
      `UPDATE data_imports SET children_completed = children_completed + 1 WHERE id = $1`,
      [imp.parent_import_id]
    );
    const { rows: parentRows } = await pool.query(
      `SELECT children_total, children_completed + 1 as done FROM data_imports WHERE id = $1`,
      [imp.parent_import_id]
    );
    if (parentRows.length > 0 && parentRows[0].done >= parentRows[0].children_total) {
      await pool.query(
        `UPDATE data_imports SET status = 'completed', completed_at = NOW() WHERE id = $1`,
        [imp.parent_import_id]
      );
      logger.log(`Parent import ${imp.parent_import_id} completed (all children done)`);
    }
  }

  // 7. Mark complete
  await pool.query(
    `UPDATE data_imports
     SET status = 'completed', completed_at = NOW(),
         records_processed = $2, facts_created = $3, facts_deduplicated = $4,
         result = $5
     WHERE id = $1`,
    [
      imp.id,
      recordsProcessed,
      totalFactsCreated,
      totalFactsDeduped + dedupSkipped,
      JSON.stringify({
        total_records: records.length,
        new_records: newRecords.length,
        dedup_skipped: dedupSkipped,
        records_processed: recordsProcessed,
        facts_created: totalFactsCreated,
        facts_deduplicated: totalFactsDeduped,
        s3_cleaned: true,
      }),
    ]
  );

  logEvent({
    action: `Data import completed: "${imp.filename}" — ${records.length} records, ${totalFactsCreated} facts created, ${totalFactsDeduped + dedupSkipped} deduplicated`,
    component: 'data-import',
    category: 'integration',
    metadata: {
      import_id: imp.id,
      filename: imp.filename,
      total_records: records.length,
      facts_created: totalFactsCreated,
      facts_deduplicated: totalFactsDeduped + dedupSkipped,
    },
  });

  logger.log(`Complete: ${records.length} records, ${totalFactsCreated} facts, ${totalFactsDeduped + dedupSkipped} deduped`);

  return {
    total_records: records.length,
    new_records: newRecords.length,
    dedup_skipped: dedupSkipped,
    facts_created: totalFactsCreated,
    facts_deduplicated: totalFactsDeduped + dedupSkipped,
  };
}

// ---------- Email/Mbox Handler ----------

/**
 * Process .mbox files (e.g. Gmail Takeout).
 * Parses into individual emails, filters out Spam/Trash/Drafts/Promotions,
 * and ingests remaining emails as conversation chunks in the Knowledge Graph.
 *
 * Each email becomes a conversation chunk with:
 *   - Entity resolution for sender (matched against contacts)
 *   - High-quality embedding via the "email" content class
 *   - Thread grouping via X-GM-THRID header
 */
async function handleMboxImport(job: TempoJob, imp: ImportRecord): Promise<Record<string, unknown>> {
  const pool = getPool();

  // 1. Download mbox from S3 (use binary download for large files)
  logger.log(`Downloading mbox: ${imp.filename} (${imp.size_bytes} bytes)`);
  const buffer = await downloadFileBinary(imp.s3_key);
  const content = buffer.toString('utf-8');
  logger.log(`Downloaded ${content.length} chars (${(buffer.length / 1024 / 1024).toFixed(1)} MB)`);

  // 2. Parse and filter
  const parseResult = parseMbox(content);
  logger.log(`Parsed mbox: ${parseResult.totalFound} total emails`);
  logger.log(`Filtered: ${parseResult.skippedSpam} spam, ${parseResult.skippedTrash} trash, ${parseResult.skippedDrafts} drafts, ${parseResult.skippedPromotions} promotions, ${parseResult.skippedOther} other skipped`);
  logger.log(`Remaining: ${parseResult.emails.length} emails to process`);

  const totalSkipped = parseResult.skippedSpam + parseResult.skippedTrash +
    parseResult.skippedDrafts + parseResult.skippedPromotions + parseResult.skippedOther;

  await pool.query(
    `UPDATE data_imports SET records_total = $2 WHERE id = $1`,
    [imp.id, parseResult.emails.length]
  );

  // 3. Dedup against already-imported emails
  const newEmails: ParsedEmail[] = [];
  let dedupSkipped = 0;
  for (const email of parseResult.emails) {
    const hash = hashRecord(email.messageId || `${email.from}|${email.subject}|${email.date}`);
    if (await isRecordImported(hash)) {
      dedupSkipped++;
    } else {
      newEmails.push(email);
    }
  }
  logger.log(`After dedup: ${newEmails.length} new emails (${dedupSkipped} already imported)`);

  // 4. Group emails by thread for conversational chunking
  const threads = groupByThread(newEmails);
  logger.log(`Grouped into ${threads.size} threads`);

  // 5. Process threads in batches → conversation chunks
  let chunksWritten = 0;
  let chunkErrors = 0;
  let emailsProcessed = 0;
  const BATCH_SIZE = 50; // threads per batch

  const threadEntries = Array.from(threads.entries());

  for (let batchStart = 0; batchStart < threadEntries.length; batchStart += BATCH_SIZE) {
    const batch = threadEntries.slice(batchStart, batchStart + BATCH_SIZE);
    const chunks: ConversationChunkInput[] = [];

    for (const [threadId, emails] of batch) {
      // Sort by date within thread
      emails.sort((a, b) => {
        const da = new Date(a.date).getTime() || 0;
        const db = new Date(b.date).getTime() || 0;
        return da - db;
      });

      // Build conversation text from thread
      const threadText = emails.map(e => {
        const datePart = e.date ? `[${e.date}]` : '';
        return `From: ${extractName(e.from)}\nTo: ${extractName(e.to)}\nSubject: ${e.subject}\n${datePart}\n\n${e.body}`;
      }).join('\n\n---\n\n');

      // Truncate very long threads to avoid embedding issues
      const truncatedText = threadText.length > 8000
        ? threadText.slice(0, 8000) + '\n\n[... truncated]'
        : threadText;

      // Build entity hint from the primary sender (not "me")
      const otherParty = findOtherParty(emails);
      const entityHint: EntityHint | undefined = otherParty
        ? { name: otherParty.name, type: 'person', aliases: otherParty.email ? [otherParty.email] : [] }
        : undefined;

      const timeStart = emails[0]?.date || undefined;
      const timeEnd = emails[emails.length - 1]?.date || undefined;

      chunks.push({
        sourceType: 'email',
        sourceIdentifier: threadId,
        entityHint,
        chunkText: truncatedText,
        chunkIndex: 0,
        messageCount: emails.length,
        timeStart,
        timeEnd,
        metadata: {
          subject: emails[0]?.subject,
          labels: emails[0]?.labels,
          import_id: imp.id,
          source_file: imp.filename,
        },
      });

      emailsProcessed += emails.length;
    }

    // Ingest batch
    const result = await ingestConversationChunks(chunks);
    chunksWritten += result.written;
    chunkErrors += result.errors;

    // Mark emails as imported
    for (const [, emails] of batch) {
      for (const email of emails) {
        const hash = hashRecord(email.messageId || `${email.from}|${email.subject}|${email.date}`);
        await markRecordImported(imp.id, hash, email.messageId || email.subject);
      }
    }

    // Update progress
    await pool.query(
      `UPDATE data_imports SET records_processed = $2, facts_created = $3 WHERE id = $1`,
      [imp.id, emailsProcessed, chunksWritten]
    );

    const batchNum = Math.floor(batchStart / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(threadEntries.length / BATCH_SIZE);
    logger.log(`Mbox batch ${batchNum}/${totalBatches}: ${chunks.length} threads, ${result.written} chunks written, ${result.errors} errors`);
  }

  // 6. Clean up S3
  try {
    await deleteFile(imp.s3_key);
    logger.log(`Deleted S3 file: ${imp.s3_key}`);
  } catch (err) {
    logger.logMinimal(`Failed to delete S3 file:`, err);
  }

  // 7. Update parent progress if this is a child (e.g., from ZIP)
  if (imp.parent_import_id) {
    await pool.query(
      `UPDATE data_imports SET children_completed = children_completed + 1 WHERE id = $1`,
      [imp.parent_import_id]
    );
    const { rows: parentRows } = await pool.query(
      `SELECT children_total, children_completed + 1 as done FROM data_imports WHERE id = $1`,
      [imp.parent_import_id]
    );
    if (parentRows.length > 0 && parentRows[0].done >= parentRows[0].children_total) {
      await pool.query(
        `UPDATE data_imports SET status = 'completed', completed_at = NOW() WHERE id = $1`,
        [imp.parent_import_id]
      );
      logger.log(`Parent import ${imp.parent_import_id} completed`);
    }
  }

  // 8. Mark complete
  const resultData = {
    total_emails: parseResult.totalFound,
    filtered_out: totalSkipped,
    spam: parseResult.skippedSpam,
    trash: parseResult.skippedTrash,
    drafts: parseResult.skippedDrafts,
    promotions: parseResult.skippedPromotions,
    other_skipped: parseResult.skippedOther,
    processed: newEmails.length,
    dedup_skipped: dedupSkipped,
    threads: threads.size,
    chunks_written: chunksWritten,
    chunk_errors: chunkErrors,
    emails_processed: emailsProcessed,
  };

  await pool.query(
    `UPDATE data_imports
     SET status = 'completed', completed_at = NOW(),
         records_processed = $2, facts_created = $3, facts_deduplicated = $4,
         result = $5
     WHERE id = $1`,
    [imp.id, emailsProcessed, chunksWritten, dedupSkipped + totalSkipped, JSON.stringify(resultData)]
  );

  logEvent({
    action: `Email import "${imp.filename}": ${parseResult.totalFound} emails, ${totalSkipped} filtered (spam/trash/drafts/promos), ${chunksWritten} conversation chunks created`,
    component: 'data-import',
    category: 'integration',
    metadata: { import_id: imp.id, ...resultData },
  });

  logger.log(`Mbox complete: ${parseResult.totalFound} emails, ${totalSkipped} filtered, ${chunksWritten} chunks written, ${chunkErrors} errors`);

  return resultData;
}

/** Group emails by thread ID */
function groupByThread(emails: ParsedEmail[]): Map<string, ParsedEmail[]> {
  const threads = new Map<string, ParsedEmail[]>();
  for (const email of emails) {
    const key = email.threadId || email.messageId || `${email.from}|${email.subject}`;
    const existing = threads.get(key) || [];
    existing.push(email);
    threads.set(key, existing);
  }
  return threads;
}

/** Extract display name from a "Name <email>" format */
function extractName(headerValue: string): string {
  const match = headerValue.match(/^"?([^"<]+)"?\s*<.*>$/);
  return match ? match[1].trim() : headerValue.split('@')[0] || headerValue;
}

/** Find the other party in an email thread (not the owner) */
function findOtherParty(emails: ParsedEmail[]): { name: string; email: string } | null {
  // Owner's likely email patterns
  const ownerPatterns = ['owner', 'owner', 'owner@', 'owner'];

  for (const email of emails) {
    const fromLower = email.from.toLowerCase();
    const isOwner = ownerPatterns.some(p => fromLower.includes(p));

    if (!isOwner && email.from) {
      const emailMatch = email.from.match(/<([^>]+)>/);
      const addr = emailMatch ? emailMatch[1] : email.from;
      const name = extractName(email.from);
      return { name, email: addr };
    }

    // Check To field if owner is the sender
    if (isOwner && email.to) {
      const toLower = email.to.toLowerCase();
      const toIsOwner = ownerPatterns.some(p => toLower.includes(p));
      if (!toIsOwner) {
        const emailMatch = email.to.match(/<([^>]+)>/);
        const addr = emailMatch ? emailMatch[1] : email.to;
        const name = extractName(email.to);
        return { name, email: addr };
      }
    }
  }
  return null;
}

// ---------- Main handler ----------

export async function handleDataImport(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as unknown as DataImportPayload;

  if (!payload.import_id) {
    return { error: 'Missing import_id' };
  }

  // Fetch import record
  const { rows } = await pool.query<ImportRecord>(
    `SELECT id, filename, s3_key, size_bytes, mime_type, file_type, parent_import_id, analysis, processing_plan
     FROM data_imports WHERE id = $1 AND status = 'approved'`,
    [payload.import_id]
  );

  if (rows.length === 0) {
    return { error: 'Import not found or not in approved state' };
  }

  const imp = rows[0];
  logger.log(`Starting import ${imp.id}: ${imp.filename} (${imp.size_bytes} bytes, type: ${imp.file_type || 'auto'})`);

  // Update status to processing
  await pool.query(
    `UPDATE data_imports SET status = 'processing' WHERE id = $1`,
    [imp.id]
  );

  try {
    // Determine file type and route to appropriate handler
    const fileType = imp.file_type || classifyFile(imp.filename);

    // Update file_type in DB if it was auto-detected
    if (!imp.file_type || imp.file_type === 'text') {
      await pool.query(`UPDATE data_imports SET file_type = $2 WHERE id = $1`, [imp.id, fileType]);
    }

    switch (fileType) {
      case 'zip':
        return await handleZipImport(job, imp);
      case 'email':
        return await handleMboxImport(job, imp);
      case 'audio':
        return await handleAudioImport(job, imp);
      case 'text':
      case 'unknown':
      default:
        return await handleTextImport(job, imp);
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logMinimal(`Failed:`, msg);

    await pool.query(
      `UPDATE data_imports SET status = 'failed', error = $2 WHERE id = $1`,
      [imp.id, msg]
    );

    return { error: msg };
  }
}
