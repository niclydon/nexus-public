/**
 * Topic Model Handler — K-means clustering on vector embeddings
 *
 * Runs k-means++ on ~500K embeddings across 26 tables, producing a two-level
 * topic hierarchy with LLM-generated labels. Executes in 4 self-chaining phases:
 *
 *   Phase 1: extract     — Stream all embeddings to a JSONL temp file
 *   Phase 2: cluster_l1  — L2-normalize + k-means (k=40) → Level 1 clusters
 *   Phase 3: cluster_l2  — Sub-cluster large L1 clusters → Level 2 clusters
 *   Phase 4: label       — Sample nearest-to-centroid items, LLM-label each cluster
 *
 * Self-chains via INSERT INTO tempo_jobs between phases.
 * Bulk job type — runs on bulk worker to avoid starving urgent jobs.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import { routeRequest } from '../lib/llm/index.js';
import { kmeans } from 'ml-kmeans';
import { spawn } from 'node:child_process';
import fs from 'fs';
import readline from 'readline';
import path from 'path';

const logger = createLogger('topic-model');

const VECTOR_FILE = '/tmp/topic-model-vectors.jsonl';
const CLUSTER_OUTPUT_FILE = '/tmp/topic-model-clusters.json';
// Python clustering script lives in nexus/scripts/, run via the dedicated venv.
// The venv is created once with `python3 -m venv .venv-topic-model` + numpy/sklearn.
const PROJECT_ROOT = process.env.NEXUS_PROJECT_ROOT || '/opt/nexus/Projects/nexus';
const PY_SCRIPT = path.join(PROJECT_ROOT, 'scripts', 'topic-model-cluster.py');
const PY_BINARY = path.join(PROJECT_ROOT, '.venv-topic-model', 'bin', 'python');

// ---------------------------------------------------------------------------
// Table configs — mirrors semantic-search TABLE_CONFIGS
// ---------------------------------------------------------------------------

interface TableDef {
  table: string;
  idColumn: string;
  idType: 'text' | 'integer' | 'uuid';
  textExpr: string;
  timestampExpr: string;
  extraWhere?: string;
}

const TABLES: TableDef[] = [
  // Communication
  {
    table: 'aurora_raw_imessage',
    idColumn: 'guid',
    idType: 'text',
    textExpr: `COALESCE(handle_id,'') || ': ' || COALESCE(text,'')`,
    timestampExpr: 'date',
  },
  {
    table: 'aurora_raw_gmail',
    idColumn: 'id',
    idType: 'text',
    textExpr: `COALESCE(from_name,from_address,'') || ': ' || COALESCE(subject,'') || ' — ' || COALESCE(LEFT(body_text,300),'')`,
    timestampExpr: 'date',
  },
  {
    table: 'aurora_raw_google_voice',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(contact_name,phone_number,'') || ': ' || COALESCE(message_text,'')`,
    timestampExpr: 'timestamp',
  },
  {
    table: 'aurora_raw_google_chat',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(sender_name,sender_email,'') || ': ' || COALESCE(message_text,'')`,
    timestampExpr: 'timestamp',
  },
  {
    table: 'bb_sms_messages',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(phone_number,'') || ': ' || COALESCE(message,'')`,
    timestampExpr: 'timestamp',
  },
  {
    table: 'archived_sent_emails',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(subject,'') || ' — ' || COALESCE(LEFT(body_text,300),'')`,
    timestampExpr: 'sent_at',
  },
  {
    table: 'site_guestbook',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(poster_name,'') || ' to ' || COALESCE(recipient_name,'') || ': ' || COALESCE(message_text,'')`,
    timestampExpr: 'posted_at',
  },

  // Knowledge
  {
    table: 'knowledge_facts',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(domain,'') || '/' || COALESCE(category,'') || ': ' || COALESCE(key,'') || ' = ' || COALESCE(LEFT(value,500),'')`,
    timestampExpr: 'created_at',
    extraWhere: 'AND superseded_by IS NULL',
  },
  {
    table: 'knowledge_entities',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(canonical_name,'') || COALESCE(' (' || entity_type || ')','') || COALESCE(' — ' || LEFT(summary,300),'')`,
    timestampExpr: 'created_at',
  },
  {
    table: 'knowledge_conversation_chunks',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(LEFT(chunk_text,500),'')`,
    timestampExpr: 'time_start',
  },

  // Media
  {
    table: 'blogger_posts',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(title,'') || ' — ' || COALESCE(LEFT(content_text,300),'')`,
    timestampExpr: 'published_at',
  },
  {
    table: 'music_library',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(track_name,'') || ' by ' || COALESCE(artist_name,'') || COALESCE(' — ' || album_name,'')`,
    timestampExpr: 'last_played_at',
  },
  {
    table: 'aurora_raw_chatgpt',
    idColumn: 'id',
    idType: 'text',
    textExpr: `COALESCE(conversation_title,'') || ' — ' || COALESCE(LEFT(content_text,300),'')`,
    timestampExpr: 'created_at',
  },
  {
    table: 'aurora_raw_claude',
    idColumn: 'id',
    idType: 'text',
    textExpr: `COALESCE(conversation_name,'') || ' — ' || COALESCE(LEFT(content_text,300),'')`,
    timestampExpr: 'created_at',
  },
  {
    table: 'aurora_raw_siri_interactions',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(domain,'') || '/' || COALESCE(type,'') || ' via ' || COALESCE(bundle_id,'')`,
    timestampExpr: 'start_date',
  },
  {
    table: 'apple_notes',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(title,'') || ' — ' || COALESCE(LEFT(body,300),'')`,
    timestampExpr: 'modified_at',
  },

  // Social
  {
    table: 'aurora_raw_facebook',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(sender,'') || ' (' || COALESCE(data_type,'') || '): ' || COALESCE(LEFT(content,300),'')`,
    timestampExpr: 'timestamp',
  },
  {
    table: 'aurora_raw_instagram',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(sender,'') || ' (' || COALESCE(data_type,'') || '): ' || COALESCE(LEFT(content,300),'')`,
    timestampExpr: 'timestamp',
  },
  {
    table: 'contacts',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(display_name,'') || COALESCE(' — ' || job_title,'') || COALESCE(' — ' || notes,'')`,
    timestampExpr: 'updated_at',
  },

  // Health
  {
    table: 'strava_activities',
    idColumn: 'id',
    idType: 'integer',
    textExpr: `COALESCE(activity_name,'') || ' (' || COALESCE(activity_type,'') || ')' || COALESCE(' — ' || description,'')`,
    timestampExpr: 'activity_date',
  },
  {
    table: 'life_transactions',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(merchant,'') || ': ' || COALESCE(description,'') || COALESCE(' $' || amount,'') || COALESCE(' [' || category || ']','')`,
    timestampExpr: 'transaction_date',
  },
  {
    table: 'looki_moments',
    idColumn: 'id',
    idType: 'text',
    textExpr: `COALESCE(title,'') || ' — ' || COALESCE(transcript,description,summary,'')`,
    timestampExpr: 'start_time',
  },

  // Biographical
  {
    table: 'life_narration',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(LEFT(narrative,500),'')`,
    timestampExpr: 'created_at',
  },
  {
    table: 'aria_journal',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(LEFT(content,500),'')`,
    timestampExpr: 'created_at',
  },
  {
    table: 'proactive_insights',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(title,'') || ' — ' || COALESCE(LEFT(body,300),'')`,
    timestampExpr: 'created_at',
  },
  {
    table: 'photo_metadata',
    idColumn: 'id',
    idType: 'uuid',
    textExpr: `COALESCE(LEFT(description,500),'')`,
    timestampExpr: 'created_at',
  },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** L2-normalize a vector in place. Returns the vector for chaining. */
function l2Normalize(vec: number[]): number[] {
  let norm = 0;
  for (let i = 0; i < vec.length; i++) norm += vec[i] * vec[i];
  norm = Math.sqrt(norm);
  if (norm > 0) {
    for (let i = 0; i < vec.length; i++) vec[i] /= norm;
  }
  return vec;
}

/** Euclidean distance between two vectors. */
function euclideanDist(a: number[], b: number[]): number {
  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    const d = a[i] - b[i];
    sum += d * d;
  }
  return Math.sqrt(sum);
}

/** Self-chain to the next phase. */
async function chainPhase(
  pool: ReturnType<typeof getPool>,
  jobId: string,
  phase: string,
  extraPayload: Record<string, unknown> = {},
): Promise<void> {
  logger.log(`Chaining to phase: ${phase}`);
  await jobLog(jobId, `Chaining to phase: ${phase}`);
  await pool.query(
    `INSERT INTO tempo_jobs (job_type, payload, status, priority, max_attempts)
     VALUES ('topic-model', $1, 'pending', 0, 1)`,
    [JSON.stringify({ phase, ...extraPayload })],
  );
}

// ---------------------------------------------------------------------------
// Phase 1: Extract embeddings to JSONL temp file
// ---------------------------------------------------------------------------

async function phaseExtract(
  job: TempoJob,
  payload: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const pool = getPool();
  const stopTimer = logger.time('phase-extract');
  const perTable: Record<string, number> = {};
  let totalRows = 0;

  logger.log('Phase 1: Extracting embeddings from', TABLES.length, 'tables');
  await jobLog(job.id, `Phase 1: Extracting embeddings from ${TABLES.length} tables`);

  const ws = fs.createWriteStream(VECTOR_FILE);

  for (const tbl of TABLES) {
    const where = `WHERE embedding IS NOT NULL ${tbl.extraWhere ?? ''}`;
    let tableCount = 0;
    let offset = 0;
    const batchSize = 1000;

    logger.logVerbose(`Extracting from ${tbl.table}...`);

    // eslint-disable-next-line no-constant-condition
    while (true) {
      // Cast vector to text in SQL, parse to array in JS — much faster than SQL casts
      const { rows } = await pool.query<{ id: string; vec_text: string }>(
        `SELECT ${tbl.idColumn}::text AS id, embedding::text AS vec_text
         FROM ${tbl.table}
         ${where}
         ORDER BY ${tbl.idColumn}
         LIMIT $1 OFFSET $2`,
        [batchSize, offset],
      );

      for (const row of rows) {
        // Parse "[0.1,0.2,...]" into number array
        const vec = row.vec_text.slice(1, -1).split(',').map(Number);
        ws.write(JSON.stringify({ table: tbl.table, id: row.id, vec }) + '\n');
        tableCount++;
      }

      offset += rows.length;
      if (rows.length < batchSize) break;

      // Backpressure — wait for drain if buffer is full
      if (!ws.write('')) {
        await new Promise<void>((resolve) => ws.once('drain', resolve));
      }
    }

    perTable[tbl.table] = tableCount;
    totalRows += tableCount;
    logger.logVerbose(`  ${tbl.table}: ${tableCount} rows`);
  }

  await new Promise<void>((resolve, reject) => {
    ws.end(() => resolve());
    ws.on('error', reject);
  });

  logger.log(`Phase 1 complete: ${totalRows} vectors extracted to ${VECTOR_FILE}`);
  await jobLog(job.id, `Phase 1 complete: ${totalRows} vectors across ${Object.keys(perTable).length} tables`);

  // Chain to L1 clustering
  await chainPhase(pool, job.id, 'cluster_l1', {
    k_l1: payload.k_l1,
    total_vectors: totalRows,
  });

  stopTimer();
  return { phase: 'extract', total_vectors: totalRows, per_table: perTable };
}

// ---------------------------------------------------------------------------
// Phase 2: Level 1 K-Means Clustering (k=40)
// ---------------------------------------------------------------------------

/** Run the Python clustering script and wait for it to finish. */
async function runPythonCluster(k: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn(PY_BINARY, [PY_SCRIPT, VECTOR_FILE, CLUSTER_OUTPUT_FILE, '--k', String(k)], {
      stdio: ['ignore', 'inherit', 'inherit'],
    });
    proc.on('error', reject);
    proc.on('exit', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`Python clustering exited with code ${code}`));
    });
  });
}

interface PyClusterResult {
  vector_count: number;
  k_l1: number;
  l1_clusters: Array<{
    cluster_index: number;
    centroid: number[];
    member_count: number;
    avg_distance: number;
    source_distribution: Record<string, number>;
  }>;
  l2_clusters: Array<{
    parent_cluster_index: number;
    sub_index: number;
    centroid: number[];
    member_count: number;
    avg_distance: number;
    source_distribution: Record<string, number>;
  }>;
  assignments: Array<{
    source_table: string;
    source_id: string;
    l1_cluster_index: number;
    l2_sub_index: number | null;
    distance: number;
  }>;
  elapsed_seconds: number;
}

async function phaseClusterL1(
  job: TempoJob,
  payload: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const pool = getPool();
  const stopTimer = logger.time('phase-cluster-l1');
  const k = (payload.k_l1 as number) || 40;

  logger.log(`Phase 2: Python sklearn MiniBatchKMeans (k=${k})`);
  await jobLog(job.id, `Phase 2: Python sklearn MiniBatchKMeans (k=${k}) — both L1 and L2 in one pass`);

  // Vector file is host-local. If we're running on a different host than the
  // one that did phase 1 extract (e.g. Secondary-Server vs Primary-Server), the file won't
  // exist. In that case, run extract inline so the job is host-independent.
  let needExtract = false;
  try {
    const stat = fs.statSync(VECTOR_FILE);
    const ageHours = (Date.now() - stat.mtimeMs) / 1000 / 3600;
    if (ageHours > 24) {
      logger.log(`Vector file is ${ageHours.toFixed(1)}h old — re-extracting`);
      needExtract = true;
    } else {
      logger.log(`Reusing existing vector file (${(stat.size / 1024 / 1024).toFixed(0)}MB, ${ageHours.toFixed(1)}h old)`);
    }
  } catch {
    logger.log(`Vector file not found on this host — extracting inline`);
    needExtract = true;
  }
  if (needExtract) {
    await jobLog(job.id, 'Vector file missing/stale — running extract inline');
    await phaseExtract(job, payload);
    // phaseExtract chained a new cluster_l1 job; we'll let THAT one run instead.
    // Cancel our chain by returning early.
    logger.log('Inline extract complete; the chained cluster_l1 job will continue from here');
    stopTimer();
    return { phase: 'cluster_l1', deferred: true, reason: 'inline_extract_chained' };
  }

  // Shell out to Python — it does L1 + L2 in one shot, writes JSON.
  // Was previously ml-kmeans in JS which timed out at 1800s on 590k vectors.
  await runPythonCluster(k);

  const raw = fs.readFileSync(CLUSTER_OUTPUT_FILE, 'utf-8');
  const result = JSON.parse(raw) as PyClusterResult;
  logger.log(`Python returned ${result.l1_clusters.length} L1 + ${result.l2_clusters.length} L2 clusters in ${result.elapsed_seconds}s`);
  await jobLog(job.id, `Python clustering: ${result.l1_clusters.length} L1, ${result.l2_clusters.length} L2, ${result.assignments.length} assignments (${result.elapsed_seconds}s)`);

  // Wipe previous run
  await pool.query('DELETE FROM topic_assignments');
  await pool.query('DELETE FROM topic_clusters');

  // Insert L1 clusters → keep cluster_index → uuid map
  const l1IdMap = new Map<number, string>();
  for (const c of result.l1_clusters) {
    const centroidStr = `[${c.centroid.join(',')}]`;
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO topic_clusters (level, cluster_index, centroid, member_count, avg_distance, source_distribution)
       VALUES (1, $1, $2::vector, $3, $4, $5)
       RETURNING id`,
      [c.cluster_index, centroidStr, c.member_count, c.avg_distance, JSON.stringify(c.source_distribution)],
    );
    l1IdMap.set(c.cluster_index, rows[0].id);
  }
  logger.log(`Inserted ${l1IdMap.size} L1 clusters`);

  // Insert L2 clusters with parent_id link → (parent_index, sub_index) → uuid
  const l2IdMap = new Map<string, string>();
  for (const c of result.l2_clusters) {
    const parentId = l1IdMap.get(c.parent_cluster_index);
    if (!parentId) continue;
    const centroidStr = `[${c.centroid.join(',')}]`;
    const { rows } = await pool.query<{ id: string }>(
      `INSERT INTO topic_clusters (level, cluster_index, parent_id, centroid, member_count, avg_distance, source_distribution)
       VALUES (2, $1, $2, $3::vector, $4, $5, $6)
       RETURNING id`,
      [c.sub_index, parentId, centroidStr, c.member_count, c.avg_distance, JSON.stringify(c.source_distribution)],
    );
    l2IdMap.set(`${c.parent_cluster_index}|${c.sub_index}`, rows[0].id);
  }
  logger.log(`Inserted ${l2IdMap.size} L2 clusters`);
  await jobLog(job.id, `Inserted ${l1IdMap.size} L1 + ${l2IdMap.size} L2 clusters`);

  // Bulk insert assignments — point each to its most-specific cluster (L2 if available, else L1)
  const ASSIGN_BATCH = 5000;
  let assignCount = 0;
  for (let batchStart = 0; batchStart < result.assignments.length; batchStart += ASSIGN_BATCH) {
    const batchEnd = Math.min(batchStart + ASSIGN_BATCH, result.assignments.length);
    const values: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    for (let i = batchStart; i < batchEnd; i++) {
      const a = result.assignments[i];
      let clusterId: string | undefined;
      if (a.l2_sub_index !== null) {
        clusterId = l2IdMap.get(`${a.l1_cluster_index}|${a.l2_sub_index}`);
      }
      if (!clusterId) clusterId = l1IdMap.get(a.l1_cluster_index);
      if (!clusterId) continue;

      values.push(`($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2}, $${paramIdx + 3})`);
      params.push(clusterId, a.source_table, a.source_id, a.distance);
      paramIdx += 4;
    }

    if (values.length > 0) {
      await pool.query(
        `INSERT INTO topic_assignments (cluster_id, source_table, source_id, distance)
         VALUES ${values.join(', ')}
         ON CONFLICT (source_table, source_id) DO UPDATE SET cluster_id = EXCLUDED.cluster_id, distance = EXCLUDED.distance`,
        params,
      );
      assignCount += values.length;
    }
  }

  logger.log(`Inserted ${assignCount} topic assignments`);
  await jobLog(job.id, `Inserted ${assignCount} assignments — chaining to label phase`);

  // Skip the legacy cluster_l2 phase entirely — Python already did L2.
  await chainPhase(pool, job.id, 'label');

  stopTimer();
  return {
    phase: 'cluster_l1',
    k,
    elapsed_seconds: result.elapsed_seconds,
    l1_clusters_created: l1IdMap.size,
    l2_clusters_created: l2IdMap.size,
    assignments: assignCount,
  };
}

// ---------------------------------------------------------------------------
// Phase 3: Level 2 Sub-Clustering
// ---------------------------------------------------------------------------

async function phaseClusterL2(
  job: TempoJob,
  _payload: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  // Legacy phase — Python now does L1 + L2 in a single pass during cluster_l1.
  // Kept as a pass-through for any in-flight jobs that chained to this phase
  // before the refactor. Just hop straight to label.
  const pool = getPool();
  logger.log('Phase 3: legacy L2 phase — skipping (Python handled it in phase 2)');
  await jobLog(job.id, 'Phase 3: skipped (consolidated into phase 2 with Python)');
  await chainPhase(pool, job.id, 'label');
  return { phase: 'cluster_l2', skipped: true, reason: 'consolidated_into_phase_2' };
}

async function phaseClusterL2_LEGACY_UNUSED(
  job: TempoJob,
  _payload: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const pool = getPool();
  const stopTimer = logger.time('phase-cluster-l2');

  logger.log('Phase 3: Level 2 sub-clustering');
  await jobLog(job.id, 'Phase 3: Level 2 sub-clustering of large L1 clusters');

  // Find L1 clusters with > 50 members
  const { rows: l1Clusters } = await pool.query<{ id: string; cluster_index: number; member_count: number }>(
    `SELECT id, cluster_index, member_count FROM topic_clusters
     WHERE level = 1 AND member_count > 50
     ORDER BY cluster_index`,
  );

  logger.log(`Found ${l1Clusters.length} L1 clusters with >50 members`);
  await jobLog(job.id, `${l1Clusters.length} L1 clusters eligible for sub-clustering`);

  // Build a lookup from (table, id) → vector index for efficient retrieval
  // Read all vectors once, build a map
  const vecMap = new Map<string, number[]>();
  const rl = readline.createInterface({
    input: fs.createReadStream(VECTOR_FILE),
    crlfDelay: Infinity,
  });
  for await (const line of rl) {
    if (!line.trim()) continue;
    const parsed = JSON.parse(line) as { table: string; id: string; vec: number[] };
    vecMap.set(`${parsed.table}|${parsed.id}`, l2Normalize(parsed.vec));
  }

  let l2Created = 0;
  let reassigned = 0;

  for (const l1 of l1Clusters) {
    const subK = Math.max(3, Math.min(12, Math.floor(l1.member_count / 200)));

    // Get assignments for this L1 cluster
    const { rows: assignments } = await pool.query<{ source_table: string; source_id: string }>(
      `SELECT source_table, source_id FROM topic_assignments WHERE cluster_id = $1`,
      [l1.id],
    );

    // Collect member vectors
    const memberMeta: Array<{ table: string; id: string }> = [];
    const memberVecs: number[][] = [];
    for (const a of assignments) {
      const vec = vecMap.get(`${a.source_table}|${a.source_id}`);
      if (vec) {
        memberMeta.push({ table: a.source_table, id: a.source_id });
        memberVecs.push(vec);
      }
    }

    if (memberVecs.length < subK * 2) {
      logger.logVerbose(`  L1 cluster ${l1.cluster_index}: only ${memberVecs.length} vectors, skipping sub-cluster`);
      continue;
    }

    logger.logVerbose(`  L1 cluster ${l1.cluster_index}: ${memberVecs.length} members → k=${subK}`);

    // Run k-means on the sub-cluster
    const subResult = kmeans(memberVecs, subK, {
      initialization: 'kmeans++',
      maxIterations: 100,
    });

    // Compute stats per sub-cluster
    const subStats = new Map<number, { count: number; totalDist: number; sources: Map<string, number> }>();
    for (let i = 0; i < subResult.clusters.length; i++) {
      const cIdx = subResult.clusters[i];
      const dist = euclideanDist(memberVecs[i], subResult.centroids[cIdx]);
      const stats = subStats.get(cIdx) ?? { count: 0, totalDist: 0, sources: new Map() };
      stats.count++;
      stats.totalDist += dist;
      stats.sources.set(memberMeta[i].table, (stats.sources.get(memberMeta[i].table) ?? 0) + 1);
      subStats.set(cIdx, stats);
    }

    // Insert L2 clusters
    const subClusterIds = new Map<number, string>();
    for (let i = 0; i < subK; i++) {
      const stats = subStats.get(i);
      if (!stats) continue;

      const centroidArray = `[${subResult.centroids[i].join(',')}]`;
      const sourceDist: Record<string, number> = {};
      for (const [src, cnt] of stats.sources) sourceDist[src] = cnt;

      const { rows } = await pool.query<{ id: string }>(
        `INSERT INTO topic_clusters (level, cluster_index, parent_id, centroid, member_count, avg_distance, source_distribution)
         VALUES (2, $1, $2, $3::vector, $4, $5, $6)
         RETURNING id`,
        [i, l1.id, centroidArray, stats.count, stats.totalDist / stats.count, JSON.stringify(sourceDist)],
      );
      subClusterIds.set(i, rows[0].id);
      l2Created++;
    }

    // Reassign members to L2 clusters
    for (let i = 0; i < subResult.clusters.length; i++) {
      const newClusterId = subClusterIds.get(subResult.clusters[i]);
      if (!newClusterId) continue;
      const dist = euclideanDist(memberVecs[i], subResult.centroids[subResult.clusters[i]]);
      await pool.query(
        `UPDATE topic_assignments SET cluster_id = $1, distance = $2
         WHERE source_table = $3 AND source_id = $4`,
        [newClusterId, dist, memberMeta[i].table, memberMeta[i].id],
      );
      reassigned++;
    }

    logger.logVerbose(`  L1 cluster ${l1.cluster_index}: created ${subClusterIds.size} sub-clusters`);
  }

  logger.log(`Phase 3 complete: ${l2Created} L2 clusters created, ${reassigned} assignments reassigned`);
  await jobLog(job.id, `Phase 3 complete: ${l2Created} L2 clusters, ${reassigned} reassignments`);

  // Chain to labeling
  await chainPhase(pool, job.id, 'label');

  stopTimer();
  return { phase: 'cluster_l2', l2_clusters: l2Created, reassigned };
}

// ---------------------------------------------------------------------------
// Phase 4: LLM Labeling
// ---------------------------------------------------------------------------

/** Build a table-name → TableDef lookup for quick access in labeling. */
const TABLE_LOOKUP = new Map<string, TableDef>();
for (const t of TABLES) TABLE_LOOKUP.set(t.table, t);

async function phaseLabel(
  job: TempoJob,
  _payload: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const pool = getPool();
  const stopTimer = logger.time('phase-label');

  const LLM_URL = process.env.FORGE_PRIORITY_URL || 'http://localhost:8088/v1';

  logger.log('Phase 4: LLM labeling of all clusters');
  await jobLog(job.id, 'Phase 4: LLM labeling clusters via Qwen 3.5');

  // Get all clusters (L1 first, then L2)
  const { rows: clusters } = await pool.query<{
    id: string;
    level: number;
    cluster_index: number;
    parent_id: string | null;
    member_count: number;
  }>(
    `SELECT id, level, cluster_index, parent_id, member_count
     FROM topic_clusters ORDER BY level, cluster_index`,
  );

  logger.log(`Labeling ${clusters.length} clusters`);

  // Skip clusters that already have labels (resume support after timeout/restart)
  const { rows: existingLabels } = await pool.query<{ id: string }>(
    `SELECT id FROM topic_clusters WHERE label IS NOT NULL`,
  );
  const alreadyLabeled = new Set(existingLabels.map(r => r.id));
  const toLabel = clusters.filter(c => !alreadyLabeled.has(c.id));
  logger.log(`${alreadyLabeled.size} already labeled, ${toLabel.length} remaining`);
  await jobLog(job.id, `${alreadyLabeled.size} clusters already labeled, ${toLabel.length} remaining`);

  let labeled = 0;
  let errors = 0;
  const PARALLEL = 8;

  async function labelOne(cluster: typeof clusters[number]): Promise<void> {
    try {
      // For L1 clusters: assignments live on the L2 children (most-specific
      // cluster wins). Sample by walking the parent_id chain.
      const { rows: samples } = await pool.query<{ source_table: string; source_id: string; distance: number }>(
        `SELECT source_table, source_id, distance FROM topic_assignments
         WHERE cluster_id = $1
            OR cluster_id IN (SELECT id FROM topic_clusters WHERE parent_id = $1)
         ORDER BY distance ASC
         LIMIT 15`,
        [cluster.id],
      );

      const sampleTexts: string[] = [];
      for (const s of samples) {
        const tblDef = TABLE_LOOKUP.get(s.source_table);
        if (!tblDef) continue;
        try {
          const { rows: textRows } = await pool.query<{ text: string }>(
            `SELECT ${tblDef.textExpr} AS text FROM ${tblDef.table}
             WHERE ${tblDef.idColumn}::text = $1
             LIMIT 1`,
            [s.source_id],
          );
          if (textRows.length > 0 && textRows[0].text?.trim()) {
            sampleTexts.push(`[${s.source_table}] ${textRows[0].text.slice(0, 200)}`);
          }
        } catch { /* skip */ }
      }

      if (sampleTexts.length === 0) {
        logger.logVerbose(`  L${cluster.level}/${cluster.cluster_index}: no sample texts, skipping label`);
        return;
      }

      // Route via the LLM router classification tier — Gemini Flash Lite is the
      // primary, with cloud-side parallelism so 8-way Promise.all actually
      // parallelizes. The local Qwen priority slot has --parallel 2 and is
      // shared with ARIA, so direct hits there serialize.
      let raw: string;
      try {
        const result = await routeRequest({
          handler: 'topic-model-label',
          taskTier: 'classification',
          systemPrompt: `You are a topic labeler. Given sample texts from a cluster of semantically similar items, provide:
- label: 2-5 word topic name
- description: one sentence description
- keywords: 5 representative keywords
Return JSON only: {"label": "...", "description": "...", "keywords": ["...", ...]}`,
          userMessage: sampleTexts.join('\n\n'),
          maxTokens: 256,
        });
        raw = result.text.trim();
      } catch {
        errors++;
        return;
      }
      const jsonMatch = raw.match(/\{[\s\S]*\}/);
      if (!jsonMatch) { errors++; return; }

      const labelData = JSON.parse(jsonMatch[0]) as {
        label?: string;
        description?: string;
        keywords?: string[];
      };

      const { rows: distRows } = await pool.query<{ source_table: string; cnt: string }>(
        `SELECT source_table, COUNT(*)::text AS cnt FROM topic_assignments
         WHERE cluster_id = $1 GROUP BY source_table`,
        [cluster.id],
      );
      const sourceDist: Record<string, number> = {};
      for (const r of distRows) sourceDist[r.source_table] = parseInt(r.cnt, 10);

      await pool.query(
        `UPDATE topic_clusters
         SET label = $1, description = $2, keywords = $3, source_distribution = $4, updated_at = NOW()
         WHERE id = $5`,
        [
          labelData.label ?? null,
          labelData.description ?? null,
          labelData.keywords ?? null,
          JSON.stringify(sourceDist),
          cluster.id,
        ],
      );

      labeled++;
      logger.logVerbose(`  L${cluster.level}/${cluster.cluster_index}: "${labelData.label}" (${cluster.member_count} members)`);
    } catch (err) {
      errors++;
      logger.logDebug(`Error labeling cluster ${cluster.id}:`, err);
    }
  }

  // Process in waves of PARALLEL
  for (let i = 0; i < toLabel.length; i += PARALLEL) {
    const batch = toLabel.slice(i, i + PARALLEL);
    await Promise.all(batch.map(labelOne));
    if ((i + PARALLEL) % 40 === 0 || i + PARALLEL >= toLabel.length) {
      await jobLog(job.id, `Labeled ${labeled}/${toLabel.length} (errors: ${errors})`);
    }
  }

  // Compute time_range for all clusters that have timestamp data
  logger.logVerbose('Computing time ranges...');
  for (const tbl of TABLES) {
    try {
      await pool.query(
        `UPDATE topic_clusters tc SET time_range = sub.tr
         FROM (
           SELECT ta.cluster_id,
                  tstzrange(MIN(t.ts), MAX(t.ts), '[]') AS tr
           FROM topic_assignments ta
           JOIN LATERAL (
             SELECT ${tbl.timestampExpr}::timestamptz AS ts
             FROM ${tbl.table}
             WHERE ${tbl.idColumn}::text = ta.source_id
           ) t ON true
           WHERE ta.source_table = $1
           GROUP BY ta.cluster_id
         ) sub
         WHERE tc.id = sub.cluster_id
           AND (tc.time_range IS NULL
                OR lower(sub.tr) < lower(tc.time_range)
                OR upper(sub.tr) > upper(tc.time_range))`,
        [tbl.table],
      );
    } catch (err) {
      logger.logDebug(`  Error computing time_range for ${tbl.table}:`, err);
    }
  }

  // Clean up temp file
  try {
    fs.unlinkSync(VECTOR_FILE);
    logger.logVerbose('Cleaned up temp vector file');
  } catch {
    logger.logDebug('Could not delete temp vector file (non-fatal)');
  }

  logger.log(`Phase 4 complete: ${labeled} clusters labeled, ${errors} errors`);
  await jobLog(job.id, `Phase 4 complete: ${labeled} labeled, ${errors} errors. Topic modeling finished.`);

  stopTimer();
  return { phase: 'label', labeled, errors, total_clusters: clusters.length };
}

// ---------------------------------------------------------------------------
// Handler entry point
// ---------------------------------------------------------------------------

export async function handleTopicModel(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = (job.payload ?? {}) as Record<string, unknown>;
  const phase = (payload.phase as string) || 'extract';

  logger.log(`Topic model handler invoked — phase: ${phase}`);
  await jobLog(job.id, `Topic model: starting phase "${phase}"`);

  switch (phase) {
    case 'extract':
      return await phaseExtract(job, payload);
    case 'cluster_l1':
      return await phaseClusterL1(job, payload);
    case 'cluster_l2':
      return await phaseClusterL2(job, payload);
    case 'label':
      return await phaseLabel(job, payload);
    default:
      throw new Error(`Unknown topic-model phase: ${phase}`);
  }
}
