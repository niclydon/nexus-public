/**
 * Communication Style Analysis Handler
 *
 * Computes per-person communication style features from
 * aurora_unified_communication (message length, emoji/question frequency,
 * timing patterns, initiation ratio, sentiment) and clusters them via
 * k-means into five style labels.
 *
 * Uses a 90-day lookback window. Minimum 20 messages per person.
 * Results stored in person_communication_style.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import type { Pool } from 'pg';

const logger = createLogger('communication-style');

const LOOKBACK_DAYS = 90;
const MIN_MESSAGES = 20;
const K = 5;
const KMEANS_ITERATIONS = 20;

interface StyleFeatures {
  person_id: number;
  contact_name: string;
  avg_message_length: number;
  message_length_stddev: number;
  emoji_frequency: number;
  question_frequency: number;
  primary_contact_hour: number;
  contact_hour_spread: number;
  weekend_ratio: number;
  initiated_ratio: number;
  avg_sentiment_score: number;
  style_cluster: number;
  cluster_label: string;
}

// Feature vector indices for k-means
const FEATURE_KEYS: (keyof StyleFeatures)[] = [
  'avg_message_length',
  'message_length_stddev',
  'emoji_frequency',
  'question_frequency',
  'primary_contact_hour',
  'contact_hour_spread',
  'weekend_ratio',
  'initiated_ratio',
  'avg_sentiment_score',
];

export async function handleCommunicationStyle(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { lookback_days?: number };
  const lookback = payload.lookback_days ?? LOOKBACK_DAYS;
  const since = new Date(Date.now() - lookback * 86400000).toISOString();

  logger.log(`Starting communication style analysis (${lookback}-day window, min ${MIN_MESSAGES} messages)`);
  await jobLog(job.id, `Computing communication style features (${lookback} days)`);

  // Ensure table exists
  await ensureTable(pool);

  // Step 1: Compute per-person style features via SQL
  logger.logVerbose('Querying per-person style features');
  const people = await computeStyleFeatures(pool, since);

  if (people.length === 0) {
    logger.log('No people with sufficient messages in lookback window');
    return { status: 'no_data', lookback_days: lookback };
  }

  logger.log(`Computed style features for ${people.length} people`);
  await jobLog(job.id, `Features computed for ${people.length} people`);

  // Step 2: Join sentiment scores
  logger.logVerbose('Joining sentiment scores');
  await joinSentiment(pool, people, since);

  // Step 3: K-means clustering
  logger.logVerbose(`Running k-means (k=${K}, ${KMEANS_ITERATIONS} iterations)`);
  const clusterSizes = kMeansCluster(people);

  logger.log(`Cluster sizes: ${JSON.stringify(clusterSizes)}`);
  await jobLog(job.id, `Clustered into ${K} groups: ${JSON.stringify(clusterSizes)}`);

  // Step 4: Store results
  logger.logVerbose('Writing results to person_communication_style');
  await storeResults(pool, people);

  const labelCounts: Record<string, number> = {};
  for (const p of people) {
    labelCounts[p.cluster_label] = (labelCounts[p.cluster_label] || 0) + 1;
  }

  logger.log(`Communication style analysis complete: ${people.length} people, labels: ${JSON.stringify(labelCounts)}`);

  return {
    people_analyzed: people.length,
    cluster_labels: labelCounts,
    lookback_days: lookback,
  };
}

// ── Table Creation ──────────────────────────────────────────

async function ensureTable(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS person_communication_style (
      person_id INTEGER PRIMARY KEY,
      contact_name TEXT,
      avg_message_length REAL,
      message_length_stddev REAL,
      emoji_frequency REAL,
      question_frequency REAL,
      primary_contact_hour REAL,
      contact_hour_spread REAL,
      weekend_ratio REAL,
      initiated_ratio REAL,
      avg_sentiment_score REAL,
      style_cluster INTEGER,
      cluster_label TEXT,
      computed_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

// ── Feature Computation ─────────────────────────────────────

async function computeStyleFeatures(pool: Pool, since: string): Promise<StyleFeatures[]> {
  const { rows } = await pool.query<{
    identity_id: number;
    contact_name: string;
    avg_len: string;
    stddev_len: string;
    emoji_freq: string;
    question_freq: string;
    primary_hour: string;
    hour_spread: string;
    weekend_ratio: string;
    initiated_ratio: string;
  }>(`
    SELECT
      identity_id,
      MAX(contact_name) AS contact_name,
      AVG(COALESCE(content_length, 0))::text AS avg_len,
      COALESCE(STDDEV_POP(COALESCE(content_length, 0)), 0)::text AS stddev_len,
      (COUNT(*) FILTER (WHERE content_text ~ '[^\x01-\x7F]')::real
        / GREATEST(COUNT(*), 1))::text AS emoji_freq,
      (COUNT(*) FILTER (WHERE content_text LIKE '%?%')::real
        / GREATEST(COUNT(*), 1))::text AS question_freq,
      (SUM(EXTRACT(HOUR FROM timestamp) * 1.0) / GREATEST(COUNT(*), 1))::text AS primary_hour,
      COALESCE(STDDEV_POP(EXTRACT(HOUR FROM timestamp)), 0)::text AS hour_spread,
      (COUNT(*) FILTER (WHERE EXTRACT(DOW FROM timestamp) IN (0, 6))::real
        / GREATEST(COUNT(*), 1))::text AS weekend_ratio,
      (COUNT(*) FILTER (WHERE direction = 'sent')::real
        / GREATEST(COUNT(*), 1))::text AS initiated_ratio
    FROM aurora_unified_communication
    WHERE identity_id > 0
      AND timestamp >= $1::timestamptz
    GROUP BY identity_id
    HAVING COUNT(*) >= $2
  `, [since, MIN_MESSAGES]);

  return rows.map(row => ({
    person_id: row.identity_id,
    contact_name: row.contact_name || 'Unknown',
    avg_message_length: parseFloat(row.avg_len),
    message_length_stddev: parseFloat(row.stddev_len),
    emoji_frequency: parseFloat(row.emoji_freq),
    question_frequency: parseFloat(row.question_freq),
    primary_contact_hour: parseFloat(row.primary_hour),
    contact_hour_spread: parseFloat(row.hour_spread),
    weekend_ratio: parseFloat(row.weekend_ratio),
    initiated_ratio: parseFloat(row.initiated_ratio),
    avg_sentiment_score: 0,
    style_cluster: 0,
    cluster_label: '',
  }));
}

// ── Sentiment Join ──────────────────────────────────────────

async function joinSentiment(pool: Pool, people: StyleFeatures[], since: string): Promise<void> {
  const personIds = people.map(p => p.person_id);
  if (personIds.length === 0) return;

  const { rows } = await pool.query<{ person_id: number; avg_score: string }>(`
    SELECT
      person_id,
      AVG(sentiment_score)::text AS avg_score
    FROM aurora_sentiment
    WHERE person_id = ANY($1)
      AND analyzed_at >= $2::timestamptz
    GROUP BY person_id
  `, [personIds, since]);

  const sentimentMap = new Map<number, number>();
  for (const row of rows) {
    sentimentMap.set(row.person_id, parseFloat(row.avg_score));
  }

  for (const p of people) {
    p.avg_sentiment_score = sentimentMap.get(p.person_id) ?? 0;
  }

  logger.logVerbose(`Joined sentiment for ${sentimentMap.size}/${people.length} people`);
}

// ── K-Means Clustering ──────────────────────────────────────

function toVector(p: StyleFeatures): number[] {
  return FEATURE_KEYS.map(k => p[k] as number);
}

function kMeansCluster(people: StyleFeatures[]): Record<number, number> {
  const n = people.length;
  const dim = FEATURE_KEYS.length;

  // Extract raw vectors
  const raw = people.map(toVector);

  // Compute min/max for normalization
  const mins = new Array(dim).fill(Infinity);
  const maxs = new Array(dim).fill(-Infinity);
  for (const v of raw) {
    for (let d = 0; d < dim; d++) {
      if (v[d] < mins[d]) mins[d] = v[d];
      if (v[d] > maxs[d]) maxs[d] = v[d];
    }
  }

  // Normalize to 0-1
  const vectors = raw.map(v =>
    v.map((val, d) => {
      const range = maxs[d] - mins[d];
      return range > 0 ? (val - mins[d]) / range : 0;
    }),
  );

  // Initialize centroids via k-means++ seeding
  const centroids = kMeansPlusPlusInit(vectors, K);

  // Assignments
  const assignments = new Array(n).fill(0);

  // Run iterations
  for (let iter = 0; iter < KMEANS_ITERATIONS; iter++) {
    // Assign each point to nearest centroid
    let changed = false;
    for (let i = 0; i < n; i++) {
      let bestCluster = 0;
      let bestDist = Infinity;
      for (let c = 0; c < K; c++) {
        const dist = euclideanDistSq(vectors[i], centroids[c]);
        if (dist < bestDist) {
          bestDist = dist;
          bestCluster = c;
        }
      }
      if (assignments[i] !== bestCluster) {
        assignments[i] = bestCluster;
        changed = true;
      }
    }

    if (!changed) {
      logger.logVerbose(`K-means converged at iteration ${iter + 1}`);
      break;
    }

    // Recompute centroids
    for (let c = 0; c < K; c++) {
      const members = vectors.filter((_, i) => assignments[i] === c);
      if (members.length === 0) continue;
      for (let d = 0; d < dim; d++) {
        centroids[c][d] = members.reduce((sum, v) => sum + v[d], 0) / members.length;
      }
    }
  }

  // Label clusters based on centroid characteristics
  const labels = labelClusters(centroids);

  // Apply results
  const clusterSizes: Record<number, number> = {};
  for (let i = 0; i < n; i++) {
    const c = assignments[i];
    people[i].style_cluster = c;
    people[i].cluster_label = labels[c];
    clusterSizes[c] = (clusterSizes[c] || 0) + 1;
  }

  return clusterSizes;
}

function kMeansPlusPlusInit(vectors: number[][], k: number): number[][] {
  const n = vectors.length;
  const dim = vectors[0].length;
  const centroids: number[][] = [];

  // Pick first centroid randomly
  centroids.push([...vectors[Math.floor(Math.random() * n)]]);

  for (let c = 1; c < k; c++) {
    // Compute distance from each point to nearest existing centroid
    const dists = new Array(n).fill(0);
    let totalDist = 0;
    for (let i = 0; i < n; i++) {
      let minDist = Infinity;
      for (const centroid of centroids) {
        const d = euclideanDistSq(vectors[i], centroid);
        if (d < minDist) minDist = d;
      }
      dists[i] = minDist;
      totalDist += minDist;
    }

    // Weighted random selection
    if (totalDist === 0) {
      centroids.push([...vectors[Math.floor(Math.random() * n)]]);
      continue;
    }
    let r = Math.random() * totalDist;
    for (let i = 0; i < n; i++) {
      r -= dists[i];
      if (r <= 0) {
        centroids.push([...vectors[i]]);
        break;
      }
    }
    if (centroids.length <= c) {
      centroids.push(new Array(dim).fill(0));
    }
  }

  return centroids;
}

function euclideanDistSq(a: number[], b: number[]): number {
  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    const d = a[i] - b[i];
    sum += d * d;
  }
  return sum;
}

// Feature index mapping for label assignment:
// 0=avg_message_length, 1=message_length_stddev, 2=emoji_frequency,
// 3=question_frequency, 4=primary_contact_hour, 5=contact_hour_spread,
// 6=weekend_ratio, 7=initiated_ratio, 8=avg_sentiment_score

function labelClusters(centroids: number[][]): string[] {
  interface ClusterScore {
    index: number;
    label: string;
    score: number;
  }

  const candidates: ClusterScore[][] = centroids.map((c, idx) => {
    const scores: ClusterScore[] = [
      {
        index: idx,
        label: 'formal_professional',
        // High message length + low emoji
        score: c[0] * 2 + (1 - c[2]) * 2 + (1 - c[6]) * 1,
      },
      {
        index: idx,
        label: 'casual_social',
        // High emoji + weekend ratio
        score: c[2] * 2 + c[6] * 2 + c[3] * 1,
      },
      {
        index: idx,
        label: 'proactive_connector',
        // High initiated ratio + broad hours (high spread)
        score: c[7] * 3 + c[5] * 1.5,
      },
      {
        index: idx,
        label: 'appreciative_responder',
        // Low initiated + high sentiment
        score: (1 - c[7]) * 2 + c[8] * 3,
      },
      {
        index: idx,
        label: 'minimal_contact',
        // Low message length + low emoji + low initiated + low sentiment
        score: (1 - c[0]) * 1.5 + (1 - c[2]) * 1 + (1 - c[7]) * 1 + (1 - c[8]) * 1,
      },
    ];
    return scores;
  });

  // Greedy assignment: for each label, find the cluster with the highest score
  // that hasn't been assigned yet. This avoids duplicate labels.
  const labels = new Array<string>(centroids.length).fill('');
  const assignedClusters = new Set<number>();
  const assignedLabels = new Set<string>();

  // Build a flat list of all (cluster, label, score) triples and sort descending
  const allScores: ClusterScore[] = candidates.flat().sort((a, b) => b.score - a.score);

  for (const entry of allScores) {
    if (assignedClusters.has(entry.index) || assignedLabels.has(entry.label)) continue;
    labels[entry.index] = entry.label;
    assignedClusters.add(entry.index);
    assignedLabels.add(entry.label);
    if (assignedClusters.size === centroids.length) break;
  }

  // Fallback: if any cluster still unlabeled (shouldn't happen with k=5 and 5 labels)
  for (let i = 0; i < labels.length; i++) {
    if (!labels[i]) labels[i] = 'uncategorized';
  }

  return labels;
}

// ── Store Results ───────────────────────────────────────────

async function storeResults(pool: Pool, people: StyleFeatures[]): Promise<void> {
  const batch: StyleFeatures[] = [];
  let stored = 0;

  for (const p of people) {
    batch.push(p);
    if (batch.length >= 50) {
      await upsertBatch(pool, batch);
      stored += batch.length;
      batch.length = 0;
    }
  }
  if (batch.length > 0) {
    await upsertBatch(pool, batch);
    stored += batch.length;
  }

  logger.logVerbose(`Stored ${stored} person_communication_style rows`);
}

async function upsertBatch(pool: Pool, batch: StyleFeatures[]): Promise<void> {
  const values: unknown[] = [];
  const placeholders: string[] = [];

  for (let i = 0; i < batch.length; i++) {
    const p = batch[i];
    const offset = i * 13;
    placeholders.push(
      `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, ` +
      `$${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, ` +
      `$${offset + 11}, $${offset + 12}, $${offset + 13})`,
    );
    values.push(
      p.person_id,
      p.contact_name,
      p.avg_message_length,
      p.message_length_stddev,
      p.emoji_frequency,
      p.question_frequency,
      p.primary_contact_hour,
      p.contact_hour_spread,
      p.weekend_ratio,
      p.initiated_ratio,
      p.avg_sentiment_score,
      p.style_cluster,
      p.cluster_label,
    );
  }

  await pool.query(`
    INSERT INTO person_communication_style (
      person_id, contact_name, avg_message_length, message_length_stddev,
      emoji_frequency, question_frequency, primary_contact_hour, contact_hour_spread,
      weekend_ratio, initiated_ratio, avg_sentiment_score, style_cluster, cluster_label
    ) VALUES ${placeholders.join(', ')}
    ON CONFLICT (person_id) DO UPDATE SET
      contact_name = EXCLUDED.contact_name,
      avg_message_length = EXCLUDED.avg_message_length,
      message_length_stddev = EXCLUDED.message_length_stddev,
      emoji_frequency = EXCLUDED.emoji_frequency,
      question_frequency = EXCLUDED.question_frequency,
      primary_contact_hour = EXCLUDED.primary_contact_hour,
      contact_hour_spread = EXCLUDED.contact_hour_spread,
      weekend_ratio = EXCLUDED.weekend_ratio,
      initiated_ratio = EXCLUDED.initiated_ratio,
      avg_sentiment_score = EXCLUDED.avg_sentiment_score,
      style_cluster = EXCLUDED.style_cluster,
      cluster_label = EXCLUDED.cluster_label,
      computed_at = NOW()
  `, values);
}
