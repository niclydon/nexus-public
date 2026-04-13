/**
 * Social Network Analysis Handler
 *
 * Builds an ego network from aurora_unified_communication and computes
 * per-person metrics (volume, reciprocity, channel diversity, recency)
 * plus network-level metrics (Dunbar layer classification, community
 * detection via label propagation, bridge identification).
 *
 * Uses a 90-day lookback window. Results stored in person_network_metrics.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import type { Pool } from 'pg';

const logger = createLogger('network-analysis');

const LOOKBACK_DAYS = 90;
const TEMPORAL_WINDOW_MIN = 5; // minutes — co-occurrence threshold for edge building

interface PersonMetrics {
  person_id: number;
  contact_name: string;
  total_interactions: number;
  sent_count: number;
  received_count: number;
  reciprocity_score: number;
  channel_count: number;
  channels: string[];
  days_since_last_contact: number;
  weekly_frequency: number;
  dunbar_layer: string;
  community_id: number;
  is_bridge: boolean;
}

interface EdgeWeight {
  a: number;
  b: number;
  weight: number;
}

export async function handleNetworkAnalysis(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { lookback_days?: number };
  const lookback = payload.lookback_days ?? LOOKBACK_DAYS;
  const since = new Date(Date.now() - lookback * 86400000).toISOString();

  logger.log(`Starting social network analysis (${lookback}-day window)`);
  await jobLog(job.id, `Building ego network from aurora_unified_communication (${lookback} days)`);

  // Ensure the results table exists
  await ensureTable(pool);

  // Step 1: Compute per-person metrics
  logger.logVerbose('Computing per-person communication metrics');
  const personMetrics = await computePersonMetrics(pool, since, lookback);

  if (personMetrics.size === 0) {
    logger.log('No communication records with identity_id > 0 in lookback window');
    return { status: 'no_data', lookback_days: lookback };
  }

  logger.log(`Computed metrics for ${personMetrics.size} people`);
  await jobLog(job.id, `Metrics computed for ${personMetrics.size} people`);

  // Step 2: Classify Dunbar layers
  logger.logVerbose('Classifying Dunbar layers');
  classifyDunbarLayers(personMetrics);

  const layerCounts: Record<string, number> = {};
  for (const m of personMetrics.values()) {
    layerCounts[m.dunbar_layer] = (layerCounts[m.dunbar_layer] || 0) + 1;
  }
  logger.log(`Dunbar layers: ${JSON.stringify(layerCounts)}`);

  // Step 3: Build person-to-person edges for community detection
  logger.logVerbose('Building person-to-person edge list for community detection');
  await jobLog(job.id, 'Building social graph edges');
  const edges = await buildEdges(pool, since);
  logger.log(`Built ${edges.length} person-to-person edges`);

  // Step 4: Community detection via label propagation
  logger.logVerbose('Running label propagation for community detection');
  const personIds = Array.from(personMetrics.keys());
  const communities = labelPropagation(personIds, edges);

  for (const [pid, cid] of communities.entries()) {
    const m = personMetrics.get(pid);
    if (m) m.community_id = cid;
  }

  const uniqueCommunities = new Set(communities.values());
  logger.log(`Detected ${uniqueCommunities.size} communities`);

  // Step 5: Bridge detection — people appearing in multiple communities
  logger.logVerbose('Detecting bridge nodes');
  detectBridges(personMetrics, edges, communities);

  const bridgeCount = Array.from(personMetrics.values()).filter(m => m.is_bridge).length;
  logger.log(`Found ${bridgeCount} bridge nodes`);

  // Step 6: Store results
  logger.logVerbose('Writing results to person_network_metrics');
  await jobLog(job.id, `Storing results for ${personMetrics.size} people (${uniqueCommunities.size} communities, ${bridgeCount} bridges)`);
  await storeResults(pool, personMetrics);

  logger.log(`Network analysis complete: ${personMetrics.size} people, ${uniqueCommunities.size} communities, ${bridgeCount} bridges`);

  return {
    people_analyzed: personMetrics.size,
    communities: uniqueCommunities.size,
    bridges: bridgeCount,
    edges: edges.length,
    dunbar_layers: layerCounts,
    lookback_days: lookback,
  };
}

// ── Table Creation ──────────────────────────────────────────

async function ensureTable(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS person_network_metrics (
      person_id INTEGER PRIMARY KEY,
      contact_name TEXT,
      dunbar_layer TEXT,
      total_interactions INTEGER,
      sent_count INTEGER,
      received_count INTEGER,
      reciprocity_score REAL,
      channel_count INTEGER,
      channels TEXT[],
      days_since_last_contact INTEGER,
      community_id INTEGER,
      is_bridge BOOLEAN DEFAULT FALSE,
      weekly_frequency REAL,
      computed_at TIMESTAMPTZ DEFAULT NOW(),
      CONSTRAINT fk_person FOREIGN KEY (person_id) REFERENCES aurora_social_identities(id)
    )
  `);
}

// ── Per-Person Metrics ──────────────────────────────────────

async function computePersonMetrics(
  pool: Pool,
  since: string,
  lookbackDays: number,
): Promise<Map<number, PersonMetrics>> {
  const { rows } = await pool.query<{
    identity_id: number;
    contact_name: string;
    total_interactions: string;
    sent_count: string;
    received_count: string;
    channels: string[];
    channel_count: string;
    last_contact: string;
  }>(`
    SELECT
      identity_id,
      MAX(contact_name) AS contact_name,
      COUNT(*)::text AS total_interactions,
      COUNT(*) FILTER (WHERE direction = 'outgoing')::text AS sent_count,
      COUNT(*) FILTER (WHERE direction = 'incoming')::text AS received_count,
      ARRAY_AGG(DISTINCT channel) AS channels,
      COUNT(DISTINCT channel)::text AS channel_count,
      MAX(timestamp)::text AS last_contact
    FROM aurora_unified_communication
    WHERE identity_id > 0
      AND timestamp >= $1::timestamptz
    GROUP BY identity_id
  `, [since]);

  const now = Date.now();
  const weeks = lookbackDays / 7;
  const metrics = new Map<number, PersonMetrics>();

  for (const row of rows) {
    const sent = parseInt(row.sent_count, 10);
    const received = parseInt(row.received_count, 10);
    const total = parseInt(row.total_interactions, 10);
    const lastContact = new Date(row.last_contact);
    const daysSince = Math.floor((now - lastContact.getTime()) / 86400000);

    // Reciprocity: 1.0 = perfectly balanced, 0 = completely one-sided
    const reciprocity = total > 0
      ? 1 - Math.abs(sent - received) / total
      : 0;

    metrics.set(row.identity_id, {
      person_id: row.identity_id,
      contact_name: row.contact_name || 'Unknown',
      total_interactions: total,
      sent_count: sent,
      received_count: received,
      reciprocity_score: Math.round(reciprocity * 1000) / 1000,
      channel_count: parseInt(row.channel_count, 10),
      channels: row.channels.filter(Boolean),
      days_since_last_contact: daysSince,
      weekly_frequency: Math.round((total / weeks) * 100) / 100,
      dunbar_layer: 'extended', // set in classification step
      community_id: 0,
      is_bridge: false,
    });
  }

  return metrics;
}

// ── Dunbar Layer Classification ─────────────────────────────

function classifyDunbarLayers(metrics: Map<number, PersonMetrics>): void {
  for (const m of metrics.values()) {
    const wf = m.weekly_frequency;
    if (wf >= 7) {
      m.dunbar_layer = 'core_5';       // daily+
    } else if (wf >= 3) {
      m.dunbar_layer = 'inner_15';     // multiple times per week
    } else if (wf >= 1) {
      m.dunbar_layer = 'affinity_50';  // weekly
    } else if (wf >= 0.25) {
      m.dunbar_layer = 'active_150';   // monthly (roughly 1/week * 0.25 = 1/month)
    } else {
      m.dunbar_layer = 'extended';     // less than monthly
    }
  }
}

// ── Edge Building ───────────────────────────────────────────

async function buildEdges(pool: Pool, since: string): Promise<EdgeWeight[]> {
  // Two sources of edges:
  // 1. Group chat co-occurrence (same chat_id in aurora_raw_imessage)
  // 2. Temporal proximity (messages to/from different people within TEMPORAL_WINDOW_MIN)

  const edgeMap = new Map<string, number>();

  const addEdge = (a: number, b: number, weight: number) => {
    if (a === b) return;
    const key = a < b ? `${a}:${b}` : `${b}:${a}`;
    edgeMap.set(key, (edgeMap.get(key) || 0) + weight);
  };

  // Source 1: Group chat co-occurrence via aurora_raw_imessage
  // People who appear in the same chat_id are socially connected
  logger.logVerbose('Building edges from group chat co-occurrence');
  const { rows: groupEdges } = await pool.query<{ a: number; b: number; co_count: string }>(`
    WITH chat_participants AS (
      SELECT DISTINCT m.chat_id, l.identity_id
      FROM aurora_raw_imessage m
      JOIN aurora_social_identity_links l ON l.identifier = m.handle_id
      WHERE m.date >= $1::timestamptz
        AND m.chat_id IS NOT NULL
        AND l.identity_id > 0
    )
    SELECT
      a.identity_id AS a,
      b.identity_id AS b,
      COUNT(DISTINCT a.chat_id)::text AS co_count
    FROM chat_participants a
    JOIN chat_participants b ON a.chat_id = b.chat_id AND a.identity_id < b.identity_id
    GROUP BY a.identity_id, b.identity_id
  `, [since]);

  for (const row of groupEdges) {
    addEdge(row.a, row.b, parseInt(row.co_count, 10) * 3); // weight group co-occurrence heavily
  }
  logger.logVerbose(`Group chat edges: ${groupEdges.length}`);

  // Source 2: Temporal proximity — messages to different people within 5 min
  // This captures "thinking about the same social context" even across channels
  logger.logVerbose('Building edges from temporal co-occurrence');
  const { rows: temporalEdges } = await pool.query<{ a: number; b: number; co_count: string }>(`
    WITH timed AS (
      SELECT identity_id, timestamp
      FROM aurora_unified_communication
      WHERE identity_id > 0
        AND timestamp >= $1::timestamptz
        AND direction = 'outgoing'
    )
    SELECT
      a.identity_id AS a,
      b.identity_id AS b,
      COUNT(*)::text AS co_count
    FROM timed a
    JOIN timed b ON a.identity_id < b.identity_id
      AND ABS(EXTRACT(EPOCH FROM a.timestamp - b.timestamp)) <= $2 * 60
    GROUP BY a.identity_id, b.identity_id
    HAVING COUNT(*) >= 3
  `, [since, TEMPORAL_WINDOW_MIN]);

  for (const row of temporalEdges) {
    addEdge(row.a, row.b, parseInt(row.co_count, 10));
  }
  logger.logVerbose(`Temporal proximity edges: ${temporalEdges.length}`);

  // Convert map to edge list
  const edges: EdgeWeight[] = [];
  for (const [key, weight] of edgeMap) {
    const [a, b] = key.split(':').map(Number);
    edges.push({ a, b, weight });
  }

  return edges;
}

// ── Label Propagation Community Detection ───────────────────

function labelPropagation(
  personIds: number[],
  edges: EdgeWeight[],
  maxIterations = 20,
): Map<number, number> {
  // Initialize: each node is its own community
  const labels = new Map<number, number>();
  for (const pid of personIds) {
    labels.set(pid, pid);
  }

  // Build adjacency list with weights
  const adj = new Map<number, Map<number, number>>();
  for (const pid of personIds) {
    adj.set(pid, new Map());
  }
  for (const { a, b, weight } of edges) {
    if (adj.has(a) && adj.has(b)) {
      adj.get(a)!.set(b, weight);
      adj.get(b)!.set(a, weight);
    }
  }

  // Iterate: each node adopts the most common label among its neighbors (weighted)
  for (let iter = 0; iter < maxIterations; iter++) {
    let changed = false;
    // Shuffle order to avoid deterministic bias
    const shuffled = [...personIds].sort(() => Math.random() - 0.5);

    for (const pid of shuffled) {
      const neighbors = adj.get(pid);
      if (!neighbors || neighbors.size === 0) continue;

      // Tally weighted votes for each label
      const votes = new Map<number, number>();
      for (const [neighbor, weight] of neighbors) {
        const nlabel = labels.get(neighbor)!;
        votes.set(nlabel, (votes.get(nlabel) || 0) + weight);
      }

      // Pick label with highest weighted vote
      let bestLabel = labels.get(pid)!;
      let bestWeight = 0;
      for (const [label, w] of votes) {
        if (w > bestWeight) {
          bestWeight = w;
          bestLabel = label;
        }
      }

      if (bestLabel !== labels.get(pid)) {
        labels.set(pid, bestLabel);
        changed = true;
      }
    }

    if (!changed) break;
  }

  // Renumber communities to sequential IDs starting at 1
  const labelToId = new Map<number, number>();
  let nextId = 1;
  const result = new Map<number, number>();
  for (const pid of personIds) {
    const raw = labels.get(pid)!;
    if (!labelToId.has(raw)) {
      labelToId.set(raw, nextId++);
    }
    result.set(pid, labelToId.get(raw)!);
  }

  return result;
}

// ── Bridge Detection ────────────────────────────────────────

function detectBridges(
  metrics: Map<number, PersonMetrics>,
  edges: EdgeWeight[],
  communities: Map<number, number>,
): void {
  // A bridge connects people from different communities.
  // For each person, count how many distinct communities their neighbors belong to.
  // If > 1, they're a bridge.

  const neighborCommunities = new Map<number, Set<number>>();

  for (const { a, b } of edges) {
    const cA = communities.get(a);
    const cB = communities.get(b);
    if (cA === undefined || cB === undefined) continue;

    if (!neighborCommunities.has(a)) neighborCommunities.set(a, new Set());
    if (!neighborCommunities.has(b)) neighborCommunities.set(b, new Set());

    neighborCommunities.get(a)!.add(cB);
    neighborCommunities.get(b)!.add(cA);
  }

  for (const [pid, comms] of neighborCommunities) {
    const m = metrics.get(pid);
    if (m && comms.size > 1) {
      m.is_bridge = true;
    }
  }
}

// ── Store Results ───────────────────────────────────────────

async function storeResults(pool: Pool, metrics: Map<number, PersonMetrics>): Promise<void> {
  // Batch upsert in groups of 50
  const batch: PersonMetrics[] = [];
  let stored = 0;

  for (const m of metrics.values()) {
    batch.push(m);
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

  logger.logVerbose(`Stored ${stored} person_network_metrics rows`);
}

async function upsertBatch(pool: Pool, batch: PersonMetrics[]): Promise<void> {
  const values: unknown[] = [];
  const placeholders: string[] = [];

  for (let i = 0; i < batch.length; i++) {
    const m = batch[i];
    const offset = i * 12;
    placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12})`);
    values.push(
      m.person_id,
      m.contact_name,
      m.dunbar_layer,
      m.total_interactions,
      m.sent_count,
      m.received_count,
      m.reciprocity_score,
      m.channel_count,
      m.channels,
      m.days_since_last_contact,
      m.community_id,
      m.is_bridge,
    );
  }

  await pool.query(`
    INSERT INTO person_network_metrics (
      person_id, contact_name, dunbar_layer, total_interactions,
      sent_count, received_count, reciprocity_score, channel_count,
      channels, days_since_last_contact, community_id, is_bridge
    ) VALUES ${placeholders.join(', ')}
    ON CONFLICT (person_id) DO UPDATE SET
      contact_name = EXCLUDED.contact_name,
      dunbar_layer = EXCLUDED.dunbar_layer,
      total_interactions = EXCLUDED.total_interactions,
      sent_count = EXCLUDED.sent_count,
      received_count = EXCLUDED.received_count,
      reciprocity_score = EXCLUDED.reciprocity_score,
      channel_count = EXCLUDED.channel_count,
      channels = EXCLUDED.channels,
      days_since_last_contact = EXCLUDED.days_since_last_contact,
      community_id = EXCLUDED.community_id,
      is_bridge = EXCLUDED.is_bridge,
      computed_at = NOW()
  `, values);
}
