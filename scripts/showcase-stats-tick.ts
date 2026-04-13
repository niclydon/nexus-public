/**
 * showcase-stats-tick.ts
 *
 * Runs on Primary-Server via systemd timer (every 60s). Reads live counts from the
 * nexus PostgreSQL database and Forge's SQLite request log, then writes a
 * compact JSON file to the showcase site's public directory. The static page
 * polls live.json for fresh stats — no HTTP endpoint, no exposed port.
 *
 * Required env: DATABASE_URL
 * Run: npx tsx scripts/showcase-stats-tick.ts
 */

import 'dotenv/config';
import * as fs from 'fs';
import pg from 'pg';
import Database from 'better-sqlite3';

const OUT = '/opt/nexus/Projects/chancery/web/public/nexus/live.json';
const FORGE_DB = '/opt/nexus/Projects/forge/data/requests.db';
const AGENT_IDS = ['aria', 'pipeline', 'infra', 'inference', 'coder', 'insight', 'circle', 'chronicler'];

async function main() {
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL not set');
  const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 2 });

  // ---- Hero stats ----
  const stats: any = {};
  const tableCount = await pool.query(
    `SELECT count(*)::int AS n FROM information_schema.tables WHERE table_schema = 'public'`,
  );
  stats.tables = tableCount.rows[0].n;

  const facts = await pool.query(`SELECT count(*)::int AS n FROM knowledge_facts WHERE superseded_by IS NULL`);
  stats.knowledge_facts = facts.rows[0].n;

  const photos = await pool.query(`SELECT count(*)::int AS n FROM photo_metadata`);
  stats.photos = photos.rows[0].n;

  // music plays = spotify + lastfm + apple_music_plays
  const music = await pool.query(`
    SELECT
      (SELECT count(*)::int FROM spotify_streaming_history) +
      (SELECT count(*)::int FROM lastfm_scrobbles) +
      (SELECT count(*)::int FROM apple_music_plays) AS n
  `);
  stats.music_plays = music.rows[0].n;

  const decisions24 = await pool.query(
    `SELECT count(*)::int AS n FROM agent_decisions WHERE created_at > now() - interval '24 hours'`,
  );
  stats.decisions_24h = decisions24.rows[0].n;

  const inboxBacklog = await pool.query(`SELECT count(*)::int AS n FROM agent_inbox WHERE read_at IS NULL`);
  stats.inbox_unread = inboxBacklog.rows[0].n;

  const jobsPending = await pool.query(`SELECT count(*)::int AS n FROM tempo_jobs WHERE status = 'pending'`);
  stats.jobs_pending = jobsPending.rows[0].n;

  // ---- Per-agent live state ----
  const agentRows = await pool.query(
    `SELECT agent_id,
            (SELECT max(created_at) FROM agent_decisions WHERE agent_id = ar.agent_id) AS last_cycle,
            (SELECT count(*)::int FROM agent_decisions WHERE agent_id = ar.agent_id AND created_at > now() - interval '24 hours') AS d24
     FROM agent_registry ar
     WHERE ar.agent_id = ANY($1)`,
    [AGENT_IDS],
  );
  const agents: Record<string, any> = {};
  for (const row of agentRows.rows) {
    agents[row.agent_id] = {
      last_cycle_at: row.last_cycle ? row.last_cycle.toISOString() : null,
      decisions_24h: row.d24,
    };
  }

  // ---- Forge perf metrics ----
  const perf: any = {};
  try {
    const fdb = new Database(FORGE_DB, { readonly: true, fileMustExist: true });
    const calls24 = (fdb.prepare(
      `SELECT count(*) AS n FROM requests WHERE timestamp > datetime('now', '-24 hours')`
    ).get() as any).n || 0;
    const tokens24 = (fdb.prepare(
      `SELECT COALESCE(sum(tokens_in),0) + COALESCE(sum(tokens_out),0) AS n
       FROM requests WHERE timestamp > datetime('now', '-24 hours')`
    ).get() as any).n || 0;
    const latency = fdb.prepare(
      `SELECT latency_ms FROM requests
       WHERE timestamp > datetime('now', '-24 hours') AND latency_ms > 0
       ORDER BY latency_ms`
    ).all() as Array<{ latency_ms: number }>;
    let p50 = 0, p95 = 0;
    if (latency.length) {
      p50 = latency[Math.floor(latency.length * 0.5)].latency_ms;
      p95 = latency[Math.floor(latency.length * 0.95)].latency_ms;
    }
    const tokensPerSec = (fdb.prepare(
      `SELECT
         COALESCE(sum(tokens_out), 0) AS toks,
         COALESCE(sum(latency_ms),  0) AS lat
       FROM requests
       WHERE timestamp > datetime('now', '-1 hour')
         AND tokens_out IS NOT NULL AND tokens_out > 0
         AND endpoint LIKE '%/chat/completions%'`,
    ).get() as any);
    const tps = tokensPerSec.lat > 0 ? Math.round((tokensPerSec.toks / (tokensPerSec.lat / 1000))) : 0;

    perf.llm_calls_24h = calls24;
    perf.tokens_24h = tokens24;
    perf.latency_p50_ms = p50;
    perf.latency_p95_ms = p95;
    perf.tokens_per_sec_recent = tps;
    fdb.close();
  } catch (e) {
    perf.error = (e as Error).message;
  }

  // ---- Cloud cost saved (best-effort estimate) ----
  // Assume average GPT-4o pricing: $2.50/M input, $10/M output
  // We don't split in/out cleanly, so use blended $5/M total tokens
  if (perf.tokens_24h) {
    perf.cost_saved_24h_usd = +(perf.tokens_24h / 1_000_000 * 5).toFixed(2);
  }

  const payload = {
    fetched_at: new Date().toISOString(),
    stats,
    agents,
    perf,
  };

  fs.writeFileSync(OUT, JSON.stringify(payload, null, 2));
  console.log(`[showcase-stats] wrote ${OUT}`, JSON.stringify(stats));
  await pool.end();
}

main().catch((e) => {
  console.error('[showcase-stats] failed:', e.message);
  process.exit(1);
});
