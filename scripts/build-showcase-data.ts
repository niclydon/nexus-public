/**
 * build-showcase-data.ts
 *
 * Builds /home/user/Projects/chancery/web/public/nexus/data.json containing:
 *   - Per-agent runtime stats (schedule, autonomy, tools granted, decision counts, last cycle)
 *   - Soul package contents (SOUL.md / IDENTITY.md / CHECKLIST.md / AGENTS.md per agent)
 *   - Commit history across every git repo in ~/Projects (monthly counts per repo)
 *
 * Runs the DB queries on Primary-Server via `ssh sudo -u postgres psql` so it works
 * from the laptop without a local DB or tunnel.
 *
 * Usage: cd ~/Projects/nexus && npx tsx scripts/build-showcase-data.ts
 */

import 'dotenv/config';
import * as fs from 'fs';
import * as path from 'path';
import { execSync, spawnSync } from 'child_process';
import Anthropic from '@anthropic-ai/sdk';

const anthropic = process.env.ANTHROPIC_API_KEY ? new Anthropic() : null;
const OPUS_MODEL = 'claude-opus-4-6';

// Path config: env-driven so this script runs on both the laptop (via ssh-psql)
// and on Primary-Server (via local psql) without changes.
const HOMEDIR = process.env.HOME || '/opt/nexus';
const OUT = process.env.SHOWCASE_OUT || `${HOMEDIR}/Projects/chancery/web/public/nexus/data.json`;
const SOULS_DIR = process.env.NEXUS_SOULS_DIR || `${HOMEDIR}/Projects/nexus/souls`;
const PROJECTS_DIR = process.env.PROJECTS_DIR || `${HOMEDIR}/Projects`;
// If running on the Primary-Server host, run psql directly. Otherwise tunnel via ssh primary-server.
import { hostname } from 'os';
const USE_LOCAL_PSQL = process.env.LOCAL_PSQL === '1' || hostname() === 'primary-server';

const AGENT_IDS = ['aria', 'pipeline', 'infra', 'inference', 'coder', 'insight', 'circle', 'chronicler'];

function ssh(sql: string): string {
  // Run via local psql when on Primary-Server, otherwise tunnel via ssh primary-server.
  // The remote shell is bash, so $'\t' (ANSI-C quoting) gives us a literal
  // tab as the field separator. For local, we use spawnSync with an array
  // arg so we can pass the tab character directly without shell quoting.
  let r;
  if (USE_LOCAL_PSQL) {
    r = spawnSync(
      'sudo',
      ['-u', 'postgres', 'psql', 'nexus', '-A', '-t', '-X', '-F', '\t'],
      { input: sql, encoding: 'utf-8', maxBuffer: 50 * 1024 * 1024 },
    );
  } else {
    r = spawnSync(
      'ssh',
      ['primary-server', `sudo -u postgres psql nexus -A -t -X -F $'\\t'`],
      { input: sql, encoding: 'utf-8', maxBuffer: 50 * 1024 * 1024 },
    );
  }
  if (r.status !== 0) throw new Error(`psql failed: ${r.stderr || r.stdout}`);
  return r.stdout.trim();
}

function loadAgentStats(): Record<string, any> {
  const sql = `
    SELECT
      ar.agent_id,
      COALESCE(ar.schedule_interval_sec::text, ''),
      COALESCE(ar.autonomy_level, ''),
      (SELECT count(*) FROM agent_tool_grants WHERE agent_id = ar.agent_id),
      (SELECT count(*) FROM agent_decisions WHERE agent_id = ar.agent_id),
      (SELECT count(*) FROM agent_decisions WHERE agent_id = ar.agent_id AND created_at > now() - interval '24 hours'),
      COALESCE((SELECT max(created_at)::text FROM agent_decisions WHERE agent_id = ar.agent_id), ''),
      COALESCE((SELECT count(*) FROM agent_memory WHERE agent_id = ar.agent_id AND is_active = true), 0)
    FROM agent_registry ar
    WHERE ar.agent_id IN ('${AGENT_IDS.join("','")}')
    ORDER BY ar.agent_id
  `;
  const out = ssh(sql);
  const result: Record<string, any> = {};
  for (const line of out.split('\n')) {
    const cols = line.split('\t');
    if (cols.length < 8) continue;
    const [id, schedule, autonomy, tools, total, last24, lastCycle, memories] = cols;
    result[id] = {
      schedule_sec: parseInt(schedule, 10) || null,
      autonomy: autonomy || null,
      tools_granted: parseInt(tools, 10) || 0,
      decisions_total: parseInt(total, 10) || 0,
      decisions_24h: parseInt(last24, 10) || 0,
      last_cycle_at: lastCycle || null,
      memories_active: parseInt(memories, 10) || 0,
    };
  }
  return result;
}

function loadSouls(): Record<string, Record<string, string>> {
  const result: Record<string, Record<string, string>> = {};
  for (const id of AGENT_IDS) {
    const dir = path.join(SOULS_DIR, id);
    if (!fs.existsSync(dir)) continue;
    const files: Record<string, string> = {};
    for (const f of ['SOUL.md', 'IDENTITY.md', 'CHECKLIST.md', 'AGENTS.md', 'soul.json']) {
      const p = path.join(dir, f);
      if (fs.existsSync(p)) files[f] = fs.readFileSync(p, 'utf-8');
    }
    if (Object.keys(files).length) result[id] = files;
  }
  return result;
}

function loadCommits(): { repos: string[]; months: string[]; data: Record<string, Record<string, number>> } {
  const repoDirs = fs
    .readdirSync(PROJECTS_DIR)
    .filter((r) => {
      const stat = fs.statSync(path.join(PROJECTS_DIR, r));
      return stat.isDirectory() && fs.existsSync(path.join(PROJECTS_DIR, r, '.git'));
    });

  const data: Record<string, Record<string, number>> = {};
  const monthSet = new Set<string>();

  for (const repo of repoDirs) {
    try {
      const out = execSync(
        `git -C ${path.join(PROJECTS_DIR, repo)} log --pretty=format:'%ad' --date=format:'%Y-%m' --all 2>/dev/null`,
        { encoding: 'utf-8', maxBuffer: 50 * 1024 * 1024 },
      ).trim();
      if (!out) continue;
      const counts: Record<string, number> = {};
      for (const m of out.split('\n')) {
        if (!m) continue;
        counts[m] = (counts[m] || 0) + 1;
        monthSet.add(m);
      }
      data[repo] = counts;
    } catch {
      // skip empty/borked repos
    }
  }

  // Filter out repos with < 5 total commits to keep the chart legible
  const reposFiltered = Object.keys(data).filter((r) => {
    return Object.values(data[r]).reduce((a, b) => a + b, 0) >= 5;
  });
  const dataFiltered: Record<string, Record<string, number>> = {};
  for (const r of reposFiltered) dataFiltered[r] = data[r];

  const months = Array.from(monthSet).sort();
  return { repos: reposFiltered.sort(), months, data: dataFiltered };
}

// ---- Decision trace + recent activity (LLM-redacted) ----

function loadCandidateTraces(): any[] {
  // Pull 5 substantive decisions from recent days as candidates
  const sql = `
    SELECT id, agent_id, created_at::text, duration_ms,
      jsonb_array_length(COALESCE(parsed_actions,'[]'::jsonb)) AS act_count,
      LEFT(state_snapshot::text, 4000) AS state_preview,
      LEFT(llm_response::text, 4000) AS llm_preview,
      LEFT(parsed_actions::text, 3000) AS actions_preview,
      LEFT(execution_results::text, 3000) AS results_preview
    FROM agent_decisions
    WHERE created_at > now() - interval '7 days'
      AND parsed_actions IS NOT NULL
      AND jsonb_array_length(parsed_actions) BETWEEN 3 AND 8
      AND state_snapshot IS NOT NULL
    ORDER BY length(llm_response::text) DESC
    LIMIT 5
  `;
  const out = ssh(sql);
  const rows: any[] = [];
  for (const line of out.split('\n')) {
    const cols = line.split('\t');
    if (cols.length < 9) continue;
    rows.push({
      id: cols[0],
      agent_id: cols[1],
      created_at: cols[2],
      duration_ms: parseInt(cols[3], 10) || 0,
      action_count: parseInt(cols[4], 10) || 0,
      state_preview: cols[5],
      llm_preview: cols[6],
      actions_preview: cols[7],
      results_preview: cols[8],
    });
  }
  return rows;
}

function loadRecentActivityRaw(): any[] {
  const sql = `
    SELECT id, agent_id, created_at::text,
      jsonb_array_length(COALESCE(parsed_actions,'[]'::jsonb)) AS act_count,
      LEFT(parsed_actions::text, 1500) AS actions_preview
    FROM agent_decisions
    WHERE created_at > now() - interval '24 hours'
      AND parsed_actions IS NOT NULL
      AND jsonb_array_length(parsed_actions) > 0
    ORDER BY created_at DESC
    LIMIT 30
  `;
  const out = ssh(sql);
  const rows: any[] = [];
  for (const line of out.split('\n')) {
    const cols = line.split('\t');
    if (cols.length < 5) continue;
    rows.push({
      id: cols[0],
      agent_id: cols[1],
      created_at: cols[2],
      action_count: parseInt(cols[3], 10) || 0,
      actions_preview: cols[4],
    });
  }
  return rows;
}

async function redactAndNarrate(candidates: any[], recent: any[]): Promise<{ featured_trace: any; recent_activity: any[] }> {
  if (!anthropic) {
    console.log('    ⚠ ANTHROPIC_API_KEY not set, skipping LLM redaction');
    return { featured_trace: null, recent_activity: [] };
  }

  const prompt = `You are sanitizing real agent decision data from the owner's personal AI platform for a public showcase website. The site is public on the internet — anything you keep needs to be safe to share with strangers.

REDACTION RULES (strict):
- Replace ANY first/last names with "[friend]", "[colleague]", "[family]" — except the owner himself, who stays as "the owner"
- Replace email addresses with "[email]"
- Replace phone numbers with "[phone]"
- Replace specific street addresses or coordinates with "[location]"
- Replace specific company names that appear personal/sensitive with "[company]"
- Keep technical names (Primary-Server, Secondary-Server, Dev-Server, ARIA, Pipeline, etc. — they're just agent/server names)
- Keep model names (Llama, Qwen, Forge), table names, port numbers, infrastructure details
- Keep generic words like "email", "calendar", "photo" — only the contents/parties get redacted
- If the decision is too personal or sensitive to redact safely, mark it as such and skip it

# TASK 1 — Featured Decision Trace

Here are 5 candidate decisions. Pick the ONE that best showcases what an agent actually does in a cycle — preference goes to decisions with a clear narrative arc (saw → reasoned → acted → result), good variety of tool calls, and not-too-personal content. Then write a 5-step walkthrough as a JSON object.

Candidates:
${JSON.stringify(candidates, null, 2).slice(0, 12000)}

Output the featured trace in this exact shape:
{
  "agent_id": "...",
  "created_at": "...",
  "duration_ms": 0,
  "action_count": 0,
  "title": "One short sentence describing what this cycle accomplished",
  "steps": [
    { "label": "State", "body": "What the agent saw when it woke up — sanitized prose, 2-3 sentences" },
    { "label": "Reasoning", "body": "How the agent thought about the situation — paraphrased from llm_response, sanitized, 2-4 sentences" },
    { "label": "Actions", "body": "What tools the agent called and why — list each action briefly" },
    { "label": "Results", "body": "What came back from those actions" },
    { "label": "Outcome", "body": "What actually changed in the world or the agent's state" }
  ]
}

# TASK 2 — Recent Activity Feed

Here are the last 30 decisions across all agents. Sanitize and pick the 12 most interesting ones. For each, write a single one-line summary (max 90 chars) describing what the agent did, in past tense, sanitized.

Recent decisions:
${JSON.stringify(recent, null, 2).slice(0, 8000)}

Output as JSON array:
[
  { "agent_id": "...", "created_at": "...", "summary": "Sanitized one-liner of what happened" },
  ...
]

# RESPONSE FORMAT

Reply with ONE JSON object, no prose, no markdown fences:

{
  "featured_trace": { ... },
  "recent_activity": [ ... ]
}
`;

  console.log('    → calling Opus for sanitization');
  const response = await anthropic.messages.create({
    model: OPUS_MODEL,
    max_tokens: 4096,
    messages: [{ role: 'user', content: prompt }],
  });

  const text = response.content
    .filter((b: any) => b.type === 'text')
    .map((b: any) => b.text)
    .join('\n');

  // Strip any accidental markdown fences
  const cleaned = text.replace(/^```json\s*/i, '').replace(/```\s*$/i, '').trim();

  try {
    const parsed = JSON.parse(cleaned);
    return {
      featured_trace: parsed.featured_trace || null,
      recent_activity: parsed.recent_activity || [],
    };
  } catch (e) {
    console.error('    ⚠ Could not parse Opus response, raw output:');
    console.error(text.slice(0, 500));
    return { featured_trace: null, recent_activity: [] };
  }
}

async function main() {
  console.log('Building showcase data...');

  console.log('  → agent stats');
  const agents = loadAgentStats();
  console.log(`    ${Object.keys(agents).length} agents loaded`);

  console.log('  → soul packages');
  const souls = loadSouls();
  console.log(`    ${Object.keys(souls).length} packages loaded`);

  console.log('  → commit history');
  const commits = loadCommits();
  console.log(`    ${commits.repos.length} repos · ${commits.months.length} months`);

  console.log('  → decision trace candidates + recent activity');
  const candidates = loadCandidateTraces();
  const recent = loadRecentActivityRaw();
  console.log(`    ${candidates.length} candidates · ${recent.length} recent`);

  const { featured_trace, recent_activity } = await redactAndNarrate(candidates, recent);
  if (featured_trace) console.log(`    ✓ featured: ${featured_trace.agent_id}/${featured_trace.steps?.length || 0} steps`);
  console.log(`    ✓ activity feed: ${recent_activity.length} items`);

  const payload = {
    generated_at: new Date().toISOString(),
    agents,
    souls,
    commits,
    featured_trace,
    recent_activity,
  };

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(payload, null, 2));
  const sizeKb = (fs.statSync(OUT).size / 1024).toFixed(1);
  console.log(`\n✓ Wrote ${OUT} (${sizeKb} KB)`);
}

main().catch((e) => {
  console.error('\n✗ Failed:', e);
  process.exit(1);
});
