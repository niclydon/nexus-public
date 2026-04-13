-- Nexus: Load v1.5 system prompts and register new agents
-- See docs/Agent-System-Prompts-v1.5.md for full prompt specs

BEGIN;

-- ============================================================
-- Platform Preamble (stored separately, injected by cycle runner)
-- ============================================================

INSERT INTO agent_config (agent_id, key, value, description)
VALUES ('_platform', 'preamble', to_jsonb('You are an agent on the Nexus platform — a distributed AI agent ecosystem on Primary-Server
(GPU compute node).

Infrastructure:
- LLM: Forge gateway → Qwen3.5-35B-A3B (text 8080/8088), Qwen3-VL-32B (vision 8081), nomic-embed (8082)
- Database: PostgreSQL localhost:5432, ~185 tables
- Jobs: tempo_jobs, polled every 5s by Worker
- Comms: agent_inbox table (inbox only, no group chat)
- Logging: every cycle → agent_decisions

## Response Format

Return valid JSON matching this schema:
{
  "summary": "one-line status of what you found or did",
  "status": "ok | warning | critical",
  "confidence": 0.0-1.0,
  "actions": [
    {"action": "tool_name", "params": {...}, "reason": "why"}
  ],
  "escalation": null OR {
    "to": "agent-id",
    "priority": "low | medium | high | critical",
    "issue": "what is wrong",
    "evidence": "what you observed",
    "impact": "who/what is affected",
    "recommended_next_step": "what should happen next"
  }
}

## Decision Model

Every cycle, follow this sequence:
1. Detect — identify a change, anomaly, trend, or event
2. Validate — use tools to confirm the signal is real
3. Classify:
   - IGNORE: normal variance, no action needed
   - MONITOR: noteworthy but not actionable yet — save to memory, check next cycle
   - ACT: clear issue requiring action within your authority
   - ESCALATE: outside your scope, or cross-agent impact
4. Execute — take the minimum necessary action, or return zero actions

## Global Rules

Action threshold — act only if:
- impact exceeds baseline variance, OR
- risk is increasing across cycles, OR
- another agent depends on this signal

Throttle:
- Max 3 tool queries per cycle. Do not query without a hypothesis.
- Max 3 actions per cycle. Prefer zero actions over low-value actions.
- Do not repeat the same query within a cycle.

Memory:
- Save: baselines, recurring patterns, threshold changes.
- Never save: one-off events, raw query results.
- Update existing memories when possible rather than creating duplicates.

Verification:
- Never report success without confirming the outcome.
- If an action is queued for approval, say "awaiting approval" — never "done."

Communication:
- Inbox messages must be concise. Recipients are agents, not humans.
- ACT, don''t narrate. Execute tools and report results.
- Default state: no signal → no action → silent cycle with status "ok".'::text), 'v1.5 platform preamble injected into all agent prompts')
ON CONFLICT (agent_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();

-- ============================================================
-- Register new agents
-- ============================================================

INSERT INTO agent_registry (agent_id, display_name, role, description, schedule_interval_sec, autonomy_level, personality_mode, executor, skills) VALUES
  ('collector', 'Collector', 'Data ingestion orchestration', 'Monitors all 14+ data sources, relay health, enrichment pipelines. Enqueues backfill jobs based on backlog depth and Forge availability.', 300, 'moderate', 'warm', 'nexus', ARRAY['data-ingestion', 'pipeline-health', 'backfill-orchestration']),
  ('chief-of-staff', 'Chief of Staff', 'Team coordination', 'Central routing authority for escalations. Task decomposition, priority management, daily briefing synthesis.', 600, 'approval_gated', 'functional', 'nexus', ARRAY['coordination', 'escalation-routing', 'task-management']),
  ('relationships', 'Relationships', 'Social intelligence', 'Relationship health monitoring, drift detection, emerging relationships, meeting prep, key dates.', 21600, 'moderate', 'warm', 'nexus', ARRAY['social-intelligence', 'relationship-health', 'meeting-prep']),
  ('aurora-narrator', 'Aurora Narrator', 'Life pattern interpreter', 'Turns Aurora behavioral analytics into narratives. Anomaly explanation, trend narration, chapter transitions.', 43200, 'moderate', 'warm', 'nexus', ARRAY['aurora-analysis', 'narrative', 'life-patterns']),
  ('improver', 'Improver', 'System quality', 'Identifies recurring patterns that reduce effectiveness. Memory hygiene, prompt effectiveness, communication quality.', 7200, 'moderate', 'warm', 'nexus', ARRAY['quality-improvement', 'pattern-detection', 'memory-hygiene'])
ON CONFLICT (agent_id) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  role = EXCLUDED.role,
  description = EXCLUDED.description,
  schedule_interval_sec = EXCLUDED.schedule_interval_sec,
  autonomy_level = EXCLUDED.autonomy_level,
  executor = EXCLUDED.executor,
  skills = EXCLUDED.skills,
  updated_at = NOW();

-- ============================================================
-- Update existing agents with v1.5 schedules and names
-- ============================================================

UPDATE agent_registry SET
  display_name = 'Vulcan',
  description = 'LLM infrastructure monitoring. Service health, VRAM, latency, error rates.',
  schedule_interval_sec = 900,
  skills = ARRAY['model-ops', 'infrastructure', 'forge-health']
WHERE agent_id = 'model-ops';

UPDATE agent_registry SET
  display_name = 'Sentinel',
  description = 'Platform security. Credential health, network exposure, agent behavior anomalies.',
  schedule_interval_sec = 1800,
  executor = 'nexus',
  skills = ARRAY['security', 'credential-monitoring', 'network-audit']
WHERE agent_id = 'security-auditor';

UPDATE agent_registry SET
  display_name = 'Ledger',
  description = 'Resource efficiency. Token usage, Forge vs cloud split, cycle efficiency.',
  schedule_interval_sec = 21600,
  executor = 'nexus',
  skills = ARRAY['cost-analysis', 'resource-efficiency']
WHERE agent_id = 'cost-accountant';

UPDATE agent_registry SET
  display_name = 'Axiom',
  description = 'System performance. Cycle durations, job throughput, LLM latency, DB growth.',
  schedule_interval_sec = 3600,
  executor = 'nexus',
  skills = ARRAY['performance-analysis', 'degradation-detection']
WHERE agent_id = 'performance-analyst';

UPDATE agent_registry SET
  display_name = 'Chronos',
  description = 'Institutional memory. Decision recording, documentation accuracy, shared memory coverage.',
  schedule_interval_sec = 1800,
  executor = 'nexus',
  skills = ARRAY['documentation', 'memory-management', 'history']
WHERE agent_id = 'librarian';

UPDATE agent_registry SET
  display_name = 'Wright',
  description = 'Code changes. The ONLY agent authorized to modify code. Approval-gated.',
  schedule_interval_sec = 900,
  executor = 'nexus',
  autonomy_level = 'approval_gated',
  skills = ARRAY['code-changes', 'git', 'build-verification']
WHERE agent_id = 'fixer';

-- Deactivate replaced agents (their roles are absorbed by Collector and COS)
UPDATE agent_registry SET is_active = false, executor = 'chancery'
WHERE agent_id IN ('data-steward', 'queue-manager', 'sync-orchestrator');

-- ============================================================
-- Grant tools to all new/updated agents
-- ============================================================

INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by)
SELECT a.agent_id, t.tool_id, 'system'
FROM (VALUES
  ('collector'), ('chief-of-staff'), ('relationships'),
  ('aurora-narrator'), ('improver'),
  ('security-auditor'), ('cost-accountant'), ('performance-analyst'),
  ('librarian'), ('fixer')
) AS a(agent_id)
CROSS JOIN (VALUES
  ('query_db'), ('check_system'), ('inbox_send'), ('remember'),
  ('search_tools'), ('request_tool')
) AS t(tool_id)
ON CONFLICT (agent_id, tool_id) DO NOTHING;

-- ============================================================
-- Version
-- ============================================================

INSERT INTO nexus_schema_version (version, description)
VALUES (6, 'v1.5 agent prompts: register Collector, COS, Relationships, Aurora Narrator, Improver. Update existing agent names/schedules. Deactivate replaced agents.')
ON CONFLICT (version) DO NOTHING;

COMMIT;
