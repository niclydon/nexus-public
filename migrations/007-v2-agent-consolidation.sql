-- Nexus: v2 agent consolidation — 12 agents → 7
-- See docs/Agent-System-Prompts-v2.md for prompt specs

BEGIN;

-- ============================================================
-- Deactivate agents being absorbed
-- ============================================================

UPDATE agent_registry SET is_active = false, executor = 'chancery'
WHERE agent_id IN (
  'keeper',              -- absorbed into monitor
  'performance-analyst', -- absorbed into monitor
  'cost-accountant',     -- absorbed into monitor
  'security-auditor',    -- absorbed into monitor
  'data-steward',        -- absorbed into collector
  'queue-manager',       -- absorbed into collector
  'sync-orchestrator',   -- absorbed into collector
  'librarian',           -- absorbed into analyst
  'chief-of-staff',      -- absorbed into aria
  'improver'             -- pattern detection → analyst, agent behavior → aria
);

-- ============================================================
-- Register/update the 7 active agents
-- ============================================================

-- 1. Platform Monitor (new — replaces keeper + perf + cost + security)
INSERT INTO agent_registry (agent_id, display_name, role, description, schedule_interval_sec, autonomy_level, personality_mode, executor, skills, is_active)
VALUES ('monitor', 'Platform Monitor', 'Infrastructure, jobs, performance, costs, security',
  'Single source of truth for platform health. DB, job queue, agent cycles, performance metrics, token costs, security posture.',
  900, 'moderate', 'functional', 'nexus',
  ARRAY['platform-health', 'job-queue', 'performance', 'cost-analysis', 'security'], true)
ON CONFLICT (agent_id) DO UPDATE SET
  display_name = EXCLUDED.display_name, role = EXCLUDED.role, description = EXCLUDED.description,
  schedule_interval_sec = EXCLUDED.schedule_interval_sec, executor = 'nexus', is_active = true,
  skills = EXCLUDED.skills, updated_at = NOW();

-- 2. Model Ops (exists — update)
UPDATE agent_registry SET
  display_name = 'Model Ops', role = 'LLM inference services',
  description = 'All inference services available, performant, and stable. Forge gateway, VRAM, latency, error rates.',
  schedule_interval_sec = 900, autonomy_level = 'approval_gated',
  executor = 'nexus', is_active = true,
  skills = ARRAY['model-ops', 'infrastructure', 'forge-health'],
  updated_at = NOW()
WHERE agent_id = 'model-ops';

-- 3. Collector (may exist from migration 006 — upsert)
INSERT INTO agent_registry (agent_id, display_name, role, description, schedule_interval_sec, autonomy_level, personality_mode, executor, skills, is_active)
VALUES ('collector', 'Collector', 'Data ingestion and enrichment',
  'All external data flows reliably into Nexus. 14 data sources, relay health, enrichment backlogs.',
  300, 'moderate', 'functional', 'nexus',
  ARRAY['data-ingestion', 'pipeline-health', 'backfill-orchestration'], true)
ON CONFLICT (agent_id) DO UPDATE SET
  display_name = EXCLUDED.display_name, role = EXCLUDED.role, description = EXCLUDED.description,
  schedule_interval_sec = EXCLUDED.schedule_interval_sec, executor = 'nexus', is_active = true,
  skills = EXCLUDED.skills, updated_at = NOW();

-- 4. Analyst (new — replaces librarian + aurora-narrator + improver)
INSERT INTO agent_registry (agent_id, display_name, role, description, schedule_interval_sec, autonomy_level, personality_mode, executor, skills, is_active)
VALUES ('analyst', 'Analyst', 'Institutional memory, behavioral intelligence',
  'Decisions are recorded. Patterns are surfaced. Aurora behavioral data becomes understanding.',
  1800, 'moderate', 'warm', 'nexus',
  ARRAY['institutional-memory', 'aurora-analysis', 'pattern-detection', 'narrative'], true)
ON CONFLICT (agent_id) DO UPDATE SET
  display_name = EXCLUDED.display_name, role = EXCLUDED.role, description = EXCLUDED.description,
  schedule_interval_sec = EXCLUDED.schedule_interval_sec, executor = 'nexus', is_active = true,
  skills = EXCLUDED.skills, updated_at = NOW();

-- 5. Fixer (exists — update)
UPDATE agent_registry SET
  display_name = 'Fixer', role = 'Code changes (only agent that can)',
  description = 'The ONLY agent authorized to modify code. Approval-gated.',
  schedule_interval_sec = 900, autonomy_level = 'approval_gated',
  executor = 'nexus', is_active = true,
  skills = ARRAY['code-changes', 'git', 'build-verification'],
  updated_at = NOW()
WHERE agent_id = 'fixer';

-- 6. Relationships (may exist from migration 006 — upsert)
INSERT INTO agent_registry (agent_id, display_name, role, description, schedule_interval_sec, autonomy_level, personality_mode, executor, skills, is_active)
VALUES ('relationships', 'Relationships', 'Social intelligence',
  'Relationship health, drift detection, emerging relationships, meeting prep, key dates.',
  21600, 'moderate', 'warm', 'nexus',
  ARRAY['social-intelligence', 'relationship-health', 'meeting-prep'], true)
ON CONFLICT (agent_id) DO UPDATE SET
  display_name = EXCLUDED.display_name, role = EXCLUDED.role, description = EXCLUDED.description,
  schedule_interval_sec = EXCLUDED.schedule_interval_sec, executor = 'nexus', is_active = true,
  skills = EXCLUDED.skills, updated_at = NOW();

-- 7. ARIA (exists — update to absorb COS)
UPDATE agent_registry SET
  display_name = 'ARIA', role = 'Personal assistant + team coordinator',
  description = 'Owner''s personal AI assistant and central coordinator. Escalation routing, task management, briefings, check-ins.',
  schedule_interval_sec = NULL, -- event-driven, not scheduled
  autonomy_level = 'high', executor = 'nexus', is_active = true,
  skills = ARRAY['personal-assistant', 'coordination', 'escalation-routing', 'briefing'],
  updated_at = NOW()
WHERE agent_id = 'aria';

-- ============================================================
-- Grant tools to new agents
-- ============================================================

INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by)
SELECT a.agent_id, t.tool_id, 'system'
FROM (VALUES ('monitor'), ('collector'), ('analyst'), ('relationships')) AS a(agent_id)
CROSS JOIN (VALUES ('query_db'), ('check_system'), ('inbox_send'), ('remember'), ('search_tools'), ('request_tool')) AS t(tool_id)
ON CONFLICT (agent_id, tool_id) DO NOTHING;

-- ARIA and Fixer also get tools
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by)
SELECT a.agent_id, t.tool_id, 'system'
FROM (VALUES ('aria'), ('fixer')) AS a(agent_id)
CROSS JOIN (VALUES ('query_db'), ('check_system'), ('inbox_send'), ('remember'), ('search_tools'), ('request_tool')) AS t(tool_id)
ON CONFLICT (agent_id, tool_id) DO NOTHING;

-- ============================================================
-- Deactivate agents from migration 006 that are no longer needed
-- ============================================================

UPDATE agent_registry SET is_active = false
WHERE agent_id IN ('aurora-narrator') AND is_active = true;

-- ============================================================
-- Version
-- ============================================================

INSERT INTO nexus_schema_version (version, description)
VALUES (7, 'v2 agent consolidation: 12 agents → 7. Monitor absorbs Keeper/Perf/Cost/Security. Analyst absorbs Librarian/Aurora Narrator. ARIA absorbs COS.')
ON CONFLICT (version) DO NOTHING;

COMMIT;
