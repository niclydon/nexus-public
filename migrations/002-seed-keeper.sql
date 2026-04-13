-- Nexus Phase 1: Seed Keeper agent and initial platform tools

BEGIN;

-- ============================================================
-- Keeper agent
-- ============================================================

INSERT INTO agent_registry (
    agent_id, display_name, role, description,
    system_prompt, schedule_interval_sec,
    autonomy_level, personality_mode, executor, skills
) VALUES (
    'keeper',
    'Keeper',
    'Schema health, housekeeping, monitoring',
    'Platform infrastructure agent responsible for database schema health, routine housekeeping tasks, and system monitoring. Runs every 15 minutes to check system state and report anomalies.',
    NULL, -- system prompt set separately via agent_prompt_history
    900,
    'moderate',
    'warm',
    'nexus',
    ARRAY['schema-health', 'monitoring', 'housekeeping']
) ON CONFLICT (agent_id) DO NOTHING;

-- ============================================================
-- Platform tools for Phase 1
-- ============================================================

INSERT INTO platform_tools (tool_id, display_name, description, category, params_schema, risk_level) VALUES
    ('query_db', 'Query Database', 'Execute read-only SELECT queries against the nexus database. Returns results as rows.', 'infrastructure',
     '{"type": "object", "properties": {"sql": {"type": "string", "description": "SELECT query to execute"}}, "required": ["sql"]}',
     'read_only'),

    ('check_system', 'Check System', 'Check systemd service status, disk space, memory usage, and other system health indicators.', 'infrastructure',
     '{"type": "object", "properties": {"check": {"type": "string", "description": "What to check: service_status, disk_space, memory, uptime"}}, "required": ["check"]}',
     'read_only'),

    ('library_search', 'Search Library', 'Search Library articles (procedures, policies, runbooks) by keyword.', 'knowledge',
     '{"type": "object", "properties": {"query": {"type": "string", "description": "Search query"}}, "required": ["query"]}',
     'read_only'),

    ('inbox_send', 'Send Inbox Message', 'Send a direct message to another agent or to the owner via inbox.', 'communication',
     '{"type": "object", "properties": {"to_agent": {"type": "string"}, "message": {"type": "string"}, "priority": {"type": "integer", "default": 0}}, "required": ["to_agent", "message"]}',
     'state_changing'),

    ('remember', 'Remember', 'Save a memory for future reference. Supports dedup and supersession.', 'state',
     '{"type": "object", "properties": {"content": {"type": "string"}, "memory_type": {"type": "string", "default": "observation"}, "category": {"type": "string"}}, "required": ["content"]}',
     'state_changing'),

    ('search_tools', 'Search Tools', 'Search the platform tool catalog by keyword or category. Use this to discover available tools.', 'meta',
     '{"type": "object", "properties": {"keyword": {"type": "string"}, "category": {"type": "string"}}, "required": []}',
     'read_only'),

    ('request_tool', 'Request Tool Access', 'Request access to a tool you don''t currently have. Read-only tools are auto-approved.', 'meta',
     '{"type": "object", "properties": {"tool_id": {"type": "string"}, "reason": {"type": "string"}}, "required": ["tool_id"]}',
     'read_only')
ON CONFLICT (tool_id) DO NOTHING;

-- ============================================================
-- Tool grants for Keeper
-- ============================================================

INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
    ('keeper', 'query_db', 'system'),
    ('keeper', 'check_system', 'system'),
    ('keeper', 'library_search', 'system'),
    ('keeper', 'inbox_send', 'system'),
    ('keeper', 'remember', 'system'),
    ('keeper', 'search_tools', 'system'),
    ('keeper', 'request_tool', 'system')
ON CONFLICT (agent_id, tool_id) DO NOTHING;

-- ============================================================
-- Version
-- ============================================================

INSERT INTO nexus_schema_version (version, description)
VALUES (2, 'Seed Keeper agent and initial platform tools')
ON CONFLICT (version) DO NOTHING;

COMMIT;
