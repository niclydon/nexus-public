-- Nexus Phase 1: Foundation tables
-- Run against the existing 'aria' database

BEGIN;

-- ============================================================
-- Agent Registry (migrated from Chancery, extended for Nexus)
-- ============================================================

CREATE TABLE IF NOT EXISTS agent_registry (
    agent_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    description TEXT,
    avatar_url TEXT,
    system_prompt TEXT,
    schedule TEXT,
    schedule_interval_sec INTEGER,
    script TEXT,
    autonomy_level TEXT NOT NULL DEFAULT 'moderate'
        CHECK (autonomy_level IN ('high', 'moderate', 'approval_gated')),
    personality_mode TEXT NOT NULL DEFAULT 'warm'
        CHECK (personality_mode IN ('full', 'warm', 'functional')),
    is_active BOOLEAN DEFAULT true,
    headline TEXT,
    bio TEXT,
    availability TEXT DEFAULT 'working'
        CHECK (availability IN ('working', 'ooo')),
    -- Nexus extensions
    executor TEXT NOT NULL DEFAULT 'nexus'
        CHECK (executor IN ('chancery', 'nexus')),
    api_key_hash TEXT,
    api_key_prefix TEXT,
    api_key_created_at TIMESTAMPTZ,
    last_check_in TIMESTAMPTZ,
    skills TEXT[] DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Agent Communication (inbox only — ARIA 1:1 chat stays on
-- existing conversations/messages tables, activity feed
-- pulls from decisions + inbox for observability)
-- ============================================================

CREATE TABLE IF NOT EXISTS agent_inbox (
    id SERIAL PRIMARY KEY,
    to_agent TEXT NOT NULL,
    from_agent TEXT NOT NULL,
    message TEXT NOT NULL,
    context JSONB,
    priority INT DEFAULT 0,
    trace_id TEXT,
    read_at TIMESTAMPTZ,
    acted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_inbox_unread ON agent_inbox(to_agent, created_at DESC)
    WHERE read_at IS NULL;

-- ============================================================
-- Agent State
-- ============================================================

CREATE TABLE IF NOT EXISTS agent_memory (
    id SERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL,
    memory_type TEXT NOT NULL,
    category TEXT,
    content TEXT NOT NULL,
    confidence FLOAT DEFAULT 1.0,
    times_reinforced INT DEFAULT 1,
    source TEXT,
    superseded_by INT REFERENCES agent_memory(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_accessed_at TIMESTAMPTZ DEFAULT NOW(),
    is_active BOOLEAN DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_agent_memory_active
    ON agent_memory(agent_id, confidence DESC, times_reinforced DESC)
    WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_agent_memory_shared
    ON agent_memory(category, confidence DESC)
    WHERE is_active = true AND category = 'shared';

CREATE TABLE IF NOT EXISTS agent_decisions (
    id SERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    state_snapshot JSONB,
    system_prompt_hash TEXT,
    user_message TEXT,
    llm_response JSONB,
    parsed_actions JSONB,
    execution_results JSONB,
    duration_ms INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_decisions_agent ON agent_decisions(agent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_decisions_trace ON agent_decisions(trace_id);

CREATE TABLE IF NOT EXISTS agent_tasks (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    assigned_to TEXT NOT NULL,
    assigned_by TEXT NOT NULL DEFAULT 'chief-of-staff',
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'in_progress', 'completed', 'blocked', 'cancelled')),
    priority INT DEFAULT 0,
    parent_task_id INTEGER REFERENCES agent_tasks(id),
    source_inbox_id INTEGER REFERENCES agent_inbox(id),
    trace_id TEXT,
    result TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_agent_tasks_assigned ON agent_tasks(assigned_to, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_tasks_open ON agent_tasks(status, created_at DESC)
    WHERE status IN ('open', 'in_progress', 'blocked');

CREATE TABLE IF NOT EXISTS agent_pending_actions (
    id SERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL,
    trace_id TEXT,
    action_type TEXT NOT NULL,
    params JSONB NOT NULL DEFAULT '{}',
    reason TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'rejected', 'expired')),
    decided_by TEXT,
    decided_at TIMESTAMPTZ,
    decision_reason TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pending_actions_status ON agent_pending_actions(status, created_at DESC)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_pending_actions_agent ON agent_pending_actions(agent_id, created_at DESC);

-- ============================================================
-- Tool Catalog
-- ============================================================

CREATE TABLE IF NOT EXISTS platform_tools (
    tool_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL,
    category TEXT NOT NULL,
    params_schema JSONB DEFAULT '{}',
    risk_level TEXT NOT NULL DEFAULT 'read_only'
        CHECK (risk_level IN ('read_only', 'state_changing', 'external')),
    provider_type TEXT NOT NULL DEFAULT 'internal'
        CHECK (provider_type IN ('internal', 'mcp', 'external')),
    provider_config JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agent_tool_grants (
    agent_id TEXT NOT NULL REFERENCES agent_registry(agent_id),
    tool_id TEXT NOT NULL REFERENCES platform_tools(tool_id),
    granted_at TIMESTAMPTZ DEFAULT NOW(),
    granted_by TEXT,
    PRIMARY KEY (agent_id, tool_id)
);

CREATE TABLE IF NOT EXISTS tool_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id TEXT NOT NULL REFERENCES agent_registry(agent_id),
    tool_id TEXT NOT NULL REFERENCES platform_tools(tool_id),
    reason TEXT,
    status TEXT DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'denied')),
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Supporting tables
-- ============================================================

CREATE TABLE IF NOT EXISTS agent_config (
    id SERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value JSONB NOT NULL,
    description TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(agent_id, key)
);

CREATE INDEX IF NOT EXISTS idx_agent_config_agent ON agent_config(agent_id);

CREATE TABLE IF NOT EXISTS agent_config_audit (
    id SERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    field TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    changed_by TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_config_audit_agent ON agent_config_audit(agent_id, created_at DESC);

CREATE TABLE IF NOT EXISTS agent_prompt_history (
    id SERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL,
    system_prompt TEXT NOT NULL,
    changed_by TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_prompt_history_agent ON agent_prompt_history(agent_id, created_at DESC);

CREATE TABLE IF NOT EXISTS agent_llm_log (
    id SERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL,
    user_message_length INT,
    response_preview TEXT,
    model TEXT,
    duration_ms INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_llm_log_agent ON agent_llm_log(agent_id, created_at DESC);

CREATE TABLE IF NOT EXISTS agent_journals (
    id SERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agent_registry(agent_id),
    title TEXT,
    content TEXT NOT NULL,
    mood TEXT,
    topics TEXT[] DEFAULT '{}',
    signals_reviewed JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_journals_agent ON agent_journals(agent_id, created_at DESC);

-- ============================================================
-- Schema version tracking
-- ============================================================

CREATE TABLE IF NOT EXISTS nexus_schema_version (
    version INT PRIMARY KEY,
    applied_at TIMESTAMPTZ DEFAULT NOW(),
    description TEXT
);

INSERT INTO nexus_schema_version (version, description)
VALUES (1, 'Foundation tables: agent registry, inbox, memory, decisions, tasks, tools, config')
ON CONFLICT (version) DO NOTHING;

COMMIT;
