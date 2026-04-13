-- Nexus: Add missing columns to existing Chancery tables
-- agent_registry has 'nickname' but we need 'display_name', plus Nexus-specific columns
-- agent_inbox needs 'trace_id'

BEGIN;

-- ============================================================
-- agent_registry: rename nickname → display_name, add Nexus columns
-- ============================================================

-- Rename nickname to display_name if nickname exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'agent_registry' AND column_name = 'nickname') THEN
        ALTER TABLE agent_registry RENAME COLUMN nickname TO display_name;
    END IF;
END $$;

-- Add missing columns (IF NOT EXISTS prevents errors if they already exist)
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS display_name TEXT;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS schedule_interval_sec INTEGER;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS autonomy_level TEXT DEFAULT 'moderate';
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS personality_mode TEXT DEFAULT 'warm';
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS headline TEXT;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS bio TEXT;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS availability TEXT DEFAULT 'working';
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS executor TEXT DEFAULT 'nexus';
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS api_key_hash TEXT;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS api_key_prefix TEXT;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS api_key_created_at TIMESTAMPTZ;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS last_check_in TIMESTAMPTZ;
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS skills TEXT[] DEFAULT '{}';
ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS avatar_url TEXT;

-- Set existing Chancery agents to executor = 'chancery'
UPDATE agent_registry SET executor = 'chancery' WHERE executor = 'nexus' AND agent_id != 'keeper';

-- ============================================================
-- agent_inbox: add trace_id
-- ============================================================

ALTER TABLE agent_inbox ADD COLUMN IF NOT EXISTS trace_id TEXT;

-- ============================================================
-- agent_tasks: add trace_id if missing
-- ============================================================

ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS trace_id TEXT;

-- ============================================================
-- Version
-- ============================================================

INSERT INTO nexus_schema_version (version, description)
VALUES (3, 'Alter existing Chancery tables: add Nexus columns to agent_registry, trace_id to inbox/tasks')
ON CONFLICT (version) DO NOTHING;

COMMIT;
