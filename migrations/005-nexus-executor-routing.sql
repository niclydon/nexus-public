-- Nexus Phase 2: Auto-route migrated job types to nexus executor
-- A trigger on INSERT ensures new jobs of migrated types always go to nexus,
-- regardless of which service creates them.

BEGIN;

-- Table of job types owned by nexus
CREATE TABLE IF NOT EXISTS nexus_job_routing (
    job_type TEXT PRIMARY KEY,
    migrated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed with tested handlers
INSERT INTO nexus_job_routing (job_type) VALUES
    ('echo'),
    ('heartbeat'),
    ('health-check'),
    ('speak-homepod'),
    ('appletv-control'),
    ('send-imessage'),
    ('data-source-health'),
    ('execute-scheduled-task'),
    ('log-monitor'),
    ('aurora-escalation')
ON CONFLICT (job_type) DO NOTHING;

-- Trigger function: override executor on insert for migrated types
CREATE OR REPLACE FUNCTION route_nexus_jobs() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM nexus_job_routing WHERE job_type = NEW.job_type) THEN
        NEW.executor := 'nexus';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger
DROP TRIGGER IF EXISTS trg_route_nexus_jobs ON tempo_jobs;
CREATE TRIGGER trg_route_nexus_jobs
    BEFORE INSERT ON tempo_jobs
    FOR EACH ROW
    EXECUTE FUNCTION route_nexus_jobs();

INSERT INTO nexus_schema_version (version, description)
VALUES (5, 'Auto-route migrated job types to nexus via trigger + nexus_job_routing table')
ON CONFLICT (version) DO NOTHING;

COMMIT;
