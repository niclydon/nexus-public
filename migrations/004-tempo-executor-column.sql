-- Nexus Phase 2: Add executor column to tempo_jobs for dual-worker routing
-- During migration, aria-tempo handles executor='tempo' jobs,
-- nexus-worker handles executor='nexus' jobs.

BEGIN;

ALTER TABLE tempo_jobs ADD COLUMN IF NOT EXISTS executor TEXT DEFAULT 'tempo';

-- Update poll index to include executor filtering
CREATE INDEX IF NOT EXISTS idx_tempo_jobs_nexus_poll
  ON tempo_jobs (priority DESC, created_at)
  WHERE status = 'pending' AND executor = 'nexus';

INSERT INTO nexus_schema_version (version, description)
VALUES (4, 'Add executor column to tempo_jobs for dual-worker routing')
ON CONFLICT (version) DO NOTHING;

COMMIT;
