-- Migration 057 — looki_moments-sync job routing

BEGIN;

INSERT INTO nexus_job_routing (job_type, executor, handler_name, default_priority, default_max_attempts)
VALUES ('looki_moments-sync', 'nexus', 'handleLookiMomentsSync', 5, 3)
ON CONFLICT (job_type) DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (57, 'Route looki_moments-sync handler')
ON CONFLICT DO NOTHING;

COMMIT;
