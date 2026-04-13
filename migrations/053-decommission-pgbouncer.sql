-- Decommission pgbouncer
--
-- pgbouncer has been installed on Primary-Server since the early lab days
-- but nothing actually uses it — DATABASE_URL on every service points
-- at postgresql:5432 directly, not pgbouncer:6432. Discovered during
-- the health watchdog v2 work on 2026-04-10 and confirmed live on
-- 2026-04-11:
--
--   ss -tn | grep :6432       → 0 connections
--   ss -tnlp | grep :6432     → pgbouncer was the only listener
--   grep -rln pgbouncer /opt/nexus/Services/nexus → docs only
--
-- The postgres-connections resource target added in migration 049
-- gives richer visibility (pct of max_connections used) than the
-- pgbouncer TCP check ever did, so removing the target doesn't blind
-- the watchdog on anything real.
--
-- Decommission steps already taken at migration time (2026-04-11):
--   sudo systemctl stop pgbouncer
--   sudo systemctl disable pgbouncer
--
-- This migration just cleans up the health-watchdog registry so the
-- state is captured in code.
--
-- If you ever want to re-enable pool-mode access, flip DATABASE_URL
-- to port 6432 and re-add a target via INSERT INTO
-- platform_health_targets.

DELETE FROM platform_health_targets WHERE service = 'pgbouncer';

INSERT INTO nexus_schema_version (version, description)
VALUES (53, 'Decommission pgbouncer — service stopped/disabled, watchdog target removed')
ON CONFLICT DO NOTHING;
