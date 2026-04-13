-- Health Watchdog v2 — add system-resource targets
--
-- Migration 045 set up the platform_health_* tables and the watchdog
-- script with http/systemd/tcp/psql check types. This migration adds
-- new targets that use a new `command` check type (added in
-- scripts/health-watchdog.ts at the same time as this migration) to
-- catch resource-exhaustion failures the earlier targets can't see.
--
-- Thresholds are deliberately wide to avoid alert fatigue — the
-- watchdog fires at `max:N` when value > N, so e.g. disk max:90 fires
-- at 91 % full. Adjust via UPDATE platform_health_targets if noisy.
--
-- Targets added:
--   disk-primary-server           — df / root filesystem on Primary-Server
--   disk-secondary-server          — same on Secondary-Server via ssh
--   memory-primary-server         — free memory% on Primary-Server
--   memory-secondary-server        — same on Secondary-Server via ssh
--   postgres-connections   — pg_stat_activity count vs max_connections
--
-- NOT added (but considered):
--   * pgbouncer pool-saturation — nothing actually routes through
--     pgbouncer right now (services hit postgresql:5432 directly per
--     DATABASE_URL). The existing pgbouncer TCP target still verifies
--     the listener is up, so no new target needed until services are
--     migrated to pool through pgbouncer.

INSERT INTO platform_health_targets (
  service, display_name, check_type, endpoint, expected,
  timeout_ms, severity_on_down, alert_after_failures
) VALUES
  (
    'disk-primary-server',
    'Disk usage (Primary-Server /)',
    'command',
    $cmd$df / | awk 'NR==2 {gsub("%","",$5); print $5}'$cmd$,
    'max:90',
    5000, 'critical', 2
  ),
  (
    'disk-secondary-server',
    'Disk usage (Secondary-Server /)',
    'command',
    $cmd$ssh -o BatchMode=yes -o ConnectTimeout=5 secondary-server "df / | awk 'NR==2 {gsub(\"%\",\"\",\$5); print \$5}'"$cmd$,
    'max:90',
    10000, 'critical', 2
  ),
  (
    'memory-primary-server',
    'Memory usage (Primary-Server)',
    'command',
    $cmd$free | awk 'NR==2 {printf "%.0f\n", ($3/$2)*100}'$cmd$,
    'max:92',
    5000, 'warning', 3
  ),
  (
    'memory-secondary-server',
    'Memory usage (Secondary-Server)',
    'command',
    $cmd$ssh -o BatchMode=yes -o ConnectTimeout=5 secondary-server "free | awk 'NR==2 {printf \"%.0f\\n\", (\$3/\$2)*100}'"$cmd$,
    'max:92',
    10000, 'warning', 3
  ),
  (
    'postgres-connections',
    'Postgres active connections',
    'command',
    $cmd$sudo -n -u postgres psql -d nexus -tAc "SELECT ROUND(100.0 * (SELECT COUNT(*) FROM pg_stat_activity) / current_setting('max_connections')::int)"$cmd$,
    'max:80',
    5000, 'warning', 2
  )
ON CONFLICT (service) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  check_type = EXCLUDED.check_type,
  endpoint = EXCLUDED.endpoint,
  expected = EXCLUDED.expected,
  timeout_ms = EXCLUDED.timeout_ms,
  severity_on_down = EXCLUDED.severity_on_down,
  alert_after_failures = EXCLUDED.alert_after_failures,
  updated_at = now();

INSERT INTO nexus_schema_version (version, description)
VALUES (49, 'Health watchdog v2 — disk/memory/pg-connections targets + command check type')
ON CONFLICT DO NOTHING;
