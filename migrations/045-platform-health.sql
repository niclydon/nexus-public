-- Platform Health Alerting
-- Tracks per-service health checks with state transitions and alert delivery.
-- Owned by the standalone nexus-health-watchdog systemd timer (separate from
-- the database-internal log-monitor job in worker; this one watches the
-- INFRASTRUCTURE that everything else depends on).

CREATE TABLE IF NOT EXISTS platform_health_checks (
  id            BIGSERIAL PRIMARY KEY,
  service       TEXT NOT NULL,
  check_type    TEXT NOT NULL,           -- 'http' | 'systemd' | 'tcp' | 'psql'
  status        TEXT NOT NULL,           -- 'ok' | 'down' | 'degraded' | 'unknown'
  latency_ms    INTEGER,
  detail        TEXT,                    -- error message or status code etc
  checked_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_phc_service_time ON platform_health_checks (service, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_phc_status_time ON platform_health_checks (status, checked_at DESC) WHERE status != 'ok';

-- One row per service tracking the current state — used for state-transition detection.
-- The watchdog UPSERTs this on every tick.
CREATE TABLE IF NOT EXISTS platform_health_state (
  service             TEXT PRIMARY KEY,
  current_status      TEXT NOT NULL,
  status_since        TIMESTAMPTZ NOT NULL,
  last_checked_at     TIMESTAMPTZ NOT NULL,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  last_alert_sent_at  TIMESTAMPTZ,
  last_alert_status   TEXT,
  detail              TEXT,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Audit trail of every alert that fired. Used for dedup, reporting, and digest views.
CREATE TABLE IF NOT EXISTS platform_health_alerts (
  id            BIGSERIAL PRIMARY KEY,
  service       TEXT NOT NULL,
  severity      TEXT NOT NULL,           -- 'critical' | 'warning' | 'recovery' | 'info'
  title         TEXT NOT NULL,
  body          TEXT,
  fingerprint   TEXT NOT NULL,           -- service|status — for dedup within suppression window
  delivered_to  TEXT[],                  -- e.g. {'pushover','email'}
  delivery_ok   BOOLEAN NOT NULL DEFAULT false,
  delivery_error TEXT,
  resolved_at   TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_pha_service_time ON platform_health_alerts (service, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pha_unresolved ON platform_health_alerts (service, severity) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_pha_fingerprint ON platform_health_alerts (fingerprint, created_at DESC);

-- Optional: a registry of services to check, so the watchdog config lives in the DB
-- rather than hardcoded. Per CLAUDE.md "Configuration as Data" principle.
CREATE TABLE IF NOT EXISTS platform_health_targets (
  service             TEXT PRIMARY KEY,
  display_name        TEXT NOT NULL,
  check_type          TEXT NOT NULL,     -- 'http' | 'systemd' | 'tcp' | 'psql'
  endpoint            TEXT,              -- url, unit name, host:port, or db name
  expected            TEXT,              -- e.g. '200' for http, 'active' for systemd
  timeout_ms          INTEGER NOT NULL DEFAULT 5000,
  severity_on_down    TEXT NOT NULL DEFAULT 'critical', -- 'critical' | 'warning'
  alert_after_failures INTEGER NOT NULL DEFAULT 1, -- alert after N consecutive failures
  mute_until          TIMESTAMPTZ,       -- temporary mute (set during planned maintenance)
  enabled             BOOLEAN NOT NULL DEFAULT true,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed the initial set of services to monitor
INSERT INTO platform_health_targets (service, display_name, check_type, endpoint, expected, severity_on_down, alert_after_failures) VALUES
  ('nexus-api',         'Nexus API (REST)',                  'http',    'http://localhost:7700/health',           '200', 'critical', 1),
  ('nexus-mcp',         'Nexus MCP Server',                  'http',    'http://localhost:7701/health',           '200', 'critical', 1),
  ('nexus-worker',      'Nexus Worker (agents + jobs)',      'systemd', 'nexus-worker.service',                   'active', 'critical', 1),
  ('forge-api',         'Forge LLM Gateway',                 'http',    'http://localhost:8642/health',           '200', 'critical', 1),
  ('llama-server',      'Llama 3.3 70B',                     'http',    'http://localhost:8080/health',           '200', 'warning', 2),
  ('llama-priority',    'Qwen 3.5 35B (priority slots)',     'http',    'http://localhost:8088/health',           '200', 'critical', 2),
  ('llama-vlm',         'Qwen3-VL-32B (vision)',             'http',    'http://localhost:8081/health',           '200', 'warning', 2),
  ('llama-embed',       'nomic-embed-text-v1.5',             'http',    'http://localhost:8082/health',           '200', 'critical', 2),
  ('postgresql',        'PostgreSQL 16',                     'psql',    'nexus',                                  'ok', 'critical', 1),
  ('pgbouncer',         'pgbouncer (transaction pool)',      'tcp',     'localhost:6432',                         'open', 'critical', 1),
  ('chancery-web',      'Chancery Dashboard',                'http',    'http://localhost:3100',                  '200', 'warning', 2),
  ('nexus-site',        'Nexus Showcase Site',               'http',    'http://localhost:8901',                  '200', 'warning', 2)
ON CONFLICT (service) DO NOTHING;

INSERT INTO nexus_schema_version (version, description) VALUES (45, 'Platform health alerting tables + service registry') ON CONFLICT DO NOTHING;
