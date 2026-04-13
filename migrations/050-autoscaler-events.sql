-- Autoscaler event log
--
-- The autoscaler on primary nexus-worker (Primary-Server) and the satellite
-- secondary-server-bulk worker (Secondary-Server) manage ephemeral bulk-runner
-- processes. Until now the only visibility was journalctl lines on
-- each host, which makes it hard to answer questions like "how many
-- times did the autoscaler scale up on Secondary-Server yesterday?" or
-- "which workers crashed vs. exited cleanly?".
--
-- This table captures spawn/exit events so the /health dashboard can
-- show a worker-activity timeline and current-alive counts per host.
--
-- Events are lightweight: one row per spawn, one row per exit. The
-- table gets ~50-200 rows/day depending on bulk workload. A retention
-- policy can be added later; for now the table just grows.

CREATE TABLE IF NOT EXISTS autoscaler_events (
  id            BIGSERIAL PRIMARY KEY,
  host          TEXT NOT NULL,                                            -- hostname (e.g. 'primary-server', 'secondary-server')
  event_type    TEXT NOT NULL CHECK (event_type IN ('spawn', 'exit', 'scale_up', 'scale_down', 'scale_to_zero')),
  worker_pid    INTEGER,
  exit_code     INTEGER,                                                  -- set on 'exit' events
  alive_count   INTEGER NOT NULL DEFAULT 0,                               -- count of live workers after this event
  queue_depth   INTEGER,                                                  -- bulk queue depth at the time of event
  detail        TEXT,
  occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_autoscaler_events_occurred
  ON autoscaler_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_autoscaler_events_host_type
  ON autoscaler_events (host, event_type, occurred_at DESC);

INSERT INTO nexus_schema_version (version, description)
VALUES (50, 'Autoscaler event log for observability')
ON CONFLICT DO NOTHING;
