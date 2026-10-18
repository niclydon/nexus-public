-- 058-agent-bus.sql
-- Agent coordination bus (public delta, October 2026).
--
-- Backing schema for packages/mcp/src/tools/agent-bus.ts: a typed message
-- stream (status / intent / done / ask / flag) with per-caller high-water
-- marks. Posts and high-water-mark writes are approval-gated in the tool
-- layer and run under a per-caller Postgres role (SET ROLE), so the
-- database itself records which seat class acted.
--
-- This is the stream piece of the coordination layer described in
-- docs/what-changed-since-2026-08.md, published as a representative sample.
-- The private system carries additional registry and conditions tables
-- that are not part of this snapshot.

CREATE SCHEMA IF NOT EXISTS agent_bus;

CREATE TABLE IF NOT EXISTS agent_bus.messages (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  author      TEXT NOT NULL,
  recipient   TEXT,
  msg_type    TEXT NOT NULL CHECK (msg_type IN ('status', 'intent', 'done', 'ask', 'flag')),
  task        TEXT,
  body        TEXT NOT NULL,
  in_reply_to BIGINT REFERENCES agent_bus.messages(id)
);

CREATE INDEX IF NOT EXISTS idx_agent_bus_messages_id ON agent_bus.messages (id);

CREATE TABLE IF NOT EXISTS agent_bus.high_water_marks (
  author       TEXT PRIMARY KEY,
  last_seen_id BIGINT NOT NULL DEFAULT 0,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per-caller roles used by the tool layer's SET ROLE. NOLOGIN: they are
-- assumed via SET ROLE by the connecting application role, never used to
-- log in directly. Names match BUS_CALLERS in the tool module.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coordinator_bus') THEN
    CREATE ROLE coordinator_bus NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'operator_bus') THEN
    CREATE ROLE operator_bus NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worker_bus') THEN
    CREATE ROLE worker_bus NOLOGIN;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA agent_bus TO coordinator_bus, operator_bus, worker_bus;
GRANT SELECT, INSERT ON agent_bus.messages TO coordinator_bus, operator_bus, worker_bus;
GRANT SELECT, INSERT, UPDATE ON agent_bus.high_water_marks TO coordinator_bus, operator_bus, worker_bus;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA agent_bus TO coordinator_bus, operator_bus, worker_bus;
