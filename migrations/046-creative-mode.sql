-- Creative Mode platform flag
-- Singleton table holding the current platform mode. When mode = 'creative',
-- always-on inference services (llama-server, vlm, embed, priority, rpc-server,
-- whisper, tts, etc.) are intentionally shut down to free GPU resources for
-- high-end image/video generation work on Primary-Server + Secondary-Server.
--
-- Read by:
--   - nexus-health-watchdog: skips all checks while in creative mode (avoids
--     spurious Pushover alerts for services we deliberately stopped)
--   - forge-api text endpoints: short-circuit with 503 + X-Creative-Mode: true
--     so the ARIA-tempo LLM router falls back to cloud immediately instead of
--     timing out against dead local backends
--
-- Per CLAUDE.md "Configuration as Data": runtime behavior lives in the DB,
-- not in env vars or config files. Toggle via UPDATE; auditable via the
-- started_at / started_by / reason columns.

CREATE TABLE IF NOT EXISTS platform_modes (
  id          INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),  -- enforce singleton
  mode        TEXT NOT NULL DEFAULT 'normal'
              CHECK (mode IN ('normal', 'creative')),
  started_at  TIMESTAMPTZ,
  started_by  TEXT,
  reason      TEXT,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO platform_modes (id, mode, updated_at)
VALUES (1, 'normal', now())
ON CONFLICT (id) DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (46, 'Creative Mode platform flag (platform_modes singleton)')
ON CONFLICT DO NOTHING;
