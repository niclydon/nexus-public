-- Platform Mode History — audit trail of every creative-mode entry/exit.
--
-- Migration 046 created platform_modes as a singleton holding the CURRENT mode.
-- That single row is overwritten on every flip, so there is no record of when
-- creative mode started/stopped, who flipped it, or why. This migration adds a
-- history table plus a trigger that logs every transition.
--
-- Each creative-mode session gets two rows eventually:
--   (a) row inserted on the 'normal' -> 'creative' transition with started_at set
--   (b) the same row updated on the 'creative' -> 'normal' transition with ended_at set
--
-- Normal -> normal transitions are no-ops (no row inserted). Only mode changes
-- produce history entries.

CREATE TABLE IF NOT EXISTS platform_mode_history (
  id          BIGSERIAL PRIMARY KEY,
  mode        TEXT NOT NULL CHECK (mode IN ('normal', 'creative')),
  started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at    TIMESTAMPTZ,
  started_by  TEXT,
  reason      TEXT,
  duration_sec INTEGER GENERATED ALWAYS AS (
    CASE WHEN ended_at IS NULL THEN NULL
         ELSE EXTRACT(EPOCH FROM (ended_at - started_at))::INTEGER
    END
  ) STORED
);

CREATE INDEX IF NOT EXISTS idx_pmh_started_at ON platform_mode_history (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pmh_mode_started ON platform_mode_history (mode, started_at DESC);
-- Partial index: only the currently-open session (at most one). Cheap lookups
-- for "how long have we been in creative mode?" via getPlatformMode().
CREATE UNIQUE INDEX IF NOT EXISTS idx_pmh_open_session
  ON platform_mode_history ((1)) WHERE ended_at IS NULL;

-- Trigger: on every platform_modes UPDATE where the mode actually changes,
-- close any open history row and open a new one for the new mode.
CREATE OR REPLACE FUNCTION platform_mode_history_log() RETURNS TRIGGER AS $$
BEGIN
  -- Only care about actual mode changes, not metadata touches.
  IF NEW.mode IS DISTINCT FROM OLD.mode THEN
    -- Close the currently-open row (if any).
    UPDATE platform_mode_history
      SET ended_at = now()
      WHERE ended_at IS NULL;

    -- Open a new row for the new mode.
    INSERT INTO platform_mode_history (mode, started_at, started_by, reason)
    VALUES (NEW.mode, COALESCE(NEW.started_at, now()), NEW.started_by, NEW.reason);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_platform_mode_history ON platform_modes;
CREATE TRIGGER trg_platform_mode_history
  AFTER UPDATE ON platform_modes
  FOR EACH ROW
  EXECUTE FUNCTION platform_mode_history_log();

-- Backfill: if we're already in creative mode when this migration runs, seed
-- a single open history row so elapsed-time queries work immediately instead
-- of waiting for the next flip.
INSERT INTO platform_mode_history (mode, started_at, started_by, reason)
SELECT mode, COALESCE(started_at, now()), started_by, reason
FROM platform_modes
WHERE id = 1 AND mode = 'creative'
  AND NOT EXISTS (SELECT 1 FROM platform_mode_history WHERE ended_at IS NULL);

INSERT INTO nexus_schema_version (version, description)
VALUES (47, 'Platform mode history + transition trigger')
ON CONFLICT DO NOTHING;
