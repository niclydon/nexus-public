-- Migration 055 — raw_ingest_watch_annotations
--
-- Public scaffold for Watch annotations (push source).

BEGIN;

CREATE TABLE IF NOT EXISTS raw_ingest_watch_annotations (
  id                UUID PRIMARY KEY,
  produced_at       TIMESTAMPTZ NOT NULL,
  received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload           JSONB NOT NULL,
  processing_status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (processing_status IN ('pending','processed','error','skipped')),
  processed_at      TIMESTAMPTZ,
  processed_by      TEXT,
  attempts          INTEGER NOT NULL DEFAULT 0,
  error             TEXT
);

CREATE INDEX IF NOT EXISTS raw_ingest_watch_annotations_pending_idx
  ON raw_ingest_watch_annotations (processing_status, received_at)
  WHERE processing_status = 'pending';

CREATE INDEX IF NOT EXISTS raw_ingest_watch_annotations_recent_idx
  ON raw_ingest_watch_annotations (received_at DESC);

INSERT INTO nexus_schema_version (version, description)
VALUES (55, 'raw_ingest_watch_annotations scaffold for Watch annotations')
ON CONFLICT DO NOTHING;

COMMIT;
