-- Migration 038: Add enrichment columns to social/messaging archive tables
-- Enables embedding, sentiment, and knowledge graph extraction for:
--   aurora_raw_google_voice (68K rows)
--   aurora_raw_facebook (38K rows)
--   aurora_raw_instagram (37K rows)
--   aurora_raw_google_chat (29K rows)

-- Embedding columns (vector 768 dims via nomic-embed)
ALTER TABLE aurora_raw_google_voice ADD COLUMN IF NOT EXISTS embedding vector(768);
ALTER TABLE aurora_raw_facebook ADD COLUMN IF NOT EXISTS embedding vector(768);
ALTER TABLE aurora_raw_instagram ADD COLUMN IF NOT EXISTS embedding vector(768);
ALTER TABLE aurora_raw_google_chat ADD COLUMN IF NOT EXISTS embedding vector(768);

-- KG ingestion watermark
ALTER TABLE aurora_raw_google_voice ADD COLUMN IF NOT EXISTS kg_ingested_at timestamptz;
ALTER TABLE aurora_raw_facebook ADD COLUMN IF NOT EXISTS kg_ingested_at timestamptz;
ALTER TABLE aurora_raw_instagram ADD COLUMN IF NOT EXISTS kg_ingested_at timestamptz;
ALTER TABLE aurora_raw_google_chat ADD COLUMN IF NOT EXISTS kg_ingested_at timestamptz;

-- Enrichment config entries
INSERT INTO enrichment_config (category, enrichment_type, job_type, is_enabled) VALUES
  ('google_voice', 'sentiment', 'sentiment-backfill', true),
  ('google_voice', 'embedding', 'embed-backfill', true),
  ('google_voice', 'knowledge', 'knowledge-backfill', true),
  ('facebook', 'sentiment', 'sentiment-backfill', true),
  ('facebook', 'embedding', 'embed-backfill', true),
  ('facebook', 'knowledge', 'knowledge-backfill', true),
  ('instagram', 'sentiment', 'sentiment-backfill', true),
  ('instagram', 'embedding', 'embed-backfill', true),
  ('instagram', 'knowledge', 'knowledge-backfill', true),
  ('google_chat', 'sentiment', 'sentiment-backfill', true),
  ('google_chat', 'embedding', 'embed-backfill', true),
  ('google_chat', 'knowledge', 'knowledge-backfill', true)
ON CONFLICT (category, enrichment_type) DO NOTHING;

-- Record migration
INSERT INTO nexus_schema_version (version, description)
VALUES (38, 'Add enrichment columns to social/messaging tables (google_voice, facebook, instagram, google_chat)');
