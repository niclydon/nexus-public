-- Migration 037: Add enrichment columns to ingested archive tables
-- Enables embedding, sentiment, and knowledge graph extraction for:
--   blogger_posts (828 rows, 2002-2012)
--   archived_sent_emails (1,357 rows, 2008-2010)
--   bb_sms_messages (1,272 rows, 2009-2010)

-- Embedding columns (vector 768 dims via nomic-embed)
ALTER TABLE blogger_posts ADD COLUMN IF NOT EXISTS embedding vector(768);
ALTER TABLE archived_sent_emails ADD COLUMN IF NOT EXISTS embedding vector(768);
ALTER TABLE bb_sms_messages ADD COLUMN IF NOT EXISTS embedding vector(768);

-- KG ingestion watermark for blogger_posts
ALTER TABLE blogger_posts ADD COLUMN IF NOT EXISTS kg_ingested_at timestamptz;

-- Person identity resolution for bb_sms_messages
ALTER TABLE bb_sms_messages ADD COLUMN IF NOT EXISTS person_id integer REFERENCES aurora_social_identities(id);

-- Enrichment config entries for new data categories
INSERT INTO enrichment_config (category, enrichment_type, job_type, is_enabled) VALUES
  ('blog', 'sentiment', 'sentiment-backfill', true),
  ('blog', 'embedding', 'embed-backfill', true),
  ('blog', 'knowledge', 'knowledge-backfill', true),
  ('work_email', 'sentiment', 'sentiment-backfill', true),
  ('work_email', 'embedding', 'embed-backfill', true),
  ('sms', 'sentiment', 'sentiment-backfill', true),
  ('sms', 'embedding', 'embed-backfill', true)
ON CONFLICT (category, enrichment_type) DO NOTHING;

-- Record migration
INSERT INTO nexus_schema_version (version, description)
VALUES (37, 'Add enrichment columns to archive tables (blogger, archived_emails, bb_sms)');
