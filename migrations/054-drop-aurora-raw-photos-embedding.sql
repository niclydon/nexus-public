-- Drop aurora_raw_photos.embedding column (table deprecated)
--
-- aurora_raw_photos was the original photo ingestion table from the ARIA era.
-- Its 162,830 rows were backfilled into photo_metadata during the nexus
-- migration (per docs/TODO-photos-backfill.md, which explicitly notes
-- "aurora_raw_photos → replaced by photo_metadata" and the backfill box
-- checked complete). The table is kept around for safety but is no longer
-- the canonical source for photos.
--
-- Problem: migration 041 still created an HNSW index on aurora_raw_photos.
-- embedding and embed-backfill.ts still had it in TABLE_CONFIGS, so it kept
-- showing up in the /data-quality dashboard as a 0% coverage table (0 of
-- 162,830 rows embedded). It was never going to be populated because every
-- embed-backfill run against it found nothing to process (the extraWhere
-- requires `description IS NOT NULL` but ARIA never wrote descriptions to
-- this table — those live in photo_metadata).
--
-- Fix: drop the embedding column + its HNSW index. This removes it from the
-- /data-quality page (which auto-discovers via information_schema.columns
-- where column_name = 'embedding') and signals to embed-backfill that the
-- table is no longer in scope. Row data is preserved.
--
-- Verified: 0 matviews reference aurora_raw_photos. 0 pg_depend view rows.

DROP INDEX IF EXISTS idx_aurora_raw_photos_embedding_hnsw;
ALTER TABLE aurora_raw_photos DROP COLUMN IF EXISTS embedding;

INSERT INTO nexus_schema_version (version, description)
VALUES (54, 'Drop aurora_raw_photos.embedding + HNSW index (table deprecated, data lives in photo_metadata)')
ON CONFLICT DO NOTHING;
