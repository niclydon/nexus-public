-- Migration 024: Add kg_ingested_at to aurora_raw_gmail for KG backfill tracking

ALTER TABLE aurora_raw_gmail ADD COLUMN IF NOT EXISTS kg_ingested_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX IF NOT EXISTS idx_aurora_raw_gmail_kg_pending
  ON aurora_raw_gmail (date DESC)
  WHERE kg_ingested_at IS NULL AND (subject IS NOT NULL OR body_text IS NOT NULL);

-- Also add embedding column to knowledge_entities for Phase 2.5
ALTER TABLE knowledge_entities ADD COLUMN IF NOT EXISTS embedding vector(768);

CREATE INDEX IF NOT EXISTS idx_knowledge_entities_embedding
  ON knowledge_entities USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

INSERT INTO nexus_schema_version (version, description)
VALUES (24, 'Add kg_ingested_at to aurora_raw_gmail, embedding to knowledge_entities');
