BEGIN;

-- Apple Notes storage
CREATE TABLE IF NOT EXISTS apple_notes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    note_identifier TEXT UNIQUE NOT NULL,
    zpk INTEGER,
    title TEXT,
    folder TEXT,
    body_text TEXT,
    note_type TEXT,
    created_at TIMESTAMPTZ,
    modified_at TIMESTAMPTZ,
    is_deleted BOOLEAN DEFAULT false,
    has_attachments BOOLEAN DEFAULT false,
    attachment_count INTEGER DEFAULT 0,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    location_name TEXT,
    checklist_items JSONB,
    classification JSONB,
    metadata JSONB DEFAULT '{}',
    embedding vector(768),
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    classified_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_apple_notes_identifier ON apple_notes(note_identifier);
CREATE INDEX IF NOT EXISTS idx_apple_notes_modified ON apple_notes(modified_at DESC);
CREATE INDEX IF NOT EXISTS idx_apple_notes_type ON apple_notes(note_type);
CREATE INDEX IF NOT EXISTS idx_apple_notes_unclassified ON apple_notes(created_at) WHERE classified_at IS NULL;

-- Register data source
INSERT INTO data_source_registry (source_key, display_name, status, ingestion_mode, enabled)
VALUES ('apple_notes', 'Apple Notes', 'pending', 'scheduled', true)
ON CONFLICT (source_key) DO NOTHING;

-- Enrichment config: document category gets sentiment too
INSERT INTO enrichment_config (category, enrichment_type, job_type) VALUES
    ('document', 'sentiment', 'sentiment-backfill')
ON CONFLICT (category, enrichment_type) DO NOTHING;

-- Job routing
INSERT INTO nexus_job_routing (job_type) VALUES ('notes-sync'), ('notes-backfill')
ON CONFLICT DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (30, 'Apple Notes: apple_notes table, data source registry, enrichment config')
ON CONFLICT (version) DO NOTHING;

COMMIT;
