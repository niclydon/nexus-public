-- Nexus: Unified ingestion pipeline

BEGIN;

-- Ingestion log — tracks every piece of data that enters the system
-- Not a replacement for source-specific tables, but a unified index
-- that drives immediate action checks and enrichment queuing
CREATE TABLE IF NOT EXISTS ingestion_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_key TEXT NOT NULL,
    source_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    category TEXT NOT NULL CHECK (category IN (
        'email', 'message', 'event', 'photo', 'health',
        'location', 'social', 'document', 'contact', 'activity'
    )),
    summary TEXT,
    entities JSONB DEFAULT '[]',
    metadata JSONB DEFAULT '{}',
    -- Processing state
    proactive_checked BOOLEAN DEFAULT false,
    enrichments_queued BOOLEAN DEFAULT false,
    -- Dedup
    UNIQUE(source_key, source_id)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_source ON ingestion_log(source_key, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_unprocessed ON ingestion_log(proactive_checked, enrichments_queued)
    WHERE proactive_checked = false OR enrichments_queued = false;
CREATE INDEX IF NOT EXISTS idx_ingestion_recent ON ingestion_log(ingested_at DESC);

-- Enrichment config — which enrichments apply to which categories
CREATE TABLE IF NOT EXISTS enrichment_config (
    id SERIAL PRIMARY KEY,
    category TEXT NOT NULL,
    enrichment_type TEXT NOT NULL,
    job_type TEXT NOT NULL,
    is_enabled BOOLEAN DEFAULT true,
    UNIQUE(category, enrichment_type)
);

INSERT INTO enrichment_config (category, enrichment_type, job_type) VALUES
    ('email', 'sentiment', 'sentiment-backfill'),
    ('email', 'embedding', 'embed-backfill'),
    ('email', 'knowledge', 'knowledge-backfill'),
    ('message', 'sentiment', 'sentiment-backfill'),
    ('message', 'embedding', 'embed-backfill'),
    ('message', 'knowledge', 'knowledge-backfill'),
    ('event', 'embedding', 'embed-backfill'),
    ('photo', 'description', 'photo-describe'),
    ('photo', 'embedding', 'embed-backfill'),
    ('health', 'embedding', 'embed-backfill'),
    ('social', 'sentiment', 'sentiment-backfill'),
    ('social', 'embedding', 'embed-backfill'),
    ('document', 'embedding', 'embed-backfill'),
    ('document', 'knowledge', 'knowledge-backfill')
ON CONFLICT (category, enrichment_type) DO NOTHING;

-- Proactive action rules — which patterns trigger immediate insights
CREATE TABLE IF NOT EXISTS proactive_rules (
    id SERIAL PRIMARY KEY,
    rule_name TEXT UNIQUE NOT NULL,
    description TEXT,
    categories TEXT[] NOT NULL,
    match_sql TEXT NOT NULL,
    insight_template JSONB NOT NULL,
    is_enabled BOOLEAN DEFAULT true
);

INSERT INTO proactive_rules (rule_name, description, categories, match_sql, insight_template) VALUES
    ('inner-circle-message', 'Message from inner circle contact', ARRAY['email', 'message'],
     $$EXISTS (SELECT 1 FROM aurora_relationships WHERE contact_name = ANY(SELECT e->>'name' FROM jsonb_array_elements(NEW.entities) e WHERE e->>'type' = 'person') AND relationship_tier IN ('inner_circle', 'close'))$$,
     '{"urgency": "standard", "category": "social", "title_template": "Message from {entity}"}'),
    ('upcoming-event', 'Calendar event within 2 hours', ARRAY['event'],
     $$NEW.timestamp < NOW() + INTERVAL '2 hours' AND NEW.timestamp > NOW()$$,
     '{"urgency": "urgent", "category": "calendar_prep", "title_template": "Event soon: {summary}"}')
ON CONFLICT (rule_name) DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (15, 'Unified ingestion pipeline: ingestion_log, enrichment_config, proactive_rules')
ON CONFLICT (version) DO NOTHING;

COMMIT;
