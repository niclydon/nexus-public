-- Migration 043: Topic Modeling
-- K-means clustering on 500K+ vector embeddings across 28 tables

CREATE TABLE IF NOT EXISTS topic_clusters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id UUID REFERENCES topic_clusters(id),  -- NULL for Level 1
    level INTEGER NOT NULL DEFAULT 1,               -- 1 or 2
    cluster_index INTEGER NOT NULL,                 -- 0-based within level
    label TEXT,                                     -- LLM-generated label
    description TEXT,                               -- LLM-generated 1-2 sentence description
    keywords TEXT[],                                -- Top representative terms
    centroid vector(768),                           -- Cluster centroid for assignment
    member_count INTEGER NOT NULL DEFAULT 0,
    avg_distance REAL,                              -- Avg distance from centroid (cohesion)
    time_range TSTZRANGE,                           -- Earliest to latest item in cluster
    source_distribution JSONB,                      -- {"aurora_raw_imessage": 1234, ...}
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_topic_clusters_parent ON topic_clusters(parent_id);
CREATE INDEX IF NOT EXISTS idx_topic_clusters_level ON topic_clusters(level);
CREATE INDEX IF NOT EXISTS idx_topic_clusters_centroid ON topic_clusters USING hnsw (centroid vector_cosine_ops) WHERE centroid IS NOT NULL;

CREATE TABLE IF NOT EXISTS topic_assignments (
    id BIGSERIAL PRIMARY KEY,
    cluster_id UUID NOT NULL REFERENCES topic_clusters(id) ON DELETE CASCADE,
    source_table TEXT NOT NULL,
    source_id TEXT NOT NULL,
    distance REAL NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_table, source_id)
);

CREATE INDEX IF NOT EXISTS idx_topic_assignments_cluster ON topic_assignments(cluster_id);
CREATE INDEX IF NOT EXISTS idx_topic_assignments_source ON topic_assignments(source_table, source_id);

INSERT INTO nexus_schema_version (version) VALUES (43);
