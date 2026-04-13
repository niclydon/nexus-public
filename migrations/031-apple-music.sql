BEGIN;

-- Apple Music listening history (recently played tracks)
CREATE TABLE IF NOT EXISTS music_listening_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    track_id TEXT NOT NULL,
    track_name TEXT NOT NULL,
    artist_name TEXT NOT NULL,
    album_name TEXT,
    genre TEXT,
    duration_ms INTEGER,
    played_at TIMESTAMPTZ,
    play_count INTEGER DEFAULT 1,
    artwork_url TEXT,
    isrc TEXT,
    preview_url TEXT,
    metadata JSONB DEFAULT '{}',
    embedding vector(768),
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(track_id, played_at)
);

CREATE INDEX IF NOT EXISTS idx_music_played ON music_listening_history(played_at DESC);
CREATE INDEX IF NOT EXISTS idx_music_artist ON music_listening_history(artist_name);
CREATE INDEX IF NOT EXISTS idx_music_genre ON music_listening_history(genre);

-- Apple Music library (songs the user has added)
CREATE TABLE IF NOT EXISTS music_library (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    track_id TEXT UNIQUE NOT NULL,
    track_name TEXT NOT NULL,
    artist_name TEXT NOT NULL,
    album_name TEXT,
    genre TEXT,
    duration_ms INTEGER,
    added_at TIMESTAMPTZ,
    play_count INTEGER DEFAULT 0,
    last_played_at TIMESTAMPTZ,
    loved BOOLEAN DEFAULT false,
    artwork_url TEXT,
    isrc TEXT,
    metadata JSONB DEFAULT '{}',
    embedding vector(768),
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_music_lib_artist ON music_library(artist_name);
CREATE INDEX IF NOT EXISTS idx_music_lib_added ON music_library(added_at DESC);
CREATE INDEX IF NOT EXISTS idx_music_lib_genre ON music_library(genre);

-- Register data source (manual ingestion mode — user token requires browser auth)
INSERT INTO data_source_registry (source_key, display_name, status, ingestion_mode, enabled)
VALUES ('apple_music', 'Apple Music', 'pending', 'scheduled', true)
ON CONFLICT (source_key) DO NOTHING;

-- Enrichment config: music category gets embeddings and sentiment
INSERT INTO enrichment_config (category, enrichment_type, job_type) VALUES
    ('music', 'embedding', 'embed-backfill'),
    ('music', 'sentiment', 'sentiment-backfill')
ON CONFLICT (category, enrichment_type) DO NOTHING;

-- Job routing
INSERT INTO nexus_job_routing (job_type) VALUES ('music-sync'), ('music-backfill')
ON CONFLICT DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (31, 'Apple Music: listening history, library tables, data source registry, enrichment config')
ON CONFLICT (version) DO NOTHING;

COMMIT;
