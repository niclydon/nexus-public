-- Apple Music Biome plays — local play history recovered from iOS Biome data
-- Source: ~/biome-venv/bin/python3 ~/ingest-apple-music-biome.py on Dev-Server
-- Cron refreshes /tmp/apple-music-biome.json every 6 hours

CREATE TABLE IF NOT EXISTS apple_music_plays (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    played_at timestamptz NOT NULL,
    track_name text NOT NULL,
    artist_name text,
    album_name text,
    genre text,
    duration_seconds integer,
    store_track_id text,
    store_album_id text,
    output_device text,
    source_file text,
    ingested_at timestamptz DEFAULT now(),
    UNIQUE(played_at, track_name, artist_name)
);

CREATE INDEX idx_apple_music_plays_date ON apple_music_plays (played_at DESC);
CREATE INDEX idx_apple_music_plays_artist ON apple_music_plays (artist_name);

INSERT INTO nexus_schema_version (version) VALUES (44);
