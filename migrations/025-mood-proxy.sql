-- Migration 025: Mood proxy table for behavioral mood estimation

CREATE TABLE IF NOT EXISTS aurora_mood_proxy (
  id SERIAL PRIMARY KEY,
  date DATE NOT NULL UNIQUE,
  mood_score REAL NOT NULL,
  signals JSONB,
  baselines JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mood_proxy_date ON aurora_mood_proxy (date DESC);
CREATE INDEX IF NOT EXISTS idx_mood_proxy_low ON aurora_mood_proxy (mood_score) WHERE mood_score < 0.3;

INSERT INTO nexus_schema_version (version, description)
VALUES (25, 'Add aurora_mood_proxy table for behavioral mood estimation');
