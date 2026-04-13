-- Migration 042: Real life chapters
-- Replaces micro-behavioral aurora_life_chapters with meaningful multi-year eras

CREATE TABLE IF NOT EXISTS life_chapters (
  id serial PRIMARY KEY,
  title text NOT NULL,
  subtitle text,                          -- one-line tagline
  start_date date NOT NULL,
  end_date date,                          -- NULL = ongoing
  narrative text NOT NULL,                -- 2-4 paragraph rich description
  defining_themes text[] DEFAULT '{}',    -- e.g. {'travel', 'career', 'family'}
  key_people text[] DEFAULT '{}',         -- names of important people this era
  key_locations text[] DEFAULT '{}',      -- cities/countries
  key_events text[] DEFAULT '{}',         -- major life events
  soundtrack text[] DEFAULT '{}',         -- top artists/songs
  data_sources text[] DEFAULT '{}',       -- which data sources cover this era
  behavioral_signature jsonb DEFAULT '{}', -- avg daily metrics for this era
  chapter_order integer NOT NULL,         -- display order (1-based)
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX idx_life_chapters_dates ON life_chapters (start_date, end_date);

-- KG backfill watermark columns for new biographical tables
ALTER TABLE bb_sms_messages ADD COLUMN IF NOT EXISTS kg_ingested_at timestamptz;
ALTER TABLE archived_sent_emails ADD COLUMN IF NOT EXISTS kg_ingested_at timestamptz;
ALTER TABLE site_guestbook ADD COLUMN IF NOT EXISTS kg_ingested_at timestamptz;

INSERT INTO nexus_schema_version (version) VALUES (42);
