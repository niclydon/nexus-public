-- Migration 026: Canonical Person Entity
--
-- Promotes aurora_social_identities as THE canonical person table.
-- Adds person_id columns to consuming tables and creates
-- a resolve_person_identity() function for real-time identity
-- resolution at ingestion time.

-- 1. Add person_id to tables that reference people
ALTER TABLE knowledge_entities ADD COLUMN IF NOT EXISTS person_id INTEGER
  REFERENCES aurora_social_identities(id);
ALTER TABLE aurora_sentiment ADD COLUMN IF NOT EXISTS person_id INTEGER
  REFERENCES aurora_social_identities(id);
ALTER TABLE photo_metadata ADD COLUMN IF NOT EXISTS person_ids INTEGER[] DEFAULT '{}';

-- 2. Indexes for person_id lookups
CREATE INDEX IF NOT EXISTS idx_ke_person_id ON knowledge_entities (person_id) WHERE person_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sentiment_person_id ON aurora_sentiment (person_id) WHERE person_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_photo_person_ids ON photo_metadata USING GIN (person_ids) WHERE person_ids != '{}';

-- 3. Identity resolution function — called at ingestion time
-- Deterministic matching: normalized identifier → person_id
-- Auto-creates new persons when no match found (if display_name provided)
-- Cross-platform linking: same identifier on different platform auto-links
CREATE OR REPLACE FUNCTION resolve_person_identity(
  p_identifier TEXT,
  p_platform TEXT,
  p_display_name TEXT DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
  v_person_id INTEGER;
  v_normalized TEXT;
  v_id_type TEXT;
BEGIN
  IF p_identifier IS NULL OR LENGTH(p_identifier) < 2 THEN RETURN NULL; END IF;

  -- Normalize identifier: strip formatting from phone numbers
  v_normalized := regexp_replace(p_identifier, '[\s\-\(\)\.]', '', 'g');
  -- Add +1 for bare 10-digit US numbers
  IF v_normalized ~ '^[2-9][0-9]{9}$' THEN v_normalized := '+1' || v_normalized; END IF;
  -- Add + for 11-digit starting with 1
  IF v_normalized ~ '^1[2-9][0-9]{9}$' THEN v_normalized := '+' || v_normalized; END IF;
  -- Lowercase emails
  IF v_normalized LIKE '%@%' THEN v_normalized := LOWER(v_normalized); END IF;

  -- Determine identifier type
  v_id_type := CASE
    WHEN v_normalized LIKE '%@%' THEN 'email'
    WHEN v_normalized ~ '^\+?[0-9]{7,15}$' THEN 'phone'
    ELSE 'handle'
  END;

  -- 1. Exact platform match
  SELECT identity_id INTO v_person_id
  FROM aurora_social_identity_links
  WHERE identifier = v_normalized AND platform = p_platform
  LIMIT 1;
  IF v_person_id IS NOT NULL THEN RETURN v_person_id; END IF;

  -- 2. Cross-platform match (same identifier, any platform)
  SELECT identity_id INTO v_person_id
  FROM aurora_social_identity_links
  WHERE identifier = v_normalized
  LIMIT 1;
  IF v_person_id IS NOT NULL THEN
    -- Auto-link to this platform too
    INSERT INTO aurora_social_identity_links
      (identity_id, platform, identifier, identifier_type, match_confidence)
    VALUES (v_person_id, p_platform, v_normalized, v_id_type, 0.95)
    ON CONFLICT DO NOTHING;
    RETURN v_person_id;
  END IF;

  -- 3. No match — create new person if we have a display name
  IF p_display_name IS NOT NULL AND LENGTH(TRIM(p_display_name)) > 1 THEN
    INSERT INTO aurora_social_identities (display_name, is_person)
    VALUES (TRIM(p_display_name), TRUE)
    RETURNING id INTO v_person_id;

    INSERT INTO aurora_social_identity_links
      (identity_id, platform, identifier, identifier_type, match_confidence)
    VALUES (v_person_id, p_platform, v_normalized, v_id_type, 1.0);

    RETURN v_person_id;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 4. Backfill: link knowledge_entities (person type) to aurora_social_identities by name match
UPDATE knowledge_entities ke
SET person_id = si.id
FROM aurora_social_identities si
WHERE ke.entity_type = 'person'
  AND ke.person_id IS NULL
  AND LOWER(ke.canonical_name) = LOWER(si.display_name)
  AND si.is_person = TRUE;

-- 5. Backfill: link aurora_sentiment to persons via identifier match
UPDATE aurora_sentiment s
SET person_id = l.identity_id
FROM aurora_social_identity_links l
WHERE s.person_id IS NULL
  AND s.contact_identifier = l.identifier;

-- 6. Classify data sources as realtime vs manual for staleness alerting
-- Agents should NOT alert on staleness for manual sources
ALTER TABLE data_source_registry ADD COLUMN IF NOT EXISTS ingestion_mode TEXT DEFAULT 'realtime';
-- realtime: continuous sync (imessage, gmail, calendar, looki, photos)
-- manual: user-initiated import (facebook, instagram, google_voice, chatgpt, claude, siri)
-- scheduled: periodic but automatic (contacts, health_data)

UPDATE data_source_registry SET ingestion_mode = 'realtime' WHERE source_key IN ('imessage', 'gmail', 'calendar', 'looki', 'photos');
UPDATE data_source_registry SET ingestion_mode = 'manual' WHERE source_key IN ('facebook', 'instagram', 'google_voice', 'google_chat', 'chatgpt', 'claude', 'siri');
UPDATE data_source_registry SET ingestion_mode = 'scheduled' WHERE source_key IN ('contacts', 'health');

-- 7. Schema version
INSERT INTO nexus_schema_version (version, description)
VALUES (26, 'Canonical person entity: person_id columns, resolve function, ingestion_mode, backfill links');
