-- Migration 040: Enrichment Infrastructure
-- Adds embedding/sentiment columns, enrichment config, expands unified views,
-- creates aurora_unified_media and aurora_unified_financial matviews.

-- Section 1: Add columns to existing tables

ALTER TABLE site_guestbook ADD COLUMN IF NOT EXISTS embedding vector(768);
ALTER TABLE site_guestbook ADD COLUMN IF NOT EXISTS sentiment_score real;
ALTER TABLE site_guestbook ADD COLUMN IF NOT EXISTS sentiment_label text;

ALTER TABLE strava_activities ADD COLUMN IF NOT EXISTS embedding vector(768);

ALTER TABLE life_transactions ADD COLUMN IF NOT EXISTS embedding vector(768);

-- Section 2: Add enrichment_config rows

INSERT INTO enrichment_config (category, enrichment_type, job_type, is_enabled) VALUES
  ('guestbook', 'embedding', 'embed-backfill', true),
  ('guestbook', 'sentiment', 'sentiment-backfill', true),
  ('guestbook', 'knowledge', 'knowledge-backfill', true),
  ('strava', 'embedding', 'embed-backfill', true),
  ('transaction', 'embedding', 'embed-backfill', true),
  ('netflix', 'embedding', 'embed-backfill', false),
  ('travel', 'embedding', 'embed-backfill', false)
ON CONFLICT (category, enrichment_type) DO NOTHING;

-- Section 3: Expand aurora_unified_communication

DROP MATERIALIZED VIEW IF EXISTS aurora_unified_communication;

CREATE MATERIALIZED VIEW aurora_unified_communication AS
SELECT DISTINCT ON (source_table, source_id) timestamp, channel, direction, contact_identifier, identity_id, contact_name, content_text, content_length, thread_id, source_table, source_id
FROM (
  -- imessage
  SELECT m.date AS timestamp, 'imessage'::text AS channel,
    CASE WHEN m.is_from_me THEN 'sent' ELSE 'received' END AS direction,
    m.handle_id AS contact_identifier,
    COALESCE(si.id, -1) AS identity_id,
    COALESCE(si.display_name, m.handle_id) AS contact_name,
    m.text AS content_text,
    LENGTH(m.text) AS content_length,
    m.chat_id AS thread_id,
    'aurora_raw_imessage'::text AS source_table,
    m.guid AS source_id
  FROM aurora_raw_imessage m
  LEFT JOIN aurora_social_identity_links l ON l.identifier = m.handle_id
  LEFT JOIN aurora_social_identities si ON si.id = l.identity_id AND si.is_person = true
  WHERE m.date IS NOT NULL

  UNION ALL
  -- gmail
  SELECT g.date, 'gmail'::text,
    CASE WHEN g.is_sent THEN 'sent' ELSE 'received' END,
    CASE WHEN g.is_sent THEN g.to_addresses[1] ELSE g.from_address END,
    COALESCE(si.id, -1),
    COALESCE(si.display_name, g.from_name, CASE WHEN g.is_sent THEN g.to_addresses[1] ELSE g.from_address END),
    g.body_text,
    LENGTH(g.body_text),
    g.thread_id,
    'aurora_raw_gmail'::text,
    g.id
  FROM aurora_raw_gmail g
  LEFT JOIN aurora_social_identity_links l ON l.identifier = CASE WHEN g.is_sent THEN g.to_addresses[1] ELSE g.from_address END
  LEFT JOIN aurora_social_identities si ON si.id = l.identity_id AND si.is_person = true
  WHERE g.date > '2000-01-01'::timestamptz

  UNION ALL
  -- instagram messages
  SELECT ig.timestamp, 'instagram'::text, 'received'::text,
    ig.sender,
    COALESCE(si.id, -1),
    COALESCE(si.display_name, ig.sender),
    ig.content,
    LENGTH(ig.content),
    NULL::text,
    'aurora_raw_instagram'::text,
    ig.id::text
  FROM aurora_raw_instagram ig
  LEFT JOIN aurora_social_identity_links l ON l.identifier = ig.sender
  LEFT JOIN aurora_social_identities si ON si.id = l.identity_id AND si.is_person = true
  WHERE ig.data_type = 'message' AND ig.timestamp IS NOT NULL

  UNION ALL
  -- facebook messages
  SELECT fb.timestamp, 'facebook'::text, 'received'::text,
    fb.sender,
    COALESCE(si.id, -1),
    COALESCE(si.display_name, fb.sender),
    fb.content,
    LENGTH(fb.content),
    NULL::text,
    'aurora_raw_facebook'::text,
    fb.id::text
  FROM aurora_raw_facebook fb
  LEFT JOIN aurora_social_identity_links l ON l.identifier = fb.sender
  LEFT JOIN aurora_social_identities si ON si.id = l.identity_id AND si.is_person = true
  WHERE fb.data_type = 'message' AND fb.timestamp IS NOT NULL

  UNION ALL
  -- google voice texts
  SELECT gv.timestamp, 'google_voice'::text,
    CASE WHEN gv.is_incoming THEN 'received' ELSE 'sent' END,
    gv.phone_number,
    COALESCE(si.id, -1),
    COALESCE(si.display_name, gv.phone_number),
    gv.message_text,
    LENGTH(gv.message_text),
    NULL::text,
    'aurora_raw_google_voice'::text,
    gv.id::text
  FROM aurora_raw_google_voice gv
  LEFT JOIN aurora_social_identity_links l ON l.identifier = gv.phone_number
  LEFT JOIN aurora_social_identities si ON si.id = l.identity_id AND si.is_person = true
  WHERE gv.record_type = 'text' AND gv.timestamp IS NOT NULL

  UNION ALL
  -- blackberry sms
  SELECT s.timestamp, 'blackberry_sms'::text AS channel,
    CASE WHEN s.direction = 'sent' THEN 'sent' ELSE 'received' END AS direction,
    s.phone_number AS contact_identifier,
    COALESCE(si.id, -1) AS identity_id,
    COALESCE(si.display_name, s.phone_number) AS contact_name,
    s.message AS content_text,
    LENGTH(s.message) AS content_length,
    NULL::text AS thread_id,
    'bb_sms_messages'::text AS source_table,
    s.id::text AS source_id
  FROM bb_sms_messages s
  LEFT JOIN aurora_social_identity_links l ON l.identifier = s.phone_number
  LEFT JOIN aurora_social_identities si ON si.id = l.identity_id AND si.is_person = true
  WHERE s.timestamp IS NOT NULL

  UNION ALL
  -- employer sent emails
  SELECT w.sent_at AS timestamp, 'archived_email'::text AS channel,
    'sent'::text AS direction,
    w.to_recipients AS contact_identifier,
    -1 AS identity_id,
    w.to_recipients AS contact_name,
    COALESCE(w.subject, '') || ' — ' || COALESCE(LEFT(w.body_text, 500), '') AS content_text,
    COALESCE(w.body_length, LENGTH(w.body_text)) AS content_length,
    NULL::text AS thread_id,
    'archived_sent_emails'::text AS source_table,
    w.id::text AS source_id
  FROM archived_sent_emails w
  WHERE w.sent_at IS NOT NULL

  UNION ALL
  -- example-personal.com guestbook
  SELECT g.posted_at AS timestamp, 'guestbook'::text AS channel,
    'received'::text AS direction,
    g.poster_name AS contact_identifier,
    -1 AS identity_id,
    g.poster_name AS contact_name,
    g.message_text AS content_text,
    LENGTH(g.message_text) AS content_length,
    g.recipient_page AS thread_id,
    'site_guestbook'::text AS source_table,
    g.id::text AS source_id
  FROM site_guestbook g
  WHERE g.posted_at IS NOT NULL
) raw;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unified_communication_source
  ON aurora_unified_communication (source_table, source_id);
CREATE INDEX IF NOT EXISTS idx_unified_communication_timestamp
  ON aurora_unified_communication (timestamp DESC);

-- Section 4: Create aurora_unified_media

DROP MATERIALIZED VIEW IF EXISTS aurora_unified_media;

CREATE MATERIALIZED VIEW aurora_unified_media AS
SELECT timestamp, platform, media_type, title, artist, album, duration_seconds, genre, source_table, source_id
FROM (
  -- Spotify
  SELECT s.played_at AS timestamp,
    'spotify'::text AS platform,
    CASE WHEN s.episode_name IS NOT NULL THEN 'podcast' ELSE 'music' END AS media_type,
    COALESCE(s.episode_name, s.track_name) AS title,
    COALESCE(s.episode_show, s.artist_name) AS artist,
    s.album_name AS album,
    (s.ms_played / 1000)::integer AS duration_seconds,
    NULL::text AS genre,
    'spotify_streaming_history'::text AS source_table,
    s.id::text AS source_id
  FROM spotify_streaming_history s
  WHERE s.played_at IS NOT NULL AND s.ms_played > 30000

  UNION ALL
  -- Last.fm
  SELECT l.scrobbled_at, 'lastfm'::text, 'music'::text,
    l.track_name, l.artist_name, l.album_name,
    NULL::integer,
    NULL::text,
    'lastfm_scrobbles'::text,
    l.id::text
  FROM lastfm_scrobbles l
  WHERE l.scrobbled_at IS NOT NULL

  UNION ALL
  -- Netflix
  SELECT n.watched_date::timestamptz, 'netflix'::text,
    CASE WHEN n.is_movie THEN 'movie' ELSE 'tv' END,
    n.title, n.show_name, NULL::text,
    NULL::integer,
    n.genre,
    'netflix_viewing_history'::text,
    n.id::text
  FROM netflix_viewing_history n
  WHERE n.watched_date IS NOT NULL

  UNION ALL
  -- Music library (Apple Music)
  SELECT m.last_played_at, 'apple_music'::text, 'music'::text,
    m.track_name, m.artist_name, m.album_name,
    (m.duration_ms / 1000)::integer,
    m.genre,
    'music_library'::text,
    m.id::text
  FROM music_library m
  WHERE m.last_played_at IS NOT NULL
) media
ORDER BY timestamp DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unified_media_source
  ON aurora_unified_media (source_table, source_id);
CREATE INDEX IF NOT EXISTS idx_unified_media_timestamp
  ON aurora_unified_media (timestamp DESC);

-- Section 5: Create aurora_unified_financial

DROP MATERIALIZED VIEW IF EXISTS aurora_unified_financial;

CREATE MATERIALIZED VIEW aurora_unified_financial AS
SELECT
  t.transaction_date AS timestamp,
  t.source AS platform,
  t.transaction_type,
  t.description,
  t.merchant,
  CASE WHEN t.amount ~ '^\d+\.?\d*$' THEN t.amount::numeric ELSE NULL END AS amount,
  t.currency,
  t.category,
  t.subcategory,
  t.location,
  t.items,
  'life_transactions'::text AS source_table,
  t.id::text AS source_id
FROM life_transactions t
WHERE t.transaction_date IS NOT NULL
ORDER BY t.transaction_date DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unified_financial_source
  ON aurora_unified_financial (source_table, source_id);
CREATE INDEX IF NOT EXISTS idx_unified_financial_timestamp
  ON aurora_unified_financial (timestamp DESC);

-- Section 6: Expand aurora_unified_travel

DROP MATERIALIZED VIEW IF EXISTS aurora_unified_travel;

CREATE MATERIALIZED VIEW aurora_unified_travel AS
SELECT timestamp, travel_type, description, origin_location, destination_location, cabin_class, purpose, duration_seconds, canceled, seat, aircraft_type, source_id
FROM (
  -- flights (from aurora_raw_flights)
  SELECT f.flight_date::timestamptz AS timestamp,
    'flight'::text AS travel_type,
    f.airline || ' ' || f.flight_number AS description,
    f.origin AS origin_location,
    f.destination AS destination_location,
    f.cabin_class,
    f.flight_reason AS purpose,
    EXTRACT(epoch FROM (COALESCE(f.actual_arrival, f.scheduled_arrival) - COALESCE(f.actual_departure, f.scheduled_departure)))::integer AS duration_seconds,
    f.canceled,
    f.seat,
    f.aircraft_type,
    f.id::text AS source_id
  FROM aurora_raw_flights f
  WHERE f.flight_date IS NOT NULL

  UNION ALL
  -- wallet passes
  SELECT wp.relevant_date AS timestamp,
    CASE
      WHEN wp.pass_type_id ILIKE '%boarding%' OR wp.description ILIKE '%flight%' OR wp.description ILIKE '%airline%' THEN 'flight'
      WHEN wp.description ILIKE '%hotel%' OR wp.description ILIKE '%resort%' OR wp.description ILIKE '%inn%' THEN 'hotel'
      WHEN wp.description ILIKE '%ticket%' OR wp.description ILIKE '%event%' OR wp.description ILIKE '%concert%' THEN 'event'
      ELSE 'other'
    END AS travel_type,
    wp.description,
    NULL::text AS origin_location,
    NULL::text AS destination_location,
    NULL::text AS cabin_class,
    NULL::text AS purpose,
    NULL::integer AS duration_seconds,
    wp.voided AS canceled,
    NULL::text AS seat,
    NULL::text AS aircraft_type,
    wp.id::text AS source_id
  FROM aurora_raw_wallet_passes wp
  WHERE wp.relevant_date IS NOT NULL

  UNION ALL
  -- trips
  SELECT tt.start_date::timestamptz AS timestamp,
    'trip'::text AS travel_type,
    tt.trip_name AS description,
    NULL::text AS origin_location,
    NULL::text AS destination_location,
    NULL::text AS cabin_class,
    NULL::text AS purpose,
    EXTRACT(epoch FROM (tt.end_date::timestamp - tt.start_date::timestamp))::integer AS duration_seconds,
    false AS canceled,
    NULL::text AS seat,
    NULL::text AS aircraft_type,
    tt.id::text AS source_id
  FROM travel_trips tt
  WHERE tt.start_date IS NOT NULL
) travel
ORDER BY timestamp DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unified_travel_source
  ON aurora_unified_travel (source_id);
CREATE INDEX IF NOT EXISTS idx_unified_travel_timestamp
  ON aurora_unified_travel (timestamp DESC);

-- Section 7: Refresh existing matviews

REFRESH MATERIALIZED VIEW knowledge_map;
REFRESH MATERIALIZED VIEW gmail_communication_heatmap;
REFRESH MATERIALIZED VIEW gmail_daily_stats;
REFRESH MATERIALIZED VIEW gmail_sender_rankings;
REFRESH MATERIALIZED VIEW gmail_yearly_evolution;

-- Section 8: Refresh expanded/new views

REFRESH MATERIALIZED VIEW aurora_unified_communication;
REFRESH MATERIALIZED VIEW aurora_unified_travel;
REFRESH MATERIALIZED VIEW aurora_unified_health;
REFRESH MATERIALIZED VIEW aurora_unified_media;
REFRESH MATERIALIZED VIEW aurora_unified_financial;

-- Section 9: Update schema version

INSERT INTO nexus_schema_version (version) VALUES (40);
