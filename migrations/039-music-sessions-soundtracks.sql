-- Migration 038: Music session detection and life event soundtracks
-- Detects listening sessions from 255K plays (187K Spotify + 68K Last.fm)
-- Maps music to life events (trips, key dates, life chapters)

BEGIN;

-- ============================================================
-- Table 1: Music Sessions (gap-based session detection)
-- ============================================================
CREATE TABLE IF NOT EXISTS music_sessions (
  id SERIAL PRIMARY KEY,
  session_start TIMESTAMPTZ NOT NULL,
  session_end TIMESTAMPTZ NOT NULL,
  duration_minutes REAL,
  play_count INT,
  unique_artists INT,
  unique_tracks INT,
  top_artist TEXT,
  top_genre TEXT,
  source TEXT NOT NULL, -- 'spotify', 'lastfm'
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_music_sessions_start ON music_sessions (session_start);
CREATE INDEX idx_music_sessions_source ON music_sessions (source);

-- ============================================================
-- Table 2: Life Event Soundtracks
-- ============================================================
CREATE TABLE IF NOT EXISTS life_event_soundtracks (
  id SERIAL PRIMARY KEY,
  event_type TEXT NOT NULL, -- 'trip', 'life_chapter', 'key_date'
  event_name TEXT NOT NULL,
  event_start DATE,
  event_end DATE,
  total_plays INT,
  total_minutes REAL,
  top_artists TEXT[], -- top 5
  top_tracks TEXT[], -- top 5
  unique_artists INT,
  source_breakdown JSONB, -- {"spotify": 120, "lastfm": 30}
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_life_event_soundtracks_type ON life_event_soundtracks (event_type);
CREATE INDEX idx_life_event_soundtracks_start ON life_event_soundtracks (event_start);

-- ============================================================
-- Populate: Spotify Sessions (30-minute gap detection)
-- ============================================================
WITH ordered AS (
  SELECT played_at, track_name, artist_name, ms_played,
    played_at - LAG(played_at) OVER (ORDER BY played_at) AS gap
  FROM spotify_streaming_history
  WHERE played_at IS NOT NULL
),
session_boundaries AS (
  SELECT *,
    SUM(CASE WHEN gap > INTERVAL '30 minutes' OR gap IS NULL THEN 1 ELSE 0 END)
      OVER (ORDER BY played_at) AS session_id
  FROM ordered
)
INSERT INTO music_sessions (session_start, session_end, duration_minutes, play_count, unique_artists, unique_tracks, top_artist, source)
SELECT
  MIN(played_at),
  MAX(played_at),
  EXTRACT(EPOCH FROM MAX(played_at) - MIN(played_at)) / 60.0,
  COUNT(*),
  COUNT(DISTINCT artist_name),
  COUNT(DISTINCT track_name),
  MODE() WITHIN GROUP (ORDER BY artist_name),
  'spotify'
FROM session_boundaries
GROUP BY session_id
HAVING COUNT(*) >= 2;

-- ============================================================
-- Populate: Last.fm Sessions (30-minute gap detection)
-- ============================================================
WITH ordered AS (
  SELECT scrobbled_at, track_name, artist_name,
    scrobbled_at - LAG(scrobbled_at) OVER (ORDER BY scrobbled_at) AS gap
  FROM lastfm_scrobbles
  WHERE scrobbled_at IS NOT NULL
),
session_boundaries AS (
  SELECT *,
    SUM(CASE WHEN gap > INTERVAL '30 minutes' OR gap IS NULL THEN 1 ELSE 0 END)
      OVER (ORDER BY scrobbled_at) AS session_id
  FROM ordered
)
INSERT INTO music_sessions (session_start, session_end, duration_minutes, play_count, unique_artists, unique_tracks, top_artist, source)
SELECT
  MIN(scrobbled_at),
  MAX(scrobbled_at),
  EXTRACT(EPOCH FROM MAX(scrobbled_at) - MIN(scrobbled_at)) / 60.0,
  COUNT(*),
  COUNT(DISTINCT artist_name),
  COUNT(DISTINCT track_name),
  MODE() WITHIN GROUP (ORDER BY artist_name),
  'lastfm'
FROM session_boundaries
GROUP BY session_id
HAVING COUNT(*) >= 2;

-- ============================================================
-- Populate: Trip Soundtracks
-- ============================================================
INSERT INTO life_event_soundtracks (event_type, event_name, event_start, event_end, total_plays, total_minutes, top_artists, top_tracks, unique_artists, source_breakdown)
SELECT
  'trip',
  t.trip_name,
  t.start_date,
  t.end_date,
  COALESCE(spotify.cnt, 0) + COALESCE(lastfm.cnt, 0),
  COALESCE(spotify.minutes, 0),
  -- Top 5 artists from combined sources
  (SELECT ARRAY(
    SELECT artist FROM (
      SELECT artist_name AS artist, COUNT(*) AS c FROM spotify_streaming_history
        WHERE played_at::date BETWEEN t.start_date AND t.end_date GROUP BY artist_name
      UNION ALL
      SELECT artist_name AS artist, COUNT(*) AS c FROM lastfm_scrobbles
        WHERE scrobbled_at::date BETWEEN t.start_date AND t.end_date GROUP BY artist_name
    ) combined GROUP BY artist ORDER BY SUM(c) DESC LIMIT 5
  )),
  -- Top 5 tracks from combined sources
  (SELECT ARRAY(
    SELECT track FROM (
      SELECT artist_name || ' - ' || track_name AS track, COUNT(*) AS c FROM spotify_streaming_history
        WHERE played_at::date BETWEEN t.start_date AND t.end_date GROUP BY artist_name, track_name
      UNION ALL
      SELECT artist_name || ' - ' || track_name AS track, COUNT(*) AS c FROM lastfm_scrobbles
        WHERE scrobbled_at::date BETWEEN t.start_date AND t.end_date GROUP BY artist_name, track_name
    ) combined GROUP BY track ORDER BY SUM(c) DESC LIMIT 5
  )),
  (SELECT COUNT(DISTINCT artist) FROM (
    SELECT artist_name AS artist FROM spotify_streaming_history WHERE played_at::date BETWEEN t.start_date AND t.end_date
    UNION
    SELECT artist_name AS artist FROM lastfm_scrobbles WHERE scrobbled_at::date BETWEEN t.start_date AND t.end_date
  ) a),
  jsonb_build_object('spotify', COALESCE(spotify.cnt, 0), 'lastfm', COALESCE(lastfm.cnt, 0))
FROM travel_trips t
LEFT JOIN LATERAL (
  SELECT COUNT(*) AS cnt, SUM(ms_played) / 60000.0 AS minutes
  FROM spotify_streaming_history
  WHERE played_at::date BETWEEN t.start_date AND t.end_date
) spotify ON TRUE
LEFT JOIN LATERAL (
  SELECT COUNT(*) AS cnt
  FROM lastfm_scrobbles
  WHERE scrobbled_at::date BETWEEN t.start_date AND t.end_date
) lastfm ON TRUE
WHERE t.start_date IS NOT NULL AND t.end_date IS NOT NULL
  AND (COALESCE(spotify.cnt, 0) + COALESCE(lastfm.cnt, 0)) > 0;

-- ============================================================
-- Populate: Key Biographical Date Soundtracks (+/- 7 days)
-- ============================================================
WITH key_dates (event_name, event_date) AS (
  VALUES
    ('[Life event 1]',  '2006-09-15'::date),
    ('[Life event 2]',  '2008-09-19'::date),
    ('[Life event 3]',  '2020-10-17'::date),
    ('[Life event 4]',  '2024-05-09'::date),
    ('[Life event 5]',  '2026-02-28'::date)
)
INSERT INTO life_event_soundtracks (event_type, event_name, event_start, event_end, total_plays, total_minutes, top_artists, top_tracks, unique_artists, source_breakdown)
SELECT
  'key_date',
  kd.event_name,
  kd.event_date - 7,
  kd.event_date + 7,
  COALESCE(spotify.cnt, 0) + COALESCE(lastfm.cnt, 0),
  COALESCE(spotify.minutes, 0),
  (SELECT ARRAY(
    SELECT artist FROM (
      SELECT artist_name AS artist, COUNT(*) AS c FROM spotify_streaming_history
        WHERE played_at::date BETWEEN kd.event_date - 7 AND kd.event_date + 7 GROUP BY artist_name
      UNION ALL
      SELECT artist_name AS artist, COUNT(*) AS c FROM lastfm_scrobbles
        WHERE scrobbled_at::date BETWEEN kd.event_date - 7 AND kd.event_date + 7 GROUP BY artist_name
    ) combined GROUP BY artist ORDER BY SUM(c) DESC LIMIT 5
  )),
  (SELECT ARRAY(
    SELECT track FROM (
      SELECT artist_name || ' - ' || track_name AS track, COUNT(*) AS c FROM spotify_streaming_history
        WHERE played_at::date BETWEEN kd.event_date - 7 AND kd.event_date + 7 GROUP BY artist_name, track_name
      UNION ALL
      SELECT artist_name || ' - ' || track_name AS track, COUNT(*) AS c FROM lastfm_scrobbles
        WHERE scrobbled_at::date BETWEEN kd.event_date - 7 AND kd.event_date + 7 GROUP BY artist_name, track_name
    ) combined GROUP BY track ORDER BY SUM(c) DESC LIMIT 5
  )),
  (SELECT COUNT(DISTINCT artist) FROM (
    SELECT artist_name AS artist FROM spotify_streaming_history WHERE played_at::date BETWEEN kd.event_date - 7 AND kd.event_date + 7
    UNION
    SELECT artist_name AS artist FROM lastfm_scrobbles WHERE scrobbled_at::date BETWEEN kd.event_date - 7 AND kd.event_date + 7
  ) a),
  jsonb_build_object('spotify', COALESCE(spotify.cnt, 0), 'lastfm', COALESCE(lastfm.cnt, 0))
FROM key_dates kd
LEFT JOIN LATERAL (
  SELECT COUNT(*) AS cnt, SUM(ms_played) / 60000.0 AS minutes
  FROM spotify_streaming_history
  WHERE played_at::date BETWEEN kd.event_date - 7 AND kd.event_date + 7
) spotify ON TRUE
LEFT JOIN LATERAL (
  SELECT COUNT(*) AS cnt
  FROM lastfm_scrobbles
  WHERE scrobbled_at::date BETWEEN kd.event_date - 7 AND kd.event_date + 7
) lastfm ON TRUE
WHERE (COALESCE(spotify.cnt, 0) + COALESCE(lastfm.cnt, 0)) > 0;

-- ============================================================
-- Update schema version
-- ============================================================
INSERT INTO nexus_schema_version (version, description)
VALUES (39, 'Music session detection and life event soundtracks');

COMMIT;
