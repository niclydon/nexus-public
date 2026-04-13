/**
 * AURORA nightly analysis handler.
 *
 * Runs daily at 2 AM ET. Processes yesterday's data across all dimensions,
 * writes behavioral signatures, then runs anomaly detection and correlation checks.
 *
 * Flow:
 *   1. Determine date (yesterday by default)
 *   2. Refresh unified materialized views
 *   3. Run each dimension processor in sequence
 *   4. Write results to aurora_signatures
 *   5. Run anomaly detection (Workstream 3 — stub)
 *   6. Run correlation check (Workstream 5 — stub)
 */
import type { TempoJob } from '../job-worker.js';
import type { Pool } from 'pg';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('aurora-nightly');

// ── Dimension result shape ───────────────────────────────────

interface DimensionResult {
  dimension: string;
  metrics: Record<string, unknown>;  // numbers for stats + arrays/objects for context enrichments
}

type DimensionProcessor = (pool: Pool, date: string) => Promise<DimensionResult | null>;

// ── Dimension processors ─────────────────────────────────────

async function processHealthDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing health dimension for', date);
  const stop = logger.time('health-dimension');

  // Query aurora_unified_health view (merges 5 sources: Apple Health export, HealthKit live,
  // Activity rings, Apple workouts, Strava). SUM for cumulative metrics, AVG for rate metrics.
  const result = await pool.query(`
    SELECT metric_type,
      CASE metric_type
        WHEN 'steps' THEN SUM(value)
        WHEN 'active_energy' THEN SUM(value)
        WHEN 'basal_energy' THEN SUM(value)
        WHEN 'distance' THEN SUM(value)
        WHEN 'flights_climbed' THEN SUM(value)
        WHEN 'exercise_time' THEN SUM(value)
        WHEN 'stand_hours' THEN SUM(value)
        WHEN 'sleep' THEN SUM(value)
        ELSE AVG(value)
      END as daily_value,
      COUNT(*) as sample_count
    FROM aurora_unified_health
    WHERE date = $1
      AND metric_type IN (
        'steps', 'heart_rate', 'resting_heart_rate', 'heart_rate_avg', 'hrv',
        'active_energy', 'distance', 'sleep', 'blood_oxygen', 'vo2_max',
        'weight', 'body_fat', 'walking_asymmetry', 'environmental_audio',
        'flights_climbed', 'exercise_time', 'stand_hours', 'respiratory_rate',
        'walking_speed', 'basal_energy'
      )
    GROUP BY metric_type
  `, [date]);

  if (result.rows.length === 0) {
    logger.logVerbose('Health dimension: no data for', date);
    stop();
    return null;
  }

  // Build metrics from unified view results
  const metricsMap = new Map(result.rows.map(r => [r.metric_type, parseFloat(r.daily_value) || 0]));
  const metrics: Record<string, number> = {
    steps: metricsMap.get('steps') || 0,
    avg_heart_rate: metricsMap.get('heart_rate_avg') || metricsMap.get('heart_rate') || 0,
    resting_heart_rate: metricsMap.get('resting_heart_rate') || 0,
    active_energy_cal: metricsMap.get('active_energy') || 0,
    distance: metricsMap.get('distance') || 0,
    blood_oxygen_pct: metricsMap.get('blood_oxygen') || 0,
    sleep_hours: metricsMap.get('sleep') || 0,
    hrv: metricsMap.get('hrv') || 0,
    vo2_max: metricsMap.get('vo2_max') || 0,
    weight: metricsMap.get('weight') || 0,
    body_fat_pct: metricsMap.get('body_fat') || 0,
    walking_asymmetry_pct: metricsMap.get('walking_asymmetry') || 0,
    environmental_audio_db: metricsMap.get('environmental_audio') || 0,
    flights_climbed: metricsMap.get('flights_climbed') || 0,
    exercise_minutes: metricsMap.get('exercise_time') || 0,
    stand_hours: metricsMap.get('stand_hours') || 0,
    respiratory_rate: metricsMap.get('respiratory_rate') || 0,
    walking_speed: metricsMap.get('walking_speed') || 0,
    basal_energy_cal: metricsMap.get('basal_energy') || 0,
  };

  // Remove zero-only metrics to keep signatures clean
  for (const [key, val] of Object.entries(metrics)) {
    if (val === 0) delete metrics[key];
  }

  // Workout count/duration
  const workoutResult = await pool.query(`
    SELECT COUNT(*) as count, COALESCE(SUM(value), 0) as total_minutes
    FROM aurora_unified_health
    WHERE date = $1 AND metric_type LIKE 'workout_%'
  `, [date]);
  const w = workoutResult.rows[0];
  if (parseInt(w?.count || '0') > 0) {
    metrics.workout_count = parseInt(w.count);
    metrics.workout_duration_minutes = Math.round(parseFloat(w.total_minutes) || 0);
  }

  logger.logDebug('Health metrics:', JSON.stringify(metrics));
  stop();
  return { dimension: 'health', metrics };
}

async function processCommunicationDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing communication dimension for', date);
  const stop = logger.time('communication-dimension');

  // Per-channel breakdown from unified communication view
  const msgResult = await pool.query(
    `SELECT
       COUNT(*) as total_messages,
       COUNT(*) FILTER (WHERE direction = 'sent') as sent,
       COUNT(*) FILTER (WHERE direction = 'received') as received,
       COUNT(DISTINCT contact_identifier) as unique_contacts,
       COUNT(DISTINCT channel) as channels_used,
       AVG(content_length) as avg_message_length,
       -- Per-channel counts
       COUNT(*) FILTER (WHERE channel = 'imessage') as imessage_count,
       COUNT(*) FILTER (WHERE channel = 'gmail') as email_count,
       COUNT(*) FILTER (WHERE channel = 'facebook') as facebook_count,
       COUNT(*) FILTER (WHERE channel = 'instagram') as instagram_count,
       COUNT(*) FILTER (WHERE channel = 'google_chat') as google_chat_count,
       COUNT(*) FILTER (WHERE channel = 'google_voice') as google_voice_text_count
     FROM aurora_unified_communication
     WHERE timestamp::date = $1`,
    [date]
  );

  // Phone calls + voice from unified calls view + FaceTime
  const callResult = await pool.query(
    `SELECT
       COUNT(*) as total_calls,
       COUNT(*) FILTER (WHERE channel = 'google_voice' AND direction = 'inbound') as calls_inbound,
       COUNT(*) FILTER (WHERE channel = 'google_voice' AND direction = 'outbound') as calls_outbound,
       COUNT(*) FILTER (WHERE channel = 'voicemail') as voicemails,
       COUNT(*) FILTER (WHERE channel = 'plaud') as plaud_recordings,
       COALESCE(SUM(duration_seconds) FILTER (WHERE channel = 'google_voice'), 0) as call_duration_seconds,
       COALESCE(SUM(duration_seconds) FILTER (WHERE channel = 'plaud'), 0) as plaud_duration_seconds,
       COUNT(DISTINCT contact_identifier) as call_contacts
     FROM aurora_unified_calls
     WHERE timestamp::date = $1`,
    [date]
  );

  // FaceTime calls (not in unified view)
  const facetimeResult = await pool.query(
    `SELECT COUNT(*) as facetime_calls,
       COALESCE(SUM(duration_seconds), 0) as facetime_duration_seconds,
       COUNT(DISTINCT from_handle) as facetime_contacts
     FROM aurora_raw_facetime
     WHERE date_created::date = $1`,
    [date]
  );

  const msg = msgResult.rows[0];
  const call = callResult.rows[0];
  const ft = facetimeResult.rows[0];
  const hasMessages = msg && msg.total_messages !== '0';
  const hasCalls = call && call.total_calls !== '0';
  const hasFacetime = ft && ft.facetime_calls !== '0';

  if (!hasMessages && !hasCalls && !hasFacetime) {
    logger.logVerbose('Communication dimension: no data for', date);
    stop();
    return null;
  }

  // Top contacts by message volume
  const topContacts = await pool.query(
    `SELECT contact_identifier, COUNT(*) as cnt
     FROM aurora_unified_communication WHERE timestamp::date = $1
     AND contact_identifier IS NOT NULL
     GROUP BY contact_identifier ORDER BY cnt DESC LIMIT 10`, [date]);

  // Response time estimate (avg gap between received → next sent for same contact)
  const responseResult = await pool.query(`
    WITH ordered AS (
      SELECT direction, timestamp,
        LEAD(timestamp) OVER (PARTITION BY contact_identifier ORDER BY timestamp) as next_ts,
        LEAD(direction) OVER (PARTITION BY contact_identifier ORDER BY timestamp) as next_dir
      FROM aurora_unified_communication
      WHERE timestamp::date = $1 AND contact_identifier IS NOT NULL
    )
    SELECT AVG(EXTRACT(EPOCH FROM next_ts - timestamp))::int as avg_response_seconds
    FROM ordered
    WHERE direction = 'received' AND next_dir = 'sent'
      AND EXTRACT(EPOCH FROM next_ts - timestamp) BETWEEN 1 AND 86400
  `, [date]);

  // Thread vs direct messages (proxy for group vs 1:1)
  const threadResult = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE thread_id IS NOT NULL) as threaded,
      COUNT(*) FILTER (WHERE thread_id IS NULL) as direct
    FROM aurora_unified_communication WHERE timestamp::date = $1
  `, [date]);

  const resp = responseResult.rows[0];
  const thr = threadResult.rows[0];

  const metrics: Record<string, unknown> = {
    // Totals
    total_messages: parseInt(msg?.total_messages || '0'),
    sent: parseInt(msg?.sent || '0'),
    received: parseInt(msg?.received || '0'),
    unique_contacts: parseInt(msg?.unique_contacts || '0'),
    channels_used: parseInt(msg?.channels_used || '0'),
    avg_message_length: parseFloat(msg?.avg_message_length || '0'),
    // Per-channel message counts
    imessage_count: parseInt(msg?.imessage_count || '0'),
    email_count: parseInt(msg?.email_count || '0'),
    facebook_count: parseInt(msg?.facebook_count || '0'),
    instagram_count: parseInt(msg?.instagram_count || '0'),
    google_chat_count: parseInt(msg?.google_chat_count || '0'),
    google_voice_text_count: parseInt(msg?.google_voice_text_count || '0'),
    // Calls & voice
    total_calls: parseInt(call?.total_calls || '0'),
    calls_inbound: parseInt(call?.calls_inbound || '0'),
    calls_outbound: parseInt(call?.calls_outbound || '0'),
    voicemails: parseInt(call?.voicemails || '0'),
    plaud_recordings: parseInt(call?.plaud_recordings || '0'),
    call_duration_seconds: parseInt(call?.call_duration_seconds || '0'),
    plaud_duration_seconds: parseInt(call?.plaud_duration_seconds || '0'),
    call_contacts: parseInt(call?.call_contacts || '0'),
    // FaceTime
    facetime_calls: parseInt(ft?.facetime_calls || '0'),
    facetime_duration_seconds: parseInt(ft?.facetime_duration_seconds || '0'),
    facetime_contacts: parseInt(ft?.facetime_contacts || '0'),
    // Response time & threading
    avg_response_seconds: parseInt(resp?.avg_response_seconds || '0'),
    threaded_messages: parseInt(thr?.threaded || '0'),
    direct_messages: parseInt(thr?.direct || '0'),
    // Context enrichments
    top_contacts: topContacts.rows.map(r => ({ name: r.contact_identifier, count: parseInt(r.cnt) })),
  };

  logger.logDebug('Communication metrics:', JSON.stringify(metrics));
  stop();

  return { dimension: 'communication', metrics };
}

async function processLocationDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing location dimension for', date);
  const stop = logger.time('location-dimension');

  const result = await pool.query(
    `SELECT
       COUNT(*) as location_points,
       COUNT(DISTINCT place_name) FILTER (WHERE place_name IS NOT NULL) as unique_places,
       COUNT(DISTINCT source) as sources
     FROM aurora_unified_location
     WHERE timestamp::date = $1`,
    [date]
  );

  const row = result.rows[0];
  if (!row || row.location_points === '0') {
    logger.logVerbose('Location dimension: no data for', date);
    stop();
    return null;
  }

  // Context: actual place names and source breakdown
  const places = await pool.query(
    `SELECT DISTINCT place_name FROM aurora_unified_location
     WHERE timestamp::date = $1 AND place_name IS NOT NULL LIMIT 20`, [date]);
  const srcBreak = await pool.query(
    `SELECT source, COUNT(*)::int as cnt FROM aurora_unified_location
     WHERE timestamp::date = $1 GROUP BY source ORDER BY cnt DESC`, [date]);

  // Total distance (haversine between sequential GPS points, guarded against jumps)
  const distResult = await pool.query(`
    WITH ordered AS (
      SELECT latitude, longitude,
        LAG(latitude) OVER (ORDER BY timestamp) as prev_lat,
        LAG(longitude) OVER (ORDER BY timestamp) as prev_lng
      FROM aurora_unified_location
      WHERE timestamp::date = $1 AND latitude IS NOT NULL
      ORDER BY timestamp
    )
    SELECT COALESCE(SUM(
      6371 * 2 * ASIN(SQRT(
        POWER(SIN(RADIANS(latitude - prev_lat) / 2), 2) +
        COS(RADIANS(prev_lat)) * COS(RADIANS(latitude)) *
        POWER(SIN(RADIANS(longitude - prev_lng) / 2), 2)
      ))
    ), 0)::real as total_distance_km
    FROM ordered
    WHERE prev_lat IS NOT NULL AND ABS(latitude - prev_lat) < 1
  `, [date]);

  const metrics: Record<string, unknown> = {
    location_points: parseInt(row.location_points),
    unique_places: parseInt(row.unique_places),
    sources: parseInt(row.sources),
    total_distance_km: Math.round((parseFloat(distResult.rows[0]?.total_distance_km || '0')) * 10) / 10,
    place_names: places.rows.map(p => p.place_name),
    source_breakdown: Object.fromEntries(srcBreak.rows.map(s => [s.source, s.cnt])),
  };

  logger.logDebug('Location metrics:', JSON.stringify(metrics));
  stop();

  return { dimension: 'location', metrics };
}

async function processScheduleDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing schedule dimension for', date);
  const stop = logger.time('schedule-dimension');

  const result = await pool.query(
    `SELECT
       COUNT(*) as events,
       COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM start_date) BETWEEN 9 AND 17) as work_hours_events,
       COUNT(*) FILTER (WHERE is_all_day = true) as all_day_events,
       COUNT(DISTINCT calendar_name) as calendars_used,
       COALESCE(SUM(EXTRACT(EPOCH FROM (end_date - start_date)) / 3600) FILTER (WHERE NOT is_all_day AND end_date IS NOT NULL), 0)::real as meeting_hours,
       COALESCE(AVG(COALESCE(jsonb_array_length(attendees), 0)), 0)::real as avg_attendees
     FROM device_calendar_events
     WHERE start_date::date = $1`,
    [date]
  );

  // Back-to-back events (starting within 15 min of previous ending)
  const densityResult = await pool.query(`
    WITH ordered AS (
      SELECT start_date, end_date,
        LAG(end_date) OVER (ORDER BY start_date) as prev_end
      FROM device_calendar_events
      WHERE start_date::date = $1 AND NOT is_all_day
    )
    SELECT COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (start_date - prev_end)) BETWEEN 0 AND 900) as back_to_back
    FROM ordered WHERE prev_end IS NOT NULL
  `, [date]);

  // Reminders for the day
  const remResult = await pool.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE is_completed) as completed
    FROM device_reminders
    WHERE (due_date::date = $1 OR completion_date::date = $1)
  `, [date]);

  const row = result.rows[0];
  const meetingHours = parseFloat(row.meeting_hours) || 0;
  const metrics: Record<string, number> = {
    events: parseInt(row.events) || 0,
    work_hours_events: parseInt(row.work_hours_events) || 0,
    all_day_events: parseInt(row.all_day_events) || 0,
    calendars_used: parseInt(row.calendars_used) || 0,
    total_meeting_hours: Math.round(meetingHours * 10) / 10,
    avg_attendees: Math.round(parseFloat(row.avg_attendees) * 10) / 10 || 0,
    back_to_back_count: parseInt(densityResult.rows[0]?.back_to_back || '0'),
    free_hours: Math.round(Math.max(0, 9 - meetingHours) * 10) / 10,
    reminder_count: parseInt(remResult.rows[0]?.total || '0'),
    reminders_completed: parseInt(remResult.rows[0]?.completed || '0'),
  };

  logger.logDebug('Schedule metrics:', JSON.stringify(metrics));
  stop();

  return { dimension: 'schedule', metrics };
}

async function processSearchDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing search_behavior dimension for', date);
  const stop = logger.time('search-dimension');

  const result = await pool.query(`
    SELECT
      COUNT(*) as total_searches,
      COUNT(*) FILTER (WHERE activity_type = 'search') as google_searches,
      COUNT(*) FILTER (WHERE activity_type = 'maps') as maps_searches,
      COUNT(*) FILTER (WHERE activity_type = 'search' AND title LIKE '%?%') as question_searches
    FROM aurora_raw_google_activity
    WHERE timestamp::date = $1 AND activity_type IN ('search', 'maps')
  `, [date]);

  const row = result.rows[0];
  if (!row || row.total_searches === '0') {
    logger.logVerbose('Search dimension: no data for', date);
    stop();
    return null;
  }

  // Time-of-day distribution
  const todResult = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp) BETWEEN 6 AND 11) as morning,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp) BETWEEN 12 AND 17) as afternoon,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp) BETWEEN 18 AND 22) as evening,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp) < 6 OR EXTRACT(HOUR FROM timestamp) > 22) as night
    FROM aurora_raw_google_activity
    WHERE timestamp::date = $1 AND activity_type IN ('search', 'maps')
  `, [date]);

  // Repeat search detection
  const repeatResult = await pool.query(`
    SELECT COUNT(*) as repeat_count FROM (
      SELECT title FROM aurora_raw_google_activity
      WHERE timestamp::date = $1 AND activity_type = 'search' AND title IS NOT NULL
      GROUP BY title HAVING COUNT(*) > 1
    ) sub
  `, [date]);

  // Sample searches for context
  const sampleSearches = await pool.query(
    `SELECT title FROM aurora_raw_google_activity
     WHERE timestamp::date = $1 AND activity_type = 'search' AND title IS NOT NULL
     ORDER BY timestamp DESC LIMIT 20`, [date]);

  const tod = todResult.rows[0];
  const metrics: Record<string, unknown> = {
    total_searches: parseInt(row.total_searches),
    google_searches: parseInt(row.google_searches),
    maps_searches: parseInt(row.maps_searches),
    question_searches: parseInt(row.question_searches),
    repeat_searches: parseInt(repeatResult.rows[0]?.repeat_count || '0'),
    search_morning: parseInt(tod?.morning || '0'),
    search_afternoon: parseInt(tod?.afternoon || '0'),
    search_evening: parseInt(tod?.evening || '0'),
    search_night: parseInt(tod?.night || '0'),
    sample_searches: sampleSearches.rows.map(r => r.title),
  };

  logger.logDebug('Search metrics:', JSON.stringify(metrics));
  stop();
  return { dimension: 'search_behavior', metrics };
}

// ── Content consumption dimension ─────────────────────────────
async function processContentConsumptionDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing content_consumption dimension for', date);
  const stop = logger.time('content-consumption-dimension');

  const result = await pool.query(`
    SELECT
      COUNT(*) as total_views,
      COUNT(*) FILTER (WHERE activity_type IN ('youtube', 'youtube.com')) as youtube_views,
      COUNT(*) FILTER (WHERE activity_type = 'image_search') as image_searches,
      COUNT(*) FILTER (WHERE activity_type = 'discover') as discover_views,
      COUNT(*) FILTER (WHERE activity_type = 'gemini_apps') as gemini_interactions
    FROM aurora_raw_google_activity
    WHERE timestamp::date = $1
      AND activity_type IN ('youtube', 'youtube.com', 'image_search', 'discover', 'gemini_apps')
  `, [date]);

  const row = result.rows[0];
  if (!row || row.total_views === '0') {
    logger.logVerbose('Content consumption: no data for', date);
    stop();
    return null;
  }

  const todResult = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp) BETWEEN 6 AND 11) as morning,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp) BETWEEN 12 AND 17) as afternoon,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp) BETWEEN 18 AND 22) as evening,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp) < 6 OR EXTRACT(HOUR FROM timestamp) > 22) as night
    FROM aurora_raw_google_activity
    WHERE timestamp::date = $1
      AND activity_type IN ('youtube', 'youtube.com', 'image_search', 'discover', 'gemini_apps')
  `, [date]);

  const sampleTitles = await pool.query(`
    SELECT title FROM aurora_raw_google_activity
    WHERE timestamp::date = $1 AND activity_type IN ('youtube', 'youtube.com') AND title IS NOT NULL
    ORDER BY timestamp DESC LIMIT 20
  `, [date]);

  const tod = todResult.rows[0];
  const metrics: Record<string, unknown> = {
    total_views: parseInt(row.total_views),
    youtube_views: parseInt(row.youtube_views),
    image_searches: parseInt(row.image_searches),
    discover_views: parseInt(row.discover_views),
    gemini_interactions: parseInt(row.gemini_interactions),
    content_morning: parseInt(tod?.morning || '0'),
    content_afternoon: parseInt(tod?.afternoon || '0'),
    content_evening: parseInt(tod?.evening || '0'),
    content_night: parseInt(tod?.night || '0'),
    sample_titles: sampleTitles.rows.map(r => r.title),
  };

  logger.logDebug('Content consumption metrics:', JSON.stringify(metrics));
  stop();
  return { dimension: 'content_consumption', metrics };
}

// ── Photos dimension ─────────────────────────────────────────
async function processPhotosDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing photos dimension for', date);
  const stop = logger.time('photos-dimension');
  const result = await pool.query(`
    SELECT COUNT(*) as photos_taken,
      COUNT(*) FILTER (WHERE latitude IS NOT NULL) as with_location,
      COUNT(*) FILTER (WHERE description IS NOT NULL) as with_description,
      COUNT(DISTINCT albums[1]) FILTER (WHERE albums IS NOT NULL) as unique_albums
    FROM photo_metadata WHERE creation_date::date = $1`, [date]);
  const faces = await pool.query(`
    SELECT COUNT(*) as faces_detected, COUNT(DISTINCT person_name) FILTER (WHERE person_name IS NOT NULL) as unique_people
    FROM aurora_raw_photo_faces pf JOIN photo_metadata pm ON pm.local_identifier = pf.asset_uuid
    WHERE pm.creation_date::date = $1`, [date]);
  const r = result.rows[0]; const f = faces.rows[0];
  if (!r || r.photos_taken === '0') { logger.logVerbose('Photos: no data for', date); stop(); return null; }
  const metrics = {
    photos_taken: parseInt(r.photos_taken), with_location: parseInt(r.with_location),
    with_description: parseInt(r.with_description), unique_albums: parseInt(r.unique_albums) || 0,
    faces_detected: parseInt(f?.faces_detected || '0'), unique_people: parseInt(f?.unique_people || '0'),
  };
  logger.logDebug('Photos metrics:', JSON.stringify(metrics)); stop();
  return { dimension: 'photos', metrics };
}

// ── Social media dimension ───────────────────────────────────
async function processSocialMediaDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing social_media dimension for', date);
  const stop = logger.time('social-media-dimension');
  const fb = await pool.query(`
    SELECT COUNT(*) FILTER (WHERE data_type = 'post') as fb_posts,
      COUNT(*) FILTER (WHERE data_type = 'comment') as fb_comments,
      COUNT(*) FILTER (WHERE data_type = 'message') as fb_messages,
      COUNT(*) FILTER (WHERE data_type = 'ad_interaction') as fb_ads
    FROM aurora_raw_facebook WHERE timestamp::date = $1`, [date]);
  const ig = await pool.query(`
    SELECT COUNT(*) FILTER (WHERE data_type = 'like') as ig_likes,
      COUNT(*) FILTER (WHERE data_type = 'message') as ig_messages,
      COUNT(*) FILTER (WHERE data_type = 'follower') as ig_followers
    FROM aurora_raw_instagram WHERE timestamp::date = $1`, [date]);
  const f = fb.rows[0]; const i = ig.rows[0];
  const total = parseInt(f?.fb_posts||'0') + parseInt(f?.fb_comments||'0') + parseInt(i?.ig_likes||'0') + parseInt(i?.ig_messages||'0');
  if (total === 0) { logger.logVerbose('Social media: no data for', date); stop(); return null; }
  const metrics = {
    facebook_posts: parseInt(f.fb_posts), facebook_comments: parseInt(f.fb_comments),
    facebook_messages: parseInt(f.fb_messages), facebook_ad_interactions: parseInt(f.fb_ads),
    instagram_likes: parseInt(i.ig_likes), instagram_messages: parseInt(i.ig_messages),
    instagram_followers: parseInt(i.ig_followers),
  };
  logger.logDebug('Social media metrics:', JSON.stringify(metrics)); stop();
  return { dimension: 'social_media', metrics };
}

// ── Fitness dimension ────────────────────────────────────────
async function processFitnessDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing fitness dimension for', date);
  const stop = logger.time('fitness-dimension');
  const strava = await pool.query(`
    SELECT COUNT(*) as activities, SUM(distance_meters) as total_distance,
      SUM(elapsed_time_seconds) as total_time, array_agg(DISTINCT activity_type) as types
    FROM aurora_raw_strava_activities WHERE activity_date::date = $1`, [date]);
  const workouts = await pool.query(`
    SELECT COUNT(*) as count FROM aurora_raw_health_workouts WHERE start_date::date = $1`, [date]);
  const rings = await pool.query(`
    SELECT active_energy_burned, active_energy_burned_goal, apple_exercise_time, apple_exercise_time_goal,
      apple_stand_hours, apple_stand_hours_goal
    FROM aurora_raw_health_summaries WHERE date_components::date = $1 LIMIT 1`, [date]);
  const s = strava.rows[0]; const wk = workouts.rows[0]; const rg = rings.rows[0];
  const hasData = parseInt(s?.activities||'0') > 0 || parseInt(wk?.count||'0') > 0 || rg;
  if (!hasData) { logger.logVerbose('Fitness: no data for', date); stop(); return null; }
  const metrics: Record<string, number> = {
    strava_activities: parseInt(s?.activities || '0'),
    strava_distance_meters: parseFloat(s?.total_distance || '0'),
    strava_duration_seconds: parseInt(s?.total_time || '0'),
    health_workouts: parseInt(wk?.count || '0'),
  };
  if (rg) {
    metrics.ring_active_energy = parseFloat(rg.active_energy_burned || '0');
    metrics.ring_active_energy_goal = parseFloat(rg.active_energy_burned_goal || '0');
    metrics.ring_exercise_minutes = parseFloat(rg.apple_exercise_time || '0');
    metrics.ring_exercise_goal = parseFloat(rg.apple_exercise_time_goal || '0');
    metrics.ring_stand_hours = parseFloat(rg.apple_stand_hours || '0');
    metrics.ring_stand_goal = parseFloat(rg.apple_stand_hours_goal || '0');
  }
  logger.logDebug('Fitness metrics:', JSON.stringify(metrics)); stop();
  return { dimension: 'fitness', metrics };
}

// ── Music dimension ──────────────────────────────────────────
async function processMusicDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing music dimension for', date);
  const stop = logger.time('music-dimension');
  const result = await pool.query(`
    SELECT COUNT(*) as tracks_played, COUNT(DISTINCT artist_name) as unique_artists,
      COUNT(DISTINCT album_title) as unique_albums, SUM(duration_ms) as total_duration_ms,
      COUNT(DISTINCT genre) as unique_genres
    FROM device_music WHERE played_at::date = $1`, [date]);
  const r = result.rows[0];
  if (!r || r.tracks_played === '0') { logger.logVerbose('Music: no data for', date); stop(); return null; }

  const todResult = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM played_at) BETWEEN 6 AND 11) as morning,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM played_at) BETWEEN 12 AND 17) as afternoon,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM played_at) BETWEEN 18 AND 22) as evening,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM played_at) < 6 OR EXTRACT(HOUR FROM played_at) > 22) as night
    FROM device_music WHERE played_at::date = $1
  `, [date]);
  const tod = todResult.rows[0];

  const metrics = {
    tracks_played: parseInt(r.tracks_played), unique_artists: parseInt(r.unique_artists),
    unique_albums: parseInt(r.unique_albums), unique_genres: parseInt(r.unique_genres),
    total_duration_seconds: Math.round(parseFloat(r.total_duration_ms || '0') / 1000),
    listen_morning: parseInt(tod?.morning || '0'), listen_afternoon: parseInt(tod?.afternoon || '0'),
    listen_evening: parseInt(tod?.evening || '0'), listen_night: parseInt(tod?.night || '0'),
  };
  logger.logDebug('Music metrics:', JSON.stringify(metrics)); stop();
  return { dimension: 'music', metrics };
}

// ── Voice conversations dimension ────────────────────────────
async function processVoiceDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing voice dimension for', date);
  const stop = logger.time('voice-dimension');
  const plaud = await pool.query(`
    SELECT COUNT(*) as recordings, SUM(word_count) as total_words,
      SUM(duration_seconds) as total_duration,
      COUNT(DISTINCT s) as unique_speakers
    FROM aurora_raw_plaud_recordings, unnest(speakers) s
    WHERE start_time::date = $1`, [date]);
  const looki = await pool.query(`
    SELECT COUNT(*) as moments FROM looki_moments WHERE start_time::date = $1`, [date]);
  const p = plaud.rows[0]; const l = looki.rows[0];
  const total = parseInt(p?.recordings||'0') + parseInt(l?.moments||'0');
  if (total === 0) { logger.logVerbose('Voice: no data for', date); stop(); return null; }
  // Context: speaker names and topics from Plaud outlines
  const speakers = await pool.query(
    `SELECT DISTINCT unnest(speakers) as name FROM aurora_raw_plaud_recordings
     WHERE start_time::date = $1 AND speakers IS NOT NULL`, [date]);
  const topics = await pool.query(
    `SELECT outline FROM aurora_raw_plaud_recordings
     WHERE start_time::date = $1 AND outline IS NOT NULL`, [date]);
  const topicList: string[] = [];
  for (const row of topics.rows) {
    try {
      const outline = typeof row.outline === 'string' ? JSON.parse(row.outline) : row.outline;
      if (Array.isArray(outline)) {
        for (const item of outline) {
          if (item.topic && !topicList.includes(item.topic)) topicList.push(item.topic);
        }
      }
    } catch { /* skip malformed outlines */ }
  }

  const metrics: Record<string, unknown> = {
    plaud_recordings: parseInt(p?.recordings || '0'), plaud_words: parseInt(p?.total_words || '0'),
    plaud_duration_seconds: parseInt(p?.total_duration || '0'), plaud_speakers: parseInt(p?.unique_speakers || '0'),
    looki_moments: parseInt(l?.moments || '0'),
    speaker_names: speakers.rows.map(r => r.name),
    meeting_topics: topicList.slice(0, 30),
  };
  logger.logDebug('Voice metrics:', JSON.stringify(metrics)); stop();
  return { dimension: 'voice', metrics };
}

// ── Digital activity dimension ───────────────────────────────
async function processDigitalActivityDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing digital_activity dimension for', date);
  const stop = logger.time('digital-activity-dimension');
  const kc = await pool.query(`
    SELECT COUNT(*) as app_events, COUNT(DISTINCT bundle_id) as unique_apps,
      SUM(duration_seconds) as total_usage_seconds, COUNT(DISTINCT stream) as streams
    FROM aurora_raw_apple_knowledge WHERE event_start::date = $1`, [date]);
  const safari = await pool.query(`
    SELECT COUNT(*) as web_visits, COUNT(DISTINCT domain) as unique_domains
    FROM aurora_raw_safari_history WHERE visit_time::date = $1`, [date]);
  const siri = await pool.query(`
    SELECT COUNT(*) as siri_interactions, COUNT(DISTINCT domain) as siri_domains,
      COUNT(DISTINCT bundle_id) as siri_apps,
      COALESCE(SUM(duration_seconds), 0) as siri_duration_seconds
    FROM aurora_raw_siri_interactions WHERE start_date::date = $1`, [date]);
  const k = kc.rows[0]; const sa = safari.rows[0]; const si = siri.rows[0];
  const total = parseInt(k?.app_events||'0') + parseInt(sa?.web_visits||'0') + parseInt(si?.siri_interactions||'0');
  if (total === 0) { logger.logVerbose('Digital activity: no data for', date); stop(); return null; }
  // Context: top apps by usage and top domains
  const topApps = await pool.query(
    `SELECT bundle_id, SUM(duration_seconds)::int as usage_sec
     FROM aurora_raw_apple_knowledge WHERE event_start::date = $1 AND bundle_id IS NOT NULL
     GROUP BY bundle_id ORDER BY usage_sec DESC LIMIT 10`, [date]);
  const topDomains = await pool.query(
    `SELECT domain, COUNT(*)::int as visits
     FROM aurora_raw_safari_history WHERE visit_time::date = $1 AND domain IS NOT NULL
     GROUP BY domain ORDER BY visits DESC LIMIT 10`, [date]);

  // App category breakdown (by bundle_id prefix)
  const categoryResult = await pool.query(`
    SELECT
      CASE
        WHEN bundle_id LIKE 'com.apple.%' THEN 'system'
        WHEN bundle_id LIKE 'com.google.%' THEN 'google'
        WHEN bundle_id LIKE 'com.facebook.%' OR bundle_id LIKE 'com.burbn.%' THEN 'social'
        WHEN bundle_id LIKE 'com.microsoft.%' THEN 'productivity'
        ELSE 'other'
      END as category,
      COUNT(*) as events, SUM(duration_seconds)::int as usage_seconds
    FROM aurora_raw_apple_knowledge
    WHERE event_start::date = $1 AND bundle_id IS NOT NULL AND stream = '/app/usage'
    GROUP BY category ORDER BY usage_seconds DESC
  `, [date]);

  // First and last app of the day
  const bookendResult = await pool.query(`
    SELECT
      (SELECT bundle_id FROM aurora_raw_apple_knowledge
       WHERE event_start::date = $1 AND bundle_id IS NOT NULL AND stream = '/app/usage'
       ORDER BY event_start ASC LIMIT 1) as first_app,
      (SELECT bundle_id FROM aurora_raw_apple_knowledge
       WHERE event_start::date = $1 AND bundle_id IS NOT NULL AND stream = '/app/usage'
       ORDER BY event_start DESC LIMIT 1) as last_app
  `, [date]);
  const bk = bookendResult.rows[0];

  const metrics: Record<string, unknown> = {
    app_events: parseInt(k?.app_events || '0'), unique_apps: parseInt(k?.unique_apps || '0'),
    app_usage_seconds: parseFloat(k?.total_usage_seconds || '0'), app_streams: parseInt(k?.streams || '0'),
    web_visits: parseInt(sa?.web_visits || '0'), unique_domains: parseInt(sa?.unique_domains || '0'),
    first_app_of_day: bk?.first_app || null,
    last_app_of_day: bk?.last_app || null,
    top_apps: topApps.rows.map(r => ({ app: r.bundle_id, seconds: r.usage_sec })),
    top_domains: topDomains.rows.map(r => ({ domain: r.domain, visits: r.visits })),
    app_categories: Object.fromEntries(categoryResult.rows.map(r => [r.category, { events: parseInt(r.events), seconds: r.usage_seconds }])),
    // Siri
    siri_interactions: parseInt(si?.siri_interactions || '0'),
    siri_domains: parseInt(si?.siri_domains || '0'),
    siri_apps: parseInt(si?.siri_apps || '0'),
    siri_duration_seconds: parseFloat(si?.siri_duration_seconds || '0'),
  };
  logger.logDebug('Digital activity metrics:', JSON.stringify(metrics)); stop();
  return { dimension: 'digital_activity', metrics };
}

// ── ARIA meta signals dimension ──────────────────────────────
async function processAriaMetaDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing aria_meta dimension for', date);
  const stop = logger.time('aria-meta-dimension');
  const result = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM aria_journal WHERE created_at::date = $1) as journal_entries,
      (SELECT COUNT(*) FROM life_narration WHERE created_at::date = $1) as narrations,
      (SELECT COUNT(*) FROM proactive_insights WHERE created_at::date = $1) as insights,
      (SELECT COUNT(*) FROM conversations WHERE created_at::date = $1) as conversations,
      (SELECT COUNT(*) FROM messages WHERE created_at::date = $1) as messages,
      (SELECT COUNT(*) FROM llm_usage WHERE created_at::date = $1) as llm_calls,
      (SELECT COALESCE(SUM(estimated_cost_cents), 0) FROM llm_usage WHERE created_at::date = $1) as llm_cost_cents,
      (SELECT COUNT(*) FROM event_log WHERE created_at::date = $1) as tool_actions
  `, [date]);
  const r = result.rows[0];
  const total = parseInt(r?.conversations||'0') + parseInt(r?.llm_calls||'0') + parseInt(r?.journal_entries||'0');
  if (total === 0) { logger.logVerbose('ARIA meta: no data for', date); stop(); return null; }
  const metrics = {
    journal_entries: parseInt(r.journal_entries), narrations: parseInt(r.narrations),
    proactive_insights: parseInt(r.insights), conversations: parseInt(r.conversations),
    messages_exchanged: parseInt(r.messages), llm_calls: parseInt(r.llm_calls),
    llm_cost_cents: parseFloat(r.llm_cost_cents), tool_actions: parseInt(r.tool_actions),
  };
  logger.logDebug('ARIA meta metrics:', JSON.stringify(metrics)); stop();
  return { dimension: 'aria_meta', metrics };
}

// ── Motion/Activity dimension ────────────────────────────────
async function processMotionDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing motion dimension for', date);
  const stop = logger.time('motion-dimension');
  const result = await pool.query(`
    SELECT COUNT(*) as events,
      AVG(CASE WHEN stationary THEN 1.0 ELSE 0.0 END) as stationary_pct,
      AVG(CASE WHEN walking THEN 1.0 ELSE 0.0 END) as walking_pct,
      AVG(CASE WHEN running THEN 1.0 ELSE 0.0 END) as running_pct,
      AVG(CASE WHEN automotive THEN 1.0 ELSE 0.0 END) as automotive_pct,
      AVG(CASE WHEN cycling THEN 1.0 ELSE 0.0 END) as cycling_pct
    FROM device_activity WHERE timestamp::date = $1`, [date]);
  const r = result.rows[0];
  if (!r || r.events === '0') { logger.logVerbose('Motion: no data for', date); stop(); return null; }
  const metrics = {
    motion_events: parseInt(r.events),
    stationary_pct: Math.round(parseFloat(r.stationary_pct || '0') * 100),
    walking_pct: Math.round(parseFloat(r.walking_pct || '0') * 100),
    running_pct: Math.round(parseFloat(r.running_pct || '0') * 100),
    automotive_pct: Math.round(parseFloat(r.automotive_pct || '0') * 100),
    cycling_pct: Math.round(parseFloat(r.cycling_pct || '0') * 100),
  };
  logger.logDebug('Motion metrics:', JSON.stringify(metrics)); stop();
  return { dimension: 'motion', metrics };
}

// All dimension processors in order
const DIMENSION_PROCESSORS: DimensionProcessor[] = [
  processHealthDimension,
  processCommunicationDimension,
  processLocationDimension,
  processScheduleDimension,
  processSearchDimension,
  processContentConsumptionDimension,
  processPhotosDimension,
  processSocialMediaDimension,
  processFitnessDimension,
  processMusicDimension,
  processVoiceDimension,
  processDigitalActivityDimension,
  processAriaMetaDimension,
  processMotionDimension,
  processWeatherDimension,
  processAiAssistantDimension,
];

// ── AI assistant dimension (ChatGPT + Claude) ──────────────
async function processAiAssistantDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing ai_assistant dimension for', date);
  const stop = logger.time('ai-assistant-dimension');

  // Unified query across both ChatGPT and Claude using UNION ALL
  const result = await pool.query(`
    WITH unified AS (
      SELECT 'chatgpt' as provider, message_role as role, content_text,
             created_at, has_attachments, model_slug, conversation_id,
             conversation_title as conv_name, 0 as tool_count_val
      FROM aurora_raw_chatgpt WHERE created_at::date = $1
      UNION ALL
      SELECT 'claude', sender, content_text,
             created_at, has_files, NULL, conversation_id,
             conversation_name, tool_count
      FROM aurora_raw_claude WHERE created_at::date = $1
    )
    SELECT
      COUNT(*) as total_messages,
      COUNT(*) FILTER (WHERE role IN ('user', 'human')) as user_messages,
      COUNT(*) FILTER (WHERE role = 'assistant') as assistant_messages,
      COUNT(*) FILTER (WHERE role = 'tool') + COALESCE(SUM(tool_count_val) FILTER (WHERE provider = 'claude'), 0) as tool_calls,
      COUNT(DISTINCT conversation_id) as conversations,
      COUNT(DISTINCT model_slug) FILTER (WHERE model_slug IS NOT NULL) as models_used,
      COUNT(*) FILTER (WHERE has_attachments) as messages_with_attachments,
      AVG(LENGTH(content_text)) FILTER (WHERE role IN ('user', 'human') AND content_text IS NOT NULL) as avg_user_message_length,
      AVG(LENGTH(content_text)) FILTER (WHERE role = 'assistant' AND content_text IS NOT NULL) as avg_assistant_response_length,
      -- Per-provider breakdown
      COUNT(*) FILTER (WHERE provider = 'chatgpt') as chatgpt_messages,
      COUNT(*) FILTER (WHERE provider = 'claude') as claude_messages,
      COUNT(DISTINCT conversation_id) FILTER (WHERE provider = 'chatgpt') as chatgpt_conversations,
      COUNT(DISTINCT conversation_id) FILTER (WHERE provider = 'claude') as claude_conversations
    FROM unified
  `, [date]);

  const row = result.rows[0];
  if (!row || row.total_messages === '0') {
    logger.logVerbose('AI assistant: no data for', date);
    stop();
    return null;
  }

  // Model breakdown (ChatGPT has model_slug, Claude is always "claude")
  const modelBreakdown = await pool.query(`
    SELECT model, COUNT(*) as cnt FROM (
      SELECT model_slug as model FROM aurora_raw_chatgpt
      WHERE created_at::date = $1 AND message_role = 'assistant' AND model_slug IS NOT NULL
      UNION ALL
      SELECT 'claude' FROM aurora_raw_claude
      WHERE created_at::date = $1 AND sender = 'assistant'
    ) m GROUP BY model ORDER BY cnt DESC LIMIT 10
  `, [date]);

  // Tool names from Claude (ChatGPT doesn't expose tool names in the export)
  const toolNames = await pool.query(`
    SELECT unnest(tool_names) as tool, COUNT(*) as cnt
    FROM aurora_raw_claude
    WHERE created_at::date = $1 AND tool_names IS NOT NULL
    GROUP BY tool ORDER BY cnt DESC LIMIT 15
  `, [date]);

  // Time-of-day distribution (user messages from both)
  const todResult = await pool.query(`
    WITH user_msgs AS (
      SELECT created_at FROM aurora_raw_chatgpt WHERE created_at::date = $1 AND message_role = 'user'
      UNION ALL
      SELECT created_at FROM aurora_raw_claude WHERE created_at::date = $1 AND sender = 'human'
    )
    SELECT
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM created_at) BETWEEN 6 AND 11) as morning,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM created_at) BETWEEN 12 AND 17) as afternoon,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM created_at) BETWEEN 18 AND 23) as evening,
      COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM created_at) < 6) as night
    FROM user_msgs
  `, [date]);
  const tod = todResult.rows[0];

  // Conversation topics from both sources
  const titles = await pool.query(`
    SELECT DISTINCT name FROM (
      SELECT conversation_title as name FROM aurora_raw_chatgpt
      WHERE created_at::date = $1 AND conversation_title IS NOT NULL
      UNION
      SELECT conversation_name FROM aurora_raw_claude
      WHERE created_at::date = $1 AND conversation_name IS NOT NULL
    ) t ORDER BY name LIMIT 20
  `, [date]);

  const metrics: Record<string, unknown> = {
    total_messages: parseInt(row.total_messages),
    user_messages: parseInt(row.user_messages),
    assistant_messages: parseInt(row.assistant_messages),
    tool_calls: parseInt(row.tool_calls),
    conversations: parseInt(row.conversations),
    models_used: parseInt(row.models_used),
    messages_with_attachments: parseInt(row.messages_with_attachments),
    avg_user_message_length: Math.round(parseFloat(row.avg_user_message_length || '0')),
    avg_assistant_response_length: Math.round(parseFloat(row.avg_assistant_response_length || '0')),
    // Per-provider
    chatgpt_messages: parseInt(row.chatgpt_messages),
    claude_messages: parseInt(row.claude_messages),
    chatgpt_conversations: parseInt(row.chatgpt_conversations),
    claude_conversations: parseInt(row.claude_conversations),
    // Time-of-day
    usage_morning: parseInt(tod?.morning || '0'),
    usage_afternoon: parseInt(tod?.afternoon || '0'),
    usage_evening: parseInt(tod?.evening || '0'),
    usage_night: parseInt(tod?.night || '0'),
    // Context enrichments
    model_breakdown: modelBreakdown.rows.map(r => ({ model: r.model, count: parseInt(r.cnt) })),
    tool_names: toolNames.rows.map(r => ({ tool: r.tool, count: parseInt(r.cnt) })),
    conversation_topics: titles.rows.map(r => r.name),
  };

  logger.logDebug('AI assistant metrics:', JSON.stringify(metrics));
  stop();
  return { dimension: 'ai_assistant', metrics };
}

// ── Weather dimension ────────────────────────────────────────
async function processWeatherDimension(pool: Pool, date: string): Promise<DimensionResult | null> {
  logger.logVerbose('Processing weather dimension for', date);
  const stop = logger.time('weather-dimension');
  const result = await pool.query(`
    SELECT temp_high_f, temp_low_f, precipitation_in, humidity_pct, conditions
    FROM aurora_context_weather WHERE date = $1 LIMIT 1
  `, [date]);
  const r = result.rows[0];
  if (!r) { logger.logVerbose('Weather: no data for', date); stop(); return null; }
  // Approximate daylight hours from latitude + day of year
  const d = new Date(date + 'T12:00:00Z');
  const dayOfYear = Math.floor((d.getTime() - new Date(d.getUTCFullYear(), 0, 0).getTime()) / 86400000);
  const lat = 0; // Set to your latitude
  const declination = 23.45 * Math.sin((2 * Math.PI / 365) * (dayOfYear - 81));
  const hourAngle = Math.acos(-Math.tan(lat * Math.PI / 180) * Math.tan(declination * Math.PI / 180));
  const daylightHours = Math.round((2 * hourAngle * 12 / Math.PI) * 10) / 10;

  const metrics = {
    temp_high_f: parseFloat(r.temp_high_f) || 0,
    temp_low_f: parseFloat(r.temp_low_f) || 0,
    temp_avg_f: Math.round(((parseFloat(r.temp_high_f) || 0) + (parseFloat(r.temp_low_f) || 0)) / 2 * 10) / 10,
    precipitation_in: parseFloat(r.precipitation_in) || 0,
    humidity_pct: parseFloat(r.humidity_pct) || 0,
    is_rainy: (parseFloat(r.precipitation_in) || 0) > 0.1 ? 1 : 0,
    daylight_hours: daylightHours,
  };
  logger.logDebug('Weather metrics:', JSON.stringify(metrics)); stop();
  return { dimension: 'weather', metrics };
}

// ── Baseline & deviation system ──────────────────────────────

async function computeBaselinesAndDeviations(
  pool: Pool,
  date: string,
  dimensions: DimensionResult[]
): Promise<{ computed: number; skipped: number }> {
  let computed = 0;
  let skipped = 0;

  for (const dim of dimensions) {
    const stop = logger.time(`baseline-${dim.dimension}`);

    // Fetch last 30 days of this dimension (before target date)
    const historyResult = await pool.query(
      `SELECT metrics FROM aurora_signatures
       WHERE dimension = $1 AND date < $2 AND date >= ($2::date - INTERVAL '30 days')
       ORDER BY date DESC`,
      [dim.dimension, date]
    );

    if (historyResult.rows.length < 7) {
      logger.logVerbose(`Baseline: skipping ${dim.dimension}, only ${historyResult.rows.length} days of history`);
      skipped++;
      stop();
      continue;
    }

    // Identify numeric keys from today's metrics
    const numericKeys: string[] = [];
    for (const [key, val] of Object.entries(dim.metrics)) {
      if (typeof val === 'number') numericKeys.push(key);
    }

    if (numericKeys.length === 0) {
      skipped++;
      stop();
      continue;
    }

    // Compute mean and stddev for each numeric key
    const baseline: Record<string, { mean: number; stddev: number; n: number }> = {};
    const deviation: Record<string, { value: number; z_score: number; pct_change: number }> = {};

    for (const key of numericKeys) {
      const values: number[] = [];
      for (const row of historyResult.rows) {
        const m = typeof row.metrics === 'string' ? JSON.parse(row.metrics) : row.metrics;
        if (m[key] !== undefined && typeof m[key] === 'number') {
          values.push(m[key]);
        }
      }

      if (values.length < 7) continue;

      const mean = values.reduce((a, b) => a + b, 0) / values.length;
      const variance = values.reduce((a, b) => a + (b - mean) ** 2, 0) / values.length;
      const stddev = Math.sqrt(variance);

      baseline[key] = {
        mean: Math.round(mean * 100) / 100,
        stddev: Math.round(stddev * 100) / 100,
        n: values.length,
      };

      const todayVal = dim.metrics[key] as number;
      const zScore = stddev > 0 ? (todayVal - mean) / stddev : 0;
      const pctChange = mean !== 0 ? ((todayVal - mean) / mean) * 100 : 0;

      deviation[key] = {
        value: todayVal,
        z_score: Math.round(zScore * 100) / 100,
        pct_change: Math.round(pctChange * 100) / 100,
      };
    }

    if (Object.keys(baseline).length === 0) {
      skipped++;
      stop();
      continue;
    }

    // UPDATE the signature row with baseline and deviation
    await pool.query(
      `UPDATE aurora_signatures
       SET baseline = $1, deviation_from_baseline = $2
       WHERE date = $3 AND dimension = $4`,
      [JSON.stringify(baseline), JSON.stringify(deviation), date, dim.dimension]
    );

    computed++;
    logger.logVerbose(`Baseline: ${dim.dimension} — ${Object.keys(baseline).length} metrics baselined`);
    stop();
  }

  return { computed, skipped };
}

// ── Anomaly detection ────────────────────────────────────────

interface AnomalyResult {
  detected: number;
  low: number;
  medium: number;
  high: number;
  critical: number;
  compound: number;
  resolved: number;
  escalated: number;
}

function classifySeverity(zScore: number): 'low' | 'medium' | 'high' | 'critical' {
  const abs = Math.abs(zScore);
  if (abs >= 4) return 'critical';
  if (abs >= 3) return 'high';
  if (abs >= 2.5) return 'medium';
  return 'low';
}

function describeAnomaly(dimension: string, metric: string, zScore: number, value: number, mean: number): string {
  const direction = zScore > 0 ? 'above' : 'below';
  const pct = mean !== 0 ? Math.abs(Math.round(((value - mean) / mean) * 100)) : 0;
  return `${dimension}/${metric}: ${value} is ${pct}% ${direction} baseline (mean ${Math.round(mean * 100) / 100}, ${Math.abs(Math.round(zScore * 100) / 100)}σ)`;
}

async function detectAnomalies(
  pool: Pool,
  date: string,
  dimensions: DimensionResult[]
): Promise<AnomalyResult> {
  const result: AnomalyResult = { detected: 0, low: 0, medium: 0, high: 0, critical: 0, compound: 0, resolved: 0, escalated: 0 };

  // Load today's baselines and deviations from DB
  const sigRows = await pool.query(
    `SELECT dimension, baseline, deviation_from_baseline
     FROM aurora_signatures
     WHERE date = $1 AND baseline IS NOT NULL AND deviation_from_baseline IS NOT NULL`,
    [date]
  );

  if (sigRows.rows.length === 0) {
    logger.logVerbose('Anomaly detection: no baselines available');
    return result;
  }

  // Collect all anomalies across dimensions for compound detection
  const allAnomalies: Array<{
    dimension: string;
    metric: string;
    severity: string;
    zScore: number;
    value: number;
    mean: number;
    description: string;
  }> = [];

  for (const row of sigRows.rows) {
    const baseline = typeof row.baseline === 'string' ? JSON.parse(row.baseline) : row.baseline;
    const deviation = typeof row.deviation_from_baseline === 'string' ? JSON.parse(row.deviation_from_baseline) : row.deviation_from_baseline;

    if (!deviation || !baseline) continue;

    for (const [metric, dev] of Object.entries(deviation)) {
      const d = dev as { value: number; z_score: number; pct_change: number };
      const b = (baseline as Record<string, { mean: number; stddev: number }>)[metric];
      if (!d || !b || Math.abs(d.z_score) < 2) continue;

      const severity = classifySeverity(d.z_score);
      const description = describeAnomaly(row.dimension, metric, d.z_score, d.value, b.mean);

      allAnomalies.push({
        dimension: row.dimension,
        metric,
        severity,
        zScore: d.z_score,
        value: d.value,
        mean: b.mean,
        description,
      });
    }
  }

  // Write anomalies to DB
  for (const a of allAnomalies) {
    await pool.query(
      `INSERT INTO aurora_anomalies
         (detected_at, category, severity, metric, expected_value, actual_value, deviation_pct, sigma, description, evidence)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [
        `${date}T23:59:59Z`,
        a.dimension,
        a.severity,
        a.metric,
        String(a.mean),
        String(a.value),
        a.mean !== 0 ? Math.round(((a.value - a.mean) / a.mean) * 100) : 0,
        Math.round(a.zScore * 100) / 100,
        a.description,
        JSON.stringify({ dimension: a.dimension, date }),
      ]
    );

    result.detected++;
    result[a.severity as keyof Pick<AnomalyResult, 'low' | 'medium' | 'high' | 'critical'>]++;
  }

  // Compound anomaly detection: if 3+ dimensions have anomalies on the same day, escalate severity
  const dimCounts = new Map<string, number>();
  for (const a of allAnomalies) {
    dimCounts.set(a.dimension, (dimCounts.get(a.dimension) || 0) + 1);
  }
  const affectedDimensions = dimCounts.size;
  if (affectedDimensions >= 3) {
    result.compound = 1;
    const compoundDesc = `Compound anomaly: ${affectedDimensions} dimensions affected (${[...dimCounts.keys()].join(', ')})`;
    await pool.query(
      `INSERT INTO aurora_anomalies
         (detected_at, category, severity, metric, description, evidence)
       VALUES ($1, 'compound', 'critical', 'multi_dimension', $2, $3)`,
      [`${date}T23:59:59Z`, compoundDesc, JSON.stringify({ dimensions: [...dimCounts.entries()].map(([d, c]) => ({ dimension: d, anomaly_count: c })), date })]
    );
    result.detected++;
    result.critical++;
  }

  // Auto-escalate critical anomalies
  const criticals = allAnomalies.filter(a => a.severity === 'critical');
  if (criticals.length > 0 || result.compound > 0) {
    const escalationTarget = criticals[0] || { metric: 'multi_dimension', value: 0, mean: 0, zScore: 4 };
    try {
      await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
         VALUES ('aurora-escalation', $1, 5, 1)`,
        [JSON.stringify({
          trigger: result.compound > 0 ? `compound_anomaly_${affectedDimensions}_dimensions` : `critical_${escalationTarget.metric}`,
          metric: escalationTarget.metric,
          current_value: escalationTarget.value,
          baseline_value: escalationTarget.mean,
          deviation_sigma: Math.abs(escalationTarget.zScore),
        })]
      );
      result.escalated = 1;
      logger.log(`Auto-escalated critical anomaly: ${escalationTarget.metric}`);
    } catch (err) {
      logger.logMinimal(`Failed to enqueue escalation: ${(err as Error).message}`);
    }
  }

  // Domain-specific anomaly rules (beyond z-score detection)

  // 1. Key contact gone silent: active contacts (grouped by identity) with no messages in 7+ days
  try {
    const silentContacts = await pool.query(`
      WITH active_contacts AS (
        SELECT identity_id, contact_name, COUNT(*) as msg_count
        FROM aurora_unified_communication
        WHERE timestamp > NOW() - INTERVAL '90 days' AND timestamp < NOW() - INTERVAL '7 days'
          AND identity_id > 0
        GROUP BY identity_id, contact_name
        HAVING COUNT(*) >= 14
      ),
      recent AS (
        SELECT DISTINCT identity_id
        FROM aurora_unified_communication
        WHERE timestamp > NOW() - INTERVAL '7 days'
          AND identity_id > 0
      )
      SELECT a.identity_id, a.contact_name, a.msg_count
      FROM active_contacts a
      LEFT JOIN recent r ON r.identity_id = a.identity_id
      WHERE r.identity_id IS NULL
      LIMIT 5
    `);

    for (const sc of silentContacts.rows) {
      await pool.query(
        `INSERT INTO aurora_anomalies
           (detected_at, category, severity, metric, actual_value, expected_value, description, evidence)
         VALUES ($1, 'communication', 'medium', 'contact_silence', '0', $2, $3, $4)`,
        [`${date}T23:59:59Z`,
         String(Math.round(sc.msg_count / 13)), // avg msgs per week
         `Key contact "${sc.contact_name}" has gone silent — no messages in 7+ days (normally ~${Math.round(sc.msg_count / 13)}/week)`,
         JSON.stringify({ identity_id: sc.identity_id, contact_name: sc.contact_name, historical_count_90d: sc.msg_count })]
      );
      result.detected++;
      result.medium++;
    }
  } catch { /* unified views may not exist */ }

  // 2. Late-night search spike: >5 searches between midnight and 5 AM
  try {
    const nightSearches = await pool.query(`
      SELECT COUNT(*) as cnt FROM aurora_raw_google_activity
      WHERE timestamp::date = $1 AND activity_type = 'search'
        AND EXTRACT(HOUR FROM timestamp) < 5
    `, [date]);
    const nightCount = parseInt(nightSearches.rows[0]?.cnt || '0');
    if (nightCount >= 5) {
      await pool.query(
        `INSERT INTO aurora_anomalies
           (detected_at, category, severity, metric, actual_value, description, evidence)
         VALUES ($1, 'search_behavior', 'low', 'late_night_searches', $2, $3, $4)`,
        [`${date}T23:59:59Z`, String(nightCount),
         `Late-night search activity: ${nightCount} searches between midnight and 5 AM`,
         JSON.stringify({ date, count: nightCount })]
      );
      result.detected++;
      result.low++;
    }
  } catch { /* table may not exist */ }

  // 3. Screen time spike: app usage > 2x the 30-day average
  try {
    const todayUsage = dimensions.find(d => d.dimension === 'digital_activity');
    const todaySeconds = (todayUsage?.metrics?.app_usage_seconds as number) || 0;
    if (todaySeconds > 0) {
      const avgResult = await pool.query(`
        SELECT AVG((metrics->>'app_usage_seconds')::real) as avg_seconds
        FROM aurora_signatures
        WHERE dimension = 'digital_activity' AND date < $1 AND date >= ($1::date - INTERVAL '30 days')
          AND metrics->>'app_usage_seconds' IS NOT NULL
      `, [date]);
      const avgSeconds = parseFloat(avgResult.rows[0]?.avg_seconds || '0');
      if (avgSeconds > 0 && todaySeconds > avgSeconds * 2) {
        await pool.query(
          `INSERT INTO aurora_anomalies
             (detected_at, category, severity, metric, actual_value, expected_value, deviation_pct, description, evidence)
           VALUES ($1, 'digital_activity', 'low', 'screen_time_spike', $2, $3, $4, $5, $6)`,
          [`${date}T23:59:59Z`, String(Math.round(todaySeconds)),
           String(Math.round(avgSeconds)),
           Math.round(((todaySeconds - avgSeconds) / avgSeconds) * 100),
           `Screen time spike: ${Math.round(todaySeconds / 3600)}h vs ${Math.round(avgSeconds / 3600)}h average`,
           JSON.stringify({ date, today: todaySeconds, avg_30d: avgSeconds })]
        );
        result.detected++;
        result.low++;
      }
    }
  } catch { /* skip */ }

  // Resolution tracking: auto-resolve anomalies older than 3 days where no recent anomaly
  // exists for the same metric/category (meaning the metric has normalized)
  const resolveResult = await pool.query(`
    UPDATE aurora_anomalies a
    SET resolved = true, resolved_at = NOW(),
        resolution_note = 'Auto-resolved: no anomaly for this metric in last 3 days'
    WHERE a.resolved = false
      AND a.detected_at < NOW() - INTERVAL '3 days'
      AND NOT EXISTS (
        SELECT 1 FROM aurora_anomalies a2
        WHERE a2.metric = a.metric AND a2.category = a.category
          AND a2.resolved = false
          AND a2.detected_at >= NOW() - INTERVAL '3 days'
      )
    RETURNING id
  `);
  result.resolved = resolveResult.rowCount ?? 0;

  return result;
}

// ── Correlation engine ───────────────────────────────────────

// Cross-dimension metric pairs worth correlating. Format: [dimA/metricA, dimB/metricB]
const CORRELATION_PAIRS: [string, string][] = [
  // Health × Communication
  ['health/steps', 'communication/total_messages'],
  ['health/sleep_hours', 'communication/total_messages'],
  ['health/resting_heart_rate', 'communication/total_messages'],
  ['health/active_energy_cal', 'communication/sent'],
  ['health/sleep_hours', 'communication/avg_response_seconds'],
  // Health × Schedule
  ['health/steps', 'schedule/events'],
  ['health/sleep_hours', 'schedule/total_meeting_hours'],
  ['health/resting_heart_rate', 'schedule/events'],
  ['health/active_energy_cal', 'schedule/free_hours'],
  // Health × Digital Activity
  ['health/steps', 'digital_activity/app_usage_seconds'],
  ['health/sleep_hours', 'digital_activity/web_visits'],
  // Health × Search
  ['health/steps', 'search_behavior/total_searches'],
  ['health/sleep_hours', 'search_behavior/search_night'],
  // Health × Photos
  ['health/steps', 'photos/photos_taken'],
  // Health × Music
  ['health/sleep_hours', 'music/tracks_played'],
  // Communication × Schedule
  ['communication/total_messages', 'schedule/events'],
  ['communication/total_messages', 'schedule/total_meeting_hours'],
  ['communication/unique_contacts', 'schedule/events'],
  // Communication × Digital Activity
  ['communication/total_messages', 'digital_activity/app_usage_seconds'],
  ['communication/sent', 'digital_activity/web_visits'],
  // Communication × Location
  ['communication/total_messages', 'location/total_distance_km'],
  ['communication/total_messages', 'location/unique_places'],
  // Schedule × Digital Activity
  ['schedule/events', 'digital_activity/app_usage_seconds'],
  ['schedule/free_hours', 'digital_activity/web_visits'],
  // Location × Photos
  ['location/total_distance_km', 'photos/photos_taken'],
  ['location/unique_places', 'photos/photos_taken'],
  // Search × Content
  ['search_behavior/total_searches', 'content_consumption/total_views'],
  // ARIA Meta × Health
  ['aria_meta/conversations', 'health/steps'],
  ['aria_meta/llm_calls', 'schedule/events'],
  // Weather × Health
  ['weather/temp_avg_f', 'health/steps'],
  ['weather/temp_avg_f', 'health/active_energy_cal'],
  ['weather/temp_avg_f', 'health/sleep_hours'],
  ['weather/precipitation_in', 'health/steps'],
  ['weather/is_rainy', 'health/steps'],
  // Weather × Location
  ['weather/temp_avg_f', 'location/total_distance_km'],
  ['weather/is_rainy', 'location/total_distance_km'],
  // Weather × Photos
  ['weather/temp_avg_f', 'photos/photos_taken'],
  ['weather/is_rainy', 'photos/photos_taken'],
  // Weather × Digital Activity
  ['weather/is_rainy', 'digital_activity/app_usage_seconds'],
  ['weather/temp_avg_f', 'digital_activity/app_usage_seconds'],
  // Weather × Music
  ['weather/temp_avg_f', 'music/tracks_played'],
];

function pearsonCorrelation(x: number[], y: number[]): { r: number; p: number; n: number } {
  const n = Math.min(x.length, y.length);
  if (n < 3) return { r: 0, p: 1, n };

  const meanX = x.reduce((a, b) => a + b, 0) / n;
  const meanY = y.reduce((a, b) => a + b, 0) / n;

  let sumXY = 0, sumX2 = 0, sumY2 = 0;
  for (let i = 0; i < n; i++) {
    const dx = x[i] - meanX;
    const dy = y[i] - meanY;
    sumXY += dx * dy;
    sumX2 += dx * dx;
    sumY2 += dy * dy;
  }

  if (sumX2 === 0 || sumY2 === 0) return { r: 0, p: 1, n };
  const r = sumXY / Math.sqrt(sumX2 * sumY2);

  // Approximate p-value using t-distribution (two-tailed)
  const t = r * Math.sqrt((n - 2) / (1 - r * r));
  // Simple approximation: p ≈ 2 * (1 - Φ(|t|)) for large n
  const absT = Math.abs(t);
  const p = n > 30
    ? 2 * (1 - normalCDF(absT))
    : Math.min(1, 2 * Math.exp(-0.717 * absT - 0.416 * absT * absT)); // rough approximation

  return { r: Math.round(r * 1000) / 1000, p: Math.round(p * 10000) / 10000, n };
}

function normalCDF(x: number): number {
  // Abramowitz and Stegun approximation
  const a1 = 0.254829592, a2 = -0.284496736, a3 = 1.421413741;
  const a4 = -1.453152027, a5 = 1.061405429, p = 0.3275911;
  const sign = x < 0 ? -1 : 1;
  x = Math.abs(x) / Math.SQRT2;
  const t = 1 / (1 + p * x);
  const y = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-x * x);
  return 0.5 * (1 + sign * y);
}

function classifyStrength(r: number): 'weak' | 'moderate' | 'strong' {
  const abs = Math.abs(r);
  if (abs >= 0.6) return 'strong';
  if (abs >= 0.3) return 'moderate';
  return 'weak';
}

function generateSummary(streamA: string, streamB: string, r: number, lag: number | null): string {
  const direction = r > 0 ? 'positively correlates with' : 'inversely correlates with';
  const strength = classifyStrength(r);
  const lagNote = lag && lag > 0 ? ` (${lag}-day lag suggests temporal precedence)` : '';
  return `${streamA.replace('/', ': ')} ${direction} ${streamB.replace('/', ': ')} (${strength}, r=${r})${lagNote}`;
}

interface CorrelationResult {
  discovered: number;
  validated: number;
  invalidated: number;
}

async function runCorrelationEngine(pool: Pool, date: string): Promise<CorrelationResult> {
  const result: CorrelationResult = { discovered: 0, validated: 0, invalidated: 0 };

  // Load 90-day window of signatures
  const sigResult = await pool.query(
    `SELECT date, dimension, metrics FROM aurora_signatures
     WHERE date <= $1 AND date >= ($1::date - INTERVAL '90 days')
     ORDER BY date`,
    [date]
  );

  if (sigResult.rows.length < 20) {
    logger.logVerbose('Correlation engine: insufficient data (<20 days)');
    return result;
  }

  // Build lookup: date → dimension → metrics
  const dateMap = new Map<string, Map<string, Record<string, unknown>>>();
  for (const row of sigResult.rows) {
    const d = row.date instanceof Date ? row.date.toISOString().split('T')[0] : String(row.date);
    if (!dateMap.has(d)) dateMap.set(d, new Map());
    const m = typeof row.metrics === 'string' ? JSON.parse(row.metrics) : row.metrics;
    dateMap.get(d)!.set(row.dimension, m);
  }

  const dates = [...dateMap.keys()].sort();

  // Load existing correlations for validation tracking
  const existingResult = await pool.query(
    `SELECT id, stream_a, stream_b, coefficient, confirmation_count, exception_count
     FROM aurora_correlations WHERE review_status != 'rejected'`
  );
  const existing = new Map(existingResult.rows.map(r => [`${r.stream_a}|${r.stream_b}`, r]));

  for (const [pairA, pairB] of CORRELATION_PAIRS) {
    const [dimA, metricA] = pairA.split('/');
    const [dimB, metricB] = pairB.split('/');

    // Extract aligned time series
    const xVals: number[] = [];
    const yVals: number[] = [];
    // Also build 1-day lagged series (xVals shifted forward by 1 day)
    const xLagged: number[] = [];
    const yLagged: number[] = [];

    for (let i = 0; i < dates.length; i++) {
      const dayData = dateMap.get(dates[i]);
      if (!dayData) continue;
      const mA = dayData.get(dimA);
      const mB = dayData.get(dimB);
      if (!mA || !mB) continue;
      const valA = mA[metricA];
      const valB = mB[metricB];
      if (typeof valA !== 'number' || typeof valB !== 'number') continue;

      xVals.push(valA);
      yVals.push(valB);

      // For lag: pair today's A with tomorrow's B
      if (i + 1 < dates.length) {
        const nextDay = dateMap.get(dates[i + 1]);
        const nextB = nextDay?.get(dimB);
        if (nextB && typeof nextB[metricB] === 'number') {
          xLagged.push(valA);
          yLagged.push(nextB[metricB] as number);
        }
      }
    }

    if (xVals.length < 20) continue;

    const corr = pearsonCorrelation(xVals, yVals);
    if (corr.p >= 0.05 || Math.abs(corr.r) < 0.2) continue; // Not significant

    // Check 1-day lag
    let lagHours: number | null = null;
    let causalEvidence: 'correlation_only' | 'temporal_precedence' = 'correlation_only';
    if (xLagged.length >= 20) {
      const lagCorr = pearsonCorrelation(xLagged, yLagged);
      if (Math.abs(lagCorr.r) > Math.abs(corr.r) && lagCorr.p < 0.05) {
        lagHours = 24;
        causalEvidence = 'temporal_precedence';
      }
    }

    const streamA = pairA;
    const streamB = pairB;
    const key = `${streamA}|${streamB}`;
    const existingCorr = existing.get(key);

    if (existingCorr) {
      // Validate existing correlation: does today's data confirm or contradict?
      const todayA = dateMap.get(dates[dates.length - 1])?.get(dimA)?.[metricA];
      const todayB = dateMap.get(dates[dates.length - 1])?.get(dimB)?.[metricB];

      if (typeof todayA === 'number' && typeof todayB === 'number') {
        // Simple confirmation: both move in expected direction relative to mean
        const meanA = xVals.reduce((a, b) => a + b, 0) / xVals.length;
        const meanB = yVals.reduce((a, b) => a + b, 0) / yVals.length;
        const sameDirection = (existingCorr.coefficient > 0)
          ? ((todayA - meanA) * (todayB - meanB) >= 0)
          : ((todayA - meanA) * (todayB - meanB) <= 0);

        const field = sameDirection ? 'confirmation_count' : 'exception_count';
        await pool.query(
          `UPDATE aurora_correlations SET ${field} = ${field} + 1, last_validated = NOW(), coefficient = $1, p_value = $2, sample_size = $3 WHERE id = $4`,
          [corr.r, corr.p, corr.n, existingCorr.id]
        );
        result.validated++;
      }
    } else {
      // New correlation discovered
      const summary = generateSummary(streamA, streamB, corr.r, lagHours);
      await pool.query(
        `INSERT INTO aurora_correlations
           (stream_a, stream_b, direction, strength, coefficient, p_value, sample_size,
            temporal_lag_hours, causal_evidence, confidence, summary, confirmation_count)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 1)
         ON CONFLICT (stream_a, stream_b) DO NOTHING`,
        [
          streamA, streamB,
          corr.r > 0 ? 'positive' : 'negative',
          classifyStrength(corr.r),
          corr.r, corr.p, corr.n,
          lagHours, causalEvidence,
          Math.min(0.9, Math.abs(corr.r) * (1 - corr.p)),
          summary,
        ]
      );
      result.discovered++;
      logger.log(`New correlation: ${summary}`);
    }
  }

  // Auto-discovery: find strong correlations across ALL cross-dimension numeric pairs
  // Only runs for pairs NOT in the curated list, with higher threshold (|r| >= 0.4, p < 0.01)
  const curatedSet = new Set(CORRELATION_PAIRS.map(([a, b]) => `${a}|${b}`));
  const allMetrics = new Map<string, Map<string, number>>(); // date → "dim/metric" → value

  for (const [dateStr, dimMap] of dateMap) {
    for (const [dim, metrics] of dimMap) {
      for (const [key, val] of Object.entries(metrics)) {
        if (typeof val !== 'number') continue;
        const fullKey = `${dim}/${key}`;
        if (!allMetrics.has(dateStr)) allMetrics.set(dateStr, new Map());
        allMetrics.get(dateStr)!.set(fullKey, val);
      }
    }
  }

  // Collect unique metric keys with enough data points
  const metricCounts = new Map<string, number>();
  for (const dayMetrics of allMetrics.values()) {
    for (const key of dayMetrics.keys()) {
      metricCounts.set(key, (metricCounts.get(key) || 0) + 1);
    }
  }
  const viableMetrics = [...metricCounts.entries()]
    .filter(([, count]) => count >= 30)
    .map(([key]) => key);

  // Test cross-dimension pairs (skip same-dimension and curated pairs)
  let autoDiscovered = 0;
  for (let i = 0; i < viableMetrics.length && autoDiscovered < 20; i++) {
    for (let j = i + 1; j < viableMetrics.length && autoDiscovered < 20; j++) {
      const [dimA] = viableMetrics[i].split('/');
      const [dimB] = viableMetrics[j].split('/');
      if (dimA === dimB) continue; // Skip same-dimension
      const key = `${viableMetrics[i]}|${viableMetrics[j]}`;
      const keyRev = `${viableMetrics[j]}|${viableMetrics[i]}`;
      if (curatedSet.has(key) || curatedSet.has(keyRev)) continue;
      if (existing.has(key) || existing.has(keyRev)) continue;

      // Build aligned series
      const xVals: number[] = [];
      const yVals: number[] = [];
      for (const [, dayMetrics] of allMetrics) {
        const a = dayMetrics.get(viableMetrics[i]);
        const b = dayMetrics.get(viableMetrics[j]);
        if (a !== undefined && b !== undefined) {
          xVals.push(a);
          yVals.push(b);
        }
      }
      if (xVals.length < 30) continue;

      const corr = pearsonCorrelation(xVals, yVals);
      if (corr.p >= 0.01 || Math.abs(corr.r) < 0.4) continue;

      const summary = generateSummary(viableMetrics[i], viableMetrics[j], corr.r, null);
      await pool.query(
        `INSERT INTO aurora_correlations
           (stream_a, stream_b, direction, strength, coefficient, p_value, sample_size,
            causal_evidence, confidence, summary, confirmation_count)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'correlation_only', $8, $9, 1)
         ON CONFLICT (stream_a, stream_b) DO NOTHING`,
        [viableMetrics[i], viableMetrics[j],
         corr.r > 0 ? 'positive' : 'negative',
         classifyStrength(corr.r),
         corr.r, corr.p, corr.n,
         Math.min(0.9, Math.abs(corr.r) * (1 - corr.p)),
         summary]
      );
      autoDiscovered++;
      result.discovered++;
      logger.log(`Auto-discovered: ${summary}`);
    }
  }

  // Invalidation: correlations with more exceptions than confirmations for 30+ days
  const invalidResult = await pool.query(`
    UPDATE aurora_correlations
    SET review_status = 'rejected', reviewed_at = NOW()
    WHERE review_status = 'pending'
      AND exception_count > confirmation_count
      AND discovered_at < NOW() - INTERVAL '30 days'
      AND confirmation_count > 0
    RETURNING id
  `);
  result.invalidated = invalidResult.rowCount ?? 0;

  return result;
}

// ── Materialized view refresh ────────────────────────────────

const MATERIALIZED_VIEWS = [
  'aurora_unified_communication',
  'aurora_unified_location',
  'aurora_unified_calls',
  'aurora_unified_health',
  'aurora_unified_ai_conversations',
  'aurora_unified_social',
  'aurora_unified_travel',
];

async function refreshMaterializedViews(pool: Pool): Promise<void> {
  logger.logVerbose('Refreshing', MATERIALIZED_VIEWS.length, 'materialized views');
  for (const view of MATERIALIZED_VIEWS) {
    const stop = logger.time(`refresh-${view}`);
    try {
      // No UNIQUE INDEX on these views, so CONCURRENTLY is not available.
      // Regular REFRESH takes a stronger lock but is the only option.
      await pool.query(`REFRESH MATERIALIZED VIEW ${view}`);
      logger.logVerbose(`Refreshed ${view}`);
      stop();
    } catch (err) {
      stop();
      // View may not exist yet if migration hasn't run
      const msg = (err as Error).message;
      logger.logMinimal(`Failed to refresh ${view}: ${msg}`);
    }
  }
}

// ── Main handler ─────────────────────────────────────────────

export async function handleAuroraNightly(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload || {};
  const jobStart = Date.now();

  // Determine date (default: yesterday)
  const date = (payload.date as string) || new Date(Date.now() - 86_400_000).toISOString().split('T')[0];

  logger.logVerbose('Entering handleAuroraNightly', { date, force: !!payload.force, dimensions: payload.dimensions });
  logger.log(`Starting nightly analysis for ${date}`);
  await jobLog(job.id, `Processing date: ${date}`);

  // Check if already processed (unless force)
  if (!payload.force) {
    const existing = await pool.query(
      `SELECT COUNT(*) as count FROM aurora_signatures WHERE date = $1`,
      [date]
    );
    if (parseInt(existing.rows[0].count) > 0) {
      logger.log(`Date ${date} already processed (${existing.rows[0].count} signatures). Use force=true to re-run.`);
      await jobLog(job.id, `Skipped: ${date} already has ${existing.rows[0].count} signatures`);
      return {
        date,
        skipped: true,
        existing_signatures: parseInt(existing.rows[0].count),
      };
    }
    logger.logVerbose('No existing signatures for date, proceeding');
  } else {
    logger.logVerbose('Force mode enabled, skipping existing-signature check');
  }

  // Step 1: Refresh unified materialized views (skip for historical backfill)
  const today = new Date().toISOString().split('T')[0];
  const isHistorical = date < today;
  if (!isHistorical) {
    logger.log('Refreshing unified views...');
    const refreshStop = logger.time('view-refresh-total');
    await refreshMaterializedViews(pool);
    refreshStop();
    await jobLog(job.id, 'Materialized views refreshed');
  } else {
    logger.logVerbose('Skipping view refresh for historical date', date);
  }

  // Step 2: Run each dimension processor
  const results: DimensionResult[] = [];
  const errors: string[] = [];
  const requestedDimensions = payload.dimensions as string[] | undefined;
  const dimStop = logger.time('all-dimensions');

  for (const processor of DIMENSION_PROCESSORS) {
    const dimName = processor.name.replace('process', '').replace('Dimension', '').toLowerCase();
    try {
      const result = await processor(pool, date);
      if (result) {
        // If specific dimensions requested, skip others
        if (requestedDimensions && requestedDimensions.length > 0 && !requestedDimensions.includes(result.dimension)) {
          logger.logVerbose(`Skipping ${result.dimension} (not in requested dimensions)`);
          continue;
        }

        results.push(result);
        const metricCount = Object.keys(result.metrics).length;

        // Write to aurora_signatures
        const writeStop = logger.time(`signature-write-${result.dimension}`);
        await pool.query(
          `INSERT INTO aurora_signatures (date, dimension, metrics, created_at)
           VALUES ($1, $2, $3, NOW())
           ON CONFLICT (date, dimension) DO UPDATE SET
             metrics = EXCLUDED.metrics,
             created_at = NOW()`,
          [date, result.dimension, JSON.stringify(result.metrics)]
        );
        writeStop();

        logger.log(`${result.dimension}: ${metricCount} metrics written`);
      } else {
        logger.logVerbose(`${dimName}: no data returned`);
      }
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal(`Dimension '${dimName}' error: ${msg}`);
      logger.logDebug(`Dimension '${dimName}' error stack:`, err instanceof Error ? err.stack : msg);
      errors.push(dimName);
    }
  }

  dimStop();
  await jobLog(job.id, `Signatures: ${results.length} dimensions processed, ${errors.length} errors`);

  // Step 2.5: Compute baselines and deviations (30-day rolling average + z-scores)
  logger.log('Computing baselines and deviations...');
  const baselineStop = logger.time('baseline-computation');
  const baselineResult = await computeBaselinesAndDeviations(pool, date, results);
  baselineStop();
  logger.log(`Baselines: ${baselineResult.computed} computed, ${baselineResult.skipped} skipped`);
  await jobLog(job.id, `Baselines: ${baselineResult.computed} computed, ${baselineResult.skipped} skipped`);

  // Step 3: Anomaly detection (scan z-scores from baseline deviations)
  const anomalyStop = logger.time('anomaly-detection');
  const anomalyResult = await detectAnomalies(pool, date, results);
  anomalyStop();
  const anomalyCount = anomalyResult.detected;
  if (anomalyCount > 0) {
    logger.log(`Anomalies: ${anomalyResult.detected} detected (${anomalyResult.critical} critical, ${anomalyResult.high} high, ${anomalyResult.medium} medium, ${anomalyResult.low} low), ${anomalyResult.resolved} resolved, ${anomalyResult.escalated} escalated`);
  } else {
    logger.logVerbose('Anomaly detection: no anomalies');
  }
  await jobLog(job.id, `Anomalies: ${anomalyCount} detected, ${anomalyResult.resolved} resolved, ${anomalyResult.escalated} escalated`);

  // Step 4: Correlation engine (discover cross-dimension metric relationships)
  const corrStop = logger.time('correlation-engine');
  const corrResult = await runCorrelationEngine(pool, date);
  corrStop();
  if (corrResult.discovered > 0 || corrResult.validated > 0) {
    logger.log(`Correlations: ${corrResult.discovered} new, ${corrResult.validated} validated, ${corrResult.invalidated} invalidated`);
  }
  await jobLog(job.id, `Correlations: ${corrResult.discovered} new, ${corrResult.validated} validated`);

  // Step 5: KG ingestion — ingest high-confidence AURORA findings into Knowledge Graph
  let kgWritten = 0;
  try {
    const { ingestFacts } = await import('../lib/knowledge.js');
    const facts: Parameters<typeof ingestFacts>[0] = [];

    // Ingest high-confidence correlations validated today
    const { rows: correlations } = await pool.query(
      `SELECT stream_a, stream_b, coefficient, confidence, summary
       FROM aurora_correlations
       WHERE confidence >= 0.75
         AND last_validated >= NOW() - INTERVAL '24 hours'`
    );
    for (const c of correlations) {
      facts.push({
        domain: 'aurora',
        category: 'correlation',
        key: `corr_${c.stream_a}_x_${c.stream_b}`,
        value: c.summary,
        confidence: c.confidence,
        source: 'aurora-nightly',
        skipEmbedding: true,
      });
    }

    // Ingest high-confidence behavioral DNA traits updated today
    const { rows: traits } = await pool.query(
      `SELECT trait, current_value, confidence
       FROM aurora_behavioral_dna
       WHERE confidence >= 0.75
         AND active = true
         AND last_updated >= NOW() - INTERVAL '24 hours'`
    );
    for (const t of traits) {
      facts.push({
        domain: 'aurora',
        category: 'behavioral_trait',
        key: `trait_${t.trait}`,
        value: typeof t.current_value === 'string' ? t.current_value : JSON.stringify(t.current_value),
        confidence: t.confidence,
        source: 'aurora-nightly',
        skipEmbedding: true,
      });
    }

    if (facts.length > 0) {
      const result = await ingestFacts(facts);
      kgWritten = result.written;
      logger.log(`KG ingestion: ${result.written} facts written, ${result.errors} errors`);
    } else {
      logger.logVerbose('KG ingestion: no high-confidence findings to ingest');
    }
  } catch (err) {
    logger.logMinimal(`KG ingestion error: ${(err as Error).message}`);
  }

  // Step 6: Daily relationship analysis (updates aurora_relationships with fresh data)
  let relAnalyzed = 0;
  let relDecaying = 0;
  try {
    const { analyzeRelationships, detectRelationshipDecay } = await import('./aurora-weekly.js');
    const today = new Date().toISOString().split('T')[0];
    const relResult = await analyzeRelationships(pool, today, job.id);
    relAnalyzed = relResult.analyzed;
    logger.log(`Relationships: ${relResult.analyzed} analyzed, ${relResult.updated} active`);

    // Also run decay detection
    const decayResult = await detectRelationshipDecay(pool, today, job.id);
    relDecaying = decayResult.decaying;
  } catch (err) {
    logger.logMinimal(`Relationship analysis error: ${(err as Error).message}`);
    await jobLog(job.id, `Relationship analysis error: ${(err as Error).message}`);
  }

  const elapsedMs = Date.now() - jobStart;
  logger.log(`Nightly complete: ${results.length} dimensions, ${anomalyCount} anomalies, ${kgWritten} KG facts, ${relAnalyzed} relationships for ${date} (${elapsedMs}ms)`);
  await jobLog(job.id, `Complete: ${results.length} dimensions, ${anomalyCount} anomalies, ${kgWritten} KG facts, ${relAnalyzed} relationships, ${elapsedMs}ms`);

  return {
    date,
    dimensions_processed: results.length,
    dimensions: results.map((r) => r.dimension),
    baselines_computed: baselineResult.computed,
    baselines_skipped: baselineResult.skipped,
    errors: errors.length > 0 ? errors : undefined,
    kg_facts_written: kgWritten,
    relationships_analyzed: relAnalyzed,
    relationships_decaying: relDecaying,
    elapsed_ms: elapsedMs,
  };
}
