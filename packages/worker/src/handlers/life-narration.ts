/**
 * Life Narration handler.
 *
 * Queries device data sources for the last 30 minutes and generates a
 * factual, third-person narrative of what the owner was doing.
 * Runs every 15-30 minutes via auto-scheduling.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';
import { formatDateTimeET, TIMEZONE } from '../lib/timezone.js';
import { ingestFacts, type FactInput } from '../lib/knowledge.js';
import { jobLog } from '../lib/job-log.js';
import { reverseGeocode } from '../lib/geocode.js';

const logger = createLogger('life-narration');

export async function handleLifeNarration(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  logger.log(`Starting life narration job ${job.id}`);
  await jobLog(job.id, 'Querying device data sources for last 30 minutes...');

  const now = new Date();
  const thirtyMinAgo = new Date(now.getTime() - 30 * 60 * 1000);

  // 1. Query device_location
  const { rows: locations } = await pool.query<{
    latitude: number; longitude: number; address: string | null; recorded_at: string;
  }>(
    `SELECT latitude, longitude, place_name AS address, timestamp::text AS recorded_at
     FROM device_location
     WHERE timestamp >= $1 AND timestamp <= $2
     ORDER BY timestamp ASC`,
    [thirtyMinAgo, now]
  );

  // 2. Query device_activity (derive activity type from boolean columns)
  const { rows: activities } = await pool.query<{
    activity_type: string; confidence: string | null; recorded_at: string;
  }>(
    `SELECT
       CASE
         WHEN running    THEN 'running'
         WHEN cycling    THEN 'cycling'
         WHEN automotive THEN 'automotive'
         WHEN walking    THEN 'walking'
         WHEN stationary THEN 'stationary'
         ELSE 'unknown'
       END AS activity_type,
       confidence,
       timestamp::text AS recorded_at
     FROM device_activity
     WHERE timestamp >= $1 AND timestamp <= $2
     ORDER BY timestamp ASC`,
    [thirtyMinAgo, now]
  );

  // 3. Query health_data (daily summaries — query today's records for context)
  // Exclude 'workout' entries: distance/type in workout records reflects cumulative daily
  // totals, not current window activity, which causes the LLM to narrate things like
  // "walked several miles" when the user is stationary. Activity type is captured by device_activity.
  const { rows: healthData } = await pool.query<{
    data_type: string; value: Record<string, unknown>;
  }>(
    `SELECT data_type, value
     FROM health_data
     WHERE date = CURRENT_DATE AND data_type != 'workout'`
  );

  // 4. Query device_calendar_events (events happening NOW)
  const { rows: calendarEvents } = await pool.query<{
    title: string; start_time: string; end_time: string; location: string | null;
  }>(
    `SELECT title, start_date::text AS start_time, end_date::text AS end_time, location
     FROM device_calendar_events
     WHERE start_date <= $1 AND end_date >= $2`,
    [now, thirtyMinAgo]
  );

  // 5. Query device_music
  const { rows: music } = await pool.query<{
    track_name: string; artist_name: string | null; album_name: string | null; played_at: string;
  }>(
    `SELECT title AS track_name, artist_name, album_title AS album_name, played_at::text
     FROM device_music
     WHERE played_at >= $1 AND played_at <= $2
     ORDER BY played_at ASC`,
    [thirtyMinAgo, now]
  );

  // 6. Query looki_realtime_events
  const { rows: lookiEvents } = await pool.query<{
    description: string; location: Record<string, unknown> | null; captured_at: string;
  }>(
    `SELECT description, location, created_at::text AS captured_at
     FROM looki_realtime_events
     WHERE created_at >= $1 AND created_at <= $2
     ORDER BY created_at ASC`,
    [thirtyMinAgo, now]
  );

  // 7. Circuit breaker: skip if no data from any source
  const totalDataPoints =
    locations.length + activities.length + healthData.length +
    calendarEvents.length + music.length + lookiEvents.length;

  if (totalDataPoints === 0) {
    logger.log(`No activity data in the last 30 minutes. Skipping.`);
    logEvent({
      action: 'Life narration skipped — no activity data',
      component: 'life-narration',
      category: 'background',
      metadata: { job_id: job.id, reason: 'no_activity' },
    });
    return { skipped: true, reason: 'no_activity' };
  }

  // 8. Build data sections for the prompt
  const timeRangeStart = formatDateTimeET(thirtyMinAgo, { hour: 'numeric', minute: '2-digit' });
  const timeRangeEnd = formatDateTimeET(now, { hour: 'numeric', minute: '2-digit' });

  let dataContext = '';

  if (locations.length > 0) {
    dataContext += '\n## Location Data\n';
    const locationLines: string[] = [];
    for (const l of locations) {
      const time = formatDateTimeET(new Date(l.recorded_at), { hour: 'numeric', minute: '2-digit' });
      // Prefer place_name from iOS, then geocode, then raw coords as last resort
      let locationLabel = l.address;
      if (!locationLabel) {
        locationLabel = await reverseGeocode(l.latitude, l.longitude);
      }
      if (locationLabel) {
        locationLines.push(`- ${time}: ${locationLabel}`);
      } else {
        locationLines.push(`- ${time}: ${l.latitude.toFixed(4)}, ${l.longitude.toFixed(4)}`);
      }
    }
    dataContext += locationLines.join('\n');
  }

  if (activities.length > 0) {
    dataContext += '\n## Activity Data\n';
    dataContext += activities.map((a) => {
      const time = formatDateTimeET(new Date(a.recorded_at), { hour: 'numeric', minute: '2-digit' });
      const conf = a.confidence ? ` (confidence: ${a.confidence})` : '';
      return `- ${time}: ${a.activity_type}${conf}`;
    }).join('\n');
  }

  if (healthData.length > 0) {
    dataContext += '\n## Health Data (full-day totals — background context only)\n';
    dataContext += healthData.map((h) => {
      const valueStr = h.value && typeof h.value === 'object' ? JSON.stringify(h.value) : String(h.value);
      return `- ${h.data_type}: ${valueStr}`;
    }).join('\n');
  }

  if (calendarEvents.length > 0) {
    dataContext += '\n## Calendar Events (in progress)\n';
    dataContext += calendarEvents.map((e) => {
      const loc = e.location ? ` at ${e.location}` : '';
      return `- ${e.title}${loc} (${e.start_time} - ${e.end_time})`;
    }).join('\n');
  }

  if (music.length > 0) {
    dataContext += '\n## Music\n';
    dataContext += music.map((m) => {
      const time = formatDateTimeET(new Date(m.played_at), { hour: 'numeric', minute: '2-digit' });
      const artist = m.artist_name ? ` by ${m.artist_name}` : '';
      const album = m.album_name ? ` (${m.album_name})` : '';
      return `- ${time}: "${m.track_name}"${artist}${album}`;
    }).join('\n');
  }

  if (lookiEvents.length > 0) {
    dataContext += '\n## Wearable (Looki) Events\n';
    dataContext += lookiEvents.map((e) => {
      const time = formatDateTimeET(new Date(e.captured_at), { hour: 'numeric', minute: '2-digit' });
      const loc = e.location && typeof e.location === 'object' && 'name' in e.location
        ? ` at ${e.location.name}`
        : '';
      return `- ${time}: ${e.description}${loc}`;
    }).join('\n');
  }

  // 9. Generate narration via LLM
  const prompt = `You are writing a factual, third-person life narration for the owner's day. Based on the sensor and device data below, write a brief chronological narrative (2-4 sentences max) describing what the owner was doing during this time window.

Time window: ${timeRangeStart} - ${timeRangeEnd} ET

${dataContext}

Rules:
- Write in third person ("He" or "the owner")
- Be factual and concise — only state what the data shows
- Use natural prose, not bullet points
- Include specific details (locations, music, activities) when available
- Do NOT speculate beyond what the data indicates
- Do NOT include greetings, sign-offs, or meta-commentary
- "Health Data" shows full-day totals for background context only — do NOT use it to describe what the owner was doing during the time window; derive current activity exclusively from Activity Data and Location Data`;

  const llmResult = await routeRequest({
    handler: 'life-narration',
    taskTier: 'generation',
    systemPrompt: 'You are a concise life narrator. Write brief, factual third-person prose based on sensor data.',
    userMessage: prompt,
    maxTokens: 2000,
    useBatch: true,
  });
  logger.log(`Generated via ${llmResult.model} (${llmResult.provider}, ${llmResult.estimatedCostCents}\u00A2)`);

  const narrative = llmResult.text.trim();

  // 10. Build source_data summary
  const sourceData: Record<string, unknown> = {};
  if (locations.length > 0) sourceData.location = locations.length;
  if (activities.length > 0) sourceData.activity = activities.length;
  if (healthData.length > 0) sourceData.health = healthData.length;
  if (calendarEvents.length > 0) sourceData.calendar = calendarEvents.length;
  if (music.length > 0) sourceData.music = music.length;
  if (lookiEvents.length > 0) sourceData.looki = lookiEvents.length;
  await jobLog(job.id, `Narrated ${totalDataPoints} data points from ${Object.keys(sourceData).join(', ')}`);

  // 11. Insert into life_narration table
  const { rows: saved } = await pool.query<{ id: string }>(
    `INSERT INTO life_narration (period_start, period_end, narrative, source_data)
     VALUES ($1, $2, $3, $4)
     RETURNING id`,
    [thirtyMinAgo, now, narrative, JSON.stringify(sourceData)]
  );

  logEvent({
    action: `Life narration written (${totalDataPoints} data points)`,
    component: 'life-narration',
    category: 'background',
    metadata: { job_id: job.id, source_data: sourceData, narrative_length: narrative.length },
  });

  logger.log(`Entry written (${totalDataPoints} data points from ${Object.keys(sourceData).join(', ')})`);

  // 12. Extract PKG facts from the narration and raw data
  try {
    const pkgPrompt = `Extract structured facts about the owner the owner from this life narration and supporting data. Return ONLY a JSON array of objects, no other text.

Each object must have:
- "domain": one of "places", "lifestyle", or "events"
- "category": one of "visited" (for places), "activities" (for lifestyle), or "life" (for events)
- "key": a short, stable identifier for this fact (e.g. "visits_starbucks", "morning_runner", "concert_2026_03")
- "value": a concise description of the fact

Rules:
- Only extract facts that are clearly supported by the data
- Always make the subject of each fact explicit — if the narration mentions other people, attribute facts to them correctly, not to the owner
- For places: extract specific named locations the owner visited (not just coordinates)
- For activities: extract recurring patterns or notable activities (e.g. "morning run", "commute by car", "listening to jazz")
- For events: extract notable one-time life events (e.g. attending a concert, a trip, a meeting)
- Use lowercase_snake_case for keys
- If no facts can be extracted, return an empty array []
- Return 0-5 facts maximum — only include genuinely meaningful ones

Narration:
${narrative}

Raw data context:
${dataContext}`;

    const pkgResult = await routeRequest({
      handler: 'life-narration',
      taskTier: 'classification',
      systemPrompt: 'You extract structured facts from life narrations. Return only valid JSON arrays.',
      userMessage: pkgPrompt,
      maxTokens: 2000,
      useBatch: true,
    });

    // Parse the JSON response
    const jsonMatch = pkgResult.text.match(/\[[\s\S]*\]/);
    if (jsonMatch) {
      const extractedFacts: Array<{ domain: string; category: string; key: string; value: string }> = JSON.parse(jsonMatch[0]);

      if (extractedFacts.length > 0) {
        const profileFacts: FactInput[] = extractedFacts
          .filter((f) => f.domain && f.category && f.key && f.value)
          .map((f) => ({
            domain: f.domain,
            category: f.category,
            key: f.key,
            value: f.value,
            confidence: 0.5,
            source: 'life_narration',
          }));

        if (profileFacts.length > 0) {
          const { written } = await ingestFacts(profileFacts);
          logger.log(`Wrote ${written}/${profileFacts.length} PKG facts`);
        }
      }
    }
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal(`PKG extraction failed (non-fatal): ${msg}`);
  }

  return {
    entry_id: saved[0].id,
    data_points: totalDataPoints,
    sources: Object.keys(sourceData),
    narrative_length: narrative.length,
  };
}
