/**
 * Music Analysis handler.
 *
 * Runs daily. Analyzes listening patterns from the last 30 days and updates
 * the music_taste_profile table with ARIA's understanding of the user's
 * music taste, preferences, and listening habits.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { routeRequest } from '../lib/llm/index.js';
import { logEvent } from '../lib/event-log.js';
import { ingestFacts, type FactInput } from '../lib/knowledge.js';

const logger = createLogger('music-analysis');

interface TasteEntry {
  category: string;
  key: string;
  value: string;
  confidence: number;
  period: string;
}

interface AnalysisResult {
  top_artists: Array<{ name: string; confidence: number }>;
  top_genres: Array<{ name: string; confidence: number }>;
  repeat_tracks: Array<{ title: string; artist: string; confidence: number }>;
  listening_patterns: Array<{ pattern: string; confidence: number }>;
  mood_associations: Array<{ mood: string; description: string; confidence: number }>;
  playlist_insights: Array<{ insight: string; confidence: number }>;
}

export async function handleMusicAnalysis(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  logger.log('Starting music taste analysis...');

  // 1. Query listening data from last 30 days
  const { rows: tracks } = await pool.query<{
    title: string;
    artist_name: string;
    album_title: string;
    genre: string | null;
    played_at: string;
    duration_ms: number | null;
    play_count: number | null;
  }>(
    `SELECT title, artist_name, album_title, genre, played_at, duration_ms, play_count
     FROM device_music
     WHERE played_at IS NOT NULL AND played_at >= NOW() - INTERVAL '30 days'
     ORDER BY played_at DESC`
  );

  if (tracks.length === 0) {
    logger.log('No music data in the last 30 days, skipping.');
    return { skipped: true, reason: 'no_data' };
  }

  // 2. Calculate stats
  const artistCounts: Record<string, number> = {};
  const genreCounts: Record<string, number> = {};
  const trackCounts: Record<string, { title: string; artist: string; count: number }> = {};
  const hourCounts: Record<number, number> = {};
  let totalDurationMs = 0;

  for (const track of tracks) {
    // Artists
    artistCounts[track.artist_name] = (artistCounts[track.artist_name] || 0) + 1;

    // Genres
    if (track.genre) {
      genreCounts[track.genre] = (genreCounts[track.genre] || 0) + 1;
    }

    // Track repeats
    const trackKey = `${track.artist_name}|||${track.title}`;
    if (!trackCounts[trackKey]) {
      trackCounts[trackKey] = { title: track.title, artist: track.artist_name, count: 0 };
    }
    trackCounts[trackKey].count++;

    // Hour of day
    const hour = new Date(track.played_at).getHours();
    hourCounts[hour] = (hourCounts[hour] || 0) + 1;

    // Duration
    if (track.duration_ms) {
      totalDurationMs += track.duration_ms;
    }
  }

  const topArtists = Object.entries(artistCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([name, count]) => ({ name, count }));

  const topGenres = Object.entries(genreCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([name, count]) => ({ name, count }));

  const repeatTracks = Object.values(trackCounts)
    .filter(t => t.count >= 3)
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  const hourlyDistribution = Object.entries(hourCounts)
    .sort((a, b) => Number(a[0]) - Number(b[0]))
    .map(([hour, count]) => ({ hour: Number(hour), count }));

  const totalHours = Math.round(totalDurationMs / 3600000 * 10) / 10;

  // 3. Fetch playlist data
  let playlistSummary = 'No playlists synced.';
  try {
    const { rows: playlists } = await pool.query<{
      name: string;
      track_count: number;
      is_user_created: boolean;
    }>(
      `SELECT p.name, p.track_count, p.is_user_created
       FROM device_music_playlists p
       ORDER BY p.track_count DESC LIMIT 20`
    );
    if (playlists.length > 0) {
      playlistSummary = playlists.map(p =>
        `${p.name} (${p.track_count} tracks, ${p.is_user_created ? 'user-created' : 'curated'})`
      ).join('; ');
    }
  } catch { /* skip */ }

  // 4. Send to LLM for analysis
  const dataDescription = `Music listening data for the last 30 days:
- Total tracks played: ${tracks.length}
- Total listening time: ${totalHours} hours
- Top 10 artists: ${topArtists.map(a => `${a.name} (${a.count}x)`).join(', ')}
- Top 5 genres: ${topGenres.map(g => `${g.name} (${g.count}x)`).join(', ')}
- Most repeated tracks (3+ plays): ${repeatTracks.map(t => `"${t.title}" by ${t.artist} (${t.count}x)`).join(', ') || 'None'}
- Hourly distribution: ${hourlyDistribution.map(h => `${h.hour}:00=${h.count}`).join(', ')}
- Playlists: ${playlistSummary}`;

  const systemPrompt = `You are a music analyst. Analyze this music listening data and return a JSON object with these fields:
- top_artists: array of { name, confidence } — top artists by significance (not just play count), confidence 0.0-1.0
- top_genres: array of { name, confidence } — dominant genres
- repeat_tracks: array of { title, artist, confidence } — tracks that indicate strong preference
- listening_patterns: array of { pattern, confidence } — time-of-day habits, session patterns, etc.
- mood_associations: array of { mood, description, confidence } — what the music suggests about mood/activity states
- playlist_insights: array of { insight, confidence } — what playlist names and composition reveal about the user

Return ONLY valid JSON, no markdown fences.`;

  let analysis: AnalysisResult;
  try {
    const result = await routeRequest({
      handler: 'music-analysis',
      taskTier: 'generation',
      systemPrompt,
      userMessage: dataDescription,
      maxTokens: 3000,
      preferredModel: 'gpt-4o-mini',
      useBatch: true,
    });

    const cleaned = result.text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    analysis = JSON.parse(cleaned);
    logger.log(`LLM analysis complete (${result.model}, ${result.estimatedCostCents}c)`);
  } catch (err) {
    logger.logMinimal('LLM analysis failed:', (err as Error).message);
    return { error: 'llm_failed', tracks_analyzed: tracks.length };
  }

  // 5. Upsert results into music_taste_profile
  const entries: TasteEntry[] = [];

  for (const artist of (analysis.top_artists ?? []).slice(0, 10)) {
    entries.push({
      category: 'top_artist',
      key: artist.name,
      value: `Play count: ${artistCounts[artist.name] ?? 0}`,
      confidence: Math.min(artist.confidence, 0.99),
      period: '30d',
    });
  }

  for (const genre of (analysis.top_genres ?? []).slice(0, 5)) {
    entries.push({
      category: 'top_genre',
      key: genre.name,
      value: `Play count: ${genreCounts[genre.name] ?? 0}`,
      confidence: Math.min(genre.confidence, 0.99),
      period: '30d',
    });
  }

  for (const track of (analysis.repeat_tracks ?? []).slice(0, 10)) {
    entries.push({
      category: 'repeat_track',
      key: `${track.title} - ${track.artist}`,
      value: `Repeated frequently`,
      confidence: Math.min(track.confidence, 0.99),
      period: '30d',
    });
  }

  for (const pattern of (analysis.listening_patterns ?? []).slice(0, 5)) {
    entries.push({
      category: 'listening_pattern',
      key: pattern.pattern.slice(0, 100),
      value: pattern.pattern,
      confidence: Math.min(pattern.confidence, 0.99),
      period: '30d',
    });
  }

  for (const mood of (analysis.mood_associations ?? []).slice(0, 5)) {
    entries.push({
      category: 'mood_association',
      key: mood.mood,
      value: mood.description,
      confidence: Math.min(mood.confidence, 0.99),
      period: '30d',
    });
  }

  for (const insight of (analysis.playlist_insights ?? []).slice(0, 5)) {
    entries.push({
      category: 'playlist_insight',
      key: insight.insight.slice(0, 100),
      value: insight.insight,
      confidence: Math.min(insight.confidence, 0.99),
      period: '30d',
    });
  }

  // Clear old 30d entries and insert fresh
  await pool.query(`DELETE FROM music_taste_profile WHERE period = '30d'`);

  for (const entry of entries) {
    await pool.query(
      `INSERT INTO music_taste_profile (category, key, value, confidence, period, updated_at)
       VALUES ($1, $2, $3, $4, $5, NOW())
       ON CONFLICT (category, key, period) DO UPDATE SET
         value = EXCLUDED.value,
         confidence = EXCLUDED.confidence,
         updated_at = NOW()`,
      [entry.category, entry.key, entry.value, entry.confidence, entry.period]
    );
  }

  logger.log(`Upserted ${entries.length} taste profile entries.`);

  // 6. Write consolidated music facts to PKG
  const pkgFacts: FactInput[] = [];
  for (const artist of (analysis.top_artists ?? []).slice(0, 5)) {
    pkgFacts.push({
      domain: 'interests',
      category: 'music',
      key: `top_artist_${artist.name.toLowerCase().replace(/\s+/g, '_')}`,
      value: `Frequently listens to ${artist.name} (${artistCounts[artist.name] ?? 0} plays in last 30 days)`,
      confidence: Math.min(artist.confidence, 0.99),
      source: 'music_analysis',
    });
  }
  for (const genre of (analysis.top_genres ?? []).slice(0, 3)) {
    pkgFacts.push({
      domain: 'interests',
      category: 'music',
      key: `preferred_genre_${genre.name.toLowerCase().replace(/\s+/g, '_')}`,
      value: `Enjoys ${genre.name} music (${genreCounts[genre.name] ?? 0} tracks in last 30 days)`,
      confidence: Math.min(genre.confidence, 0.99),
      source: 'music_analysis',
    });
  }
  for (const pattern of (analysis.listening_patterns ?? []).slice(0, 3)) {
    pkgFacts.push({
      domain: 'lifestyle',
      category: 'habits',
      key: `music_habit_${pattern.pattern.slice(0, 40).toLowerCase().replace(/\s+/g, '_')}`,
      value: pattern.pattern,
      confidence: Math.min(pattern.confidence, 0.99),
      source: 'music_analysis',
    });
  }
  const { written } = await ingestFacts(pkgFacts);
  if (written > 0) {
    logger.log(`Wrote ${written} facts to knowledge graph`);
  }

  logEvent({
    action: `Music analysis: analyzed ${tracks.length} tracks, updated ${entries.length} taste profile entries`,
    component: 'music',
    category: 'background',
    metadata: {
      tracks_analyzed: tracks.length,
      profile_entries: entries.length,
      top_artist: topArtists[0]?.name,
      total_hours: totalHours,
    },
  });

  return {
    tracks_analyzed: tracks.length,
    profile_entries: entries.length,
    top_artists: topArtists.slice(0, 3).map(a => a.name),
    total_hours: totalHours,
  };
}
