/**
 * Adaptive Fast-Track Pattern Generator.
 *
 * Routes through the LLM router (Forge primary) to periodically regenerate
 * regex patterns for fast-tracking Looki realtime events through PIE.
 * Patterns adapt to the user's current context (location, activity, time
 * of day, recent events).
 *
 * Universal safety patterns (911, SOS, emergency, etc.) are always active
 * regardless of context. Adaptive patterns are regenerated every 10 minutes
 * or when significant context changes are detected.
 */
import type { Pool } from 'pg';
import { getPool, createLogger } from '@nexus/core';
import { routeRequest } from '../llm/index.js';
import { reverseGeocode } from '../geocode.js';

const logger = createLogger('proactive-fast-track');

// ─── Types ──────────────────────────────────────────────────────────

export interface AdaptivePattern {
  pattern: string;
  reason: string;
}

export interface FastTrackSettings {
  auto_adapt_enabled: boolean;
  universal_patterns: string[];
  adaptive_patterns: AdaptivePattern[];
  context_summary: string;
  last_regenerated_at: string | null;
}

const DEFAULT_SETTINGS: FastTrackSettings = {
  auto_adapt_enabled: true,
  universal_patterns: [
    '\\b911\\b', '\\bSOS\\b', '\\bemergency\\b',
    '\\bfall\\b', '\\bfell\\b', '\\baccident\\b', '\\bcrash\\b',
  ],
  adaptive_patterns: [],
  context_summary: '',
  last_regenerated_at: null,
};

// ─── In-Memory Cache ────────────────────────────────────────────────

let cachedPatterns: RegExp[] | null = null;
let cacheLoadedAt = 0;
const CACHE_TTL_MS = 60_000; // 60 seconds

// ─── Regeneration Throttle ──────────────────────────────────────────

const MIN_REGENERATION_INTERVAL_MS = 10 * 60 * 1000; // 10 minutes

// (LLM calls routed through central router)

// ─── Public API ─────────────────────────────────────────────────────

/**
 * Get current fast-track settings from DB.
 */
export async function getFastTrackSettings(pool?: Pool): Promise<FastTrackSettings> {
  const p = pool ?? getPool();
  const { rows } = await p.query<{ value: FastTrackSettings }>(
    `SELECT value FROM proactive_settings WHERE key = 'fast_track_patterns'`
  );
  if (rows.length === 0) return { ...DEFAULT_SETTINGS };
  return { ...DEFAULT_SETTINGS, ...rows[0].value };
}

/**
 * Get all fast-track patterns (universal + adaptive) as compiled RegExps.
 * Uses a 60-second in-memory cache.
 */
export async function getAllFastTrackPatterns(pool?: Pool): Promise<RegExp[]> {
  const now = Date.now();
  if (cachedPatterns && (now - cacheLoadedAt) < CACHE_TTL_MS) {
    logger.logDebug('Returning cached fast-track patterns');
    return cachedPatterns;
  }
  logger.logVerbose('getAllFastTrackPatterns() loading from DB');

  try {
    const settings = await getFastTrackSettings(pool);
    const allPatternStrings = [
      ...settings.universal_patterns,
      ...settings.adaptive_patterns.map(p => p.pattern),
    ];

    cachedPatterns = [];
    for (const ps of allPatternStrings) {
      try {
        cachedPatterns.push(new RegExp(ps, 'i'));
      } catch {
        logger.logMinimal(`Invalid regex skipped: ${ps}`);
      }
    }
    cacheLoadedAt = now;
    return cachedPatterns;
  } catch (err) {
    logger.logMinimal('Failed to load patterns, using universal fallback:', err);
    // Fallback to hardcoded universal patterns
    return DEFAULT_SETTINGS.universal_patterns.map(p => new RegExp(p, 'i'));
  }
}

/**
 * Check whether patterns should be regenerated.
 */
export function shouldRegenerate(settings: FastTrackSettings): boolean {
  if (!settings.auto_adapt_enabled) return false;
  if (!settings.last_regenerated_at) return true;

  const elapsed = Date.now() - new Date(settings.last_regenerated_at).getTime();
  return elapsed >= MIN_REGENERATION_INTERVAL_MS;
}

/**
 * Build a context summary from the latest data sources.
 */
export async function buildContextSummary(pool?: Pool): Promise<string> {
  logger.logVerbose('buildContextSummary() entry');
  const p = pool ?? getPool();
  const parts: string[] = [];

  // Current time
  const now = new Date();
  const timeStr = now.toLocaleString('en-US', {
    weekday: 'long', hour: 'numeric', minute: '2-digit',
    hour12: true, timeZone: 'America/New_York',
  });
  parts.push(`Time: ${timeStr} ET`);

  // Latest location (only if recent — within 2 hours)
  try {
    const { rows } = await p.query<{ latitude: number; longitude: number; place_name: string | null; locality: string | null }>(
      `SELECT latitude, longitude, place_name, locality
       FROM device_location
       WHERE timestamp > NOW() - INTERVAL '2 hours'
       ORDER BY timestamp DESC LIMIT 1`
    );
    if (rows.length > 0) {
      const loc = rows[0];
      let label = loc.place_name || loc.locality;
      if (!label) {
        label = await reverseGeocode(loc.latitude, loc.longitude) || `${loc.latitude.toFixed(4)}, ${loc.longitude.toFixed(4)}`;
      }
      parts.push(`Location: ${label}`);
    }
  } catch { /* table may not exist */ }

  // Latest activity (only if recent — within 2 hours)
  try {
    const { rows } = await p.query<{
      stationary: boolean; walking: boolean; running: boolean;
      cycling: boolean; automotive: boolean; confidence: string;
    }>(
      `SELECT stationary, walking, running, cycling, automotive, confidence
       FROM device_activity
       WHERE timestamp > NOW() - INTERVAL '2 hours'
       ORDER BY timestamp DESC LIMIT 1`
    );
    if (rows.length > 0) {
      const a = rows[0];
      const activityType = a.automotive ? 'automotive'
        : a.cycling ? 'cycling'
        : a.running ? 'running'
        : a.walking ? 'walking'
        : 'stationary';
      parts.push(`Activity: ${activityType} (${a.confidence} confidence)`);
    }
  } catch { /* table may not exist */ }

  // Recent Looki events (last 3)
  try {
    const { rows } = await p.query<{ description: string }>(
      `SELECT description FROM looki_realtime_events ORDER BY created_at DESC LIMIT 3`
    );
    if (rows.length > 0) {
      const descs = rows.map(r => r.description.substring(0, 100));
      parts.push(`Recent wearable events:\n${descs.map(d => `  - ${d}`).join('\n')}`);
    }
  } catch { /* table may not exist */ }

  // Recent health data (today or yesterday)
  try {
    const { rows } = await p.query<{ data_type: string; value: unknown }>(
      `SELECT data_type, value
       FROM health_data
       WHERE data_type IN ('heart_rate', 'steps', 'active_energy')
         AND date >= CURRENT_DATE - INTERVAL '1 day'
       ORDER BY date DESC, updated_at DESC LIMIT 5`
    );
    if (rows.length > 0) {
      const healthLines = rows.map(r => {
        const v = typeof r.value === 'object' && r.value !== null ? JSON.stringify(r.value) : String(r.value);
        return `${r.data_type}: ${v}`;
      });
      parts.push(`Health: ${healthLines.join(', ')}`);
    }
  } catch { /* table may not exist */ }

  return parts.join('\n');
}

// ─── Pattern Regeneration ───────────────────────────────────────────

const GENERATION_PROMPT = `You are a pattern generator for a personal AI assistant's real-time event monitoring system. Your job is to generate regex patterns that determine which incoming wearable/sensor events should be FAST-TRACKED for immediate analysis rather than batched.

Current context:
{CONTEXT}

Generate 10-25 regex patterns (JavaScript-compatible, case-insensitive) that would indicate a NOTEWORTHY event worth fast-tracking in this context. Consider ALL of the following categories:

**Safety & Urgency** — Context-specific physical risks and hazards
  Examples: hiking → terrain/wildlife/exposure; driving → collision/swerving; swimming → cramp/current

**Social & Communication** — Signals about people, messages, calls, or social interactions
  Examples: incoming call, message from specific person, meeting starting, someone arriving/leaving

**Work & Productivity** — Signals about tasks, deadlines, meetings, or work context
  Examples: calendar alert, deadline, reminder, task due, meeting in N minutes

**Home & Environment** — Signals about home state, weather, appliances, arrivals
  Examples: door open, temperature change, package delivered, weather alert, device status

**Health & Wellness** — Context-specific health signals beyond emergencies
  Examples: elevated heart rate for current activity, sedentary too long, hydration reminder, sleep quality

**Location & Movement** — Significant location changes or travel events
  Examples: arrived at destination, left usual area, traffic delay, transit alert, geofence trigger

Do NOT include universal emergency patterns (911, SOS, emergency, fall, accident, crash) — those are always active separately.

Focus on what's specifically relevant RIGHT NOW given the context above. A pattern relevant while hiking is different from one while at the office or at home in the evening.

Respond as a JSON array only, no other text:
[{"pattern": "\\\\bsteep\\\\b", "reason": "Hiking — terrain hazard"}, {"pattern": "\\\\bmeeting\\\\b", "reason": "Workday — calendar awareness"}, ...]`;

/**
 * Regenerate adaptive patterns using Gemini Flash-Lite based on current context.
 */
export async function regenerateAdaptivePatterns(pool?: Pool, contextSummary?: string): Promise<void> {
  logger.logVerbose('regenerateAdaptivePatterns() entry');
  const p = pool ?? getPool();

  const context = contextSummary ?? await buildContextSummary(p);
  const prompt = GENERATION_PROMPT.replace('{CONTEXT}', context);

  try {
    const stopGenerate = logger.time('pattern-generation');
    const result = await routeRequest({
      handler: 'adaptive-fast-track',
      taskTier: 'classification',
      systemPrompt: 'Generate regex patterns as requested. Respond with a JSON array only, no other text.',
      userMessage: prompt,
      maxTokens: 4096,
    });

    stopGenerate();
    const text = result.text;
    logger.logDebug('Pattern generation response:', text.slice(0, 500));
    const cleaned = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();

    let patterns: AdaptivePattern[];
    try {
      patterns = JSON.parse(cleaned);
    } catch {
      logger.logMinimal('Failed to parse LLM response:', cleaned.substring(0, 200));
      return;
    }

    // Validate each pattern is a valid regex
    const validPatterns: AdaptivePattern[] = [];
    for (const p of patterns) {
      if (!p.pattern || !p.reason) continue;
      try {
        new RegExp(p.pattern, 'i');
        validPatterns.push({ pattern: p.pattern, reason: p.reason });
      } catch {
        logger.logMinimal(`Invalid regex from LLM, skipping: ${p.pattern}`);
      }
    }

    // Read current settings to preserve universal_patterns and auto_adapt_enabled
    const current = await getFastTrackSettings(p);

    const updated: FastTrackSettings = {
      ...current,
      adaptive_patterns: validPatterns,
      context_summary: context,
      last_regenerated_at: new Date().toISOString(),
    };

    await p.query(
      `INSERT INTO proactive_settings (key, value) VALUES ('fast_track_patterns', $1::jsonb)
       ON CONFLICT (key) DO UPDATE SET value = $1::jsonb, updated_at = NOW()`,
      [JSON.stringify(updated)]
    );

    // Invalidate cache
    cachedPatterns = null;
    cacheLoadedAt = 0;

    logger.log(`Regenerated ${validPatterns.length} adaptive patterns for context: ${context.substring(0, 80)}...`);
  } catch (err) {
    logger.logMinimal('Pattern regeneration failed:', err instanceof Error ? err.message : err);
  }
}
