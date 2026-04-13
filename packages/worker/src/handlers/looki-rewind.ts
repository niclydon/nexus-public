/**
 * Looki rewind handler.
 *
 * Generates a life digest from the Looki wearable API for the specified
 * time range (week or month) and delivers a summary via email and inbox.
 *
 * Requires: LOOKI_API_KEY, LOOKI_BASE_URL, ANTHROPIC_API_KEY
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { deliverReport } from '../lib/report-delivery.js';
import { routeRequest } from '../lib/llm/index.js';
import { formatDateET } from '../lib/timezone.js';

const logger = createLogger('looki-rewind');

function getLookiConfig(): { baseUrl: string; apiKey: string } {
  const baseUrl = process.env.LOOKI_BASE_URL;
  const apiKey = process.env.LOOKI_API_KEY;
  if (!baseUrl || !apiKey) {
    throw new Error('LOOKI_BASE_URL and LOOKI_API_KEY must be set for looki rewind');
  }
  return { baseUrl, apiKey };
}

/**
 * Fetch recent moments from the Looki API.
 */
async function fetchLookiMoments(range: 'week' | 'month'): Promise<string> {
  const { baseUrl, apiKey } = getLookiConfig();
  const daysBack = range === 'week' ? 7 : 30;
  const since = new Date();
  since.setDate(since.getDate() - daysBack);

  try {
    const response = await fetch(`${baseUrl}/moments?since=${since.toISOString()}&limit=50`, {
      headers: {
        'X-API-Key': apiKey,
        'Accept': 'application/json',
      },
    });

    if (!response.ok) {
      logger.logMinimal(`Looki API returned ${response.status}`);
      return 'Looki data unavailable.';
    }

    const data = await response.json() as { moments?: Array<{ title?: string; summary?: string; timestamp?: string; tags?: string[] }> };
    const moments = data.moments ?? [];

    if (moments.length === 0) return 'No moments captured in this period.';

    const lines = moments.map(m => {
      const date = m.timestamp ? formatDateET(new Date(m.timestamp), { month: 'short', day: 'numeric', weekday: 'short' }) : '';
      const tags = m.tags?.length ? ` [${m.tags.join(', ')}]` : '';
      return `  - ${date}: ${m.title || m.summary || '(untitled)'}${tags}`;
    });

    return `Captured moments (${moments.length} total):\n${lines.join('\n')}`;
  } catch (err) {
    logger.logMinimal('Failed to fetch moments:', err);
    return 'Looki data unavailable.';
  }
}

/**
 * Fetch highlights/key moments from Looki if available.
 */
async function fetchLookiHighlights(range: 'week' | 'month'): Promise<string> {
  const { baseUrl, apiKey } = getLookiConfig();

  try {
    const response = await fetch(`${baseUrl}/highlights?range=${range}`, {
      headers: {
        'X-API-Key': apiKey,
        'Accept': 'application/json',
      },
    });

    if (!response.ok) return '';

    const data = await response.json() as { highlights?: Array<{ title?: string; description?: string }> };
    const highlights = data.highlights ?? [];

    if (highlights.length === 0) return '';

    const lines = highlights.map(h => `  - ${h.title || ''}: ${h.description || ''}`);
    return `Highlights:\n${lines.join('\n')}`;
  } catch {
    return '';
  }
}

export async function handleLookiRewind(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { range: 'week' | 'month'; recipient: string };

  // Check if subscription is enabled
  const pool = getPool();
  const { rows: subRows } = await pool.query(
    `SELECT is_enabled FROM report_subscriptions WHERE report_type = $1 LIMIT 1`,
    ['looki-rewind']
  );
  if (subRows.length > 0 && !subRows[0].is_enabled) {
    logger.log('Subscription disabled, skipping');
    return { skipped: true, reason: 'subscription_disabled' };
  }

  logger.log(`Generating ${payload.range} rewind for ${payload.recipient}`);

  // Gather Looki data in parallel
  const [moments, highlights] = await Promise.all([
    fetchLookiMoments(payload.range),
    fetchLookiHighlights(payload.range),
  ]);

  // Also fetch recent memory for richer context
  const daysBack = payload.range === 'week' ? 7 : 30;
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - daysBack);

  const { rows: recentMemories } = await pool.query(
    `SELECT category, key, value FROM core_memory
     WHERE superseded_by IS NULL
       AND created_at > $1
     ORDER BY created_at DESC
     LIMIT 15`,
    [cutoff.toISOString()]
  );

  const memoryContext = recentMemories.length > 0
    ? `New things learned:\n${recentMemories.map(r => `  - [${r.category}] ${r.key}: ${r.value}`).join('\n')}`
    : '';

  const rangeLabel = payload.range === 'week' ? 'This Week' : 'This Month';

  const result = await routeRequest({
    handler: 'looki-rewind',
    taskTier: 'generation',
    systemPrompt: `You are ARIA, a personal AI assistant with access to your owner's wearable life-capture device (Looki). Compose a "${rangeLabel} in Review" life digest. Be reflective but concise. Identify patterns, notable moments, and overall themes. End with one gentle observation or insight. No emojis. Max 1500 characters.`,
    userMessage: `Generate my ${payload.range}ly rewind.\n\n${moments}\n\n${highlights}\n\n${memoryContext}`,
    maxTokens: 1200,
    useBatch: true,
  });

  const rewindText = result.text;
  logger.log(`Generated via ${result.model} (${result.provider}, ${result.estimatedCostCents}¢)`);

  const deliveredVia = await deliverReport({
    reportType: 'looki-rewind',
    title: `Looki ${rangeLabel} Rewind`,
    body: rewindText,
    category: 'general',
    metadata: { range: payload.range },
  });

  logger.log(`Delivered via: ${deliveredVia.join(', ') || 'none'}`);

  await pool.query(
    `INSERT INTO actions_log (action_type, description, metadata)
     VALUES ($1, $2, $3)`,
    ['looki_rewind', `${rangeLabel} rewind delivered via ${deliveredVia.join(', ')}`, JSON.stringify({ deliveredVia, range: payload.range })]
  );

  return { delivered_via: deliveredVia, range: payload.range, rewind_length: rewindText.length };
}
