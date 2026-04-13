/**
 * Significance Gate — Gate 2 of the Proactive Intelligence pipeline.
 *
 * Routes through the LLM router (Forge primary) to classify whether
 * detected changes are significant enough to warrant full analysis (Gate 3).
 */
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../event-log.js';
import { routeRequest } from '../llm/index.js';
import { formatDateTimeET } from '../timezone.js';

const logger = createLogger('proactive-significance');

interface SnapshotRow {
  id: string;
  source: string;
  change_summary: string;
  change_data: Record<string, unknown>;
}

export interface SignificanceResult {
  snapshot_id: string;
  significance: 'insignificant' | 'notable' | 'significant' | 'urgent';
  reason: string;
  suggested_category: string;
}

const CLASSIFICATION_PROMPT = `You are a significance classifier for a personal AI assistant named ARIA, serving the owner. Given detected changes from monitoring data sources, classify each change's significance.

IMPORTANT: Be SELECTIVE. Most things are insignificant. Only classify as "significant" if it would be worth sending a push notification to the owner's phone. When in doubt, classify as "notable" (logged but no alert).

ALWAYS classify as "insignificant":
- Newsletters, marketing, promotions
- System notifications (Chancery escalations, agent alerts, sync status, build deploys)
- Automated reports, DMARC reports, domain notices
- Generic order confirmations
- App update notifications
- Emails from the user's own system (Chancery, Nexus, ARIA)

Significance levels:
- "insignificant": Most things. Marketing, system noise, automated notifications, routine updates.
- "notable": Worth logging but NOT alerting — order confirmations, routine calendar events, informational notices
- "significant": Worth a push notification — personal emails from real people needing a response, financial matters with deadlines, health alerts, travel changes, messages from close contacts
- "urgent": Needs action in the next 2 hours — imminent meetings, security alerts, emergency messages

Examples:
- "Capital One: You're pre-approved for auto financing" → significant (financial, decision point)
- "Sullivan Tire: Your appointment is confirmed" → significant (calendar_prep, action may be needed)
- "Google Calendar: You have no events today" → insignificant
- "LinkedIn: Don't miss conversations in LegalTech" → insignificant (marketing)
- "Litter-Robot: Meet Cataire luxury line" → insignificant (product marketing)
- "Cloudflare: Domain Transfer Complete" → significant (infrastructure change)
- "iMessage from family member" → significant (social)
- "DoorDash: Order confirmation" → notable (expected purchase)
- "Chancery: Escalation from agent" → significant (system alert)

Insight categories: calendar_prep, follow_up, health_alert, pattern_observation, commitment_reminder, anomaly, suggestion, briefing, social, financial, infrastructure, general

Respond with a JSON array (no markdown fences):
[
  {
    "snapshot_id": "<id>",
    "significance": "insignificant|notable|significant|urgent",
    "reason": "Brief explanation",
    "suggested_category": "one of the insight categories"
  }
]`;

/**
 * Classify the significance of a batch of context snapshots.
 * Returns classifications for each snapshot.
 */
export async function classifySignificance(snapshotIds: string[]): Promise<SignificanceResult[]> {
  logger.logVerbose(`classifySignificance() entry, ${snapshotIds.length} snapshot(s)`);
  if (snapshotIds.length === 0) return [];

  const pool = getPool();
  const placeholders = snapshotIds.map((_, i) => `$${i + 1}`).join(', ');
  const { rows } = await pool.query<SnapshotRow>(
    `SELECT id, source, change_summary, change_data FROM context_snapshots
     WHERE id IN (${placeholders})`,
    snapshotIds
  );

  if (rows.length === 0) return [];

  // Build the context for Gemini
  const now = new Date();
  const contextLines = rows.map(r =>
    `[${r.id}] Source: ${r.source}\n  Summary: ${r.change_summary}`
  );

  const prompt = `Current time: ${now.toISOString()} (${formatDateTimeET(now, { weekday: 'long', hour: 'numeric', minute: '2-digit', hour12: true })})\n\nChanges detected:\n${contextLines.join('\n\n')}\n\nClassify each change:`;

  try {
    const stopClassify = logger.time('significance-classification');
    const result = await routeRequest({
      handler: 'significance-gate',
      taskTier: 'classification',
      systemPrompt: CLASSIFICATION_PROMPT,
      userMessage: prompt,
      maxTokens: 1024,
    });

    stopClassify();
    const text = result.text;
    logger.logDebug('Classification raw response:', text.slice(0, 500));
    const cleaned = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    const results: SignificanceResult[] = JSON.parse(cleaned);

    // Update snapshots with classifications
    for (const result of results) {
      await pool.query(
        `UPDATE context_snapshots SET significance = $1 WHERE id = $2`,
        [result.significance, result.snapshot_id]
      );
    }

    const significant = results.filter(r => r.significance === 'significant' || r.significance === 'urgent');
    logger.log(`Classified ${results.length} snapshot(s): ${significant.length} significant/urgent`);
    if (significant.length > 0) {
      logEvent({
        action: `Significance gate: ${results.length} classified, ${significant.length} significant/urgent`,
        component: 'proactive',
        category: 'background',
        metadata: { total: results.length, significant: significant.length },
      });
    }

    return results;
  } catch (err) {
    logger.logMinimal('Classification failed:', err instanceof Error ? err.message : err);
    // On failure, mark all as notable so they don't get stuck
    for (const id of snapshotIds) {
      await pool.query(`UPDATE context_snapshots SET significance = 'notable' WHERE id = $1`, [id]);
    }
    return snapshotIds.map(id => ({
      snapshot_id: id,
      significance: 'notable' as const,
      reason: 'Classification failed, defaulting to notable',
      suggested_category: 'general',
    }));
  }
}
