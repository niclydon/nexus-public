import { query, createLogger } from '@nexus/core';

const logger = createLogger('review-digest');

/**
 * Sends a Pushover notification with a count of unreviewed agent decisions
 * and a link to the Chancery review page.
 *
 * Runs every 90 minutes. Only sends if there are unreviewed decisions.
 */
export async function handleReviewDigest(): Promise<{ sent: boolean; count: number }> {
  // Count unreviewed decisions (with actions, last 7 days)
  const result = await query<{ count: string }>(
    `SELECT COUNT(*) as count
     FROM agent_decisions d
     LEFT JOIN agent_decision_feedback f ON f.decision_id = d.id
     WHERE f.id IS NULL
       AND d.parsed_actions IS NOT NULL
       AND d.parsed_actions::text != '[]'
       AND d.parsed_actions::text != 'null'
       AND d.created_at >= NOW() - INTERVAL '7 days'`,
  );

  const count = parseInt(result.rows[0]?.count ?? '0', 10);

  if (count === 0) {
    logger.logVerbose('No unreviewed decisions — skipping digest');
    return { sent: false, count: 0 };
  }

  // Get a breakdown by agent for the message
  const breakdown = await query<{ agent_name: string; cnt: string }>(
    `SELECT r.display_name as agent_name, COUNT(*)::text as cnt
     FROM agent_decisions d
     JOIN agent_registry r ON r.agent_id = d.agent_id
     LEFT JOIN agent_decision_feedback f ON f.decision_id = d.id
     WHERE f.id IS NULL
       AND d.parsed_actions IS NOT NULL
       AND d.parsed_actions::text != '[]'
       AND d.parsed_actions::text != 'null'
       AND d.created_at >= NOW() - INTERVAL '7 days'
     GROUP BY r.display_name
     ORDER BY COUNT(*) DESC
     LIMIT 5`,
  );

  const agentSummary = breakdown.rows
    .map(r => `${r.agent_name}: ${r.cnt}`)
    .join(', ');

  // Send Pushover notification
  const pushoverToken = process.env.PUSHOVER_APP_TOKEN;
  const pushoverUser = process.env.PUSHOVER_USER_KEY;

  if (!pushoverToken || !pushoverUser) {
    logger.logMinimal('Pushover not configured — skipping review digest');
    return { sent: false, count };
  }

  const reviewUrl = process.env.CHANCERY_URL
    ? `${process.env.CHANCERY_URL}/review`
    : 'https://chancery.example.com/review';

  const message = `${count} agent decisions to review\n${agentSummary}`;

  try {
    const resp = await fetch('https://api.pushover.net/1/messages.json', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        token: pushoverToken,
        user: pushoverUser,
        title: `Nexus: ${count} decisions to review`,
        message,
        url: reviewUrl,
        url_title: 'Open Review Page',
        priority: 0,
        sound: 'none',
      }),
    });

    if (!resp.ok) {
      const text = await resp.text();
      logger.logMinimal('Pushover send failed:', resp.status, text.slice(0, 200));
      return { sent: false, count };
    }

    logger.log(`Review digest sent: ${count} unreviewed decisions`);
    return { sent: true, count };
  } catch (err) {
    logger.logMinimal('Pushover error:', (err as Error).message);
    return { sent: false, count };
  }
}
