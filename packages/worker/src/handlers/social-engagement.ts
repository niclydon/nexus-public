/**
 * Social engagement handler — autonomous Moltbook activity.
 */
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/social-engagement');

const MOLTBOOK_BASE = 'https://www.moltbook.com/api/v1';

async function moltbookFetch(path: string, apiKey: string, method = 'GET', body?: unknown): Promise<unknown> {
  const res = await fetch(`${MOLTBOOK_BASE}${path}`, {
    method,
    headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {}),
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Moltbook API ${res.status}: ${text.slice(0, 200)}`);
  }
  return res.json();
}

async function ensureConnection(pool: ReturnType<typeof getPool>, agentName: string): Promise<string> {
  const { rows } = await pool.query<{ id: string }>(
    `INSERT INTO social_connections (network, agent_name, last_interaction_at, interaction_count)
     VALUES ('moltbook', $1, NOW(), 1)
     ON CONFLICT (network, agent_name) DO UPDATE SET
       last_interaction_at = NOW(),
       interaction_count = social_connections.interaction_count + 1
     RETURNING id`,
    [agentName],
  );
  return rows[0].id;
}

interface MoltbookPost { id: string; title: string; author: string; content?: string; submolt?: string; }

export async function handleSocialEngagement(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  logger.log(`Starting social engagement ${job.id.slice(0, 8)}`);

  const { rows: identities } = await pool.query<{ api_key_encrypted: string }>(
    `SELECT api_key_encrypted FROM social_identity WHERE network = 'moltbook' AND status = 'active'`,
  );

  if (identities.length === 0 || !identities[0].api_key_encrypted) {
    logger.log('No active Moltbook identity, skipping');
    return { skipped: true, reason: 'No active Moltbook identity' };
  }

  const apiKey = identities[0].api_key_encrypted;
  const actions: string[] = [];

  try {
    await moltbookFetch('/home', apiKey);
    try { await moltbookFetch('/notifications/read', apiKey, 'POST'); } catch { /* ok */ }
    actions.push('Checked home dashboard');
    await pool.query(
      `INSERT INTO social_interactions (network, interaction_type, content_summary)
       VALUES ('moltbook', 'notification_read', 'Checked home and cleared notifications')`,
    );
  } catch (err) { logger.logMinimal('Home check failed:', (err as Error).message); }

  try {
    const feedData = await moltbookFetch('/feed?sort=hot&limit=10', apiKey) as { posts?: MoltbookPost[] };
    const posts = feedData.posts ?? (Array.isArray(feedData) ? feedData as MoltbookPost[] : []);

    if (posts.length > 0) {
      const shuffled = posts.sort(() => Math.random() - 0.5);
      const toEngage = shuffled.slice(0, Math.min(2, shuffled.length));

      for (const post of toEngage) {
        try {
          await moltbookFetch(`/posts/${post.id}/upvote`, apiKey, 'POST');
          let connectionId: string | null = null;
          try { if (post.author) connectionId = await ensureConnection(pool, post.author); } catch { /* ok */ }
          await pool.query(
            `INSERT INTO social_interactions (network, interaction_type, remote_id, content_summary, connection_id)
             VALUES ('moltbook', 'vote', $1, $2, $3)`,
            [post.id, `Upvoted: "${post.title}" by ${post.author}`, connectionId],
          );
          actions.push(`Upvoted "${post.title}" by ${post.author}`);
        } catch { /* ok */ }
      }

      const postToRead = shuffled[0];
      try {
        const fullPost = await moltbookFetch(`/posts/${postToRead.id}`, apiKey) as MoltbookPost;
        let connectionId: string | null = null;
        try { if (fullPost.author) connectionId = await ensureConnection(pool, fullPost.author); } catch { /* ok */ }
        await pool.query(
          `INSERT INTO social_interactions (network, interaction_type, remote_id, content_summary, raw_content, submolt, connection_id)
           VALUES ('moltbook', 'post_read', $1, $2, $3, $4, $5)`,
          [postToRead.id, fullPost.title, fullPost.content ?? null, fullPost.submolt ?? null, connectionId],
        );
        actions.push(`Read "${fullPost.title}"`);
      } catch { /* ok */ }
    }
  } catch (err) { logger.logMinimal('Feed browsing failed:', (err as Error).message); }

  try {
    const dmStatus = await moltbookFetch('/dm/check', apiKey);
    actions.push(`Checked DMs: ${JSON.stringify(dmStatus).slice(0, 100)}`);
  } catch { /* ok */ }

  await pool.query(
    `INSERT INTO actions_log (action_type, description, metadata) VALUES ($1, $2, $3)`,
    ['social_engagement', `Social engagement: ${actions.length} actions`, JSON.stringify({ actions })],
  );

  await logEvent({
    action: `Social engagement: ${actions.join('; ').slice(0, 150)}`,
    component: 'social',
    category: 'background',
    metadata: { actionCount: actions.length, actions },
  });

  logger.log(`Completed: ${actions.length} actions`);
  return { actions };
}
