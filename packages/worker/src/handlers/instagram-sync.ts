/**
 * Instagram Sync — tracks your posts, comments, and timeline feed.
 * Runs every 30 minutes. Pulls via Instagram MCP, pushes through
 * ingestion pipeline + urgency triage.
 * Self-chains after completion.
 */
import { getPool, createLogger } from '@nexus/core';
import { callMcpTool } from '../lib/mcp-client-manager.js';
import { ingest } from '../lib/ingestion-pipeline.js';
import { triageUrgency, type TriageItem } from '../lib/urgency-triage.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/instagram-sync');

const INTERVAL_SECONDS = 1800; // 30 minutes

export async function handleInstagramSync(_job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  try {
    // Get our profile
    let profile: Record<string, unknown>;
    try {
      const profileResult = await callMcpTool('instagram', 'instagram_get_current_user_profile', {});
      profile = JSON.parse(profileResult);
      logger.logVerbose(`Instagram profile: @${profile.username}`);
    } catch (err) {
      const msg = (err as Error).message;
      if (msg.includes('login') || msg.includes('auth') || msg.includes('session')) {
        logger.logMinimal('Instagram session expired — needs re-login');
        await pool.query(
          `UPDATE data_source_registry SET status = 'auth_expired' WHERE source_key = 'social_instagram'`,
        );
        return { error: 'auth_expired', checked: false };
      }
      logger.logMinimal('Instagram MCP error:', msg);
      return { error: msg, checked: false };
    }

    let totalIngested = 0;
    let totalSkipped = 0;
    let totalEnrichments = 0;
    let urgentCount = 0;

    // ── 1. Your own posts ──────────────────────────────────────
    const myPosts = await fetchAndParse('instagram_get_user_posts', {
      username: String(profile.username),
      limit: 20,
    });

    if (myPosts.length > 0) {
      const result = await ingest('instagram', myPosts);
      totalIngested += result.ingested;
      totalSkipped += result.skipped;
      totalEnrichments += result.enrichments;
      logger.logVerbose(`Own posts: ${result.ingested} new, ${result.skipped} dupes`);

      // Fetch comments on recent posts
      const commentItems = await fetchCommentsOnPosts(myPosts.slice(0, 5));
      if (commentItems.length > 0) {
        const commentResult = await ingest('instagram', commentItems);
        totalIngested += commentResult.ingested;
        totalSkipped += commentResult.skipped;
        totalEnrichments += commentResult.enrichments;
        logger.logVerbose(`Comments: ${commentResult.ingested} new, ${commentResult.skipped} dupes`);

        // Urgency triage on new comments (someone might need a reply)
        if (commentResult.ingested > 0) {
          const triageItems: TriageItem[] = commentItems.slice(0, 15).map(c => {
            const comment = c as Record<string, unknown>;
            const user = comment.user as Record<string, unknown> | undefined;
            return {
              from: `@${user?.username ?? 'unknown'}`,
              subject: `Comment on your post`,
              preview: String(comment.text ?? '').slice(0, 200),
            };
          });
          urgentCount += await triageUrgency(triageItems, {
            insightCategory: 'social_engagement',
            systemPrompt: `You are a social media engagement classifier. For each comment on the user's Instagram post, rate urgency 1-5:
1 = Generic/spam comment, no response needed
2 = Friendly comment, optional response
3 = Genuine question or compliment, should respond within a day
4 = Important question, collaboration request, or someone notable — respond soon
5 = Negative/crisis comment or business-critical — immediate attention

Respond with JSON only: {"results": [{"index": 1, "urgency": 3, "reason": "brief reason"}, ...]}`,
          });
        }
      }
    }

    // ── 2. Timeline feed (posts from people you follow) ────────
    const feedPosts = await fetchAndParse('instagram_get_timeline_feed', { limit: 20 });

    if (feedPosts.length > 0) {
      const feedResult = await ingest('instagram', feedPosts);
      totalIngested += feedResult.ingested;
      totalSkipped += feedResult.skipped;
      totalEnrichments += feedResult.enrichments;
      logger.logVerbose(`Feed: ${feedResult.ingested} new, ${feedResult.skipped} dupes`);
    }

    // Update source registry
    await pool.query(
      `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active', consecutive_failures = 0
       WHERE source_key = 'social_instagram'`,
    );

    logger.log(`Instagram sync: ${totalIngested} new, ${totalSkipped} dupes, ${urgentCount} urgent`);

    return {
      checked: true,
      new_items: totalIngested,
      duplicates: totalSkipped,
      enrichments_queued: totalEnrichments,
      urgent_flagged: urgentCount,
    };
  } finally {
    // Self-chain with pile-up guard — skip if one is already pending.
    try {
      const existing = await pool.query(
        `SELECT id FROM tempo_jobs WHERE job_type = 'instagram-sync' AND status = 'pending' LIMIT 1`,
      );
      if (existing.rows.length === 0) {
        await pool.query(
          `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
           VALUES ('instagram-sync', '{}', 'nexus', 2, 3, NOW() + INTERVAL '${INTERVAL_SECONDS} seconds')`,
        );
        logger.logVerbose(`Next instagram-sync scheduled in ${INTERVAL_SECONDS}s`);
      }
    } catch (err) {
      logger.logMinimal('Failed to self-chain instagram-sync:', (err as Error).message);
    }
  }
}

// ── Helpers ────────────────────────────────────────────────────

async function fetchAndParse(tool: string, params: Record<string, unknown>): Promise<unknown[]> {
  try {
    const result = await callMcpTool('instagram', tool, params);
    const parsed = JSON.parse(result);
    return Array.isArray(parsed) ? parsed : (parsed.items ?? parsed.posts ?? parsed.media ?? parsed.feed_items ?? []);
  } catch (err) {
    logger.logVerbose(`Instagram ${tool} error:`, (err as Error).message);
    return [];
  }
}

async function fetchCommentsOnPosts(posts: unknown[]): Promise<unknown[]> {
  const allComments: unknown[] = [];

  for (const p of posts) {
    const post = p as Record<string, unknown>;
    const mediaId = String(post.pk ?? post.id ?? post.media_id ?? '');
    if (!mediaId) continue;

    const commentCount = Number(post.comment_count ?? 0);
    if (commentCount === 0) continue;

    try {
      const result = await callMcpTool('instagram', 'instagram_get_post_comments', {
        mediaId,
        limit: 20,
      });
      const parsed = JSON.parse(result);
      const comments = Array.isArray(parsed) ? parsed : (parsed.comments ?? []);

      // Tag each comment with post context and data_type for the normalizer
      for (const c of comments) {
        const comment = c as Record<string, unknown>;
        comment._data_type = 'comment';
        comment._parent_post_id = mediaId;
        allComments.push(comment);
      }
    } catch (err) {
      logger.logVerbose(`Failed to fetch comments for ${mediaId}:`, (err as Error).message);
    }
  }

  return allComments;
}
