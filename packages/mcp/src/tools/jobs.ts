import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { getPool } from '@nexus/core';

// Safe job types that can be triggered via MCP. Anything not on this list
// is rejected. Lives here as fallback; production reads from `settings` table.
const DEFAULT_SAFELIST = new Set([
  'embed-backfill', 'sentiment-backfill', 'knowledge-backfill', 'knowledge-summarize',
  'gmail-sync', 'calendar-sync', 'photos-sync', 'contacts-sync', 'imessage-sync',
  'notes-sync', 'music-sync', 'reverse-geocode', 'generate-merge-candidates',
  'data-hygiene', 'refresh-views', 'topic-model', 'daily-briefing', 'health-check',
  'llm-cost-report', 'aurora-nightly', 'log-monitor', 'music-backfill',
  'people-enrich', 'photo-describe-mcp',
]);

async function getSafelist(): Promise<Set<string>> {
  try {
    const pool = getPool();
    const { rows } = await pool.query(
      `SELECT value FROM settings WHERE key = 'mcp_job_safelist'`,
    );
    if (rows.length > 0 && rows[0].value) {
      const parsed = JSON.parse(rows[0].value);
      if (Array.isArray(parsed)) return new Set(parsed);
    }
  } catch {
    // fallback to default
  }
  return DEFAULT_SAFELIST;
}

export function registerJobTools(server: McpServer): void {
  // ── Run Job ─────────────────────────────────────────────
  server.tool(
    'nexus_run_job',
    'Enqueue a job for the Nexus worker (backfill, sync, report, analysis, etc.)',
    {
      job_type: z.string().describe('Handler name (e.g., embed-backfill, gmail-sync, daily-briefing)'),
      payload: z.record(z.unknown()).optional().describe('Job-specific payload (JSON object)'),
      priority: z.number().optional().default(0).describe('0=normal, 1=high, 2=urgent'),
    },
    async ({ job_type, payload, priority }) => {
      const safelist = await getSafelist();
      if (!safelist.has(job_type)) {
        return {
          content: [{
            type: 'text' as const,
            text: `ERROR: "${job_type}" is not on the MCP job safelist.\nAllowed: ${[...safelist].sort().join(', ')}`,
          }],
        };
      }

      const pool = getPool();

      // Check for existing pending job of same type to avoid duplicates
      const { rows: existing } = await pool.query(
        `SELECT id FROM tempo_jobs WHERE job_type = $1 AND status = 'pending' LIMIT 1`,
        [job_type],
      );
      if (existing.length > 0) {
        return {
          content: [{
            type: 'text' as const,
            text: `Already pending: ${job_type} (job_id: ${existing[0].id}). Not creating a duplicate.`,
          }],
        };
      }

      const { rows } = await pool.query(
        `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, status)
         VALUES ($1, $2::jsonb, 'nexus', $3, 3, 'pending')
         RETURNING id`,
        [job_type, JSON.stringify(payload ?? {}), priority],
      );
      return {
        content: [{
          type: 'text' as const,
          text: `Job enqueued: ${job_type} (id: ${rows[0].id}, priority: ${priority}). Worker will pick it up within ~5s.`,
        }],
      };
    },
  );
}
