import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { getPool, platformMode } from '@nexus/core';

const FORGE_BASE_URL = (process.env.FORGE_BASE_URL || 'http://localhost:8642')
  .replace(/\/v1\/?$/, '')
  .replace(/\/$/, '');
const FORGE_API_KEY = process.env.FORGE_API_KEY || '';

async function forgeFetch(path: string): Promise<unknown | null> {
  try {
    const headers: Record<string, string> = {};
    if (FORGE_API_KEY) headers.Authorization = `Bearer ${FORGE_API_KEY}`;
    const res = await fetch(`${FORGE_BASE_URL}${path}`, { headers, signal: AbortSignal.timeout(10_000) });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export function registerHealthTools(server: McpServer): void {
  // ── Platform Health ─────────────────────────────────────
  server.tool(
    'nexus_platform_health',
    'Combined health dashboard: platform mode, job queue, data source freshness, Forge backends',
    {},
    async () => {
      const pool = getPool();
      const [mode, jobQueue, stuckJobs, dataSources, backends] = await Promise.all([
        platformMode.getPlatformMode({ bypassCache: true }),
        pool.query(
          `SELECT status, COUNT(*)::int as count FROM tempo_jobs
           WHERE updated_at > NOW() - INTERVAL '1 hour'
           GROUP BY status ORDER BY count DESC`,
        ),
        pool.query(
          `SELECT job_type, COUNT(*)::int as count FROM tempo_jobs
           WHERE status = 'processing' AND updated_at < NOW() - INTERVAL '20 minutes'
           GROUP BY job_type`,
        ),
        pool.query(
          `SELECT source_key, display_name, ingestion_mode, status,
                  last_synced_at,
                  EXTRACT(EPOCH FROM NOW() - last_synced_at)::int as stale_seconds
           FROM data_source_registry
           WHERE is_active = true
           ORDER BY last_synced_at ASC NULLS FIRST`,
        ),
        forgeFetch('/health/backends'),
      ]);

      const result = {
        platform_mode: {
          mode: mode.mode,
          elapsed: platformMode.formatElapsed(mode.elapsed_ms),
          started_by: mode.started_by,
          reason: mode.reason,
        },
        job_queue: jobQueue.rows,
        stuck_jobs: stuckJobs.rows,
        data_sources: dataSources.rows.map((r: any) => ({
          source: r.source_key,
          name: r.display_name,
          mode: r.ingestion_mode,
          status: r.status,
          last_sync: r.last_synced_at,
          stale: r.stale_seconds != null
            ? r.stale_seconds < 3600 ? `${Math.round(r.stale_seconds / 60)}m`
            : r.stale_seconds < 86400 ? `${Math.round(r.stale_seconds / 3600)}h`
            : `${Math.round(r.stale_seconds / 86400)}d`
            : 'never',
        })),
        forge_backends: backends ?? 'unreachable',
      };
      return { content: [{ type: 'text' as const, text: JSON.stringify(result, null, 2) }] };
    },
  );

  // ── Forge Status ────────────────────────────────────────
  server.tool(
    'nexus_forge_status',
    'Forge LLM gateway: available models, backend health, and usage stats',
    {
      period: z.enum(['24h', '7d', '30d']).optional().default('24h').describe('Usage stats period'),
    },
    async ({ period }) => {
      const [models, backends, usage] = await Promise.all([
        forgeFetch('/v1/models'),
        forgeFetch('/health/backends'),
        forgeFetch(`/api/usage?period=${period}`),
      ]);
      const result = {
        models: models ?? 'unreachable',
        backends: backends ?? 'unreachable',
        usage: usage ?? 'unreachable',
      };
      return { content: [{ type: 'text' as const, text: JSON.stringify(result, null, 2) }] };
    },
  );

  // ── Data Sources ────────────────────────────────────────
  server.tool(
    'nexus_data_sources',
    'List all data source sync statuses with freshness indicators',
    {
      status: z.enum(['active', 'stale', 'error', 'all']).optional().default('all').describe('Filter by status'),
    },
    async ({ status }) => {
      const pool = getPool();
      const { rows } = await pool.query(
        `SELECT source_key, display_name, category, ingestion_mode, status,
                last_synced_at, sync_interval_sec, consecutive_failures,
                EXTRACT(EPOCH FROM NOW() - last_synced_at)::int as stale_seconds
         FROM data_source_registry
         WHERE is_active = true
         ORDER BY last_synced_at ASC NULLS FIRST`,
      );

      let filtered = rows;
      if (status === 'stale') {
        filtered = rows.filter((r: any) =>
          r.sync_interval_sec && r.stale_seconds > r.sync_interval_sec * 2,
        );
      } else if (status === 'error') {
        filtered = rows.filter((r: any) => r.consecutive_failures > 0);
      } else if (status === 'active') {
        filtered = rows.filter((r: any) => r.status === 'active' && (r.consecutive_failures ?? 0) === 0);
      }

      const result = filtered.map((r: any) => ({
        source: r.source_key,
        name: r.display_name,
        category: r.category,
        mode: r.ingestion_mode,
        status: r.status,
        last_sync: r.last_synced_at,
        failures: r.consecutive_failures,
        freshness: r.stale_seconds != null
          ? r.stale_seconds < 3600 ? `${Math.round(r.stale_seconds / 60)}m ago`
          : r.stale_seconds < 86400 ? `${Math.round(r.stale_seconds / 3600)}h ago`
          : `${Math.round(r.stale_seconds / 86400)}d ago`
          : 'never synced',
      }));
      return { content: [{ type: 'text' as const, text: JSON.stringify(result, null, 2) }] };
    },
  );
}
