/**
 * Refresh Materialized Views & Schema Registries
 *
 * Refreshes all aurora_unified_* materialized views and updates
 * 4 schema registry tables: table_catalog, fk_catalog, enum_registry, table_stats.
 *
 * Scheduled every 4 hours to keep dashboard data and schema metadata fresh.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('refresh-views');

const MATERIALIZED_VIEWS = [
  'aurora_unified_communication',
  'aurora_unified_location',
  'aurora_unified_calls',
  'aurora_unified_health',
  'aurora_unified_ai_conversations',
  'aurora_unified_social',
  'aurora_unified_travel',
];

export async function handleRefreshViews(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const refreshed: string[] = [];
  const failed: string[] = [];

  logger.log(`Refreshing ${MATERIALIZED_VIEWS.length} materialized views + 4 schema registries`);
  await jobLog(job.id, `Refreshing ${MATERIALIZED_VIEWS.length} views + schema registries`);

  // 1. Refresh materialized views
  for (const view of MATERIALIZED_VIEWS) {
    try {
      const start = Date.now();
      await pool.query(`REFRESH MATERIALIZED VIEW ${view}`);
      const elapsed = Date.now() - start;
      logger.logVerbose(`Refreshed ${view} in ${elapsed}ms`);
      refreshed.push(view);
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal(`Failed to refresh ${view}: ${msg}`);
      failed.push(view);
    }
  }

  // 2. Refresh table_catalog (columns + types for all tables and mat views)
  try {
    await pool.query(`
      INSERT INTO table_catalog (table_name, column_name, data_type, is_nullable, column_default)
      SELECT t.table_name, c.column_name, c.data_type, c.is_nullable = 'YES', c.column_default
      FROM information_schema.tables t
      JOIN information_schema.columns c ON c.table_name = t.table_name AND c.table_schema = t.table_schema
      WHERE t.table_schema = 'public' AND t.table_type IN ('BASE TABLE', 'VIEW')
      ON CONFLICT (table_name, column_name) DO UPDATE SET
        data_type = EXCLUDED.data_type, is_nullable = EXCLUDED.is_nullable,
        column_default = EXCLUDED.column_default, updated_at = NOW()
    `);
    await pool.query(`
      INSERT INTO table_catalog (table_name, column_name, data_type, is_nullable, column_default)
      SELECT c.relname, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), NOT a.attnotnull, NULL
      FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid
      WHERE c.relkind = 'm' AND a.attnum > 0 AND NOT a.attisdropped
      ON CONFLICT (table_name, column_name) DO UPDATE SET
        data_type = EXCLUDED.data_type, updated_at = NOW()
    `);
    logger.logVerbose('Refreshed table_catalog');
  } catch (err) {
    logger.logMinimal(`Failed to refresh table_catalog: ${(err as Error).message}`);
  }

  // 3. Refresh fk_catalog (foreign key relationships)
  try {
    await pool.query(`
      INSERT INTO fk_catalog (constraint_name, source_table, source_column, target_table, target_column, on_delete, on_update)
      SELECT
        con.conname, src.relname, sa.attname, tgt.relname, ta.attname,
        CASE con.confdeltype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' END,
        CASE con.confupdtype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' END
      FROM pg_constraint con
      JOIN pg_class src ON src.oid = con.conrelid
      JOIN pg_class tgt ON tgt.oid = con.confrelid
      JOIN pg_attribute sa ON sa.attrelid = con.conrelid AND sa.attnum = ANY(con.conkey)
      JOIN pg_attribute ta ON ta.attrelid = con.confrelid AND ta.attnum = ANY(con.confkey)
      WHERE con.contype = 'f'
      ON CONFLICT (constraint_name) DO UPDATE SET
        source_table = EXCLUDED.source_table, source_column = EXCLUDED.source_column,
        target_table = EXCLUDED.target_table, target_column = EXCLUDED.target_column,
        on_delete = EXCLUDED.on_delete, on_update = EXCLUDED.on_update, updated_at = NOW()
    `);
    logger.logVerbose('Refreshed fk_catalog');
  } catch (err) {
    logger.logMinimal(`Failed to refresh fk_catalog: ${(err as Error).message}`);
  }

  // 4. Refresh table_stats (row counts + sizes)
  try {
    await pool.query(`
      INSERT INTO table_stats (table_name, row_count, size_bytes)
      SELECT relname, reltuples::bigint, pg_total_relation_size(c.oid)
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND c.relkind IN ('r', 'm')
      ON CONFLICT (table_name) DO UPDATE SET
        row_count = EXCLUDED.row_count, size_bytes = EXCLUDED.size_bytes, updated_at = NOW()
    `);
    logger.logVerbose('Refreshed table_stats');
  } catch (err) {
    logger.logMinimal(`Failed to refresh table_stats: ${(err as Error).message}`);
  }

  // 5. Refresh enum_registry (distinct values for enum-like text columns)
  try {
    await pool.query(`
      DO $$
      DECLARE
        rec RECORD;
        vals TEXT[];
        cnt INTEGER;
      BEGIN
        FOR rec IN
          SELECT tc.table_name, tc.column_name
          FROM table_catalog tc
          WHERE tc.data_type IN ('text', 'character varying', 'USER-DEFINED')
            AND tc.table_name NOT LIKE 'pg_%'
            AND tc.table_name NOT IN ('table_catalog', 'enum_registry', 'fk_catalog', 'table_stats')
          ORDER BY tc.table_name, tc.column_name
        LOOP
          BEGIN
            EXECUTE format(
              'SELECT ARRAY(SELECT DISTINCT %I::text FROM %I WHERE %I IS NOT NULL ORDER BY 1 LIMIT 50)',
              rec.column_name, rec.table_name, rec.column_name
            ) INTO vals;
            cnt := array_length(vals, 1);
            IF cnt IS NOT NULL AND cnt <= 30 THEN
              INSERT INTO enum_registry (table_name, column_name, distinct_values, sample_count)
              VALUES (rec.table_name, rec.column_name, vals, cnt)
              ON CONFLICT (table_name, column_name) DO UPDATE SET
                distinct_values = EXCLUDED.distinct_values, sample_count = EXCLUDED.sample_count, updated_at = NOW();
            END IF;
          EXCEPTION WHEN OTHERS THEN
            NULL;
          END;
        END LOOP;
      END;
      $$
    `);
    logger.logVerbose('Refreshed enum_registry');
  } catch (err) {
    logger.logMinimal(`Failed to refresh enum_registry: ${(err as Error).message}`);
  }

  const result = { refreshed: refreshed.length, failed: failed.length, failed_views: failed };
  logger.log(`Complete: ${refreshed.length} views refreshed, ${failed.length} failed, 4 registries updated`);
  await jobLog(job.id, `Views: ${refreshed.length}/${MATERIALIZED_VIEWS.length}, registries: table_catalog + fk_catalog + enum_registry + table_stats`);
  return result;
}
