/**
 * Self-improve DB handler.
 *
 * Gathers database metrics, asks an LLM for safe maintenance actions,
 * validates and executes them in transactions, and logs the results.
 *
 * Requires: LLM provider
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';

const logger = createLogger('self-improve-db');

/** Tables that are NEVER touched by automatic maintenance */
const PROTECTED_TABLES = new Set([
  'conversations',
  'messages',
  'google_tokens',
  'sessions',
  'core_memory',
  'settings',
]);

/** Tables that are allowed for DELETE operations */
const ALLOWED_DELETE_TABLES = new Set([
  'context_snapshots',
  'tempo_jobs',
  'anticipation_patterns',
  'anticipation_feedback',
  'event_log',
  'actions_log',
  'aria_journal_queue',
  'device_activity',
  'device_location',
  'health_data',
  'social_daemon_runs',
  'llm_usage',
]);

interface DbMetrics {
  tableSizes: Array<{ relname: string; n_live_tup: number }>;
  staleSnapshots: number;
  oldFailedJobs: number;
  lowConfidencePatterns: number;
  memoryStats: Array<{ category: string; count: number }>;
  feedbackStats: Array<{ rating: string; count: number }>;
}

interface MaintenanceAction {
  action: string;
  query: string;
  params: unknown[];
  reason: string;
}

/**
 * Gather all DB metrics for analysis.
 */
async function gatherMetrics(): Promise<DbMetrics> {
  const pool = getPool();

  const metrics: DbMetrics = {
    tableSizes: [],
    staleSnapshots: 0,
    oldFailedJobs: 0,
    lowConfidencePatterns: 0,
    memoryStats: [],
    feedbackStats: [],
  };

  try {
    const { rows } = await pool.query(
      `SELECT relname, n_live_tup::int FROM pg_stat_user_tables ORDER BY n_live_tup DESC`
    );
    metrics.tableSizes = rows;
  } catch {
    logger.logMinimal('Could not fetch table sizes');
  }

  try {
    const { rows } = await pool.query(
      `SELECT COUNT(*)::int as count FROM context_snapshots WHERE analyzed = true AND created_at < NOW() - INTERVAL '7 days'`
    );
    metrics.staleSnapshots = rows[0]?.count ?? 0;
  } catch {
    // Table may not exist
  }

  try {
    const { rows } = await pool.query(
      `SELECT COUNT(*)::int as count FROM tempo_jobs WHERE status = 'failed' AND created_at < NOW() - INTERVAL '30 days'`
    );
    metrics.oldFailedJobs = rows[0]?.count ?? 0;
  } catch {
    // Ignore
  }

  try {
    const { rows } = await pool.query(
      `SELECT COUNT(*)::int as count FROM anticipation_patterns WHERE confidence < 0.3`
    );
    metrics.lowConfidencePatterns = rows[0]?.count ?? 0;
  } catch {
    // Table may not exist
  }

  try {
    const { rows } = await pool.query(
      `SELECT category, COUNT(*)::int as count FROM core_memory WHERE superseded_by IS NULL GROUP BY category`
    );
    metrics.memoryStats = rows;
  } catch {
    // Ignore
  }

  try {
    const { rows } = await pool.query(
      `SELECT rating, COUNT(*)::int as count FROM anticipation_feedback GROUP BY rating`
    );
    metrics.feedbackStats = rows;
  } catch {
    // Table may not exist
  }

  return metrics;
}

/**
 * Format metrics into a readable block for the LLM.
 */
function formatMetrics(metrics: DbMetrics): string {
  const lines: string[] = [];

  lines.push('## Table Sizes');
  for (const t of metrics.tableSizes) {
    lines.push(`  ${t.relname}: ${t.n_live_tup.toLocaleString()} rows`);
  }

  lines.push('');
  lines.push(`## Stale Data`);
  lines.push(`  Analyzed snapshots older than 7 days: ${metrics.staleSnapshots}`);
  lines.push(`  Failed jobs older than 30 days: ${metrics.oldFailedJobs}`);
  lines.push(`  Low-confidence patterns (< 0.3): ${metrics.lowConfidencePatterns}`);

  lines.push('');
  lines.push(`## Memory Stats (active, by category)`);
  for (const m of metrics.memoryStats) {
    lines.push(`  ${m.category}: ${m.count}`);
  }

  lines.push('');
  lines.push(`## Feedback Stats`);
  if (metrics.feedbackStats.length > 0) {
    for (const f of metrics.feedbackStats) {
      lines.push(`  ${f.rating}: ${f.count}`);
    }
  } else {
    lines.push('  No feedback recorded.');
  }

  return lines.join('\n');
}

/**
 * Validate that a maintenance action is safe to execute.
 * Returns an error string if unsafe, or null if safe.
 */
function validateAction(action: MaintenanceAction): string | null {
  const query = action.query.trim().toUpperCase();

  // Must be a DELETE statement
  if (!query.startsWith('DELETE')) {
    return `Only DELETE queries are allowed, got: ${query.slice(0, 30)}`;
  }

  // Must have a WHERE clause
  if (!query.includes('WHERE')) {
    return 'DELETE without WHERE clause is not allowed';
  }

  // Extract table name from DELETE FROM <table>
  const deleteMatch = action.query.trim().match(/^DELETE\s+FROM\s+(\w+)/i);
  if (!deleteMatch) {
    return 'Could not parse table name from DELETE query';
  }

  const tableName = deleteMatch[1].toLowerCase();

  // Check protected tables
  if (PROTECTED_TABLES.has(tableName)) {
    return `Table "${tableName}" is protected and cannot be modified`;
  }

  // Check allowed tables
  if (!ALLOWED_DELETE_TABLES.has(tableName)) {
    return `Table "${tableName}" is not in the allowed list for automatic maintenance`;
  }

  return null;
}

export async function handleSelfImproveDb(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = (job.payload ?? {}) as Record<string, unknown>;
  logger.log(`Starting DB maintenance job ${job.id}`);

  // Gather metrics
  const metrics = await gatherMetrics();
  const metricsText = formatMetrics(metrics);
  logger.log(`Gathered metrics: ${metrics.tableSizes.length} tables`);

  // Ask LLM for maintenance suggestions
  const result = await routeRequest({
    handler: 'self-improve-db',
    taskTier: 'generation',
    systemPrompt:
      'You are a database maintenance advisor for a personal AI assistant platform (PostgreSQL). ' +
      'Given the current database state, suggest 1-3 safe, incremental maintenance actions. ' +
      'Return ONLY valid JSON (no markdown fences): an array of objects: ' +
      '[{ "action": "description of what this does", "query": "DELETE FROM table WHERE condition", "params": [], "reason": "why this is safe and beneficial" }]. ' +
      'Rules: ' +
      '- Only suggest DELETE for genuinely stale data (old analyzed snapshots, old failed jobs, low-confidence patterns). ' +
      '- NEVER touch: conversations, messages, google_tokens, sessions, core_memory, settings. ' +
      '- Every DELETE must have a WHERE clause with a time or threshold condition. ' +
      '- Use parameterized queries where possible (use $1, $2 placeholders). ' +
      '- If the database looks healthy, return an empty array []. ' +
      '- Be conservative — it is better to do nothing than to delete useful data.',
    userMessage: `Here is the current database state:\n\n${metricsText}\n\nSuggest safe maintenance actions.`,
    maxTokens: 1000,
    useBatch: true,
  });

  logger.log(`LLM response via ${result.model} (${result.provider}, ${result.estimatedCostCents}¢)`);

  // Parse actions
  let actions: MaintenanceAction[] = [];
  try {
    const cleaned = result.text.replace(/```json\s*/g, '').replace(/```\s*/g, '').trim();
    const parsed = JSON.parse(cleaned);
    actions = Array.isArray(parsed) ? parsed : [];
  } catch {
    logger.logMinimal('Failed to parse LLM response:', result.text.slice(0, 500));
    // Don't throw — log the issue and return gracefully
    logEvent({
      action: 'Self-improve DB: failed to parse LLM maintenance suggestions',
      component: 'self-improvement',
      category: 'self_maintenance',
      status: 'error',
      metadata: { response_preview: result.text.slice(0, 300), model: result.model },
    });
    return { actions_executed: 0, message: 'LLM response could not be parsed', model: result.model };
  }

  if (actions.length === 0) {
    logger.log('No maintenance actions suggested — database is healthy');
    return { actions_executed: 0, message: 'Database is healthy, no maintenance needed' };
  }

  // Execute each action
  const pool = getPool();
  const executed: Array<{ action: string; rows_affected: number; reason: string }> = [];
  const skipped: Array<{ action: string; reason: string }> = [];

  for (const action of actions) {
    // Validate safety
    const validationError = validateAction(action);
    if (validationError) {
      logger.logMinimal(`Skipping unsafe action: ${validationError}`);
      skipped.push({ action: action.action, reason: validationError });
      continue;
    }

    // Execute in a transaction with statement timeout
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`SET LOCAL statement_timeout = '30000'`);

      const queryResult = await client.query(
        action.query,
        Array.isArray(action.params) ? action.params : []
      );
      const rowsAffected = queryResult.rowCount ?? 0;

      await client.query('COMMIT');

      logger.log(`Executed: ${action.action} — ${rowsAffected} rows affected`);
      executed.push({
        action: action.action,
        rows_affected: rowsAffected,
        reason: action.reason,
      });
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      const msg = (err as Error).message;
      logger.logMinimal(`Failed to execute action "${action.action}": ${msg}`);
      skipped.push({ action: action.action, reason: `Execution failed: ${msg}` });
    } finally {
      client.release();
    }
  }

  // Write summary to aria_self_improvement
  const totalRowsDeleted = executed.reduce((sum, e) => sum + e.rows_affected, 0);
  const summaryText = executed
    .map((e) => `${e.action}: ${e.rows_affected} rows (${e.reason})`)
    .join('; ');

  try {
    await pool.query(
      `INSERT INTO aria_self_improvement
       (category, status, title, description, source, outcome)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        'database_tune',
        'implemented',
        `DB Maintenance: ${totalRowsDeleted} rows cleaned`,
        summaryText || 'No actions executed',
        'automated',
        JSON.stringify({ executed, skipped }),
      ]
    );
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal(`Failed to log to aria_self_improvement: ${msg}`);
  }

  // Log to event_log
  logEvent({
    action: `Self-improve DB: executed ${executed.length} action(s), ${totalRowsDeleted} rows cleaned`,
    component: 'self-improvement',
    category: 'self_maintenance',
    metadata: {
      executed_count: executed.length,
      skipped_count: skipped.length,
      total_rows_deleted: totalRowsDeleted,
      actions: executed,
    },
  });

  return {
    actions_executed: executed.length,
    actions_skipped: skipped.length,
    total_rows_deleted: totalRowsDeleted,
    executed,
    skipped,
  };
}
