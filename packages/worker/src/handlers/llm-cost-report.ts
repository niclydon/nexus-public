/**
 * LLM Cost Report handler.
 *
 * Generates a daily report summarizing AI model usage, cost, latency,
 * and fallback events. Delivered via the unified report system.
 *
 * Scheduled daily at 8am ET via report_subscriptions.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { deliverReport } from '../lib/report-delivery.js';
import { TIMEZONE } from '../lib/timezone.js';

const logger = createLogger('llm-cost-report');

interface UsageRow {
  model: string;
  provider: string;
  handler: string;
  task_tier: string;
  calls: string;
  total_input: string;
  total_output: string;
  total_cost: string;
  avg_latency: string;
  fallbacks: string;
}

export async function handleLlmCostReport(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Check if subscription is enabled
  const { rows: subRows } = await pool.query(
    `SELECT is_enabled FROM report_subscriptions WHERE report_type = $1 LIMIT 1`,
    ['llm-cost-report']
  );
  if (subRows.length > 0 && !subRows[0].is_enabled) {
    logger.log('Subscription disabled, skipping');
    return { skipped: true, reason: 'subscription_disabled' };
  }

  logger.log('Generating daily cost report');

  // Today's usage grouped by model
  const { rows: byModel } = await pool.query<UsageRow>(
    `SELECT model, provider,
            COUNT(*) as calls,
            SUM(input_tokens) as total_input,
            SUM(output_tokens) as total_output,
            ROUND(SUM(estimated_cost_cents)::numeric, 2) as total_cost,
            ROUND(AVG(latency_ms)::numeric) as avg_latency,
            COUNT(*) FILTER (WHERE was_fallback) as fallbacks
     FROM llm_usage
     WHERE created_at > NOW() - INTERVAL '24 hours'
     GROUP BY model, provider
     ORDER BY SUM(estimated_cost_cents) DESC`
  );

  // Today's usage grouped by handler
  const { rows: byHandler } = await pool.query<UsageRow>(
    `SELECT handler, task_tier,
            COUNT(*) as calls,
            SUM(input_tokens) as total_input,
            SUM(output_tokens) as total_output,
            ROUND(SUM(estimated_cost_cents)::numeric, 2) as total_cost,
            ROUND(AVG(latency_ms)::numeric) as avg_latency,
            COUNT(*) FILTER (WHERE was_fallback) as fallbacks
     FROM llm_usage
     WHERE created_at > NOW() - INTERVAL '24 hours'
     GROUP BY handler, task_tier
     ORDER BY SUM(estimated_cost_cents) DESC`
  );

  // Yesterday's total for comparison
  const { rows: yesterdayRow } = await pool.query<{ total: string }>(
    `SELECT ROUND(COALESCE(SUM(estimated_cost_cents), 0)::numeric, 2) as total
     FROM llm_usage
     WHERE created_at > NOW() - INTERVAL '48 hours'
       AND created_at <= NOW() - INTERVAL '24 hours'`
  );

  // Fallback details
  const { rows: fallbackDetails } = await pool.query<{
    model: string; provider: string; handler: string; fallback_reason: string; created_at: string;
  }>(
    `SELECT model, provider, handler, fallback_reason, created_at
     FROM llm_usage
     WHERE was_fallback = true AND created_at > NOW() - INTERVAL '24 hours'
     ORDER BY created_at DESC
     LIMIT 10`
  );

  // Build report
  const todayTotal = byModel.reduce((sum, r) => sum + parseFloat(r.total_cost || '0'), 0);
  const yesterdayTotal = parseFloat(yesterdayRow[0]?.total || '0');
  const totalCalls = byModel.reduce((sum, r) => sum + parseInt(r.calls || '0', 10), 0);
  const totalFallbacks = byModel.reduce((sum, r) => sum + parseInt(r.fallbacks || '0', 10), 0);

  const trend = yesterdayTotal > 0
    ? `${todayTotal > yesterdayTotal ? '+' : ''}${Math.round(((todayTotal - yesterdayTotal) / yesterdayTotal) * 100)}%`
    : 'no comparison';

  const lines: string[] = [];
  lines.push(`LLM Cost Report`);
  lines.push('');
  lines.push(`TOTAL: $${(todayTotal / 100).toFixed(2)} (yesterday: $${(yesterdayTotal / 100).toFixed(2)}, ${trend})`);
  lines.push(`Calls: ${totalCalls} | Fallbacks: ${totalFallbacks}`);
  lines.push('');

  // By model
  lines.push('BY MODEL:');
  if (byModel.length === 0) {
    lines.push('  No LLM calls in the last 24 hours.');
  } else {
    for (const r of byModel) {
      const cost = (parseFloat(r.total_cost) / 100).toFixed(3);
      lines.push(`  ${r.model} (${r.provider})  $${cost}  (${r.calls} calls, avg ${r.avg_latency}ms${parseInt(r.fallbacks) > 0 ? `, ${r.fallbacks} fallbacks` : ''})`);
    }
  }
  lines.push('');

  // By handler
  lines.push('BY HANDLER:');
  if (byHandler.length === 0) {
    lines.push('  No handler data.');
  } else {
    for (const r of byHandler) {
      const cost = (parseFloat(r.total_cost) / 100).toFixed(3);
      const tokens = `${Math.round(parseInt(r.total_input) / 1000)}K in / ${Math.round(parseInt(r.total_output) / 1000)}K out`;
      lines.push(`  ${r.handler} [${r.task_tier}]  $${cost}  (${r.calls} calls, ${tokens})`);
    }
  }

  // Fallbacks
  if (fallbackDetails.length > 0) {
    lines.push('');
    lines.push('FALLBACK EVENTS:');
    for (const f of fallbackDetails) {
      const time = new Date(f.created_at).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', hour12: true, timeZone: TIMEZONE });
      lines.push(`  ${time} ${f.handler}: used ${f.model} (${f.provider}) -- ${f.fallback_reason || 'primary unavailable'}`);
    }
  }

  const reportBody = lines.join('\n');
  const title = totalCalls > 0
    ? `LLM Cost: $${(todayTotal / 100).toFixed(2)} (${totalCalls} calls, ${trend} vs yesterday)`
    : 'LLM Cost: No calls in the last 24 hours';

  const deliveredVia = await deliverReport({
    reportType: 'llm-cost-report',
    title,
    body: reportBody,
    category: 'diagnostic',
    metadata: {
      today_cost_cents: todayTotal,
      yesterday_cost_cents: yesterdayTotal,
      total_calls: totalCalls,
      total_fallbacks: totalFallbacks,
    },
  });

  logger.log(`Delivered via: ${deliveredVia.join(', ') || 'none'}`);

  // ── Budget alert: push notification when daily spend >= 80% of budget ──
  try {
    const { rows: budgetRows } = await pool.query<{ value: string }>(
      `SELECT value FROM settings WHERE key = 'llm_daily_budget_cents'`
    );
    const dailyBudgetCents = parseInt(budgetRows[0]?.value || '500', 10);
    const threshold = dailyBudgetCents * 0.8;

    if (todayTotal >= threshold) {
      // Check if we already sent a budget alert today
      const { rows: existingAlerts } = await pool.query<{ id: string }>(
        `SELECT id FROM tempo_jobs
         WHERE job_type = $1
           AND payload::text LIKE '%LLM spending alert%'
           AND created_at >= CURRENT_DATE
         LIMIT 1`,
        ['push-notification']
      );

      if (existingAlerts.length === 0) {
        // Find a push delivery method to get the device token
        const { rows: pushMethods } = await pool.query<{ config: Record<string, string> }>(
          `SELECT config FROM notification_delivery_methods
           WHERE channel = 'push' AND is_enabled = true
           LIMIT 1`
        );

        if (pushMethods.length > 0) {
          const deviceToken = pushMethods[0].config?.device_token;
          const isSandbox = pushMethods[0].config?.is_sandbox === 'true';
          if (deviceToken) {
            const pct = Math.round((todayTotal / dailyBudgetCents) * 100);
            const spendDollars = (todayTotal / 100).toFixed(2);
            await pool.query(
              `INSERT INTO tempo_jobs (job_type, payload, priority, max_attempts)
               VALUES ($1, $2, $3, $4)`,
              [
                'push-notification',
                JSON.stringify({
                  device_token: deviceToken,
                  title: 'LLM Spending Alert',
                  body: `LLM spending alert: $${spendDollars} today (${pct}% of daily budget)`,
                  priority: 'default',
                  category: 'general',
                  is_sandbox: isSandbox,
                }),
                1,
                2,
              ]
            );
            logger.log(`Budget alert sent: $${spendDollars} (${pct}% of $${(dailyBudgetCents / 100).toFixed(2)} budget)`);
          }
        }
      } else {
        logger.log('Budget alert already sent today, skipping');
      }
    }
  } catch (err) {
    logger.logMinimal('Failed to check/send budget alert:', err instanceof Error ? err.message : err);
  }

  return {
    delivered_via: deliveredVia,
    today_cost_cents: todayTotal,
    total_calls: totalCalls,
    total_fallbacks: totalFallbacks,
  };
}
