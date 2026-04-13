/**
 * Self-improvement report handler.
 *
 * Every day at 3pm ET, ARIA reviews her own platform — recent conversations,
 * tool usage patterns, errors, memory state, job history, and available
 * capabilities — then generates a single actionable technical enhancement
 * suggestion. Delivered via email and push notification.
 *
 * Requires: ANTHROPIC_API_KEY, AWS SES (aria-email-send), OWNER_EMAIL
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { deliverReport } from '../lib/report-delivery.js';
import { routeRequest } from '../lib/llm/index.js';
import { TIMEZONE } from '../lib/timezone.js';

const logger = createLogger('self-improvement');


/**
 * Fetch recent conversation stats (last 24 hours).
 */
async function fetchConversationStats(): Promise<string> {
  const pool = getPool();
  try {
    const { rows } = await pool.query(`
      SELECT
        COUNT(DISTINCT c.id) as conversation_count,
        COUNT(m.id) as message_count,
        MAX(m.created_at) as last_message_at
      FROM conversations c
      LEFT JOIN messages m ON m.conversation_id = c.id
      WHERE m.created_at > NOW() - INTERVAL '24 hours'
    `);
    const r = rows[0];
    return `Conversations (24h): ${r.conversation_count}, Messages: ${r.message_count}, Last activity: ${r.last_message_at || 'none'}`;
  } catch {
    return 'Conversation stats unavailable.';
  }
}

/**
 * Fetch recent tool usage patterns from event_log.
 */
async function fetchToolUsagePatterns(): Promise<string> {
  const pool = getPool();
  try {
    const { rows } = await pool.query(`
      SELECT
        component,
        category,
        COUNT(*) as count
      FROM event_log
      WHERE created_at > NOW() - INTERVAL '7 days'
      GROUP BY component, category
      ORDER BY count DESC
      LIMIT 20
    `);
    if (rows.length === 0) return 'No tool usage recorded in last 7 days.';
    const lines = rows.map(r => `  ${r.component}/${r.category}: ${r.count} events`);
    return `Tool usage (7d):\n${lines.join('\n')}`;
  } catch {
    return 'Tool usage stats unavailable.';
  }
}

/**
 * Fetch recent job outcomes from tempo_jobs.
 */
async function fetchJobStats(): Promise<string> {
  const pool = getPool();
  try {
    const { rows } = await pool.query(`
      SELECT
        job_type,
        status,
        COUNT(*) as count
      FROM tempo_jobs
      WHERE created_at > NOW() - INTERVAL '7 days'
      GROUP BY job_type, status
      ORDER BY job_type, status
    `);
    if (rows.length === 0) return 'No jobs processed in last 7 days.';

    // Group by type
    const byType: Record<string, Record<string, number>> = {};
    for (const r of rows) {
      if (!byType[r.job_type]) byType[r.job_type] = {};
      byType[r.job_type][r.status] = parseInt(r.count);
    }

    const lines = Object.entries(byType).map(([type, statuses]) => {
      const parts = Object.entries(statuses).map(([s, c]) => `${s}:${c}`);
      return `  ${type}: ${parts.join(', ')}`;
    });
    return `Job stats (7d):\n${lines.join('\n')}`;
  } catch {
    return 'Job stats unavailable.';
  }
}

/**
 * Fetch recent errors and failures.
 */
async function fetchRecentErrors(): Promise<string> {
  const pool = getPool();
  try {
    const { rows } = await pool.query(`
      SELECT job_type, last_error, created_at
      FROM tempo_jobs
      WHERE status = 'failed'
        AND created_at > NOW() - INTERVAL '7 days'
      ORDER BY created_at DESC
      LIMIT 10
    `);
    if (rows.length === 0) return 'No failed jobs in last 7 days.';
    const lines = rows.map(r => `  [${r.job_type}] ${(r.last_error || 'unknown error').substring(0, 200)}`);
    return `Recent failures (7d):\n${lines.join('\n')}`;
  } catch {
    return 'Error history unavailable.';
  }
}

/**
 * Fetch memory stats.
 */
async function fetchMemoryStats(): Promise<string> {
  const pool = getPool();
  try {
    const { rows } = await pool.query(`
      SELECT
        category,
        COUNT(*) as count
      FROM core_memory
      WHERE superseded_by IS NULL
      GROUP BY category
      ORDER BY count DESC
    `);
    if (rows.length === 0) return 'No active memories.';
    const lines = rows.map(r => `  ${r.category}: ${r.count} facts`);
    const total = rows.reduce((sum, r) => sum + parseInt(r.count), 0);
    return `Active memory: ${total} total facts\n${lines.join('\n')}`;
  } catch {
    return 'Memory stats unavailable.';
  }
}

/**
 * Fetch proactive intelligence stats.
 */
async function fetchProactiveStats(): Promise<string> {
  const pool = getPool();
  try {
    const { rows: insightRows } = await pool.query(`
      SELECT status, COUNT(*) as count
      FROM proactive_insights
      WHERE created_at > NOW() - INTERVAL '7 days'
      GROUP BY status
    `);

    const { rows: patternRows } = await pool.query(`
      SELECT CASE WHEN is_active THEN 'active' ELSE 'inactive' END as status, COUNT(*) as count
      FROM anticipation_patterns
      GROUP BY is_active
    `);

    const insightLines = insightRows.map(r => `${r.status}: ${r.count}`).join(', ');
    const patternLines = patternRows.map(r => `${r.status}: ${r.count}`).join(', ');

    return `Proactive insights (7d): ${insightLines || 'none'}\nAnticipation patterns: ${patternLines || 'none'}`;
  } catch {
    return 'Proactive stats unavailable (tables may not exist yet).';
  }
}

/**
 * Fetch health data availability.
 */
async function fetchHealthDataStats(): Promise<string> {
  const pool = getPool();
  try {
    const { rows } = await pool.query(`
      SELECT
        data_type,
        COUNT(*) as count,
        MAX(created_at) as latest
      FROM health_data
      WHERE created_at > NOW() - INTERVAL '7 days'
      GROUP BY data_type
    `);
    if (rows.length === 0) return 'No health data in last 7 days.';
    const lines = rows.map(r => `  ${r.data_type}: ${r.count} records (latest: ${r.latest})`);
    return `Health data (7d):\n${lines.join('\n')}`;
  } catch {
    return 'Health data stats unavailable.';
  }
}

/**
 * Fetch connected integrations status.
 */
async function fetchIntegrationStatus(): Promise<string> {
  const pool = getPool();
  const status: string[] = [];

  try {
    const { rows: googleRows } = await pool.query(
      `SELECT COUNT(*) as count FROM google_tokens WHERE access_token IS NOT NULL`
    );
    status.push(`Google OAuth: ${parseInt(googleRows[0].count) > 0 ? 'connected' : 'not connected'}`);
  } catch { status.push('Google OAuth: unknown'); }

  try {
    const { rows: contactRows } = await pool.query(
      `SELECT COUNT(*) as count FROM contacts`
    );
    status.push(`Contacts: ${contactRows[0].count} records`);
  } catch { status.push('Contacts: unknown'); }

  try {
    const { rows: locationRows } = await pool.query(
      `SELECT COUNT(*) as count FROM device_location WHERE created_at > NOW() - INTERVAL '24 hours'`
    );
    status.push(`Location updates (24h): ${locationRows[0].count}`);
  } catch { status.push('Location: unknown'); }

  try {
    const { rows: skillRows } = await pool.query(
      `SELECT name FROM skills WHERE active = true`
    );
    status.push(`Active skills: ${skillRows.length > 0 ? skillRows.map(r => r.name).join(', ') : 'none'}`);
  } catch { status.push('Skills: unknown'); }

  const envIntegrations = [
    ['Twilio', !!process.env.TWILIO_ACCOUNT_SID],
    ['ElevenLabs', !!process.env.ELEVENLABS_API_KEY],
    ['Looki', !!process.env.LOOKI_API_KEY],
    ['Gemini', !!process.env.GOOGLE_GENAI_API_KEY],
    ['Push Notifications', !!process.env.AWS_SNS_PLATFORM_APP_ARN],
    ['ARIA Email', !!process.env.ARIA_EMAIL_ADDRESS],
  ];
  for (const [name, configured] of envIntegrations) {
    status.push(`${name}: ${configured ? 'configured' : 'not configured'}`);
  }

  return `Integration status:\n${status.map(s => `  ${s}`).join('\n')}`;
}

export async function handleSelfImprovementReport(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = (job.payload ?? {}) as Record<string, unknown>;

  // Check if subscription is enabled
  const pool = getPool();
  const { rows: subRows } = await pool.query(
    `SELECT is_enabled FROM report_subscriptions WHERE report_type = $1 LIMIT 1`,
    ['self-improvement-report']
  );
  if (subRows.length > 0 && !subRows[0].is_enabled) {
    logger.log('Subscription disabled, skipping');
    return { skipped: true, reason: 'subscription_disabled' };
  }

  logger.log(`Generating daily self-improvement report`);

  // Gather all platform telemetry in parallel
  const [
    conversationStats,
    toolUsage,
    jobStats,
    recentErrors,
    memoryStats,
    proactiveStats,
    healthStats,
    integrationStatus,
  ] = await Promise.all([
    fetchConversationStats(),
    fetchToolUsagePatterns(),
    fetchJobStats(),
    fetchRecentErrors(),
    fetchMemoryStats(),
    fetchProactiveStats(),
    fetchHealthDataStats(),
    fetchIntegrationStatus(),
  ]);

  const now = new Date();
  const dateStr = now.toLocaleDateString('en-US', {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    timeZone: TIMEZONE,
  });

  const result = await routeRequest({
    handler: 'self-improvement-report',
    taskTier: 'generation',
    systemPrompt: `You are ARIA, a personal AI assistant platform. You are reviewing your own platform telemetry to suggest ONE specific, actionable technical enhancement that would make you better at your job.

You have deep knowledge of your own architecture:
- Next.js 14 web app (aria) with 13 integrations, 85 tools, SSE streaming chat
- iOS app (aria-ios) syncing health, location, contacts, calendar, reminders
- Background worker (aria-tempo) with 22 job types including proactive intelligence pipeline
- MCP server (aria-mcp-server) for external AI client access
- Shared PostgreSQL database with 22+ tables

When suggesting an enhancement, be specific and technical:
- What exactly should change (files, functions, database schema)
- Why it matters (what problem it solves, what it enables)
- How to implement it (high-level steps)
- Estimated complexity (small/medium/large)

Focus on enhancements that would make a real difference to your owner's daily experience. Prioritize things that:
- Fix pain points visible in the data (errors, unused features, gaps)
- Unlock new capabilities by combining existing features in new ways
- Improve your ability to be proactive and anticipate needs
- Make you more reliable, faster, or smarter

Do NOT suggest vague improvements like "add more logging" or "improve error handling". Be specific and opinionated. One suggestion only.`,
    userMessage: `Today is ${dateStr}. Here is your current platform telemetry. Review it and suggest one specific technical enhancement.

${conversationStats}

${toolUsage}

${jobStats}

${recentErrors}

${memoryStats}

${proactiveStats}

${healthStats}

${integrationStatus}

Based on this data, what is the single most impactful technical enhancement you'd suggest for the ARIA platform today? Be specific and actionable.`,
    maxTokens: 3000,
    useBatch: true,
  });

  const reportText = result.text;
  logger.log(`Generated via ${result.model} (${result.provider}, ${result.estimatedCostCents}¢)`);

  // Deliver via configured methods
  const deliveredVia = await deliverReport({
    reportType: 'self-improvement-report',
    title: `Self-Improvement Report — ${dateStr}`,
    body: reportText,
    category: 'self_improvement',
  });

  logger.log(`Delivered via: ${deliveredVia.join(', ') || 'none'}`);

  // Extract a title from the first meaningful line of the report
  const lines = reportText.split('\n').filter(l => l.trim().length > 0);
  let title = lines[0] || 'Self-improvement suggestion';
  // Strip markdown heading markers
  title = title.replace(/^#+\s*/, '').replace(/\*+/g, '').trim();
  if (title.length > 200) title = title.substring(0, 197) + '...';

  // Insert into aria_self_improvement so the dashboard can display it
  try {
    await pool.query(
      `INSERT INTO aria_self_improvement (category, status, title, description, source, priority, estimated_effort, confidence)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        'feature_idea',
        'idea',
        title,
        reportText,
        'telemetry',
        'medium',
        null,
        0.5,
      ]
    );
  } catch (err) {
    logger.logMinimal('Failed to insert into aria_self_improvement:', (err as Error).message);
  }

  await pool.query(
    `INSERT INTO actions_log (action_type, description, metadata)
     VALUES ($1, $2, $3)`,
    [
      'self_improvement_report',
      `Self-improvement report delivered via ${deliveredVia.join(', ')}`,
      JSON.stringify({ deliveredVia, report_length: reportText.length }),
    ]
  );

  return { delivered_via: deliveredVia, report_length: reportText.length };
}
