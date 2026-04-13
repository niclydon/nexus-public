/**
 * Polls aria_scheduled_tasks for due tasks and executes them via the ARIA API.
 */
import { query, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import type { TempoJob } from '../job-worker.js';

const logger = createLogger('handler/execute-scheduled-task');

export async function handleExecuteScheduledTask(job: TempoJob): Promise<Record<string, unknown>> {
  const { rows: dueTasks } = await query<{
    id: string;
    tool_name: string;
    description: string | null;
    scheduled_for: Date;
  }>(
    `SELECT id, tool_name, description, scheduled_for
     FROM aria_scheduled_tasks
     WHERE status = 'pending' AND scheduled_for <= NOW()
     ORDER BY scheduled_for ASC
     LIMIT 10`,
  );

  if (dueTasks.length === 0) {
    logger.logVerbose('No due tasks');
    return { executed: 0 };
  }

  logger.log(`Found ${dueTasks.length} due task(s)`);
  await jobLog(job.id, `Found ${dueTasks.length} due task(s)`);

  const ariaBaseUrl = process.env.ARIA_BASE_URL || process.env.NEXTAUTH_URL || 'https://aria.example.io';
  const apiToken = process.env.ARIA_API_TOKEN;

  if (!apiToken) {
    logger.logMinimal('ARIA_API_TOKEN not set — cannot execute tasks');
    return { executed: 0, error: 'ARIA_API_TOKEN not configured' };
  }

  let executed = 0;
  let failed = 0;

  for (const task of dueTasks) {
    const label = task.description || task.tool_name;
    logger.log(`Executing: "${label}" (${task.id})`);
    await jobLog(job.id, `Executing: "${label}"`);

    try {
      const response = await fetch(`${ariaBaseUrl}/api/tasks/execute`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${apiToken}`,
        },
        body: JSON.stringify({ task_id: task.id }),
      });

      const result = await response.json() as {
        status?: string;
        error?: string;
        attempts?: number;
        max_attempts?: number;
      };

      if (response.ok && result.status === 'completed') {
        executed++;
        logger.log(`Completed: "${label}"`);
        await jobLog(job.id, `Completed: "${label}"`);
      } else if (result.status === 'pending') {
        logger.log(`Retryable failure for "${label}" (attempt ${result.attempts}/${result.max_attempts}): ${result.error}`);
        await jobLog(job.id, `Retryable failure: "${label}" — ${result.error}`);
      } else {
        failed++;
        logger.log(`Failed: "${label}" — ${result.error || response.statusText}`);
        await jobLog(job.id, `Failed: "${label}" — ${result.error || response.statusText}`);
      }
    } catch (err) {
      failed++;
      const msg = (err as Error).message;
      logger.logMinimal(`Network error for "${label}":`, msg);
      await jobLog(job.id, `Network error for "${label}": ${msg}`);

      await query(
        `UPDATE aria_scheduled_tasks SET status = 'pending' WHERE id = $1 AND status = 'executing'`,
        [task.id],
      );
    }
  }

  const summary = `Executed ${executed}, failed ${failed} of ${dueTasks.length} due tasks`;
  logger.log(summary);
  await jobLog(job.id, summary);
  return { executed, failed, total: dueTasks.length };
}
