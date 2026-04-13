/**
 * Structured job logging — writes to job_log table for real-time
 * progress tracking in the Chancery UI.
 */
import { getPool } from '@nexus/core';

export async function jobLog(
  jobId: string,
  message: string,
  level: 'info' | 'warn' | 'error' = 'info',
): Promise<void> {
  try {
    await getPool().query(
      `INSERT INTO job_log (job_id, message, level) VALUES ($1, $2, $3)`,
      [jobId, message, level],
    );
  } catch {
    // Non-fatal
  }
}
