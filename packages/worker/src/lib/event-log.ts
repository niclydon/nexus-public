/**
 * Structured event logging — writes to the shared event_log table.
 * Fire-and-forget — errors are swallowed so logging never breaks jobs.
 */
import { getPool, createLogger } from '@nexus/core';

const logger = createLogger('event-log');

export type EventCategory =
  | 'direct_response'
  | 'background'
  | 'interaction'
  | 'self_maintenance'
  | 'integration'
  | 'system';

export async function logEvent(params: {
  action: string;
  component: string;
  category: EventCategory;
  status?: 'success' | 'error';
  metadata?: Record<string, unknown>;
}): Promise<void> {
  try {
    await getPool().query(
      `INSERT INTO event_log (action, component, category, status, metadata)
       VALUES ($1, $2, $3, $4, $5)`,
      [
        params.action,
        params.component,
        params.category,
        params.status ?? 'success',
        params.metadata ? JSON.stringify(params.metadata) : null,
      ],
    );
  } catch (err) {
    logger.logDebug('Event log write failed:', (err as Error).message);
  }
}
