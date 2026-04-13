import pg from 'pg';
import { createLogger } from './logger.js';

const logger = createLogger('db');

let pool: pg.Pool | null = null;

export function getPool(): pg.Pool {
  if (!pool) {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) {
      throw new Error('DATABASE_URL environment variable is required');
    }

    pool = new pg.Pool({
      connectionString,
      max: 20,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    });

    pool.on('error', (err) => {
      logger.logMinimal('Unexpected pool error:', err.message);
    });

    logger.log('Connection pool created');
  }

  return pool;
}

export async function query<T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[],
): Promise<pg.QueryResult<T>> {
  const stop = logger.time('query');
  try {
    const result = await getPool().query<T>(text, params);
    stop();
    logger.logDebug('Query:', text.slice(0, 200), '| rows:', result.rowCount);
    return result;
  } catch (err) {
    stop();
    logger.logMinimal('Query error:', (err as Error).message);
    logger.logDebug('Failed query:', text);
    throw err;
  }
}

export async function shutdown(): Promise<void> {
  if (pool) {
    logger.log('Shutting down connection pool');
    await pool.end();
    pool = null;
  }
}
