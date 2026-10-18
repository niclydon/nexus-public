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

/**
 * Parameterized query helper built from a template literal.
 *
 *   const { rows } = await query(...sql`SELECT * FROM t WHERE id = ${id}`);
 *
 * Interpolated values are always sent as bound parameters ($1, $2, ...),
 * never concatenated into the SQL text. Prefer this over calling query()
 * with a hand-built string.
 */
export function sql(strings: TemplateStringsArray, ...values: unknown[]): { text: string; params: unknown[] } {
  let text = strings[0];
  const params: unknown[] = [];
  for (let i = 0; i < values.length; i++) {
    params.push(values[i]);
    text += `$${i + 1}` + strings[i + 1];
  }
  return { text, params };
}

/**
 * Run a query. `text` must be trusted SQL written by a developer, with all
 * runtime values passed through `params` as bound parameters. Never build
 * `text` by interpolating caller- or user-supplied values; use sql`` above
 * so interpolation is parameterized by construction.
 */
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
