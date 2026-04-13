import type { Request, Response, NextFunction } from 'express';
import bcrypt from 'bcrypt';
import { query } from '../db.js';
import { createLogger } from '../logger.js';
import type { CallerIdentity } from '../types/index.js';

const logger = createLogger('auth');

declare global {
  namespace Express {
    interface Request {
      caller?: CallerIdentity;
    }
  }
}

const SYSTEM_KEY_PREFIX = 'nexus_sys_';

export function authMiddleware() {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const header = req.headers.authorization;
    if (!header?.startsWith('Bearer ')) {
      res.status(401).json({ error: 'Missing or invalid Authorization header' });
      return;
    }

    const token = header.slice(7);

    // System keys (for worker and Chancery internal calls)
    if (token.startsWith(SYSTEM_KEY_PREFIX)) {
      const valid = token === process.env.NEXUS_SYSTEM_KEY;
      if (!valid) {
        logger.logMinimal('Invalid system key attempt');
        res.status(401).json({ error: 'Invalid system key' });
        return;
      }
      req.caller = { type: 'system', id: 'system' };
      logger.logDebug('Authenticated as system');
      next();
      return;
    }

    // Agent API keys: nexus_<agent_id>_<random32>
    // Prefix is first 8 chars of the full key, used for lookup
    const prefix = token.slice(0, 8);
    const stop = logger.time('auth-lookup');

    try {
      const result = await query<{ agent_id: string; api_key_hash: string; is_active: boolean }>(
        'SELECT agent_id, api_key_hash, is_active FROM agent_registry WHERE api_key_prefix = $1',
        [prefix],
      );
      stop();

      if (result.rows.length === 0) {
        logger.logVerbose('No agent found for key prefix:', prefix);
        res.status(401).json({ error: 'Invalid API key' });
        return;
      }

      const agent = result.rows[0];

      if (!agent.is_active) {
        logger.logVerbose('Inactive agent attempted auth:', agent.agent_id);
        res.status(403).json({ error: 'Agent is inactive' });
        return;
      }

      if (!agent.api_key_hash) {
        logger.logMinimal('Agent has no key hash:', agent.agent_id);
        res.status(401).json({ error: 'Invalid API key' });
        return;
      }

      const match = await bcrypt.compare(token, agent.api_key_hash);
      if (!match) {
        logger.logVerbose('Key mismatch for agent:', agent.agent_id);
        res.status(401).json({ error: 'Invalid API key' });
        return;
      }

      req.caller = { type: 'agent', id: agent.agent_id };
      logger.logDebug('Authenticated agent:', agent.agent_id);
      next();
    } catch (err) {
      stop();
      logger.logMinimal('Auth error:', (err as Error).message);
      res.status(500).json({ error: 'Authentication failed' });
    }
  };
}

export function requireCaller(types: CallerIdentity['type'][]) {
  return (req: Request, res: Response, next: NextFunction): void => {
    if (!req.caller || !types.includes(req.caller.type)) {
      res.status(403).json({ error: 'Insufficient permissions' });
      return;
    }
    next();
  };
}
