import type { Request, Response, NextFunction } from 'express';

/**
 * Minimal in-memory fixed-window rate limiter (no external dependency).
 * Keyed by caller identity when authenticated, else by IP. Suitable for a
 * single-instance showcase deployment; a multi-instance deployment should
 * back this with a shared store.
 */
export function rateLimit(options: { windowMs: number; max: number }) {
  const hits = new Map<string, { count: number; resetAt: number }>();
  return (req: Request, res: Response, next: NextFunction): void => {
    const now = Date.now();
    const key = (req as Request & { caller?: string }).caller ?? req.ip ?? 'unknown';
    let entry = hits.get(key);
    if (!entry || now >= entry.resetAt) {
      entry = { count: 0, resetAt: now + options.windowMs };
      hits.set(key, entry);
    }
    entry.count += 1;
    if (entry.count > options.max) {
      res.status(429).json({ error: 'rate limit exceeded' });
      return;
    }
    next();
  };
}
