import { rateLimit as expressRateLimit } from 'express-rate-limit';

/**
 * Rate limiting for state-changing or externally reachable routes.
 * Wraps express-rate-limit (the maintained standard implementation, with
 * an in-memory store suited to a single-instance showcase deployment;
 * a multi-instance deployment should back it with a shared store).
 */
export function rateLimit(options: { windowMs: number; max: number }) {
  return expressRateLimit({
    windowMs: options.windowMs,
    limit: options.max,
    standardHeaders: true,
    legacyHeaders: false,
  });
}
