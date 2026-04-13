import { Router } from 'express';
import { createLogger, platformMode } from '@nexus/core';

const logger = createLogger('routes/platform');
const router = Router();

/**
 * GET /v1/platform/mode
 *
 * Returns the current platform mode plus recent history. Read-only — mode
 * is flipped externally by the smithy creative-mode-on/off scripts.
 *
 * Response:
 * {
 *   current: { mode, started_at, started_by, reason, elapsed_ms, elapsed_human },
 *   history: [ { id, mode, started_at, ended_at, duration_sec, started_by, reason }, ... ]
 * }
 */
router.get('/mode', async (_req, res) => {
  try {
    const [current, history] = await Promise.all([
      platformMode.getPlatformMode({ bypassCache: true }),
      platformMode.getPlatformModeHistory(20),
    ]);

    res.json({
      current: {
        ...current,
        elapsed_human: platformMode.formatElapsed(current.elapsed_ms),
      },
      history,
    });
  } catch (e: any) {
    logger.logMinimal('GET /v1/platform/mode failed:', e.message);
    res.status(500).json({ error: 'platform_mode_lookup_failed', detail: e.message });
  }
});

export default router;
