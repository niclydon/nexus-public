import express from 'express';
import { authMiddleware, createLogger, shutdown, platformMode } from '@nexus/core';
import agentsRouter from './routes/agents.js';
import toolsRouter from './routes/tools.js';
import inboxRouter from './routes/inbox.js';
import approvalsRouter from './routes/approvals.js';
import decisionsRouter from './routes/decisions.js';
import mergeRouter, { mergeAuditRouter } from './routes/merge.js';
import biographyRouter from './routes/biography.js';
import ingestRouter from './routes/ingest.js';
import iosRouter from './routes/ios.js';
import searchRouter from './routes/search.js';
import platformRouter from './routes/platform.js';
import forgeRouter from './routes/forge.js';

const logger = createLogger('api');
const app = express();
const port = parseInt(process.env.PORT ?? '7700', 10);

app.use(express.json({ limit: '50mb' }));

// Health check (unauthenticated). Includes platform mode so external monitors
// (and the Chancery banner, though it reads the DB directly) can tell at a
// glance whether the platform is intentionally paused for creative work.
app.get('/health', async (_req, res) => {
  const mode = await platformMode.getPlatformMode().catch(() => null);
  res.json({
    status: 'ok',
    service: 'nexus-api',
    timestamp: new Date().toISOString(),
    platform_mode: mode
      ? {
          mode: mode.mode,
          started_at: mode.started_at,
          elapsed_human: platformMode.formatElapsed(mode.elapsed_ms),
          reason: mode.reason,
        }
      : null,
  });
});

// Ingest routes — BEFORE general auth (uses X-Ingest-Token, not agent API keys)
app.use('/v1/ingest', (req, res, next) => {
  const token = req.headers['x-ingest-token'] || req.headers.authorization?.replace('Bearer ', '');
  if (token !== process.env.NEXUS_INGEST_TOKEN && token !== process.env.NEXUS_SYSTEM_KEY) {
    res.status(401).json({ error: 'Invalid ingest token' });
    return;
  }
  next();
}, ingestRouter);

// iOS app routes (uses own auth, matches /api/* paths the iOS app expects)
app.use('/api', iosRouter);

// All other /v1/* routes require agent/system auth
app.use('/v1', authMiddleware());
app.use('/v1/agents', agentsRouter);
app.use('/v1/tools', toolsRouter);
app.use('/v1/inbox', inboxRouter);
app.use('/v1/approvals', approvalsRouter);
app.use('/v1/decisions', decisionsRouter);
app.use('/v1/merge-candidates', mergeRouter);
app.use('/v1/merge-audit', mergeAuditRouter);
app.use('/v1/biography', biographyRouter);
app.use('/v1/search', searchRouter);
app.use('/v1/platform', platformRouter);
app.use('/v1/forge', forgeRouter);

const server = app.listen(port, () => {
  logger.log(`Nexus API listening on port ${port}`);
});

async function gracefulShutdown(signal: string) {
  logger.log(`Received ${signal}, shutting down`);
  server.close(() => {
    shutdown().then(() => process.exit(0));
  });
  setTimeout(() => {
    logger.logMinimal('Forced shutdown after timeout');
    process.exit(1);
  }, 10_000);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
