import { Router } from 'express';
import { comms, createLogger } from '@nexus/core';
import { param } from '../util.js';

const logger = createLogger('routes/inbox');
const router = Router();

// GET /v1/inbox/:agentId — get inbox messages for an agent
router.get('/:agentId', async (req, res) => {
  try {
    const includeRead = String(req.query.include_read ?? 'false') === 'true';
    const limit = req.query.limit ? parseInt(String(req.query.limit), 10) : undefined;
    const messages = await comms.getInbox(param(req.params.agentId), { includeRead, limit });
    res.json({ messages });
  } catch (err) {
    logger.logMinimal('Error getting inbox:', (err as Error).message);
    res.status(500).json({ error: 'Failed to get inbox' });
  }
});

// POST /v1/inbox/send — send an inbox message
router.post('/send', async (req, res) => {
  try {
    const { to_agent, from_agent, message, context, priority, trace_id } = req.body;
    if (!to_agent || !from_agent || !message) {
      res.status(400).json({ error: 'to_agent, from_agent, and message are required' });
      return;
    }
    const msg = await comms.sendInbox({ to_agent, from_agent, message, context, priority, trace_id });
    res.status(201).json(msg);
  } catch (err) {
    logger.logMinimal('Error sending inbox:', (err as Error).message);
    res.status(500).json({ error: 'Failed to send inbox message' });
  }
});

// POST /v1/inbox/:messageId/read — mark message as read
router.post('/:messageId/read', async (req, res) => {
  try {
    await comms.markInboxRead(parseInt(param(req.params.messageId), 10));
    res.json({ status: 'read' });
  } catch (err) {
    logger.logMinimal('Error marking read:', (err as Error).message);
    res.status(500).json({ error: 'Failed to mark message read' });
  }
});

// POST /v1/inbox/:messageId/acted — mark message as acted upon
router.post('/:messageId/acted', async (req, res) => {
  try {
    await comms.markInboxActed(parseInt(param(req.params.messageId), 10));
    res.json({ status: 'acted' });
  } catch (err) {
    logger.logMinimal('Error marking acted:', (err as Error).message);
    res.status(500).json({ error: 'Failed to mark message acted' });
  }
});

export default router;
