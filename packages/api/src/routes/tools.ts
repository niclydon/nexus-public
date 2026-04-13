import { Router } from 'express';
import { toolCatalog, requireCaller, createLogger } from '@nexus/core';
import { param } from '../util.js';

const logger = createLogger('routes/tools');
const router = Router();

// GET /v1/tools — search platform tools
router.get('/', async (req, res) => {
  try {
    const keyword = String(req.query.keyword ?? '');
    const category = req.query.category ? String(req.query.category) : undefined;
    const limit = req.query.limit ? parseInt(String(req.query.limit), 10) : undefined;
    const tools = await toolCatalog.searchTools(keyword, { category, limit });
    res.json({ tools });
  } catch (err) {
    logger.logMinimal('Error searching tools:', (err as Error).message);
    res.status(500).json({ error: 'Failed to search tools' });
  }
});

// GET /v1/tools/agent/:id — get tools granted to an agent
router.get('/agent/:id', async (req, res) => {
  try {
    const tools = await toolCatalog.getToolsForAgent(param(req.params.id));
    res.json({ tools });
  } catch (err) {
    logger.logMinimal('Error getting agent tools:', (err as Error).message);
    res.status(500).json({ error: 'Failed to get agent tools' });
  }
});

// POST /v1/tools/request — request access to a tool
router.post('/request', async (req, res) => {
  try {
    const { agent_id, tool_id, reason } = req.body;
    if (!agent_id || !tool_id) {
      res.status(400).json({ error: 'agent_id and tool_id are required' });
      return;
    }
    const result = await toolCatalog.requestTool(agent_id, tool_id, reason ?? null);
    res.json(result);
  } catch (err) {
    logger.logMinimal('Error requesting tool:', (err as Error).message);
    res.status(500).json({ error: 'Failed to request tool' });
  }
});

// POST /v1/tools/approve/:requestId — approve a tool request (system/user only)
router.post('/approve/:requestId',
  requireCaller(['system', 'user']),
  async (req, res) => {
    try {
      const reviewedBy = req.caller!.id;
      await toolCatalog.approveTool(param(req.params.requestId), reviewedBy);
      res.json({ status: 'approved' });
    } catch (err) {
      logger.logMinimal('Error approving tool:', (err as Error).message);
      res.status(500).json({ error: 'Failed to approve tool' });
    }
  },
);

// POST /v1/tools/deny/:requestId — deny a tool request (system/user only)
router.post('/deny/:requestId',
  requireCaller(['system', 'user']),
  async (req, res) => {
    try {
      const reviewedBy = req.caller!.id;
      await toolCatalog.denyTool(param(req.params.requestId), reviewedBy);
      res.json({ status: 'denied' });
    } catch (err) {
      logger.logMinimal('Error denying tool:', (err as Error).message);
      res.status(500).json({ error: 'Failed to deny tool' });
    }
  },
);

export default router;
