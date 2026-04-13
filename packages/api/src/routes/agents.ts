import { Router } from 'express';
import { agentRegistry, requireCaller, createLogger } from '@nexus/core';
import { param } from '../util.js';

const logger = createLogger('routes/agents');
const router = Router();

// GET /v1/agents — list all active agents
router.get('/', async (_req, res) => {
  try {
    const agents = await agentRegistry.listAgents();
    res.json({ agents });
  } catch (err) {
    logger.logMinimal('Error listing agents:', (err as Error).message);
    res.status(500).json({ error: 'Failed to list agents' });
  }
});

// GET /v1/agents/:id — get agent detail
router.get('/:id', async (req, res) => {
  try {
    const agent = await agentRegistry.getAgent(param(req.params.id));
    if (!agent) {
      res.status(404).json({ error: 'Agent not found' });
      return;
    }
    // Strip sensitive fields
    const { api_key_hash, ...safe } = agent;
    res.json(safe);
  } catch (err) {
    logger.logMinimal('Error getting agent:', (err as Error).message);
    res.status(500).json({ error: 'Failed to get agent' });
  }
});

// POST /v1/agents/:id/api-key — generate new API key (system/user only)
router.post('/:id/api-key',
  requireCaller(['system', 'user']),
  async (req, res) => {
    try {
      const agent = await agentRegistry.getAgent(param(req.params.id));
      if (!agent) {
        res.status(404).json({ error: 'Agent not found' });
        return;
      }
      const key = await agentRegistry.generateApiKey(param(req.params.id));
      res.json({ api_key: key, agent_id: param(req.params.id) });
    } catch (err) {
      logger.logMinimal('Error generating API key:', (err as Error).message);
      res.status(500).json({ error: 'Failed to generate API key' });
    }
  },
);

// GET /v1/agents/:id/memories — get agent memories
router.get('/:id/memories', async (req, res) => {
  try {
    const includeShared = String(req.query.include_shared ?? 'true') !== 'false';
    const limit = req.query.limit ? parseInt(String(req.query.limit), 10) : undefined;
    const memories = await agentRegistry.getAgentMemories(param(req.params.id), { includeShared, limit });
    res.json({ memories });
  } catch (err) {
    logger.logMinimal('Error getting memories:', (err as Error).message);
    res.status(500).json({ error: 'Failed to get memories' });
  }
});

export default router;
