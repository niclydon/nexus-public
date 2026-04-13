import { Router } from 'express';
import { query, getPool, requireCaller, createLogger } from '@nexus/core';
import { param } from '../util.js';

const logger = createLogger('routes/approvals');
const router = Router();

// GET /v1/approvals — list pending approvals
router.get('/', async (_req, res) => {
  try {
    const result = await query(
      `SELECT pa.*, r.display_name as agent_name
       FROM agent_pending_actions pa
       LEFT JOIN agent_registry r ON r.agent_id = pa.agent_id
       WHERE pa.status = 'pending'
       ORDER BY pa.created_at DESC
       LIMIT 50`,
    );
    res.json({ approvals: result.rows });
  } catch (err) {
    logger.logMinimal('Error listing approvals:', (err as Error).message);
    res.status(500).json({ error: 'Failed to list approvals' });
  }
});

// GET /v1/approvals/all — list all approvals (including decided)
router.get('/all', async (_req, res) => {
  try {
    const result = await query(
      `SELECT pa.*, r.display_name as agent_name
       FROM agent_pending_actions pa
       LEFT JOIN agent_registry r ON r.agent_id = pa.agent_id
       ORDER BY pa.created_at DESC
       LIMIT 100`,
    );
    res.json({ approvals: result.rows });
  } catch (err) {
    logger.logMinimal('Error listing approvals:', (err as Error).message);
    res.status(500).json({ error: 'Failed to list approvals' });
  }
});

// POST /v1/approvals/:id/decide — unified decision endpoint (accept/edit/respond/ignore)
//
// Body: { type: "accept" | "edit" | "respond" | "ignore", modified_params?: {...}, response?: "text", reason?: "text" }
//
// - accept:  approve action as-is → agent executes original params on next cycle
// - edit:    approve with modified params → agent executes modified_params on next cycle
// - respond: send text guidance instead of approving → agent reads response in inbox, action stays rejected
// - ignore:  dismiss without action → agent notified, action marked rejected
//
router.post('/:id/decide',
  requireCaller(['system', 'user']),
  async (req, res) => {
    const id = parseInt(param(req.params.id), 10);
    const responseType = req.body?.type as string;
    const modifiedParams = req.body?.modified_params as Record<string, unknown> | undefined;
    const responseText = req.body?.response as string | undefined;
    const reason = req.body?.reason as string | undefined;

    if (!['accept', 'edit', 'respond', 'ignore'].includes(responseType)) {
      res.status(400).json({ error: 'type must be accept, edit, respond, or ignore' });
      return;
    }

    try {
      // Check the action exists and what's allowed
      const pending = await query<{
        id: number; agent_id: string; action_type: string; params: unknown;
        allow_edit: boolean; allow_respond: boolean; allow_ignore: boolean;
      }>(
        `SELECT id, agent_id, action_type, params, allow_edit, allow_respond, allow_ignore
         FROM agent_pending_actions WHERE id = $1 AND status = 'pending'`,
        [id],
      );

      if (pending.rows.length === 0) {
        res.status(404).json({ error: 'Approval not found or already decided' });
        return;
      }

      const action = pending.rows[0];

      // Validate the response type is allowed for this action
      if (responseType === 'edit' && !action.allow_edit) {
        res.status(400).json({ error: 'Editing params is not allowed for this action' });
        return;
      }
      if (responseType === 'respond' && !action.allow_respond) {
        res.status(400).json({ error: 'Text response is not allowed for this action' });
        return;
      }
      if (responseType === 'ignore' && !action.allow_ignore) {
        res.status(400).json({ error: 'Ignoring is not allowed for this action' });
        return;
      }

      if (responseType === 'edit' && !modifiedParams) {
        res.status(400).json({ error: 'modified_params required for edit response' });
        return;
      }

      // Determine the resulting status
      const isPositive = responseType === 'accept' || responseType === 'edit';
      const status = isPositive ? 'approved' : 'rejected';

      // Update the pending action
      await query(
        `UPDATE agent_pending_actions
         SET status = $2, response_type = $3, modified_params = $4, response_text = $5,
             decided_by = $6, decided_at = NOW(), decision_reason = $7
         WHERE id = $1`,
        [id, status, responseType, modifiedParams ? JSON.stringify(modifiedParams) : null,
         responseText ?? null, req.caller?.id ?? 'system', reason ?? null],
      );

      // Build the inbox message for the agent
      let inboxMessage: string;
      let priority: number;

      switch (responseType) {
        case 'accept':
          inboxMessage = `Action approved: ${action.action_type} (id: ${id}). Execute on your next cycle.`;
          priority = 2;
          break;
        case 'edit':
          inboxMessage = `Action approved with modifications: ${action.action_type} (id: ${id}). Use modified params: ${JSON.stringify(modifiedParams).slice(0, 300)}. Execute on your next cycle.`;
          priority = 2;
          break;
        case 'respond':
          inboxMessage = `Action not approved — guidance provided: ${action.action_type} (id: ${id}). Response: ${responseText ?? '(no text)'}`;
          priority = 1;
          break;
        case 'ignore':
          inboxMessage = `Action dismissed: ${action.action_type} (id: ${id}).${reason ? ` Reason: ${reason}` : ''}`;
          priority = 0;
          break;
        default:
          inboxMessage = `Decision on action ${id}: ${responseType}`;
          priority = 1;
      }

      await getPool().query(
        `INSERT INTO agent_inbox (to_agent, from_agent, message, priority, context)
         VALUES ($1, 'system', $2, $3, $4)`,
        [action.agent_id, inboxMessage, priority,
         JSON.stringify({ type: 'approval_decision', action_id: id, response_type: responseType, modified_params: modifiedParams ?? null })],
      );

      logger.log(`Decision on ${action.action_type} by ${action.agent_id} (id: ${id}): ${responseType}`);
      res.json({ status, response_type: responseType, agent_notified: true });
    } catch (err) {
      logger.logMinimal('Error deciding:', (err as Error).message);
      res.status(500).json({ error: 'Failed to process decision' });
    }
  },
);

// Legacy endpoints — kept for backward compatibility, redirect to /decide

// POST /v1/approvals/:id/approve — legacy, maps to decide type=accept
router.post('/:id/approve',
  requireCaller(['system', 'user']),
  async (req, res) => {
    req.body = { ...req.body, type: 'accept' };
    // Forward to the decide handler by re-dispatching
    const id = parseInt(param(req.params.id), 10);
    try {
      const { rows } = await query(
        `UPDATE agent_pending_actions
         SET status = 'approved', response_type = 'accept', decided_by = $2, decided_at = NOW()
         WHERE id = $1 AND status = 'pending'
         RETURNING id, agent_id, action_type`,
        [id, req.caller?.id ?? 'system'],
      );

      if (rows.length === 0) {
        res.status(404).json({ error: 'Approval not found or already decided' });
        return;
      }

      const agentId = (rows[0] as { agent_id: string }).agent_id;
      await getPool().query(
        `INSERT INTO agent_inbox (to_agent, from_agent, message, priority)
         VALUES ($1, 'system', $2, 2)`,
        [agentId, `Action approved: ${(rows[0] as { action_type: string }).action_type} (id: ${id}). Execute on your next cycle.`],
      );

      logger.log(`Approved (legacy): ${(rows[0] as { action_type: string }).action_type} by ${agentId} (id: ${id})`);
      res.json({ status: 'approved', agent_notified: true });
    } catch (err) {
      logger.logMinimal('Error approving:', (err as Error).message);
      res.status(500).json({ error: 'Failed to approve action' });
    }
  },
);

// POST /v1/approvals/:id/reject — legacy, maps to decide type=ignore
router.post('/:id/reject',
  requireCaller(['system', 'user']),
  async (req, res) => {
    const id = parseInt(param(req.params.id), 10);
    const reason = req.body?.reason ?? 'Rejected by reviewer';
    try {
      const { rows } = await query(
        `UPDATE agent_pending_actions
         SET status = 'rejected', response_type = 'ignore', decided_by = $2, decided_at = NOW(), decision_reason = $3
         WHERE id = $1 AND status = 'pending'
         RETURNING id, agent_id`,
        [id, req.caller?.id ?? 'system', reason],
      );

      if (rows.length === 0) {
        res.status(404).json({ error: 'Approval not found or already decided' });
        return;
      }

      const agentId = (rows[0] as { agent_id: string }).agent_id;
      await getPool().query(
        `INSERT INTO agent_inbox (to_agent, from_agent, message, priority)
         VALUES ($1, 'system', $2, 1)`,
        [agentId, `Action rejected (id: ${id}): ${reason}`],
      );

      logger.log(`Rejected (legacy): id ${id} — ${reason}`);
      res.json({ status: 'rejected' });
    } catch (err) {
      logger.logMinimal('Error rejecting:', (err as Error).message);
      res.status(500).json({ error: 'Failed to reject action' });
    }
  },
);

export default router;
