import { Router } from 'express';
import { query, createLogger, requireCaller } from '@nexus/core';
import { param } from '../util.js';

const logger = createLogger('routes/decisions');
const router = Router();

/**
 * GET /v1/decisions/review — unreviewed decisions with actions (for human feedback)
 * Returns recent decisions that have actions and haven't been rated yet.
 */
router.get('/review', async (req, res) => {
  const limit = Math.min(parseInt(String(req.query.limit ?? '20'), 10), 50);
  const agentId = req.query.agent_id ? String(req.query.agent_id) : null;

  try {
    const conditions = ['f.id IS NULL', "d.parsed_actions IS NOT NULL", "d.parsed_actions::text != '[]'", "d.parsed_actions::text != 'null'", "d.created_at >= NOW() - INTERVAL '7 days'"];
    const params: unknown[] = [];

    if (agentId) {
      params.push(agentId);
      conditions.push(`d.agent_id = $${params.length}`);
    }

    params.push(limit);

    const result = await query(
      `SELECT
        d.id as decision_id,
        d.agent_id,
        d.trace_id,
        r.display_name as agent_name,
        r.avatar_url,
        d.state_snapshot->>'summary' as summary,
        d.state_snapshot->>'status' as status,
        d.state_snapshot->>'confidence' as confidence,
        d.parsed_actions,
        d.execution_results,
        d.duration_ms,
        d.created_at
      FROM agent_decisions d
      JOIN agent_registry r ON r.agent_id = d.agent_id
      LEFT JOIN agent_decision_feedback f ON f.decision_id = d.id
      WHERE ${conditions.join(' AND ')}
      ORDER BY d.created_at DESC
      LIMIT $${params.length}`,
      params,
    );

    res.json({ decisions: result.rows, count: result.rowCount });
  } catch (err) {
    logger.logMinimal('Error fetching unreviewed decisions:', (err as Error).message);
    res.status(500).json({ error: 'Failed to fetch decisions' });
  }
});

/**
 * GET /v1/decisions/feedback — list all feedback (for calibration data export)
 */
router.get('/feedback', async (req, res) => {
  const limit = Math.min(parseInt(String(req.query.limit ?? '100'), 10), 500);
  const agentId = req.query.agent_id ? String(req.query.agent_id) : null;
  const rating = req.query.rating ? parseInt(String(req.query.rating), 10) : null;

  try {
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (agentId) {
      params.push(agentId);
      conditions.push(`f.agent_id = $${params.length}`);
    }
    if (rating !== null && (rating === 1 || rating === -1)) {
      params.push(rating);
      conditions.push(`f.rating = $${params.length}`);
    }

    params.push(limit);
    const where = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';

    const result = await query(
      `SELECT f.*, r.display_name as agent_name,
              d.state_snapshot->>'summary' as decision_summary,
              d.parsed_actions,
              d.execution_results
       FROM agent_decision_feedback f
       JOIN agent_registry r ON r.agent_id = f.agent_id
       JOIN agent_decisions d ON d.id = f.decision_id
       ${where}
       ORDER BY f.created_at DESC
       LIMIT $${params.length}`,
      params,
    );

    res.json({ feedback: result.rows, count: result.rowCount });
  } catch (err) {
    logger.logMinimal('Error fetching feedback:', (err as Error).message);
    res.status(500).json({ error: 'Failed to fetch feedback' });
  }
});

/**
 * GET /v1/decisions/stats — feedback summary per agent
 */
router.get('/stats', async (req, res) => {
  try {
    const result = await query(
      `SELECT
        f.agent_id,
        r.display_name as agent_name,
        COUNT(*) as total_reviewed,
        COUNT(*) FILTER (WHERE f.rating = 1) as thumbs_up,
        COUNT(*) FILTER (WHERE f.rating = -1) as thumbs_down,
        ROUND(100.0 * COUNT(*) FILTER (WHERE f.rating = 1) / GREATEST(COUNT(*), 1), 1) as approval_rate
       FROM agent_decision_feedback f
       JOIN agent_registry r ON r.agent_id = f.agent_id
       GROUP BY f.agent_id, r.display_name
       ORDER BY total_reviewed DESC`,
    );

    // Count unreviewed
    const unreviewed = await query(
      `SELECT COUNT(*) as count FROM unreviewed_decisions`,
    );

    res.json({
      stats: result.rows,
      unreviewed_count: (unreviewed.rows[0] as { count: string })?.count ?? '0',
    });
  } catch (err) {
    logger.logMinimal('Error fetching stats:', (err as Error).message);
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

/**
 * POST /v1/decisions/:id/feedback — submit thumbs up/down
 * Body: { rating: 1 | -1, comment?: string }
 */
router.post('/:id/feedback', async (req, res) => {
  const decisionId = parseInt(param(req.params.id), 10);
  const rating = req.body?.rating;
  const comment = req.body?.comment ?? null;

  if (rating !== 1 && rating !== -1) {
    res.status(400).json({ error: 'rating must be 1 (thumbs up) or -1 (thumbs down)' });
    return;
  }

  try {
    // Get the decision to extract agent_id and trace_id
    const decision = await query<{ agent_id: string; trace_id: string }>(
      'SELECT agent_id, trace_id FROM agent_decisions WHERE id = $1',
      [decisionId],
    );

    if (decision.rows.length === 0) {
      res.status(404).json({ error: 'Decision not found' });
      return;
    }

    const { agent_id, trace_id } = decision.rows[0];

    // Check for duplicate feedback
    const existing = await query(
      'SELECT id FROM agent_decision_feedback WHERE decision_id = $1',
      [decisionId],
    );

    if (existing.rows.length > 0) {
      // Update existing
      await query(
        'UPDATE agent_decision_feedback SET rating = $1, comment = $2, created_at = NOW() WHERE decision_id = $3',
        [rating, comment, decisionId],
      );
      logger.log(`Updated feedback for decision ${decisionId}: ${rating === 1 ? 'thumbs up' : 'thumbs down'}`);
      res.json({ status: 'updated', decision_id: decisionId, rating });
      return;
    }

    // Insert new feedback
    await query(
      `INSERT INTO agent_decision_feedback (decision_id, agent_id, trace_id, rating, comment)
       VALUES ($1, $2, $3, $4, $5)`,
      [decisionId, agent_id, trace_id, rating, comment],
    );

    logger.log(`Feedback for decision ${decisionId} (${agent_id}): ${rating === 1 ? 'thumbs up' : 'thumbs down'}`);
    res.json({ status: 'created', decision_id: decisionId, rating });
  } catch (err) {
    logger.logMinimal('Error submitting feedback:', (err as Error).message);
    res.status(500).json({ error: 'Failed to submit feedback' });
  }
});

export default router;
