import { Router } from 'express';
import { query, getPool, createLogger } from '@nexus/core';

const logger = createLogger('routes/ingest');
const router = Router();

/**
 * POST /v1/ingest/email — receive an email for an agent
 *
 * Called by Cloudflare email worker when an email arrives at
 * *@nexus.example.io or aria@example.io.
 *
 * Extracts the agent name from the to address, creates an inbox
 * message, and the LISTEN/NOTIFY trigger wakes the agent.
 */
router.post('/email', async (req, res) => {
  try {
    const { to, from, subject, body, html } = req.body;

    if (!to || !from) {
      res.status(400).json({ error: 'to and from are required' });
      return;
    }

    // Extract agent ID from email address
    // aria@example.io → aria
    // monitor@nexus.example.io → monitor
    // modelops@nexus.example.io → model-ops (normalize)
    const localPart = String(to).split('@')[0].toLowerCase();

    // Map email local parts to agent IDs
    const agentMap: Record<string, string> = {
      'aria': 'aria',
      'monitor': 'monitor',
      'modelops': 'model-ops',
      'collector': 'collector',
      'analyst': 'analyst',
      'fixer': 'fixer',
      'relationships': 'relationships',
    };

    const agentId = agentMap[localPart] ?? localPart;

    // Verify agent exists
    const agentCheck = await query(
      `SELECT agent_id FROM agent_registry WHERE agent_id = $1 AND is_active = true`,
      [agentId],
    );

    if (agentCheck.rows.length === 0) {
      // Unknown agent — route to ARIA as default
      logger.logVerbose(`Unknown agent email ${to}, routing to ARIA`);
    }

    const targetAgent = agentCheck.rows.length > 0 ? agentId : 'aria';

    // Compose the inbox message
    const messageText = [
      `EMAIL from ${from}`,
      subject ? `Subject: ${subject}` : '',
      '',
      body || '(no body)',
    ].filter(Boolean).join('\n');

    // Insert into agent_inbox (triggers LISTEN/NOTIFY for immediate wake-up)
    const pool = getPool();
    const result = await pool.query(
      `INSERT INTO agent_inbox (to_agent, from_agent, message, priority, context)
       VALUES ($1, 'email', $2, 1, $3)
       RETURNING id`,
      [
        targetAgent,
        messageText.slice(0, 5000),
        JSON.stringify({
          type: 'email',
          from,
          to,
          subject: subject ?? null,
          has_html: !!html,
        }),
      ],
    );

    logger.log(`Email ingested: ${from} → ${targetAgent} (inbox ${result.rows[0].id})`);

    res.status(201).json({
      status: 'delivered',
      agent: targetAgent,
      inbox_id: result.rows[0].id,
    });
  } catch (err) {
    logger.logMinimal('Email ingest error:', (err as Error).message);
    res.status(500).json({ error: 'Failed to ingest email' });
  }
});

export default router;
