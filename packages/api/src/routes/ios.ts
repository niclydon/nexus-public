/**
 * iOS App Routes — serves the ARIA iOS app (Chat, HealthKit, Location).
 *
 * These endpoints match the paths the iOS app expects:
 *   POST /api/chat          — SSE streaming chat with ARIA agent
 *   GET  /api/conversations — List conversations
 *   GET  /api/conversations/:id — Get messages
 *   DELETE /api/conversations/:id — Delete conversation
 *   POST /api/health-data   — Batch sync HealthKit records
 *   POST /api/device-location — Sync GPS position
 *
 * Auth: Bearer token from iOS Keychain (NEXUS_SYSTEM_KEY or user token).
 */
import { Router, type Request, type Response } from 'express';
import { query, createLogger } from '@nexus/core';

const router = Router();
const logger = createLogger('api/ios');

// ── Auth middleware for iOS routes ─────────────────────────
router.use((req: Request, res: Response, next) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  const systemKey = process.env.NEXUS_SYSTEM_KEY;
  const iosToken = process.env.IOS_AUTH_TOKEN ?? systemKey;

  if (!token || (token !== systemKey && token !== iosToken)) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }
  next();
});

// ── Conversations ──────────────────────────────────────────

router.get('/conversations', async (_req: Request, res: Response) => {
  try {
    const result = await query(
      `SELECT id, title, created_at, updated_at,
        (SELECT content FROM messages WHERE conversation_id = c.id ORDER BY created_at DESC LIMIT 1) as last_message
       FROM conversations c
       ORDER BY updated_at DESC LIMIT 50`,
    );
    res.json({ conversations: result.rows });
  } catch (err) {
    logger.logMinimal('Error listing conversations:', (err as Error).message);
    res.status(500).json({ error: 'Failed to list conversations' });
  }
});

router.get('/conversations/:id', async (req: Request, res: Response) => {
  try {
    const result = await query(
      `SELECT id, conversation_id, role, content, created_at FROM messages
       WHERE conversation_id = $1 ORDER BY created_at ASC`,
      [req.params.id],
    );
    res.json({ messages: result.rows });
  } catch (err) {
    logger.logMinimal('Error getting messages:', (err as Error).message);
    res.status(500).json({ error: 'Failed to get messages' });
  }
});

router.delete('/conversations/:id', async (req: Request, res: Response) => {
  try {
    await query('DELETE FROM messages WHERE conversation_id = $1', [req.params.id]);
    await query('DELETE FROM conversations WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    logger.logMinimal('Error deleting conversation:', (err as Error).message);
    res.status(500).json({ error: 'Failed to delete conversation' });
  }
});

// ── Chat (SSE streaming) ───────────────────────────────────

router.post('/chat', async (req: Request, res: Response) => {
  const { content, conversationId, voice_mode } = req.body;
  if (!content) {
    res.status(400).json({ error: 'content is required' });
    return;
  }

  logger.log('Chat message:', content.slice(0, 80), voice_mode ? '(voice)' : '');

  // Set up SSE
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  try {
    // Get or create conversation
    let convId = conversationId;
    if (!convId) {
      const result = await query(
        `INSERT INTO conversations (title) VALUES ($1) RETURNING id`,
        [content.slice(0, 50)],
      );
      convId = result.rows[0].id;
      res.write(`data: ${JSON.stringify({ type: 'start', conversationId: convId })}\n\n`);
    }

    // Save user message
    await query(
      `INSERT INTO messages (conversation_id, role, content) VALUES ($1, 'user', $2)`,
      [convId, content],
    );

    // Route to ARIA via Forge LLM
    const forgeUrl = process.env.FORGE_PRIORITY_URL ?? process.env.FORGE_URL ?? 'http://localhost:8088';
    const forgeKey = process.env.FORGE_API_KEY ?? '';

    // Build context: load recent messages for this conversation
    const history = await query(
      `SELECT role, content FROM messages WHERE conversation_id = $1 ORDER BY created_at ASC LIMIT 20`,
      [convId],
    );

    const messages = [
      {
        role: 'system',
        content: `You are ARIA — the owner's personal AI assistant. Be warm, helpful, direct.

You are the CONVERSATIONAL interface — you respond quickly and naturally. You do NOT have direct tool access in this chat mode.

When the owner asks you to DO something that requires action (archive emails, send messages, search data, schedule things, modify settings, look something up in his data), respond conversationally AND include a JSON block at the end of your message like this:

<agent_task>{"action": "brief description of what to do", "details": "specific instructions for the agent version of yourself"}</agent_task>

This will be routed to your agent self which has full tool access (Gmail, Calendar, iMessage, Photos, Notion, GitHub, Cloudflare, etc.) and will execute the task within seconds.

Examples:
- "Archive my Chancery emails" → respond warmly, then <agent_task>{"action": "archive Gmail emails", "details": "Search Gmail for all emails from Chancery using gmail__search_emails, then use gmail__batch_modify_emails to remove INBOX label from each"}</agent_task>
- "What meetings do I have tomorrow?" → Just answer from context, no agent task needed
- "Send a message to [Contact]" → respond, then <agent_task>{"action": "send iMessage", "details": "Send iMessage to [Contact Name] via send-imessage tool"}</agent_task>

Only use <agent_task> when actual tool execution is needed. Pure conversation, questions, and advice don't need it.`,
      },
      ...history.rows.map((m: Record<string, unknown>) => ({
        role: String(m.role) === 'assistant' ? 'assistant' : 'user',
        content: String(m.content ?? ''),
      })),
    ];

    const llmResponse = await fetch(`${forgeUrl}/v1/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(forgeKey ? { Authorization: `Bearer ${forgeKey}` } : {}),
      },
      body: JSON.stringify({
        model: 'qwen3-next-chat-80b',
        messages,
        max_tokens: 2000,
        temperature: 0.7,
        stream: true,
        chat_template_kwargs: { enable_thinking: false },
      }),
    });

    if (!llmResponse.ok || !llmResponse.body) {
      const errText = await llmResponse.text();
      logger.logMinimal('LLM error:', llmResponse.status, errText.slice(0, 200));
      res.write(`data: ${JSON.stringify({ type: 'error', message: 'ARIA is temporarily unavailable. Please try again.' })}\n\n`);
      res.end();
      return;
    }

    // Stream LLM response as SSE
    let fullResponse = '';
    let modelUsed = 'qwen3-next-chat-80b';
    let inTaskBlock = false;
    const reader = llmResponse.body.getReader();
    const decoder = new TextDecoder();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const chunk = decoder.decode(value, { stream: true });
      const lines = chunk.split('\n').filter(l => l.startsWith('data: '));

      for (const line of lines) {
        const payload = line.slice(6);
        if (payload === '[DONE]') continue;

        try {
          const data = JSON.parse(payload);
          if (data.model) modelUsed = String(data.model);
          const delta = data.choices?.[0]?.delta?.content ?? '';
          if (delta) {
            fullResponse += delta;
            // Don't stream <agent_task> blocks to the UI
            if (!fullResponse.includes('<agent_task>') || fullResponse.includes('</agent_task>')) {
              // Either no task block started, or it already closed — safe to check
              if (!delta.includes('<agent_task') && !delta.includes('</agent_task') && !inTaskBlock) {
                res.write(`data: ${JSON.stringify({ type: 'chunk', text: delta })}\n\n`);
              }
            }
            // Track if we're inside a task block
            if (delta.includes('<agent_task>')) inTaskBlock = true;
            if (delta.includes('</agent_task>')) inTaskBlock = false;
          }
        } catch {
          // Skip malformed chunks
        }
      }
    }

    // Extract and route agent tasks before saving
    let cleanResponse = fullResponse;
    const taskMatch = fullResponse.match(/<agent_task>([\s\S]*?)<\/agent_task>/);
    if (taskMatch) {
      // Strip the task block from the displayed message
      cleanResponse = fullResponse.replace(/<agent_task>[\s\S]*?<\/agent_task>/, '').trim();

      // Send to ARIA agent inbox via LISTEN/NOTIFY for immediate wake-up
      try {
        const taskData = JSON.parse(taskMatch[1]);
        await query(
          `INSERT INTO agent_inbox (to_agent, from_agent, message, priority, context)
           VALUES ('aria', 'owner', $1, 3, $2)`,
          [
            `[Chat Request] ${taskData.action}: ${taskData.details}`,
            JSON.stringify({ source: 'ios_chat', conversation_id: convId }),
          ],
        );
        logger.log('Routed agent task from chat:', taskData.action);
      } catch (err) {
        logger.logMinimal('Failed to parse/route agent task:', (err as Error).message);
      }
    }

    // Save assistant message (cleaned of task blocks)
    if (cleanResponse) {
      await query(
        `INSERT INTO messages (conversation_id, role, content) VALUES ($1, 'assistant', $2)`,
        [convId, cleanResponse],
      );
      await query(
        `UPDATE conversations SET updated_at = NOW(), title = COALESCE(title, $1) WHERE id = $2`,
        [content.slice(0, 50), convId],
      );
    }

    // Save the message ID for the done event
    let messageId = '';
    if (fullResponse) {
      const msgResult = await query(
        `SELECT id FROM messages WHERE conversation_id = $1 AND role = 'assistant' ORDER BY created_at DESC LIMIT 1`,
        [convId],
      );
      messageId = msgResult.rows[0]?.id ?? '';
    }
    res.write(`data: ${JSON.stringify({ type: 'done', messageId, model: modelUsed })}\n\n`);
    res.end();
  } catch (err) {
    logger.logMinimal('Chat error:', (err as Error).message);
    res.write(`data: ${JSON.stringify({ type: 'error', message: (err as Error).message })}\n\n`);
    res.end();
  }
});

// ── Health Data ────────────────────────────────────────────

router.post('/health-data', async (req: Request, res: Response) => {
  const { records } = req.body;
  if (!Array.isArray(records)) {
    res.status(400).json({ error: 'records array is required' });
    return;
  }

  logger.log(`Receiving ${records.length} health records`);

  let inserted = 0;
  let skipped = 0;

  for (const record of records) {
    try {
      // Value can be a number or a JSON object — extract numeric or store object in metadata
      let numericValue: number | null = null;
      let extraMetadata: Record<string, unknown> = {};
      const rawValue = record.value;

      if (typeof rawValue === 'number') {
        numericValue = rawValue;
      } else if (typeof rawValue === 'string' && !isNaN(Number(rawValue))) {
        numericValue = Number(rawValue);
      } else if (typeof rawValue === 'object' && rawValue !== null) {
        // JSON object (e.g. {"count": 12362} or {"resting": 61, "max": 99})
        extraMetadata = rawValue as Record<string, unknown>;
        // Try to extract a primary numeric value
        const firstNum = Object.values(rawValue).find(v => typeof v === 'number');
        if (typeof firstNum === 'number') numericValue = firstNum;
      }

      const metadata = { ...(record.metadata ?? {}), ...extraMetadata };

      const result = await query(
        `INSERT INTO aurora_raw_health_records
         (record_type, source_name, unit, value, start_date, end_date, creation_date, device, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT DO NOTHING`,
        [
          record.type ?? record.record_type,
          record.source_name ?? record.sourceName ?? 'iPhone',
          record.unit,
          numericValue,
          record.start_date ?? record.startDate,
          record.end_date ?? record.endDate,
          record.creation_date ?? record.creationDate,
          record.device,
          Object.keys(metadata).length > 0 ? JSON.stringify(metadata) : null,
        ],
      );
      if (result.rowCount && result.rowCount > 0) inserted++;
      else skipped++;
    } catch (err) {
      logger.logDebug('Health record insert error:', (err as Error).message);
      skipped++;
    }
  }

  // Update data source registry
  await query(
    `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active', consecutive_failures = 0
     WHERE source_key = 'healthkit'`,
  ).catch(() => {});

  logger.log(`Health sync: ${inserted} inserted, ${skipped} skipped`);
  res.json({ synced: inserted, inserted, skipped, total: records.length });
});

// ── Location ───────────────────────────────────────────────

router.post('/device-location', async (req: Request, res: Response) => {
  const { location } = req.body;
  if (!location) {
    res.status(400).json({ error: 'location object is required' });
    return;
  }

  logger.logVerbose('Location update:', location.latitude, location.longitude);

  try {
    await query(
      `INSERT INTO device_location
       (latitude, longitude, accuracy, speed, course, timestamp, source)
       VALUES ($1, $2, $3, $4, $5, $6, 'ios')`,
      [
        location.latitude,
        location.longitude,
        location.horizontal_accuracy ?? location.horizontalAccuracy ?? location.accuracy ?? 0,
        location.speed,
        location.course,
        location.timestamp ?? new Date().toISOString(),
      ],
    );

    // Update data source registry
    await query(
      `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active', consecutive_failures = 0
       WHERE source_key = 'device_location'`,
    ).catch(() => {});

    res.json({ saved: true });
  } catch (err) {
    logger.logMinimal('Location insert error:', (err as Error).message);
    res.status(500).json({ error: 'Failed to save location' });
  }
});

// ── Visits (CLVisit) ───────────────────────────────────────

router.post('/device-visits', async (req: Request, res: Response) => {
  const { visit } = req.body;
  if (!visit) {
    res.status(400).json({ error: 'visit object is required' });
    return;
  }

  logger.logVerbose('Visit:', visit.latitude, visit.longitude);

  try {
    await query(
      `INSERT INTO device_location
       (latitude, longitude, accuracy, timestamp, source)
       VALUES ($1, $2, $3, $4, 'ios-visit')`,
      [
        visit.latitude,
        visit.longitude,
        visit.horizontal_accuracy ?? visit.horizontalAccuracy ?? 0,
        visit.arrival_date ?? visit.arrivalDate ?? new Date().toISOString(),
      ],
    );

    await query(
      `UPDATE data_source_registry SET last_sync_at = NOW(), status = 'active'
       WHERE source_key = 'device_location'`,
    ).catch(() => {});

    res.json({ saved: 1 });
  } catch (err) {
    logger.logMinimal('Visit insert error:', (err as Error).message);
    res.status(500).json({ error: 'Failed to save visit' });
  }
});

// ── Relay log reporter ingest ──────────────────────────────
//
// Receives per-60s health snapshots from the ARIA Relay.app (Swift,
// LogReporterService). The Relay was originally pointed at the retired
// aria-api, which had this endpoint. It was never ported to nexus-api
// until this session. Re-ported 2026-04-11 as part of the latent-bug
// audit that discovered the Relay had been getting 401s against the
// retired backend since at least 2026-03-25, leaving log_reporter_
// snapshots empty and log-monitor's local-services check blind.
//
// Payload shape (from LogReporterService.swift):
//   {
//     daemon: { ... state blob ... },
//     services: { ... per-service states ... },
//     toolkit: { ... toolkit stats from tracking.db ... },
//     recent_errors: [ { message: "..." }, ... ]
//   }
//
// Everything gets stored as JSONB in log_reporter_snapshots. The
// log-monitor job scans this table for stale / unhealthy signals.
router.post('/log-reporter/ingest', async (req: Request, res: Response) => {
  const { daemon, services, toolkit, recent_errors } = (req.body ?? {}) as {
    daemon?: unknown;
    services?: unknown;
    toolkit?: unknown;
    recent_errors?: unknown;
  };

  // At least ONE of the fields must be present for the snapshot to be meaningful.
  if (daemon == null && services == null && toolkit == null && recent_errors == null) {
    res.status(400).json({ error: 'snapshot must include at least one of daemon / services / toolkit / recent_errors' });
    return;
  }

  try {
    const result = await query<{ id: string }>(
      `INSERT INTO log_reporter_snapshots (daemon, services, toolkit, recent_errors)
       VALUES ($1::jsonb, $2::jsonb, $3::jsonb, $4::jsonb)
       RETURNING id`,
      [
        daemon == null ? null : JSON.stringify(daemon),
        services == null ? null : JSON.stringify(services),
        toolkit == null ? null : JSON.stringify(toolkit),
        recent_errors == null ? null : JSON.stringify(recent_errors),
      ],
    );
    logger.logVerbose(`Relay snapshot stored: ${result.rows[0]?.id}`);
    res.status(201).json({ ok: true, id: result.rows[0]?.id });
  } catch (err) {
    logger.logMinimal('Log reporter insert error:', (err as Error).message);
    res.status(500).json({ error: 'Failed to store snapshot' });
  }
});

export default router;
