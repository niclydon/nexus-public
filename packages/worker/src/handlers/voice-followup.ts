/**
 * Voice followup handler.
 *
 * Runs ~30 minutes after a voice conversation ends. Searches Gmail for the
 * ElevenLabs transcript/summary email, reads it, and uses Claude to:
 *   1. Extract important details and save them to core memory
 *   2. Identify follow-up tasks or action items
 *   3. Log what was learned for the activity stream
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';

const logger = createLogger('voice-followup');

/**
 * Search gmail_archive for the ElevenLabs transcript email.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
async function findTranscriptEmail(sessionId: string): Promise<{ subject: string; body: string } | null> {
  const pool = getPool();

  // Search gmail_archive for ElevenLabs transcript emails from the last 2 hours
  const result = await pool.query(
    `SELECT subject, body_text FROM gmail_archive
     WHERE from_address ILIKE '%elevenlabs%'
       AND date > NOW() - INTERVAL '2 hours'
       AND (subject ILIKE '%conversation%' OR subject ILIKE '%transcript%' OR subject ILIKE '%summary%')
       AND body_text IS NOT NULL AND body_text != ''
     ORDER BY date DESC LIMIT 5`
  );

  if (result.rows.length === 0) return null;

  // Return the most recent match
  return {
    subject: result.rows[0].subject ?? '',
    body: result.rows[0].body_text,
  };
}

export async function handleVoiceFollowup(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = job.payload as { session_id: string; source: string };
  logger.log(`Processing followup for session ${payload.session_id} (${payload.source})`);

  const pool = getPool();

  // 1. Find the ElevenLabs transcript email
  const email = await findTranscriptEmail(payload.session_id);

  if (!email) {
    logger.log('No transcript email found yet');
    // If this is the first attempt, the email might not have arrived yet
    if (job.attempts <= 1) {
      throw new Error('Transcript email not found yet — will retry');
    }
    return { skipped: true, reason: 'No transcript email found after retries' };
  }

  logger.log(`Found email: "${email.subject}"`);

  // 2. Also fetch any existing transcript we saved from the session
  const { rows: sessions } = await pool.query(
    `SELECT transcript, source, duration_seconds FROM voice_sessions WHERE id = $1`,
    [payload.session_id]
  );
  const sessionTranscript = sessions[0]?.transcript;

  // 3. Use LLM router to analyze the transcript and extract actionable items
  const analysisPrompt = `You are ARIA, a personal AI assistant. You just finished a voice conversation with your owner. Below is the transcript and summary from that conversation.

Analyze this carefully and produce a JSON response with:
1. "memories" — an array of important facts, preferences, or details worth remembering. Each item should have:
   - "category": one of "people", "preferences", "goals", "recurring", "context", "other"
   - "key": a short, descriptive key name (e.g. "morning_routine", "project_deadline")
   - "value": the fact to remember
   Only include things that are genuinely worth storing long-term. Don't store trivial small talk.

2. "tasks" — an array of follow-up actions or things the owner mentioned needing to do. Each item should have:
   - "description": what needs to be done
   - "urgency": "high", "medium", or "low"
   Only include actual action items, not vague intentions.

3. "summary" — a 1-2 sentence summary of what the conversation was about.

Respond with ONLY valid JSON, no markdown fences.

${sessionTranscript ? `LOCAL TRANSCRIPT:\n${JSON.stringify(sessionTranscript)}\n\n` : ''}EMAIL FROM ELEVENLABS:
Subject: ${email.subject}
Body:
${email.body.slice(0, 8000)}`;

  const llmResult = await routeRequest({
    handler: 'voice-followup',
    taskTier: 'generation',
    systemPrompt: 'You are ARIA, analyzing a voice conversation transcript to extract memories and action items.',
    userMessage: analysisPrompt,
    maxTokens: 3000,
  });
  logger.log(`Analyzed via ${llmResult.model} (${llmResult.provider}, ${llmResult.estimatedCostCents}¢)`);

  const analysisText = llmResult.text;

  let analysis: {
    memories?: Array<{ category: string; key: string; value: string }>;
    tasks?: Array<{ description: string; urgency: string }>;
    summary?: string;
  };

  try {
    analysis = JSON.parse(analysisText);
  } catch {
    logger.logMinimal('Failed to parse analysis:', analysisText.slice(0, 500));
    analysis = { memories: [], tasks: [], summary: 'Voice conversation followup (analysis failed to parse)' };
  }

  // 4. Save memories to core_memory
  const memoriesSaved: string[] = [];
  for (const mem of analysis.memories ?? []) {
    if (!mem.category || !mem.key || !mem.value) continue;
    try {
      // Use the same upsert pattern as the main app
      const dbClient = await pool.connect();
      try {
        await dbClient.query('BEGIN');
        const existing = await dbClient.query(
          `SELECT id, version FROM core_memory WHERE category = $1 AND key = $2 AND superseded_by IS NULL LIMIT 1`,
          [mem.category, mem.key]
        );
        const nextVersion = existing.rows.length > 0 ? existing.rows[0].version + 1 : 1;
        const { rows: newRows } = await dbClient.query(
          `INSERT INTO core_memory (category, key, value, version) VALUES ($1, $2, $3, $4) RETURNING id`,
          [mem.category, mem.key, mem.value, nextVersion]
        );
        if (existing.rows.length > 0) {
          await dbClient.query(`UPDATE core_memory SET superseded_by = $1 WHERE id = $2`, [newRows[0].id, existing.rows[0].id]);
        }
        await dbClient.query('COMMIT');
        memoriesSaved.push(`[${mem.category}] ${mem.key}`);
      } catch {
        await dbClient.query('ROLLBACK');
      } finally {
        dbClient.release();
      }
    } catch {
      // Skip failed memory saves
    }
  }

  // 5. Log tasks to actions_log (visible in the activity stream)
  const tasks = analysis.tasks ?? [];
  for (const task of tasks) {
    await pool.query(
      `INSERT INTO actions_log (action_type, description, metadata)
       VALUES ('voice_followup_task', $1, $2)`,
      [task.description, JSON.stringify({ urgency: task.urgency, session_id: payload.session_id })]
    );
  }

  // 6. Log the followup event
  logEvent({
    action: `Voice followup: ${analysis.summary ?? 'analyzed conversation'}. ${memoriesSaved.length} memories saved, ${tasks.length} tasks identified.`,
    component: 'voice',
    category: 'self_maintenance',
    metadata: {
      session_id: payload.session_id,
      source: payload.source,
      memoriesSaved,
      taskCount: tasks.length,
      summary: analysis.summary,
    },
  });

  await pool.query(
    `INSERT INTO actions_log (action_type, description, metadata)
     VALUES ('voice_followup', $1, $2)`,
    [
      `Voice followup complete: ${memoriesSaved.length} memories, ${tasks.length} tasks`,
      JSON.stringify({ session_id: payload.session_id, summary: analysis.summary, memoriesSaved, tasks }),
    ]
  );

  logger.log(`Complete: ${memoriesSaved.length} memories saved, ${tasks.length} tasks identified`);

  return {
    summary: analysis.summary,
    memories_saved: memoriesSaved.length,
    tasks_identified: tasks.length,
  };
}
