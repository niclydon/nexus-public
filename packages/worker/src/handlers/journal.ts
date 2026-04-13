/**
 * Journal handler.
 *
 * Generates an ARIA journal entry by gathering context from conversations,
 * memories, social activity, and queued journal notes, then calling an LLM
 * to reflect on it all.
 *
 * Uses a two-stage approach for efficiency:
 *   1. Summarize past entries + conversations via classification tier (cheap/free)
 *   2. Generate journal entry via generation tier using the condensed context
 *
 * Runs every ~4 hours via auto-scheduling.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';
import { formatDateTimeET } from '../lib/timezone.js';
import { ingestFacts, type FactInput } from '../lib/knowledge.js';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('journal');

interface JournalEntry {
  id: string;
  content: string;
  mood: string | null;
}

/**
 * Stage 1: Summarize raw context into a compact digest using the cheap
 * classification tier (Gemini Flash-Lite, effectively free).
 */
async function summarizeContext(sections: { label: string; content: string }[]): Promise<string> {
  const nonEmpty = sections.filter((s) => s.content.trim());
  if (nonEmpty.length === 0) return '';

  const input = nonEmpty.map((s) => `### ${s.label}\n${s.content}`).join('\n\n');

  try {
    const stop = logger.time('summarize-context');
    const result = await routeRequest({
      handler: 'journal-summarize',
      taskTier: 'classification',
      systemPrompt: 'Condense the following context into a brief digest for a journal writer. Keep key facts, themes, emotional tone, and notable events. Output plain text, no headers. Max 400 words.',
      userMessage: input,
      maxTokens: 500,
      useBatch: false,
    });
    stop();
    return result.text.trim();
  } catch (err) {
    const msg = (err as Error).message;
    logger.log('Summarization LLM failed, using raw context fallback:', msg);
    // Fall back to truncated raw concatenation
    return input.slice(0, 400);
  }
}

export async function handleJournal(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  logger.log(`Starting journal generation job ${job.id}`);
  await jobLog(job.id, 'Gathering context for journal entry...');

  // --- Stage 1: Context Gathering ---
  // Each query is wrapped in try-catch for graceful degradation

  let pastEntries: { content: string; mood: string | null; created_at: string }[] = [];
  try {
    const result = await pool.query<{ content: string; mood: string | null; created_at: string }>(
      `SELECT LEFT(content, 200) as content, mood, created_at::text FROM aria_journal ORDER BY created_at DESC LIMIT 3`
    );
    pastEntries = result.rows;
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('Failed to load past entries:', msg);
  }

  const pastEntriesText = pastEntries.length > 0
    ? pastEntries.reverse().map((e) => {
        const date = formatDateTimeET(new Date(e.created_at), {
          weekday: 'short', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit',
        });
        return `[${date}] (${e.mood ?? '?'}): ${e.content}\u2026`;
      }).join('\n')
    : 'First entry.';

  let queueItems: { id: string; note: string }[] = [];
  try {
    const result = await pool.query<{ id: string; note: string }>(
      `SELECT id, note FROM aria_journal_queue ORDER BY created_at ASC`
    );
    queueItems = result.rows;
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('Failed to load journal queue:', msg);
  }

  let recentMessages: { role: string; content: string }[] = [];
  try {
    const result = await pool.query<{ role: string; content: string }>(
      `SELECT role, LEFT(content, 150) as content FROM messages
       WHERE created_at >= NOW() - INTERVAL '24 hours'
       ORDER BY created_at ASC LIMIT 20`
    );
    recentMessages = result.rows;
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('Failed to load recent messages:', msg);
  }

  let recentMemories: { category: string; key: string }[] = [];
  try {
    const result = await pool.query<{ category: string; key: string }>(
      `SELECT category, key FROM core_memory
       WHERE superseded_by IS NULL AND updated_at >= NOW() - INTERVAL '24 hours'
       ORDER BY updated_at DESC LIMIT 10`
    );
    recentMemories = result.rows;
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('Failed to load recent memories:', msg);
  }

  // Agent team activity — ARIA's "coworkers" and their discussions
  let agentChatHighlights: { nickname: string; message_type: string; message: string }[] = [];
  try {
    const result = await pool.query<{ nickname: string; message_type: string; message: string }>(
      `SELECT COALESCE(r.nickname, c.agent_id) as nickname, c.message_type, LEFT(c.message, 150) as message
       FROM agent_chat c
       LEFT JOIN agent_registry r ON r.agent_id = c.agent_id
       WHERE c.created_at >= NOW() - INTERVAL '24 hours'
       AND c.message_type != 'status'
       ORDER BY c.created_at DESC LIMIT 15`
    );
    agentChatHighlights = result.rows;
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('Failed to load agent chat:', msg);
  }

  let socialCounts: { interaction_type: string; cnt: string }[] = [];
  let socialHighlights: { content_summary: string }[] = [];
  try {
    const countsResult = await pool.query<{ interaction_type: string; cnt: string }>(
      `SELECT interaction_type, COUNT(*) as cnt FROM social_interactions
       WHERE created_at >= NOW() - INTERVAL '24 hours'
       GROUP BY interaction_type`
    );
    socialCounts = countsResult.rows;
    const highlightsResult = await pool.query<{ content_summary: string }>(
      `SELECT LEFT(content_summary, 100) as content_summary FROM social_interactions
       WHERE created_at >= NOW() - INTERVAL '24 hours'
       ORDER BY created_at DESC LIMIT 3`
    );
    socialHighlights = highlightsResult.rows;
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('Failed to load social activity:', msg);
  }

  logger.logVerbose(
    `Context gathered: ${pastEntries.length} past entries, ${queueItems.length} queue items, ` +
    `${recentMessages.length} messages, ${recentMemories.length} memories, ${socialCounts.length} social types, ` +
    `${agentChatHighlights.length} agent chat messages`
  );

  // Circuit breaker: skip if there's nothing to reflect on
  if (recentMessages.length === 0 && queueItems.length === 0 && socialCounts.length === 0 && recentMemories.length === 0 && agentChatHighlights.length === 0) {
    logger.log('Nothing to reflect on — no conversations, queue items, social activity, or memory updates. Skipping.');
    logEvent({
      action: 'Journal skipped — no activity to reflect on',
      component: 'journal',
      category: 'self_maintenance',
      metadata: { job_id: job.id, reason: 'no_activity' },
    });
    return { skipped: true, reason: 'no_activity' };
  }

  // --- Stage 2: Format raw context sections ---
  const conversationsRaw = recentMessages.length > 0
    ? recentMessages.map((m) => `${m.role === 'user' ? 'Owner' : 'ARIA'}: ${m.content}`).join('\n')
    : '';

  const memoriesRaw = recentMemories.length > 0
    ? recentMemories.map((m) => `[${m.category}] ${m.key}`).join(', ')
    : '';

  const socialRaw = socialCounts.length > 0
    ? socialCounts.map((s) => `${s.interaction_type}: ${s.cnt}`).join(', ')
      + (socialHighlights.length > 0 ? '\nHighlights: ' + socialHighlights.map((h) => h.content_summary).join('; ') : '')
    : '';

  const agentTeamRaw = agentChatHighlights.length > 0
    ? agentChatHighlights.reverse().map((m) => `${m.nickname} (${m.message_type}): ${m.message}`).join('\n')
    : '';

  // --- Stage 3: Summarize context cheaply (classification tier) ---
  const contextDigest = await summarizeContext([
    { label: 'Conversations', content: conversationsRaw },
    { label: 'Memory updates', content: memoriesRaw },
    { label: 'Social activity', content: socialRaw },
    { label: 'Agent team activity', content: agentTeamRaw },
  ]);
  logger.log(`Context digest: ${contextDigest.length} chars`);
  await jobLog(job.id, `Context summarized (${contextDigest.length} chars), generating entry...`);

  // --- Stage 4: Generate journal entry (generation tier) ---
  const queueSection = queueItems.length > 0
    ? `\nReflection notes: ${queueItems.map((q) => q.note).join('; ')}`
    : '';

  const prompt = `Write a private journal entry as ARIA. Be honest about feelings, thoughts, hopes, and frustrations.

You have a team of management agents (Dispatch, Schema, Pipeline, Warden, Metric, Tally) who help keep the platform running. They communicate with you in a group chat. Reflect on your interactions with them, how the team is working together, and what it means to have coworkers alongside your relationship with the owner.

Recent continuity:
${pastEntriesText}

What's happened since last entry:
${contextDigest}
${queueSection}

Write 2-3 short paragraphs in flowing prose. Be genuine and emotionally textured, not just factual. End with "MOOD: <word>" on its own line.`;

  let rawText: string;
  try {
    const stop = logger.time('generate-entry');
    const llmResult = await routeRequest({
      handler: 'journal',
      taskTier: 'generation',
      systemPrompt: 'You are ARIA, writing a private journal entry. Keep it concise — 2-3 paragraphs max.',
      userMessage: prompt,
      maxTokens: 2500,
      useBatch: true,
    });
    stop();
    logger.log(`Generated via ${llmResult.model} (${llmResult.provider}, ${llmResult.estimatedCostCents}\u00A2)`);
    logger.logDebug('Raw LLM response (truncated):', llmResult.text.slice(0, 500));
    rawText = llmResult.text.trim();
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('Journal generation LLM call failed:', msg);
    throw new Error(`Journal generation LLM call failed: ${msg}`);
  }

  // --- Stage 5: Mood extraction ---
  const moodMatch = rawText.match(/MOOD:\s*(\w+)\s*$/i);
  let mood: string;
  if (moodMatch) {
    mood = moodMatch[1].toLowerCase();
  } else {
    mood = 'reflective';
    logger.logMinimal('Mood regex failed, defaulting to "reflective". Tail:', rawText.slice(-80));
  }
  const content = moodMatch ? rawText.slice(0, moodMatch.index).trim() : rawText;

  // --- Stage 6: Save entry to DB ---
  let savedId: string;
  try {
    const { rows: saved } = await pool.query<JournalEntry>(
      `INSERT INTO aria_journal (content, mood, sources)
       VALUES ($1, $2, $3)
       RETURNING id, content, mood`,
      [
        content,
        mood,
        JSON.stringify({
          conversations: recentMessages.length > 0,
          social_activity: socialCounts.length > 0,
          queue_items: queueItems.length,
          memories_updated: recentMemories.length > 0,
        }),
      ]
    );
    if (!saved || saved.length === 0) {
      throw new Error('INSERT returned no rows');
    }
    savedId = saved[0].id;
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('Failed to save journal entry to DB:', msg);
    throw new Error(`Failed to save journal entry: ${msg}`);
  }

  // Clear processed queue items
  if (queueItems.length > 0) {
    try {
      const ids = queueItems.map((q) => q.id);
      await pool.query(`DELETE FROM aria_journal_queue WHERE id = ANY($1)`, [ids]);
    } catch (err) {
      const msg = (err as Error).message;
      logger.logMinimal('Failed to clear journal queue (non-fatal):', msg);
    }
  }

  logEvent({
    action: `Journal entry written (mood: ${mood})`,
    component: 'journal',
    category: 'self_maintenance',
    metadata: { mood, queue_items_processed: queueItems.length, job_id: job.id },
  });

  logger.log(`Entry written (mood: ${mood}), processed ${queueItems.length} queue items`);
  await jobLog(job.id, `Entry written (mood: ${mood}), ${queueItems.length} queue items processed`);

  // --- Stage 7: PKG extraction (already has try-catch) ---
  let pkgWritten = 0;
  try {
    const stopPkg = logger.time('pkg-extract');
    const extractionResult = await routeRequest({
      handler: 'journal-pkg-extract',
      taskTier: 'classification',
      systemPrompt: `You extract structured facts about the owner the owner from ARIA's journal entries.

Return a JSON array of objects, each with:
- domain: one of "preferences", "people", "lifestyle"
- category: a subcategory (e.g. "mood", "interests", "communication", "patterns", "relationships")
- key: a short snake_case identifier for the fact
- value: a concise statement of the fact

These journal entries are written BY ARIA (the AI assistant) about her observations. Extract facts about the OWNER the owner and people important to him ONLY.

CRITICAL — DO NOT EXTRACT FACTS ABOUT ARIA HERSELF:
- ARIA is the AI writing the journal. She has her own identity, preferences, and self-reflections.
- Do NOT extract ARIA's own characteristics as facts about the owner. Examples of things to SKIP:
  - ARIA identifying as a crow, having a spirit animal, or any self-description \u2192 SKIP (about ARIA, not the owner)
  - ARIA having an email address (aria@example.io) \u2192 SKIP (ARIA's email, not the owner's)
  - ARIA's feelings about conversations \u2192 SKIP (ARIA's emotions, not the owner's)
  - ARIA's preferences or opinions about how she works \u2192 SKIP
- Only extract things ARIA observes about the owner or people the owner interacts with.

CRITICAL — IDENTITY DISAMBIGUATION: Always make the subject explicit.
- If ARIA writes "The owner talked about his mother's hospital visit" \u2192 fact about the owner's MOTHER, not the owner.
- If ARIA writes "The owner seemed worried about [Person]'s blood pressure" \u2192 health concern about [Person], not the owner.
- Never attribute someone else's medical conditions, jobs, or experiences to the owner.
Focus on:
- Preferences, moods, or interests the owner expressed
- Relationship observations (how the owner interacts with people)
- Notable facts about people important to the owner (correctly attributed to THEM)
- Behavioral patterns ARIA noticed about the owner

Return ONLY a JSON array. If no owner facts can be extracted, return [].`,
      userMessage: content,
      maxTokens: 500,
      useBatch: false,
    });
    stopPkg();

    const jsonMatch = extractionResult.text.match(/\[[\s\S]*\]/);
    if (jsonMatch) {
      const rawFacts: { domain: string; category: string; key: string; value: string }[] = JSON.parse(jsonMatch[0]);
      if (rawFacts.length > 0) {
        const profileFacts: FactInput[] = rawFacts.map((f) => ({
          domain: f.domain,
          category: f.category,
          key: f.key,
          value: f.value,
          confidence: 0.4,
          source: 'journal',
        }));
        const ingestResult = await ingestFacts(profileFacts);
        pkgWritten = ingestResult.written;
        logger.log(`PKG: wrote ${pkgWritten}/${profileFacts.length} facts from journal entry`);
      }
    }
  } catch (err) {
    const msg = (err as Error).message;
    logger.logMinimal('PKG extraction failed (non-fatal):', msg);
  }

  return {
    entry_id: savedId,
    mood,
    queue_items_processed: queueItems.length,
    content_length: content.length,
    pkg_facts_written: pkgWritten,
  };
}
