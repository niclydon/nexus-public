/**
 * PKG Backfill handler.
 *
 * One-time job that re-processes existing data sources through PKG extraction:
 * 1. core_memory — promotes all active facts to owner_profile with domain mapping
 * 2. journal — re-runs PKG extraction prompts on all journal entries
 * 3. life_narration — re-runs place/activity/event extraction on all narrations
 * 4. proactive_insights — extracts patterns from existing insights
 *
 * Safe to run multiple times — upsertProfileFacts handles dedup via
 * domain/category/key unique constraint and evidence bumping.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { routeRequest } from '../lib/llm/index.js';
import { upsertProfileFacts, type ProfileFact } from '../lib/owner-profile.js';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('pkg-backfill');

const DOMAIN_MAP: Record<string, string> = {
  people: 'people',
  preferences: 'preferences',
  context: 'events',
  recurring: 'lifestyle',
  goals: 'interests',
  other: 'interests',
};

export async function handlePkgBackfill(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { sources?: string[] };
  const sources = payload?.sources || ['core_memory', 'journal', 'life_narration', 'proactive_insights'];
  await jobLog(job.id, `Starting PKG backfill for sources: ${sources.join(', ')}`);

  const result: Record<string, unknown> = {};

  // 1. Core Memory → PKG
  if (sources.includes('core_memory')) {
    logger.log('Processing core_memory...');
    const { rows: memories } = await pool.query<{
      category: string; key: string; value: string;
    }>(
      `SELECT category, key, value FROM core_memory
       WHERE superseded_by IS NULL
       ORDER BY category, key`
    );

    const facts: ProfileFact[] = memories.map((m) => ({
      domain: DOMAIN_MAP[m.category] ?? 'interests',
      category: m.category,
      key: m.key,
      value: m.value,
      confidence: 0.7,
      source: 'memory_maintenance',
    }));

    const written = await upsertProfileFacts(facts);
    logger.log(`core_memory: promoted ${written}/${facts.length} facts to PKG`);
    await jobLog(job.id, `core_memory: promoted ${written}/${facts.length} facts to PKG`);
    result.core_memory = { total: facts.length, written };
  }

  // 2. Journal Entries → PKG (LLM extraction in batches)
  if (sources.includes('journal')) {
    logger.log('Processing journal entries...');
    const { rows: entries } = await pool.query<{
      id: string; content: string; created_at: string;
    }>(
      `SELECT id, content, created_at::text FROM aria_journal
       ORDER BY created_at ASC`
    );

    let totalExtracted = 0;
    let totalWritten = 0;
    const BATCH_SIZE = 5;

    for (let i = 0; i < entries.length; i += BATCH_SIZE) {
      const batch = entries.slice(i, i + BATCH_SIZE);
      const batchContent = batch.map((e) =>
        `[Journal entry from ${e.created_at}]\n${e.content}`
      ).join('\n\n---\n\n');

      try {
        const extractionResult = await routeRequest({
          handler: 'pkg-backfill-journal',
          taskTier: 'classification',
          systemPrompt: `You extract structured facts about the owner the owner from ARIA's journal entries.

Return a JSON array of objects, each with:
- domain: one of "preferences", "people", "lifestyle", "interests", "events", "places", "career", "health", "communication"
- category: a subcategory
- key: a short snake_case identifier for the fact
- value: a concise statement of the fact

These journal entries are written BY ARIA (the AI assistant). Extract facts about the OWNER the owner and people important to him ONLY.
DO NOT extract facts about ARIA herself (her identity, email, spirit animal, feelings, self-reflections — these are about the AI, not the owner).
CRITICAL — IDENTITY DISAMBIGUATION: Always make the subject explicit. Attribute facts to the correct person (e.g. "the owner's mother had a JP drain procedure" NOT "Had a JP drain procedure").
Focus on the owner's own preferences, behavioral patterns, moods, interests, life events, places, and notable facts about people in his life.
Return ONLY a JSON array. If no facts can be extracted, return [].`,
          userMessage: batchContent,
          maxTokens: 1000,
          useBatch: false,
        });

        const jsonMatch = extractionResult.text.match(/\[[\s\S]*\]/);
        if (jsonMatch) {
          const rawFacts: { domain: string; category: string; key: string; value: string }[] = JSON.parse(jsonMatch[0]);
          if (rawFacts.length > 0) {
            const profileFacts: ProfileFact[] = rawFacts.map((f) => ({
              domain: f.domain,
              category: f.category,
              key: f.key,
              value: f.value,
              confidence: 0.4,
              source: 'journal',
            }));
            const written = await upsertProfileFacts(profileFacts);
            totalExtracted += rawFacts.length;
            totalWritten += written;
          }
        }
      } catch (err) {
        const msg = (err as Error).message;
        logger.logMinimal(`Journal batch ${i}-${i + BATCH_SIZE} failed: ${msg}`);
      }

      logger.log(`journal: processed ${Math.min(i + BATCH_SIZE, entries.length)}/${entries.length} entries`);
      await jobLog(job.id, `journal: processed ${Math.min(i + BATCH_SIZE, entries.length)}/${entries.length} entries`);
    }

    logger.log(`journal: extracted ${totalExtracted}, wrote ${totalWritten}`);
    await jobLog(job.id, `journal: extracted ${totalExtracted}, wrote ${totalWritten}`);
    result.journal = { entries: entries.length, extracted: totalExtracted, written: totalWritten };
  }

  // 3. Life Narrations → PKG (LLM extraction in batches)
  if (sources.includes('life_narration')) {
    logger.log('Processing life narrations...');
    const { rows: narrations } = await pool.query<{
      id: number; content: string; created_at: string;
    }>(
      `SELECT id, narrative AS content, created_at::text FROM life_narration
       ORDER BY created_at ASC`
    );

    let totalExtracted = 0;
    let totalWritten = 0;
    const BATCH_SIZE = 5;

    for (let i = 0; i < narrations.length; i += BATCH_SIZE) {
      const batch = narrations.slice(i, i + BATCH_SIZE);
      const batchContent = batch.map((n) =>
        `[Narration from ${n.created_at}]\n${n.content}`
      ).join('\n\n---\n\n');

      try {
        const extractionResult = await routeRequest({
          handler: 'pkg-backfill-narration',
          taskTier: 'classification',
          systemPrompt: `You extract structured facts about the owner the owner from life narration entries — factual third-person accounts of what the owner was doing.

Return a JSON array of objects, each with:
- domain: one of "places", "lifestyle", "events", "health", "interests", "people", "career"
- category: a subcategory (e.g. "location", "routine", "activity", "exercise", "commute")
- key: a short snake_case identifier
- value: a concise statement of the fact

Focus on: places the owner visited, the owner's activities, routines, exercise, commute patterns, locations, meals, social interactions.
CRITICAL — IDENTITY DISAMBIGUATION: Always make the subject of each fact explicit. Do NOT attribute other people's activities to the owner.
Return ONLY a JSON array. If no facts can be extracted, return [].`,
          userMessage: batchContent,
          maxTokens: 1000,
          useBatch: false,
        });

        const jsonMatch = extractionResult.text.match(/\[[\s\S]*\]/);
        if (jsonMatch) {
          const rawFacts: { domain: string; category: string; key: string; value: string }[] = JSON.parse(jsonMatch[0]);
          if (rawFacts.length > 0) {
            const profileFacts: ProfileFact[] = rawFacts.map((f) => ({
              domain: f.domain,
              category: f.category,
              key: f.key,
              value: f.value,
              confidence: 0.5,
              source: 'life_narration',
            }));
            const written = await upsertProfileFacts(profileFacts);
            totalExtracted += rawFacts.length;
            totalWritten += written;
          }
        }
      } catch (err) {
        const msg = (err as Error).message;
        logger.logMinimal(`Narration batch ${i}-${i + BATCH_SIZE} failed: ${msg}`);
      }

      logger.log(`life_narration: processed ${Math.min(i + BATCH_SIZE, narrations.length)}/${narrations.length} narrations`);
      await jobLog(job.id, `life_narration: processed ${Math.min(i + BATCH_SIZE, narrations.length)}/${narrations.length} narrations`);
    }

    logger.log(`life_narration: extracted ${totalExtracted}, wrote ${totalWritten}`);
    await jobLog(job.id, `life_narration: extracted ${totalExtracted}, wrote ${totalWritten}`);
    result.life_narration = { narrations: narrations.length, extracted: totalExtracted, written: totalWritten };
  }

  // 4. Proactive Insights → PKG
  if (sources.includes('proactive_insights')) {
    logger.log('Processing proactive insights...');
    const { rows: insights } = await pool.query<{
      id: number; title: string; body: string; category: string; created_at: string;
    }>(
      `SELECT id, title, body, category, created_at::text FROM proactive_insights
       WHERE body IS NOT NULL AND body != ''
       ORDER BY created_at ASC`
    );

    let totalExtracted = 0;
    let totalWritten = 0;
    const BATCH_SIZE = 10;

    for (let i = 0; i < insights.length; i += BATCH_SIZE) {
      const batch = insights.slice(i, i + BATCH_SIZE);
      const batchContent = batch.map((ins) =>
        `[Insight from ${ins.created_at} — ${ins.category}] ${ins.title}\n${ins.body}`
      ).join('\n\n---\n\n');

      try {
        const extractionResult = await routeRequest({
          handler: 'pkg-backfill-insights',
          taskTier: 'classification',
          systemPrompt: `You extract structured facts about the owner the owner from proactive intelligence insights — observations ARIA (the AI assistant) made about patterns in the owner's data.

Return a JSON array of objects, each with:
- domain: one of "lifestyle", "preferences", "people", "health", "communication", "events", "interests", "career", "places"
- category: a subcategory
- key: a short snake_case identifier
- value: a concise statement of the pattern or fact

Do NOT extract facts about ARIA herself (her capabilities, email, identity). Only extract facts about the owner and people in his life.
Focus on: the owner's behavioral patterns, communication habits, schedule patterns, health trends, relationship dynamics, and notable facts about people in his life.
CRITICAL — IDENTITY DISAMBIGUATION: Always make the subject explicit. If an insight mentions other people's attributes, attribute them to the correct person, not to the owner.
Return ONLY a JSON array. If no facts can be extracted, return [].`,
          userMessage: batchContent,
          maxTokens: 1000,
          useBatch: false,
        });

        const jsonMatch = extractionResult.text.match(/\[[\s\S]*\]/);
        if (jsonMatch) {
          const rawFacts: { domain: string; category: string; key: string; value: string }[] = JSON.parse(jsonMatch[0]);
          if (rawFacts.length > 0) {
            const profileFacts: ProfileFact[] = rawFacts.map((f) => ({
              domain: f.domain,
              category: f.category,
              key: f.key,
              value: f.value,
              confidence: 0.6,
              source: 'proactive_intelligence',
            }));
            const written = await upsertProfileFacts(profileFacts);
            totalExtracted += rawFacts.length;
            totalWritten += written;
          }
        }
      } catch (err) {
        const msg = (err as Error).message;
        logger.logMinimal(`Insights batch ${i}-${i + BATCH_SIZE} failed: ${msg}`);
      }

      logger.log(`proactive_insights: processed ${Math.min(i + BATCH_SIZE, insights.length)}/${insights.length} insights`);
      await jobLog(job.id, `proactive_insights: processed ${Math.min(i + BATCH_SIZE, insights.length)}/${insights.length} insights`);
    }

    logger.log(`proactive_insights: extracted ${totalExtracted}, wrote ${totalWritten}`);
    await jobLog(job.id, `proactive_insights: extracted ${totalExtracted}, wrote ${totalWritten}`);
    result.proactive_insights = { insights: insights.length, extracted: totalExtracted, written: totalWritten };
  }

  logger.log('Complete:', JSON.stringify(result));
  return result;
}
