/**
 * Biographical Interview Handler
 *
 * Rotates through 4 strategies to discover relationships, life events,
 * connections, and timeline markers from existing data sources. Each run
 * picks one strategy (tracked via batch_id pattern), queries relevant data,
 * sends it through the LLM for hypothesis generation, computes confidence
 * scores, and inserts results into bio_inference_queue.
 *
 * Self-chains with 6h delay after each run.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { routeRequest } from '../lib/llm/index.js';
import { jobLog } from '../lib/job-log.js';
import type { Pool } from 'pg';

const logger = createLogger('biographical-interview');

const CHAIN_DELAY_SECONDS = 2 * 3600; // 2 hours — run 12x/day across 5 strategies

const STRATEGIES = [
  'relationship-discovery',
  'life-event-mining',
  'connection-mapping',
  'timeline-reconstruction',
  'cross-source-deep-dive',
] as const;

type Strategy = typeof STRATEGIES[number];

interface Hypothesis {
  inference_type: string;
  subject_person_id?: number;
  related_person_id?: number;
  hypothesis: string;
  evidence: unknown[];
  rules_matched: string[];
}

// ── Strategy Implementations ────────────────────────────────────

async function relationshipDiscovery(pool: Pool, batchId: string): Promise<Hypothesis[]> {
  logger.logVerbose('Strategy: relationship-discovery — finding unconnected people with 30+ messages');

  const { rows: unconnected } = await pool.query<{
    person_id: number; display_name: string; msg_count: number;
  }>(`
    SELECT s.id AS person_id, s.display_name, COUNT(*)::int AS msg_count
    FROM aurora_unified_communication uc
    JOIN aurora_social_identities s ON s.id = uc.identity_id
    LEFT JOIN person_connections pc ON pc.person_id = s.id OR pc.related_person_id = s.id
    WHERE pc.id IS NULL AND s.is_person = TRUE
    GROUP BY s.id, s.display_name
    HAVING COUNT(*) >= 30
    ORDER BY COUNT(*) DESC
    LIMIT 5
  `);

  if (unconnected.length === 0) {
    logger.log('No unconnected people with 30+ messages found');
    return [];
  }

  logger.log(`Found ${unconnected.length} unconnected people to analyze`);
  const hypotheses: Hypothesis[] = [];

  for (const person of unconnected) {
    // Sample recent messages for context
    const { rows: messages } = await pool.query<{ content: string; sent_at: string; direction: string }>(`
      SELECT LEFT(content_text, 200) AS content, timestamp::text AS sent_at, direction
      FROM aurora_unified_communication
      WHERE identity_id = $1 AND content_text IS NOT NULL AND LENGTH(content_text) > 10
      ORDER BY timestamp DESC
      LIMIT 20
    `, [person.person_id]);

    // Check contacts for relation field
    const { rows: contactRels } = await pool.query<{ relation: string }>(`
      SELECT r.value AS relation
      FROM contacts c,
        LATERAL jsonb_array_elements_text(COALESCE(c.relations, '[]'::jsonb)) AS r(value)
      WHERE c.id IN (
        SELECT DISTINCT ci.id FROM contacts ci
        JOIN aurora_social_identity_links l ON
          l.identifier = ANY(SELECT jsonb_array_elements_text(COALESCE(ci.phone_numbers, '[]'::jsonb)))
          OR l.identifier = ANY(SELECT jsonb_array_elements_text(COALESCE(ci.email_addresses, '[]'::jsonb)))
        WHERE l.identity_id = $1
      )
      LIMIT 3
    `, [person.person_id]);

    // Check photo co-occurrence
    const { rows: coOccurrence } = await pool.query<{ other_id: number; other_name: string; count: number }>(`
      SELECT
        CASE WHEN person_a_id = $1 THEN person_b_id ELSE person_a_id END AS other_id,
        CASE WHEN person_a_id = $1 THEN b.display_name ELSE a.display_name END AS other_name,
        co_occurrence_count AS count
      FROM person_co_occurrence
      JOIN aurora_social_identities a ON a.id = person_a_id
      JOIN aurora_social_identities b ON b.id = person_b_id
      WHERE (person_a_id = $1 OR person_b_id = $1)
      ORDER BY co_occurrence_count DESC
      LIMIT 5
    `, [person.person_id]);

    const evidence: unknown[] = [];
    const rulesMatched: string[] = [];

    if (contactRels.length > 0) {
      evidence.push({ source: 'contacts', relation: contactRels[0].relation });
      rulesMatched.push('contacts_relation_explicit');
    }
    if (coOccurrence.length > 0) {
      evidence.push({ source: 'photos', co_occurrences: coOccurrence.map(c => ({ name: c.other_name, count: c.count })) });
      if (coOccurrence.some(c => c.count > 10)) rulesMatched.push('co_occurrence_high');
    }

    const messageSummary = messages.map(m => `[${m.direction}] ${m.content}`).join('\n');

    const systemPrompt = `You are a biographical analyst. Given message samples and contextual data about a person, hypothesize their likely relationship to the user the owner. Return a JSON object with: { "relationship": "friend|family|coworker|acquaintance|partner|other", "confidence_reasoning": "...", "key_signals": ["..."] }. Be conservative — only suggest relationships supported by evidence.`;

    const userMessage = `Person: ${person.display_name} (${person.msg_count} messages)
${contactRels.length > 0 ? `Contact relation field: ${contactRels[0].relation}` : 'No explicit contact relation.'}
${coOccurrence.length > 0 ? `Photo co-occurrences: ${coOccurrence.map(c => `${c.other_name} (${c.count}x)`).join(', ')}` : ''}

Recent message samples:
${messageSummary}`;

    try {
      const result = await routeRequest({
        handler: 'biographical-interview',
        taskTier: 'generation',
        systemPrompt,
        userMessage,
        maxTokens: 2000,
      });

      const parsed = parseJsonResponse(result.text) as Record<string, unknown> | null;
      if (parsed && !Array.isArray(parsed) && parsed.relationship) {
        hypotheses.push({
          inference_type: 'relationship',
          subject_person_id: person.person_id,
          hypothesis: `${person.display_name} is likely a ${parsed.relationship}: ${parsed.confidence_reasoning ?? ''}`,
          evidence: [...evidence, { source: 'llm', key_signals: parsed.key_signals ?? [] }],
          rules_matched: rulesMatched,
        });
      }
    } catch (err) {
      logger.logMinimal(`LLM failed for ${person.display_name}:`, (err as Error).message);
    }
  }

  return hypotheses;
}

async function lifeEventMining(pool: Pool, batchId: string): Promise<Hypothesis[]> {
  logger.logVerbose('Strategy: life-event-mining — scanning messages for move/job/travel signals');

  // Search for life event signals in recent messages
  const { rows: signals } = await pool.query<{
    identity_id: number; display_name: string; content: string; sent_at: string; source: string;
  }>(`
    SELECT uc.identity_id, s.display_name, LEFT(uc.content_text, 300) AS content,
           uc.timestamp::text AS sent_at, uc.source_table AS source
    FROM aurora_unified_communication uc
    JOIN aurora_social_identities s ON s.id = uc.identity_id
    WHERE uc.content_text ~* '(moved to|new job|started at|got married|engaged|pregnant|baby|graduated|retired|promotion|laid off|fired|new house|buying a house|moving to)'
      AND uc.timestamp > NOW() - INTERVAL '180 days'
      AND s.is_person = TRUE
    ORDER BY uc.timestamp DESC
    LIMIT 30
  `);

  if (signals.length === 0) {
    logger.log('No life event signals found in recent messages');
    return [];
  }

  logger.log(`Found ${signals.length} potential life event signals`);

  // Group by person
  const byPerson = new Map<number, typeof signals>();
  for (const sig of signals) {
    const arr = byPerson.get(sig.identity_id) ?? [];
    arr.push(sig);
    byPerson.set(sig.identity_id, arr);
  }

  const hypotheses: Hypothesis[] = [];

  for (const [personId, msgs] of byPerson) {
    const name = msgs[0].display_name;
    const messageSummary = msgs.map(m => `[${m.sent_at}] ${m.content}`).join('\n');

    const systemPrompt = `You are a biographical analyst. Given messages that contain life event signals, identify specific life events. Return a JSON array of events: [{ "event_type": "move|job_change|marriage|birth|graduation|retirement|promotion|other", "description": "...", "approximate_date": "YYYY-MM or YYYY-MM-DD", "confidence_reasoning": "..." }]. Only include events with clear evidence.`;

    const userMessage = `Messages from/about ${name}:\n${messageSummary}`;

    try {
      const result = await routeRequest({
        handler: 'biographical-interview',
        taskTier: 'generation',
        systemPrompt,
        userMessage,
        maxTokens: 2000,
      });

      const parsed = parseJsonResponse(result.text);
      const rawEvents = Array.isArray(parsed) ? parsed : (parsed as Record<string, unknown>)?.events;
      const events = (Array.isArray(rawEvents) ? rawEvents : []) as Record<string, unknown>[];

      for (const event of events) {
        hypotheses.push({
          inference_type: 'life_event',
          subject_person_id: personId,
          hypothesis: `${name}: ${event.event_type} — ${event.description} (approx. ${event.approximate_date})`,
          evidence: [
            { source: 'messages', samples: msgs.map(m => ({ content: m.content, date: m.sent_at })) },
            { source: 'llm', reasoning: event.confidence_reasoning },
          ],
          rules_matched: [],
        });
      }
    } catch (err) {
      logger.logMinimal(`LLM failed for life events of ${name}:`, (err as Error).message);
    }
  }

  return hypotheses;
}

async function connectionMapping(pool: Pool, batchId: string): Promise<Hypothesis[]> {
  logger.logVerbose('Strategy: connection-mapping — finding people appearing together');

  // People with high co-occurrence but no person_connections link
  const { rows: pairs } = await pool.query<{
    person_a_id: number; person_b_id: number; name_a: string; name_b: string; count: number;
  }>(`
    SELECT co.person_a_id, co.person_b_id,
           a.display_name AS name_a, b.display_name AS name_b,
           co.co_occurrence_count AS count
    FROM person_co_occurrence co
    JOIN aurora_social_identities a ON a.id = co.person_a_id
    JOIN aurora_social_identities b ON b.id = co.person_b_id
    LEFT JOIN person_connections pc
      ON (pc.person_id = co.person_a_id AND pc.related_person_id = co.person_b_id)
      OR (pc.person_id = co.person_b_id AND pc.related_person_id = co.person_a_id)
    WHERE pc.id IS NULL AND co.co_occurrence_count >= 3
    ORDER BY co.co_occurrence_count DESC
    LIMIT 10
  `);

  if (pairs.length === 0) {
    logger.log('No unmapped co-occurrence pairs found');
    return [];
  }

  logger.log(`Found ${pairs.length} unmapped co-occurrence pairs`);
  const hypotheses: Hypothesis[] = [];

  // Also check for shared calendar events between pairs
  for (const pair of pairs) {
    const { rows: sharedEvents } = await pool.query<{ title: string; start_date: string }>(`
      SELECT DISTINCT e.title, e.start_date::text
      FROM device_calendar_events e
      WHERE e.title ~* ($1 || '|' || $2)
      LIMIT 5
    `, [pair.name_a.split(' ')[0], pair.name_b.split(' ')[0]]);

    // Check if they share a last name
    const lastA = pair.name_a.split(' ').pop()?.toLowerCase() ?? '';
    const lastB = pair.name_b.split(' ').pop()?.toLowerCase() ?? '';
    const sameLastName = lastA.length > 2 && lastA === lastB;

    const evidence: unknown[] = [
      { source: 'photos', co_occurrence_count: pair.count },
    ];
    const rulesMatched: string[] = [];

    if (sameLastName) {
      evidence.push({ source: 'name_analysis', shared_last_name: lastA });
      rulesMatched.push('same_last_name_family');
    }
    if (sharedEvents.length > 0) {
      evidence.push({ source: 'calendar', shared_events: sharedEvents });
      rulesMatched.push('calendar_shared_events');
    }

    const systemPrompt = `You are a biographical analyst. Given two people who frequently appear together in photos and possibly share other connections, hypothesize their likely relationship to each other. Return a JSON object: { "relationship": "family|friends|couple|coworkers|other", "confidence_reasoning": "...", "key_signals": ["..."] }. Be conservative.`;

    const userMessage = `Person A: ${pair.name_a}
Person B: ${pair.name_b}
Photo co-occurrences: ${pair.count}
${sameLastName ? `Shared last name: ${lastA}` : 'Different last names'}
${sharedEvents.length > 0 ? `Shared calendar events: ${sharedEvents.map(e => e.title).join(', ')}` : 'No shared calendar events found'}`;

    try {
      const result = await routeRequest({
        handler: 'biographical-interview',
        taskTier: 'generation',
        systemPrompt,
        userMessage,
        maxTokens: 2000,
      });

      const parsed = parseJsonResponse(result.text) as Record<string, unknown> | null;
      if (parsed && !Array.isArray(parsed) && parsed.relationship) {
        hypotheses.push({
          inference_type: 'connection',
          subject_person_id: pair.person_a_id,
          related_person_id: pair.person_b_id,
          hypothesis: `${pair.name_a} and ${pair.name_b} are likely ${parsed.relationship}: ${parsed.confidence_reasoning ?? ''}`,
          evidence: [...evidence, { source: 'llm', key_signals: parsed.key_signals ?? [] }],
          rules_matched: rulesMatched,
        });
      }
    } catch (err) {
      logger.logMinimal(`LLM failed for pair ${pair.name_a}/${pair.name_b}:`, (err as Error).message);
    }
  }

  return hypotheses;
}

async function timelineReconstruction(pool: Pool, batchId: string): Promise<Hypothesis[]> {
  logger.logVerbose('Strategy: timeline-reconstruction — finding date markers');

  // Look for explicit date references and milestones in messages
  const { rows: dateMarkers } = await pool.query<{
    identity_id: number; display_name: string; content: string; sent_at: string;
  }>(`
    SELECT uc.identity_id, s.display_name, LEFT(uc.content_text, 300) AS content, uc.timestamp::text AS sent_at
    FROM aurora_unified_communication uc
    JOIN aurora_social_identities s ON s.id = uc.identity_id
    WHERE uc.content_text ~* '(anniversary|birthday|\\d+ years|since \\d{4}|back in \\d{4}|first met|known .+ for|remember when)'
      AND s.is_person = TRUE
      AND uc.timestamp > NOW() - INTERVAL '365 days'
    ORDER BY uc.timestamp DESC
    LIMIT 30
  `);

  if (dateMarkers.length === 0) {
    logger.log('No timeline markers found in recent messages');
    return [];
  }

  logger.log(`Found ${dateMarkers.length} timeline markers`);

  // Group by person
  const byPerson = new Map<number, typeof dateMarkers>();
  for (const m of dateMarkers) {
    const arr = byPerson.get(m.identity_id) ?? [];
    arr.push(m);
    byPerson.set(m.identity_id, arr);
  }

  const hypotheses: Hypothesis[] = [];

  for (const [personId, msgs] of byPerson) {
    const name = msgs[0].display_name;
    const messageSummary = msgs.map(m => `[${m.sent_at}] ${m.content}`).join('\n');

    // Also pull any existing life chapters mentioning this person
    const { rows: chapters } = await pool.query<{ title: string; start_date: string; end_date: string }>(`
      SELECT title, start_date::text, end_date::text
      FROM aurora_life_chapters
      WHERE $1 = ANY(key_people) OR title ~* $2
      LIMIT 5
    `, [personId, name.split(' ')[0]]);

    const systemPrompt = `You are a biographical analyst. Given messages with date and timeline references, extract specific timeline events for a relationship or life history. Return a JSON array: [{ "event": "...", "approximate_date": "YYYY-MM or YYYY-MM-DD", "timeline_type": "relationship_start|milestone|recurring|life_chapter", "confidence_reasoning": "..." }]. Only include events with clear date evidence.`;

    const userMessage = `Messages from/about ${name}:\n${messageSummary}
${chapters.length > 0 ? `\nExisting life chapters: ${chapters.map(c => `${c.title} (${c.start_date} - ${c.end_date})`).join(', ')}` : ''}`;

    try {
      const result = await routeRequest({
        handler: 'biographical-interview',
        taskTier: 'generation',
        systemPrompt,
        userMessage,
        maxTokens: 2000,
      });

      const parsed = parseJsonResponse(result.text);
      const rawEvents = Array.isArray(parsed) ? parsed : (parsed as Record<string, unknown>)?.events;
      const events = (Array.isArray(rawEvents) ? rawEvents : []) as Record<string, unknown>[];

      for (const event of events) {
        hypotheses.push({
          inference_type: 'timeline',
          subject_person_id: personId,
          hypothesis: `${name}: ${event.timeline_type} — ${event.event} (approx. ${event.approximate_date})`,
          evidence: [
            { source: 'messages', samples: msgs.map(m => ({ content: m.content, date: m.sent_at })) },
            { source: 'llm', reasoning: event.confidence_reasoning },
          ],
          rules_matched: [],
        });
      }
    } catch (err) {
      logger.logMinimal(`LLM failed for timeline of ${name}:`, (err as Error).message);
    }
  }

  return hypotheses;
}

async function crossSourceDeepDive(pool: Pool, batchId: string): Promise<Hypothesis[]> {
  logger.logVerbose('Strategy: cross-source-deep-dive — correlating BB texts, guestbook, photos, knowledge facts, music');

  const hypotheses: Hypothesis[] = [];

  // 1. Find people who appear in multiple old data sources (BB, guestbook, MotoQ photos)
  const { rows: crossSourcePeople } = await pool.query<{
    display_name: string; person_id: number;
    has_bb_sms: boolean; has_bb_calls: boolean; has_guestbook: boolean;
    has_knowledge: boolean; has_imessage: boolean;
  }>(`
    WITH bb_people AS (
      SELECT DISTINCT c.contact_name AS name FROM bb_phone_calls c WHERE c.contact_name != ''
    ),
    gb_people AS (
      SELECT DISTINCT guest_name AS name FROM site_guestbook
      UNION SELECT DISTINCT page_owner AS name FROM site_guestbook
    ),
    combined AS (
      SELECT s.id AS person_id, s.display_name,
        EXISTS(SELECT 1 FROM bb_sms_messages m
               JOIN aurora_social_identity_links l ON l.identifier = m.phone_number
               WHERE l.identity_id = s.id) AS has_bb_sms,
        EXISTS(SELECT 1 FROM bb_phone_calls c WHERE LOWER(c.contact_name) = LOWER(s.display_name)) AS has_bb_calls,
        EXISTS(SELECT 1 FROM site_guestbook g
               WHERE LOWER(g.guest_name) LIKE '%' || LOWER(SPLIT_PART(s.display_name, ' ', 1)) || '%'
               OR LOWER(g.page_owner) LIKE '%' || LOWER(SPLIT_PART(s.display_name, ' ', 1)) || '%') AS has_guestbook,
        EXISTS(SELECT 1 FROM knowledge_facts kf
               WHERE kf.domain = 'biography' AND kf.value ILIKE '%' || s.display_name || '%') AS has_knowledge,
        EXISTS(SELECT 1 FROM aurora_unified_communication uc
               WHERE uc.identity_id = s.id AND uc.source = 'imessage') AS has_imessage
      FROM aurora_social_identities s
      WHERE s.is_person = TRUE
    )
    SELECT * FROM combined
    WHERE (has_bb_sms::int + has_bb_calls::int + has_guestbook::int + has_knowledge::int + has_imessage::int) >= 2
    ORDER BY (has_bb_sms::int + has_bb_calls::int + has_guestbook::int + has_knowledge::int + has_imessage::int) DESC
    LIMIT 5
  `);

  // Fallback if the fancy query fails (typo in table name, etc.)
  // Try a simpler cross-reference: people in both BB calls and knowledge_facts
  let people = crossSourcePeople;
  if (people.length === 0) {
    logger.logVerbose('Cross-source query returned 0 — trying simpler approach');
    const { rows: simpler } = await pool.query<{
      display_name: string; person_id: number; fact_count: number;
    }>(`
      SELECT s.display_name, s.id AS person_id, COUNT(kf.id)::int AS fact_count
      FROM aurora_social_identities s
      JOIN knowledge_facts kf ON kf.domain = 'biography'
        AND kf.value ILIKE '%' || s.display_name || '%'
      WHERE s.is_person = TRUE
      GROUP BY s.id, s.display_name
      HAVING COUNT(kf.id) >= 2
      ORDER BY COUNT(kf.id) DESC
      LIMIT 5
    `);

    for (const p of simpler) {
      // For each person with multiple knowledge facts, check what other sources mention them
      const { rows: facts } = await pool.query<{ key: string; value: string }>(`
        SELECT key, LEFT(value, 300) AS value
        FROM knowledge_facts
        WHERE domain = 'biography' AND value ILIKE '%' || $1 || '%'
        LIMIT 10
      `, [p.display_name]);

      const { rows: bbCalls } = await pool.query<{ timestamp: string; call_type: string }>(`
        SELECT timestamp::text, call_type FROM bb_phone_calls
        WHERE LOWER(contact_name) = LOWER($1)
        ORDER BY timestamp DESC LIMIT 5
      `, [p.display_name]);

      const { rows: recentMsgs } = await pool.query<{ content: string; sent_at: string }>(`
        SELECT LEFT(content_text, 200) AS content, timestamp::text AS sent_at
        FROM aurora_unified_communication
        WHERE identity_id = $1 AND content_text IS NOT NULL AND LENGTH(content_text) > 10
        ORDER BY timestamp DESC LIMIT 10
      `, [p.person_id]);

      const evidenceParts: string[] = [];
      if (facts.length > 0) {
        evidenceParts.push(`KNOWLEDGE FACTS (${facts.length}):\n${facts.map(f => `- [${f.key}] ${f.value}`).join('\n')}`);
      }
      if (bbCalls.length > 0) {
        evidenceParts.push(`BB CALL LOG (${bbCalls.length} recent):\n${bbCalls.map(c => `- ${c.timestamp} (${c.call_type})`).join('\n')}`);
      }
      if (recentMsgs.length > 0) {
        evidenceParts.push(`RECENT MESSAGES (${recentMsgs.length}):\n${recentMsgs.map(m => `[${m.sent_at}] ${m.content}`).join('\n')}`);
      }

      if (evidenceParts.length < 2) continue;

      const systemPrompt = `You are a biographical intelligence analyst with access to data spanning 2004-2026 about the owner's life. Given cross-referenced data from multiple sources (knowledge graph, phone records, messages), generate biographical interview questions that would help confirm or expand what we know. Return JSON: { "questions": [{ "question": "...", "data_basis": "...", "expected_answer_type": "confirm|expand|correct", "priority": 1-10 }], "narrative_thread": "A 1-2 sentence summary of this person's arc across the data" }`;

      const userMessage = `Person: ${p.display_name}\nData sources: ${evidenceParts.length}\n\n${evidenceParts.join('\n\n')}`;

      try {
        const result = await routeRequest({
          handler: 'biographical-interview',
          taskTier: 'generation',
          systemPrompt,
          userMessage,
          maxTokens: 3000,
        });

        const parsed = parseJsonResponse(result.text) as Record<string, unknown> | null;
        if (parsed) {
          const questions = (parsed.questions ?? []) as Record<string, unknown>[];
          const narrative = (parsed.narrative_thread ?? '') as string;

          for (const q of questions.slice(0, 3)) {
            hypotheses.push({
              inference_type: 'cross_source_question',
              subject_person_id: p.person_id,
              hypothesis: `[Q for the owner about ${p.display_name}] ${q.question}`,
              evidence: [
                { source: 'cross_reference', data_basis: q.data_basis, narrative },
                ...facts.slice(0, 3).map(f => ({ source: 'knowledge_facts', key: f.key, value: f.value })),
              ],
              rules_matched: ['cross_reference_mention'],
            });
          }
        }
      } catch (err) {
        logger.logMinimal(`LLM failed for cross-source analysis of ${p.display_name}:`, (err as Error).message);
      }
    }
  }

  // 2. Find temporal overlaps: people in BB texts from 2010 who also appear in guestbook 2004-2005
  try {
    const { rows: timeSpanners } = await pool.query<{
      name: string; bb_count: number; gb_count: number;
    }>(`
      WITH bb_names AS (
        SELECT LOWER(contact_name) AS name, COUNT(*)::int AS cnt
        FROM bb_phone_calls WHERE contact_name != ''
        GROUP BY LOWER(contact_name)
      ),
      gb_names AS (
        SELECT LOWER(guest_name) AS name, COUNT(*)::int AS cnt
        FROM site_guestbook WHERE guest_name != ''
        GROUP BY LOWER(guest_name)
      )
      SELECT bb.name, bb.cnt AS bb_count, gb.cnt AS gb_count
      FROM bb_names bb
      JOIN gb_names gb ON gb.name LIKE '%' || SPLIT_PART(bb.name, ' ', 1) || '%'
        OR bb.name LIKE '%' || gb.name || '%'
      WHERE bb.cnt >= 2 AND gb.cnt >= 2
      ORDER BY bb.cnt + gb.cnt DESC
      LIMIT 5
    `);

    for (const ts of timeSpanners) {
      hypotheses.push({
        inference_type: 'cross_source_question',
        hypothesis: `[Temporal span] "${ts.name}" appears in both 2004-2005 guestbook (${ts.gb_count} posts) and 2010 BB call logs (${ts.bb_count} calls). How did this relationship evolve over 5+ years?`,
        evidence: [
          { source: 'bb_phone_calls', count: ts.bb_count },
          { source: 'site_guestbook', count: ts.gb_count },
        ],
        rules_matched: ['cross_reference_mention'],
      });
    }
  } catch (err) {
    logger.logVerbose('Time-span query failed (expected if table missing):', (err as Error).message);
  }

  // 3. Look for BB contacts with example-personal.com emails — these are inner circle
  try {
    const { rows: ownerEmails } = await pool.query<{ first_name: string; emails: string[] }>(`
      SELECT first_name, emails FROM bb_contacts
      WHERE EXISTS (SELECT 1 FROM unnest(emails) e WHERE e LIKE '%@example-personal.com%')
    `);

    for (const c of ownerEmails) {
      const email = c.emails.find(e => e.includes('@example-personal.com'));
      hypotheses.push({
        inference_type: 'cross_source_question',
        hypothesis: `[Inner circle] ${c.first_name} had a @example-personal.com email (${email}) — they were given a personal email address on the family site. What was their role in the example-personal.com community?`,
        evidence: [
          { source: 'bb_contacts', email },
          { source: 'site_guestbook', note: 'had a personal page on example-personal.com' },
        ],
        rules_matched: ['email_domain_career'],
      });
    }
  } catch (err) {
    logger.logVerbose('example-personal.com email query failed:', (err as Error).message);
  }

  return hypotheses;
}

// ── Confidence Scoring ──────────────────────────────────────────

async function computeConfidence(
  pool: Pool,
  hypothesis: Hypothesis,
): Promise<number> {
  const evidenceCount = hypothesis.evidence.length;
  const sources = new Set(hypothesis.evidence.map(e => ((e as Record<string, unknown>).source as string)));
  const sourceDiversity = sources.size;

  // Check recency of evidence
  let hasRecentEvidence = false;
  for (const e of hypothesis.evidence) {
    const ev = e as Record<string, unknown>;
    if (ev.samples && Array.isArray(ev.samples)) {
      const dates = (ev.samples as { date: string }[]).map(s => new Date(s.date).getTime());
      const thirtyDaysAgo = Date.now() - 30 * 86400000;
      if (dates.some(d => d > thirtyDaysAgo)) hasRecentEvidence = true;
    }
  }

  // Look up rule boosts
  let ruleBoost = 0;
  if (hypothesis.rules_matched.length > 0) {
    const { rows: rules } = await pool.query<{ confidence_boost: number }>(`
      SELECT confidence_boost FROM bio_inference_rules
      WHERE rule_key = ANY($1) AND is_active = TRUE
    `, [hypothesis.rules_matched]);
    ruleBoost = rules.reduce((sum, r) => sum + r.confidence_boost, 0);
  }

  // Weighted confidence: evidence_count(0.4) + source_diversity(0.3) + recency(0.05) + rule_boost
  const evidenceScore = Math.min(evidenceCount / 5, 1) * 0.4;
  const diversityScore = Math.min(sourceDiversity / 3, 1) * 0.3;
  const recencyScore = hasRecentEvidence ? 0.05 : 0;

  const confidence = Math.min(evidenceScore + diversityScore + recencyScore + ruleBoost, 1.0);
  return Math.round(confidence * 100) / 100;
}

// ── Helpers ─────────────────────────────────────────────────────

function parseJsonResponse(text: string): Record<string, unknown> | unknown[] | null {
  try {
    // Extract JSON from markdown code blocks if present
    const jsonMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/);
    const jsonStr = jsonMatch ? jsonMatch[1].trim() : text.trim();
    return JSON.parse(jsonStr);
  } catch {
    logger.logVerbose('Failed to parse LLM response as JSON, trying to extract');
    // Try to find JSON object or array in the text
    const objMatch = text.match(/(\{[\s\S]*\})/);
    const arrMatch = text.match(/(\[[\s\S]*\])/);
    try {
      if (arrMatch) return JSON.parse(arrMatch[1]);
      if (objMatch) return JSON.parse(objMatch[1]);
    } catch {
      // fall through
    }
    logger.logMinimal('Could not extract JSON from LLM response');
    return null;
  }
}

function getStrategyForBatch(batchId: string): Strategy {
  // Rotate through strategies based on a counter in the batch_id
  const match = batchId.match(/cycle-(\d+)/);
  const cycle = match ? parseInt(match[1]) : 0;
  return STRATEGIES[cycle % STRATEGIES.length];
}

// ── Main Handler ────────────────────────────────────────────────

export async function handleBiographicalInterview(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const payload = job.payload as { cycle?: number; strategy?: number };
  const cycle = payload.cycle ?? payload.strategy ?? 0;
  const batchId = `bio-interview-cycle-${cycle}`;
  const strategy = getStrategyForBatch(batchId);

  logger.log(`Starting biographical interview — strategy: ${strategy}, cycle: ${cycle}`);
  await jobLog(job.id, `Running strategy: ${strategy} (cycle ${cycle})`);

  let hypotheses: Hypothesis[] = [];

  switch (strategy) {
    case 'relationship-discovery':
      hypotheses = await relationshipDiscovery(pool, batchId);
      break;
    case 'life-event-mining':
      hypotheses = await lifeEventMining(pool, batchId);
      break;
    case 'connection-mapping':
      hypotheses = await connectionMapping(pool, batchId);
      break;
    case 'timeline-reconstruction':
      hypotheses = await timelineReconstruction(pool, batchId);
      break;
    case 'cross-source-deep-dive':
      hypotheses = await crossSourceDeepDive(pool, batchId);
      break;
  }

  logger.log(`Strategy ${strategy} produced ${hypotheses.length} hypotheses`);

  // Compute confidence and insert
  let inserted = 0;
  for (const h of hypotheses) {
    const confidence = await computeConfidence(pool, h);

    // Skip very low confidence results
    if (confidence < 0.15) {
      logger.logVerbose(`Skipping low-confidence hypothesis (${confidence}): ${h.hypothesis.slice(0, 80)}`);
      continue;
    }

    // Dedup: skip if similar hypothesis already exists for this person
    const { rows: existing } = await pool.query(
      `SELECT id FROM bio_inference_queue
       WHERE inference_type = $1
         AND subject_person_id IS NOT DISTINCT FROM $2
         AND related_person_id IS NOT DISTINCT FROM $3
         AND status IN ('pending', 'confirmed')
       LIMIT 1`,
      [h.inference_type, h.subject_person_id ?? null, h.related_person_id ?? null],
    );

    if (existing.length > 0) {
      logger.logVerbose(`Skipping duplicate hypothesis for person ${h.subject_person_id}`);
      continue;
    }

    // Find matching rule IDs
    let ruleId: string | null = null;
    if (h.rules_matched.length > 0) {
      const { rows: rules } = await pool.query<{ id: string }>(
        `SELECT id FROM bio_inference_rules WHERE rule_key = $1 LIMIT 1`,
        [h.rules_matched[0]],
      );
      if (rules.length > 0) ruleId = rules[0].id;

      // Increment total_applied for matched rules
      await pool.query(
        `UPDATE bio_inference_rules SET total_applied = total_applied + 1, updated_at = NOW()
         WHERE rule_key = ANY($1)`,
        [h.rules_matched],
      );
    }

    await pool.query(
      `INSERT INTO bio_inference_queue
         (inference_type, subject_person_id, related_person_id, hypothesis, evidence, confidence, batch_id, rule_id, priority)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        h.inference_type,
        h.subject_person_id ?? null,
        h.related_person_id ?? null,
        h.hypothesis,
        JSON.stringify(h.evidence),
        confidence,
        batchId,
        ruleId,
        Math.round(confidence * 100),
      ],
    );
    inserted++;
  }

  logger.log(`Inserted ${inserted} hypotheses into bio_inference_queue`);
  await jobLog(job.id, `${strategy}: ${hypotheses.length} hypotheses, ${inserted} inserted`);

  // Self-chain for next strategy
  try {
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
       VALUES ('biographical-interview', $1, 'nexus', 1, 3, NOW() + INTERVAL '${CHAIN_DELAY_SECONDS} seconds')`,
      [JSON.stringify({ cycle: cycle + 1 })],
    );
    logger.logVerbose(`Self-chained next run (cycle ${cycle + 1}) in ${CHAIN_DELAY_SECONDS / 3600}h`);
  } catch (err) {
    logger.logMinimal('Failed to self-chain biographical-interview:', (err as Error).message);
  }

  return {
    strategy,
    cycle,
    hypotheses_generated: hypotheses.length,
    hypotheses_inserted: inserted,
    batch_id: batchId,
  };
}
