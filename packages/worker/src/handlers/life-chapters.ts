/**
 * Life Chapters handler — two-pass architecture.
 *
 * Pass 1: Send compact yearly summary to LLM, get 12-20 chapter boundaries (~30s).
 * Pass 2: For each chapter, gather focused data for that date range, generate
 *         rich narrative (~60-90s each, sequential).
 *
 * Data sources: daily_behavior_profile, aurora_raw_flights, relationship_timeline,
 * knowledge_facts (biography), spotify_streaming_history, lastfm_scrobbles,
 * blogger_posts, communication_style_evolution
 *
 * Model: Llama 3.3 70B via localhost:8080
 * Target: life_chapters table (full rebuild each run)
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { jobLog } from '../lib/job-log.js';
import OpenAI from 'openai';
import type { Pool } from 'pg';

const logger = createLogger('life-chapters');

function getLLM(): OpenAI {
  return new OpenAI({
    apiKey: 'not-needed',
    baseURL: process.env.LLAMA_PRIORITY_URL || 'http://localhost:8088/v1',
    timeout: 300_000,
  });
}

function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4);
}

/** Strip markdown code fences and parse JSON */
function parseJSON<T>(raw: string): T {
  let jsonStr = raw.trim();
  // Strip markdown code fences
  const fenceMatch = jsonStr.match(/```(?:json)?\s*\n?([\s\S]*?)\n?\s*```/);
  if (fenceMatch) {
    jsonStr = fenceMatch[1].trim();
  }
  // Try direct parse first
  try { return JSON.parse(jsonStr); } catch { /* fall through */ }
  // Try extracting first JSON object or array from the text
  const objMatch = jsonStr.match(/(\{[\s\S]*\})\s*$/);
  if (objMatch) { try { return JSON.parse(objMatch[1]); } catch { /* fall through */ } }
  const arrMatch = jsonStr.match(/(\[[\s\S]*\])\s*$/);
  if (arrMatch) { try { return JSON.parse(arrMatch[1]); } catch { /* fall through */ } }
  throw new Error(`Could not parse JSON from response: ${jsonStr.slice(0, 100)}`);
}

// ---------- Types ----------

interface ChapterOutline {
  title: string;
  subtitle: string;
  start_date: string;
  end_date: string | null;
  defining_themes: string[];
}

interface ChapterDetail {
  narrative: string;
  key_people: string[];
  key_locations: string[];
  key_events: string[];
  soundtrack: string[];
  data_sources: string[];
}

interface BehavioralSignature {
  avg_messages: number | null;
  avg_contacts: number | null;
  avg_plays: number | null;
  avg_photos: number | null;
  total_flights: number | null;
  avg_steps: number | null;
}

// ---------- Pass 1 Prompts ----------

const DISCOVERY_SYSTEM = `You are a biographical analyst. Given 24 years of behavioral data for one person, identify 12-20 natural life chapters. Each chapter should be a meaningful era lasting months to years, defined by where they lived, what they did, who they were with, and major events.

Return ONLY a JSON array. Each element:
{"title": "2-5 word name", "subtitle": "one-line tagline", "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD or null if ongoing", "defining_themes": ["theme1", "theme2"]}

Rules:
- Chapters must not overlap and must cover the full timeline
- Earlier chapters can be longer (less data), recent chapters can be shorter
- Focus on real inflection points: moves, career changes, relationship shifts, travel patterns
- Return valid JSON only — no markdown, no explanation`;

// ---------- Pass 2 Prompts ----------

const NARRATIVE_SYSTEM = `You are writing one chapter of a person's life story. Given detailed data about a specific period, write a vivid, evocative narrative in third person. This is someone's life story, not a data report.

Format your response with these sections:

NARRATIVE:
(2-4 paragraphs telling the story of this era)

KEY PEOPLE:
(comma-separated list of important people)

KEY LOCATIONS:
(comma-separated list of cities/countries)

KEY EVENTS:
(comma-separated list of major events)

SOUNDTRACK:
(comma-separated list of top artists/songs)`;

// ---------- Overview Data (Pass 1) ----------

async function gatherOverview(pool: Pool): Promise<string> {
  const sections: string[] = [];

  // Yearly behavioral averages
  const { rows: behavior } = await pool.query<{
    year: number; days: number; avg_msgs: number; avg_contacts: number;
    avg_plays: number; avg_photos: number; flights: number; transactions: number;
  }>(`
    SELECT EXTRACT(YEAR FROM date)::int AS year, COUNT(*)::int AS days,
      ROUND(AVG(total_messages))::int AS avg_msgs,
      ROUND(AVG(unique_contacts))::int AS avg_contacts,
      ROUND(AVG(spotify_plays))::int AS avg_plays,
      ROUND(AVG(photos_taken))::int AS avg_photos,
      SUM(flights)::int AS flights,
      SUM(transactions)::int AS transactions
    FROM daily_behavior_profile
    GROUP BY EXTRACT(YEAR FROM date)
    ORDER BY year
  `);
  if (behavior.length > 0) {
    const lines = behavior.map(r =>
      `${r.year}: ${r.days}d, ${r.avg_msgs} msgs/d, ${r.avg_contacts} contacts/d, ` +
      `${r.avg_plays} plays/d, ${r.avg_photos} photos/d, ${r.flights} flights, ${r.transactions} txns`
    );
    sections.push(`## Yearly Behavior\n${lines.join('\n')}`);
  }

  // Travel by year
  const { rows: travel } = await pool.query<{
    year: number; flights: number; destinations: string[];
  }>(`
    SELECT EXTRACT(YEAR FROM flight_date)::int AS year, COUNT(*)::int AS flights,
      array_agg(DISTINCT destination) AS destinations
    FROM aurora_raw_flights
    GROUP BY EXTRACT(YEAR FROM flight_date)
    ORDER BY year
  `);
  if (travel.length > 0) {
    const lines = travel.map(r =>
      `${r.year}: ${r.flights} flights → ${(r.destinations || []).join(', ')}`
    );
    sections.push(`## Travel\n${lines.join('\n')}`);
  }

  // Top contacts by year (top 3 for compactness)
  const { rows: contacts } = await pool.query<{
    year: number; contact_name: string; interactions: number;
  }>(`
    SELECT * FROM (
      SELECT EXTRACT(YEAR FROM rt.month)::int AS year, si.display_name AS contact_name,
        SUM(rt.total_messages)::int AS interactions,
        ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM rt.month) ORDER BY SUM(rt.total_messages) DESC) AS rn
      FROM relationship_timeline rt
      JOIN aurora_social_identities si ON si.id = rt.person_id
      GROUP BY EXTRACT(YEAR FROM rt.month), si.display_name
    ) sub WHERE rn <= 3 AND interactions > 10
    ORDER BY year, rn
  `);
  if (contacts.length > 0) {
    const byYear = new Map<number, string[]>();
    for (const r of contacts) {
      if (!byYear.has(r.year)) byYear.set(r.year, []);
      byYear.get(r.year)!.push(`${r.contact_name}(${r.interactions})`);
    }
    const lines = Array.from(byYear.entries()).map(([y, c]) => `${y}: ${c.join(', ')}`);
    sections.push(`## Top Contacts\n${lines.join('\n')}`);
  }

  // Top Spotify artists by year (top 2 for compactness)
  const { rows: spotify } = await pool.query<{
    year: number; artist_name: string; plays: number;
  }>(`
    SELECT * FROM (
      SELECT EXTRACT(YEAR FROM played_at)::int AS year, artist_name, COUNT(*)::int AS plays,
        ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM played_at) ORDER BY COUNT(*) DESC) AS rn
      FROM spotify_streaming_history
      WHERE ms_played > 30000
      GROUP BY EXTRACT(YEAR FROM played_at), artist_name
    ) sub WHERE rn <= 2
    ORDER BY year, rn
  `);
  if (spotify.length > 0) {
    const byYear = new Map<number, string[]>();
    for (const r of spotify) {
      if (!byYear.has(r.year)) byYear.set(r.year, []);
      byYear.get(r.year)!.push(`${r.artist_name}(${r.plays})`);
    }
    const lines = Array.from(byYear.entries()).map(([y, a]) => `${y}: ${a.join(', ')}`);
    sections.push(`## Top Spotify\n${lines.join('\n')}`);
  }

  // Top Last.fm artists by year (top 2)
  const { rows: lastfm } = await pool.query<{
    year: number; artist_name: string; plays: number;
  }>(`
    SELECT * FROM (
      SELECT EXTRACT(YEAR FROM scrobbled_at)::int AS year, artist_name, COUNT(*)::int AS plays,
        ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM scrobbled_at) ORDER BY COUNT(*) DESC) AS rn
      FROM lastfm_scrobbles
      GROUP BY EXTRACT(YEAR FROM scrobbled_at), artist_name
    ) sub WHERE rn <= 2
    ORDER BY year, rn
  `);
  if (lastfm.length > 0) {
    const byYear = new Map<number, string[]>();
    for (const r of lastfm) {
      if (!byYear.has(r.year)) byYear.set(r.year, []);
      byYear.get(r.year)!.push(`${r.artist_name}(${r.plays})`);
    }
    const lines = Array.from(byYear.entries()).map(([y, a]) => `${y}: ${a.join(', ')}`);
    sections.push(`## Top Last.fm\n${lines.join('\n')}`);
  }

  // Biographical facts (compact)
  const { rows: facts } = await pool.query<{
    category: string; key: string; value_preview: string;
  }>(`
    SELECT category, key, LEFT(value, 150) AS value_preview
    FROM knowledge_facts
    WHERE domain = 'biography' AND superseded_by IS NULL
      AND category IN ('career', 'events', 'family', 'personal', 'relationships')
    ORDER BY category, key
  `);
  if (facts.length > 0) {
    const lines = facts.map(r => `[${r.category}] ${r.key}: ${r.value_preview}`);
    sections.push(`## Bio Facts\n${lines.join('\n')}`);
  }

  // Blogging activity
  const { rows: blog } = await pool.query<{ year: number; posts: number }>(`
    SELECT EXTRACT(YEAR FROM published_at)::int AS year, COUNT(*)::int AS posts
    FROM blogger_posts
    GROUP BY EXTRACT(YEAR FROM published_at)
    ORDER BY year
  `);
  if (blog.length > 0) {
    const lines = blog.map(r => `${r.year}: ${r.posts} posts`);
    sections.push(`## Blog Posts\n${lines.join('\n')}`);
  }

  // Communication style (compact)
  const { rows: style } = await pool.query<{
    period: string; source: string; avg_words: number;
  }>(`
    SELECT period, source, ROUND(avg_words_per_message)::int AS avg_words
    FROM communication_style_evolution
    WHERE source IN ('imessage', 'gmail', 'blogger')
    ORDER BY period_start
  `);
  if (style.length > 0) {
    const lines = style.map(r => `${r.period} (${r.source}): ${r.avg_words} words/msg`);
    sections.push(`## Comm Style\n${lines.join('\n')}`);
  }

  return sections.join('\n\n');
}

// ---------- Per-Chapter Data (Pass 2) ----------

async function gatherChapterData(pool: Pool, startDate: string, endDate: string | null): Promise<string> {
  const end = endDate || '2099-12-31';
  const sections: string[] = [];

  // Yearly behavioral averages within range
  const { rows: behavior } = await pool.query<{
    year: number; avg_msgs: number; avg_contacts: number; avg_plays: number;
    avg_photos: number; flights: number; avg_steps: number;
  }>(`
    SELECT EXTRACT(YEAR FROM date)::int AS year,
      ROUND(AVG(total_messages))::int AS avg_msgs,
      ROUND(AVG(unique_contacts))::int AS avg_contacts,
      ROUND(AVG(spotify_plays))::int AS avg_plays,
      ROUND(AVG(photos_taken))::int AS avg_photos,
      SUM(flights)::int AS flights,
      ROUND(AVG(steps))::int AS avg_steps
    FROM daily_behavior_profile
    WHERE date >= $1::date AND date < $2::date
    GROUP BY EXTRACT(YEAR FROM date)
    ORDER BY year
  `, [startDate, end]);
  if (behavior.length > 0) {
    const lines = behavior.map(r =>
      `${r.year}: ${r.avg_msgs} msgs/d, ${r.avg_contacts} contacts/d, ${r.avg_plays} plays/d, ` +
      `${r.avg_photos} photos/d, ${r.flights} flights, ${r.avg_steps} steps/d`
    );
    sections.push(`## Yearly Averages\n${lines.join('\n')}`);
  }

  // Specific flights
  const { rows: flights } = await pool.query<{
    flight_date: string; airline: string; origin: string; destination: string;
  }>(`
    SELECT flight_date::text, airline, origin, destination
    FROM aurora_raw_flights
    WHERE flight_date >= $1::date AND flight_date < $2::date
    ORDER BY flight_date
  `, [startDate, end]);
  if (flights.length > 0) {
    const lines = flights.map(f => `${f.flight_date}: ${f.airline} ${f.origin}→${f.destination}`);
    sections.push(`## Flights\n${lines.join('\n')}`);
  }

  // Top 5 contacts
  const { rows: contacts } = await pool.query<{
    contact_name: string; interactions: number;
  }>(`
    SELECT si.display_name AS contact_name, SUM(rt.total_messages)::int AS interactions
    FROM relationship_timeline rt
    JOIN aurora_social_identities si ON si.id = rt.person_id
    WHERE rt.month >= $1::timestamptz AND rt.month < $2::timestamptz
    GROUP BY si.display_name
    ORDER BY interactions DESC
    LIMIT 5
  `, [startDate, end]);
  if (contacts.length > 0) {
    const lines = contacts.map(c => `${c.contact_name}: ${c.interactions} messages`);
    sections.push(`## Top Contacts\n${lines.join('\n')}`);
  }

  // Top 5 Spotify artists
  const { rows: spotify } = await pool.query<{
    artist_name: string; plays: number;
  }>(`
    SELECT artist_name, COUNT(*)::int AS plays
    FROM spotify_streaming_history
    WHERE played_at >= $1::timestamptz AND played_at < $2::timestamptz
      AND ms_played > 30000
    GROUP BY artist_name
    ORDER BY plays DESC
    LIMIT 5
  `, [startDate, end]);
  if (spotify.length > 0) {
    const lines = spotify.map(s => `${s.artist_name}: ${s.plays} plays`);
    sections.push(`## Top Spotify Artists\n${lines.join('\n')}`);
  }

  // Top 5 Last.fm artists
  const { rows: lastfm } = await pool.query<{
    artist_name: string; plays: number;
  }>(`
    SELECT artist_name, COUNT(*)::int AS plays
    FROM lastfm_scrobbles
    WHERE scrobbled_at >= $1::timestamptz AND scrobbled_at < $2::timestamptz
    GROUP BY artist_name
    ORDER BY plays DESC
    LIMIT 5
  `, [startDate, end]);
  if (lastfm.length > 0) {
    const lines = lastfm.map(s => `${s.artist_name}: ${s.plays} plays`);
    sections.push(`## Top Last.fm Artists\n${lines.join('\n')}`);
  }

  // Biographical facts
  const { rows: facts } = await pool.query<{
    category: string; key: string; value_preview: string;
  }>(`
    SELECT category, key, LEFT(value, 200) AS value_preview
    FROM knowledge_facts
    WHERE domain = 'biography' AND superseded_by IS NULL
      AND category IN ('career', 'events', 'family', 'personal', 'relationships')
    ORDER BY category, key
  `);
  if (facts.length > 0) {
    const lines = facts.map(r => `[${r.category}] ${r.key}: ${r.value_preview}`);
    sections.push(`## Bio Facts\n${lines.join('\n')}`);
  }

  // Blog post titles
  const { rows: blog } = await pool.query<{
    published_at: string; title: string;
  }>(`
    SELECT published_at::text, title
    FROM blogger_posts
    WHERE published_at >= $1::timestamptz AND published_at < $2::timestamptz
    ORDER BY published_at
  `, [startDate, end]);
  if (blog.length > 0) {
    if (blog.length <= 20) {
      const lines = blog.map(b => `${b.published_at}: ${b.title}`);
      sections.push(`## Blog Posts\n${lines.join('\n')}`);
    } else {
      // Too many — summarize
      const sample = blog.slice(0, 10).map(b => `${b.published_at}: ${b.title}`);
      sections.push(`## Blog Posts (${blog.length} total, showing first 10)\n${sample.join('\n')}`);
    }
  }

  // Communication style
  const { rows: style } = await pool.query<{
    period: string; source: string; avg_words: number; emoji_pct: number; contraction_pct: number;
  }>(`
    SELECT period, source, ROUND(avg_words_per_message)::int AS avg_words,
      ROUND(emoji_pct::numeric, 1)::float AS emoji_pct,
      ROUND(contraction_pct::numeric, 1)::float AS contraction_pct
    FROM communication_style_evolution
    WHERE source IN ('imessage', 'gmail', 'blogger')
      AND period_start >= $1::timestamptz AND period_start < $2::timestamptz
    ORDER BY period_start
  `, [startDate, end]);
  if (style.length > 0) {
    const lines = style.map(r =>
      `${r.period} (${r.source}): ${r.avg_words} words/msg, ${r.emoji_pct}% emoji, ${r.contraction_pct}% contractions`
    );
    sections.push(`## Communication Style\n${lines.join('\n')}`);
  }

  return sections.join('\n\n');
}

// ---------- Behavioral Signature ----------

async function computeBehavioralSignature(pool: Pool, startDate: string, endDate: string | null): Promise<BehavioralSignature> {
  const end = endDate || '2099-12-31';
  const { rows } = await pool.query<{
    avg_messages: string | null; avg_contacts: string | null; avg_plays: string | null;
    avg_photos: string | null; total_flights: string | null; avg_steps: string | null;
  }>(`
    SELECT
      ROUND(AVG(total_messages)) AS avg_messages,
      ROUND(AVG(unique_contacts)) AS avg_contacts,
      ROUND(AVG(spotify_plays)) AS avg_plays,
      ROUND(AVG(photos_taken)) AS avg_photos,
      SUM(flights) AS total_flights,
      ROUND(AVG(steps)) AS avg_steps
    FROM daily_behavior_profile
    WHERE date >= $1::date AND date < $2::date
  `, [startDate, end]);

  const r = rows[0];
  return {
    avg_messages: r?.avg_messages ? Number(r.avg_messages) : null,
    avg_contacts: r?.avg_contacts ? Number(r.avg_contacts) : null,
    avg_plays: r?.avg_plays ? Number(r.avg_plays) : null,
    avg_photos: r?.avg_photos ? Number(r.avg_photos) : null,
    total_flights: r?.total_flights ? Number(r.total_flights) : null,
    avg_steps: r?.avg_steps ? Number(r.avg_steps) : null,
  };
}

// ---------- LLM Calls ----------

async function discoverChapters(llm: OpenAI, overview: string): Promise<ChapterOutline[]> {
  logger.logVerbose('Pass 1: discovering chapter boundaries');
  const stopTimer = logger.time('pass1-discover');

  const completion = await llm.chat.completions.create({
    model: 'llama-3.3-70b',
    messages: [
      { role: 'system', content: DISCOVERY_SYSTEM },
      { role: 'user', content: overview },
    ],
    temperature: 0.7,
    max_tokens: 4096,
    // @ts-expect-error — Forge-specific parameter to disable Qwen3 thinking
    chat_template_kwargs: { enable_thinking: false },
  });

  const raw = completion.choices[0]?.message?.content ?? '';
  stopTimer();
  logger.log(`Pass 1 response: ${raw.length} chars (~${estimateTokens(raw)} tokens)`);
  logger.logDebug(`Pass 1 raw (first 500): ${raw.slice(0, 500)}`);

  const outlines = parseJSON<ChapterOutline[]>(raw);
  if (!Array.isArray(outlines) || outlines.length === 0) {
    throw new Error('Pass 1 returned empty or non-array result');
  }

  // Validate each outline has required fields
  const valid = outlines.filter(o => o.title && o.start_date && Array.isArray(o.defining_themes));
  if (valid.length === 0) {
    throw new Error('Pass 1 returned no valid chapter outlines');
  }

  logger.log(`Pass 1: ${valid.length} chapter outlines identified`);
  return valid;
}

async function generateNarrative(llm: OpenAI, outline: ChapterOutline, chapterData: string): Promise<ChapterDetail> {
  logger.logVerbose(`Pass 2: generating narrative for "${outline.title}"`);
  const stopTimer = logger.time(`pass2-${outline.title}`);

  const userMessage = `# Chapter: ${outline.title}\n` +
    `Period: ${outline.start_date} to ${outline.end_date || 'present'}\n` +
    `Themes: ${outline.defining_themes.join(', ')}\n` +
    `Subtitle: ${outline.subtitle}\n\n` +
    `## Data for this period:\n\n${chapterData}`;

  const completion = await llm.chat.completions.create({
    model: 'llama-3.3-70b',
    messages: [
      { role: 'system', content: NARRATIVE_SYSTEM },
      { role: 'user', content: userMessage },
    ],
    temperature: 0.7,
    max_tokens: 2048,
    // @ts-expect-error — Forge-specific parameter to disable Qwen3 thinking
    chat_template_kwargs: { enable_thinking: false },
  });

  const raw = completion.choices[0]?.message?.content ?? '';
  stopTimer();
  logger.logVerbose(`Pass 2 "${outline.title}": ${raw.length} chars`);
  logger.logDebug(`Pass 2 raw (first 300): ${raw.slice(0, 300)}`);

  // Parse structured text sections
  const extractSection = (label: string): string => {
    const pattern = new RegExp(`${label}:\\s*\\n([\\s\\S]*?)(?=\\n(?:KEY PEOPLE|KEY LOCATIONS|KEY EVENTS|SOUNDTRACK|DATA SOURCES):|$)`, 'i');
    const match = raw.match(pattern);
    return match?.[1]?.trim() || '';
  };

  const splitList = (text: string): string[] =>
    text.split(/[,\n]/).map(s => s.replace(/^[-•*]\s*/, '').trim()).filter(Boolean);

  // If the LLM returned JSON anyway, try that first
  try {
    const detail = parseJSON<ChapterDetail>(raw);
    if (detail.narrative) {
      return {
        narrative: detail.narrative,
        key_people: detail.key_people || [],
        key_locations: detail.key_locations || [],
        key_events: detail.key_events || [],
        soundtrack: detail.soundtrack || [],
        data_sources: detail.data_sources || [],
      };
    }
  } catch { /* not JSON, parse as text */ }

  const narrative = extractSection('NARRATIVE');
  return {
    narrative: narrative || raw.slice(0, 2000), // fallback: use entire response as narrative
    key_people: splitList(extractSection('KEY PEOPLE')),
    key_locations: splitList(extractSection('KEY LOCATIONS')),
    key_events: splitList(extractSection('KEY EVENTS')),
    soundtrack: splitList(extractSection('SOUNDTRACK')),
    data_sources: [],
  };
}

// ---------- Main Handler ----------

export async function handleLifeChapters(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  const llm = getLLM();

  logger.log(`Starting life chapters synthesis (two-pass), job ${job.id}`);
  await jobLog(job.id, 'Phase 1: Gathering overview data...');

  // Phase 1: Gather compact overview
  const stopGather = logger.time('data-gathering');
  const overview = await gatherOverview(pool);
  stopGather();

  if (!overview) {
    logger.log('No behavioral data found — skipping chapter synthesis');
    await jobLog(job.id, 'No data available for chapter synthesis', 'warn');
    return { status: 'skipped', reason: 'no_data' };
  }

  const overviewTokens = estimateTokens(overview);
  logger.log(`Overview assembled: ~${overviewTokens} tokens`);
  await jobLog(job.id, `Overview: ~${overviewTokens} tokens. Discovering chapters...`);

  // Phase 2: Discover chapter boundaries (Pass 1)
  let outlines: ChapterOutline[];
  try {
    outlines = await discoverChapters(llm, overview);
    await jobLog(job.id, `Discovered ${outlines.length} chapters`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.logMinimal(`Pass 1 failed: ${msg}`);
    await jobLog(job.id, `Pass 1 (discovery) failed: ${msg}`, 'error');
    throw err;
  }

  // Phase 3: Generate per-chapter narratives (Pass 2)
  await jobLog(job.id, `Generating narratives for ${outlines.length} chapters sequentially...`);

  // Upsert: delete only chapters we're about to regenerate, preserve others
  // Never TRUNCATE — completed chapters from prior runs must be preserved

  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < outlines.length; i++) {
    const outline = outlines[i];
    const label = `${i + 1}/${outlines.length}: "${outline.title}"`;
    await jobLog(job.id, `Chapter ${label} — gathering data...`);

    try {
      // Gather focused data for this chapter's date range
      const stopData = logger.time(`data-${outline.title}`);
      const chapterData = await gatherChapterData(pool, outline.start_date, outline.end_date);
      stopData();

      const dataTokens = estimateTokens(chapterData);
      logger.logVerbose(`Chapter ${label}: ~${dataTokens} tokens of focused data`);

      // Generate narrative
      await jobLog(job.id, `Chapter ${label} — calling LLM (~${dataTokens} token input)...`);
      const detail = await generateNarrative(llm, outline, chapterData);

      // Compute behavioral signature
      const sig = await computeBehavioralSignature(pool, outline.start_date, outline.end_date);

      // Upsert into database — never lose existing chapters
      await pool.query(`
        INSERT INTO life_chapters (
          title, subtitle, start_date, end_date, narrative,
          defining_themes, key_people, key_locations, key_events,
          soundtrack, data_sources, behavioral_signature, chapter_order
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (chapter_order) DO UPDATE SET
          title = EXCLUDED.title, subtitle = EXCLUDED.subtitle,
          start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date,
          narrative = EXCLUDED.narrative, defining_themes = EXCLUDED.defining_themes,
          key_people = EXCLUDED.key_people, key_locations = EXCLUDED.key_locations,
          key_events = EXCLUDED.key_events, soundtrack = EXCLUDED.soundtrack,
          data_sources = EXCLUDED.data_sources, behavioral_signature = EXCLUDED.behavioral_signature,
          updated_at = now()
      `, [
        outline.title,
        outline.subtitle || null,
        outline.start_date,
        outline.end_date || null,
        detail.narrative,
        outline.defining_themes,
        detail.key_people,
        detail.key_locations,
        detail.key_events,
        detail.soundtrack,
        detail.data_sources,
        JSON.stringify(sig),
        i + 1,
      ]);

      successCount++;
      logger.log(`Chapter ${label} saved`);
      await jobLog(job.id, `Chapter ${label} — done`);
    } catch (err) {
      failCount++;
      const msg = err instanceof Error ? err.message : String(err);
      logger.logMinimal(`Chapter ${label} failed: ${msg}`);
      await jobLog(job.id, `Chapter ${label} FAILED: ${msg}`, 'error');
      // Continue to next chapter — don't let one failure kill the whole job
    }
  }

  const summary = `Synthesized ${successCount} chapters (${failCount} failed) spanning ` +
    `${outlines[0].start_date} to ${outlines[outlines.length - 1].end_date || 'present'}`;
  logger.log(summary);
  await jobLog(job.id, summary);

  if (successCount === 0) {
    throw new Error('All chapter narratives failed — no chapters written');
  }

  return {
    status: 'complete',
    chapters_count: successCount,
    chapters_failed: failCount,
    earliest: outlines[0].start_date,
    latest: outlines[outlines.length - 1].end_date || 'ongoing',
  };
}
