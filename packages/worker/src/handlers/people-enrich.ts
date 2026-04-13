/**
 * People Enrichment Handler
 *
 * Discovers social media profiles for persons in aurora_social_identities
 * using Brave Search (via MCP) and LLM verification.
 *
 * Processes one person per run, then self-chains with 30s delay.
 * Stops when no more unenriched persons with identifiers remain.
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { callMcpTool } from '../lib/mcp-client-manager.js';
import { routeRequest } from '../lib/llm/index.js';
import { jobLog } from '../lib/job-log.js';

const logger = createLogger('people-enrich');

const PLATFORMS = ['instagram', 'linkedin', 'facebook', 'twitter'] as const;
const PLATFORM_DOMAINS: Record<string, string> = {
  instagram: 'instagram.com',
  linkedin: 'linkedin.com',
  facebook: 'facebook.com',
  twitter: 'x.com',
};
const CONFIDENCE_THRESHOLD = 0.6;
const SELF_CHAIN_DELAY_SECONDS = 30;

interface PersonToEnrich {
  id: number;
  display_name: string;
  emails: string[] | null;
  phones: string[] | null;
}

interface LlmVerification {
  matches: boolean;
  confidence: number;
  username: string | null;
  profile_url: string | null;
  reason: string;
}

export async function handlePeopleEnrich(job: TempoJob): Promise<Record<string, unknown>> {
  const pool = getPool();
  logger.log('Starting people enrichment cycle');

  // 1. Find next person who has NEVER been attempted (excludes the owner and any prior attempts)
  const { rows } = await pool.query<PersonToEnrich>(`
    SELECT s.id, s.display_name,
      array_agg(DISTINCT l.identifier) FILTER (WHERE l.identifier_type = 'email') as emails,
      array_agg(DISTINCT l.identifier) FILTER (WHERE l.identifier_type = 'phone') as phones
    FROM aurora_social_identities s
    JOIN aurora_social_identity_links l ON l.identity_id = s.id
    LEFT JOIN person_social_profiles sp ON sp.person_id = s.id
    LEFT JOIN person_enrichment_attempts pea ON pea.person_id = s.id
    WHERE s.is_person = TRUE
      AND sp.id IS NULL
      AND pea.person_id IS NULL
      AND s.display_name NOT ILIKE '%%owner%%'
      AND s.display_name NOT ILIKE '%%owner%%'
    GROUP BY s.id, s.display_name
    HAVING COUNT(l.id) > 0
    ORDER BY (SELECT COUNT(*) FROM aurora_unified_communication WHERE identity_id = s.id) DESC
    LIMIT 1
  `);

  if (rows.length === 0) {
    logger.log('No more persons to enrich — done');
    await jobLog(job.id, 'No unenriched persons remaining');
    return { status: 'complete', reason: 'no_more_persons' };
  }

  const person = rows[0];
  logger.log(`Enriching person: ${person.display_name} (id=${person.id})`);
  await jobLog(job.id, `Enriching: ${person.display_name} (${person.id})`);

  let profilesFound = 0;
  let searchesRun = 0;

  // 2. Search each platform
  for (const platform of PLATFORMS) {
    const domain = PLATFORM_DOMAINS[platform];
    logger.logVerbose(`Searching ${platform} for ${person.display_name}`);

    // Build search queries
    const queries: string[] = [];
    queries.push(`"${person.display_name}" site:${domain}`);
    if (person.emails?.length) {
      queries.push(`"${person.emails[0]}" site:${domain}`);
    }

    for (const searchQuery of queries) {
      searchesRun++;
      let searchResults: string;
      try {
        searchResults = await callMcpTool('brave-search', 'brave_web_search', {
          query: searchQuery,
          count: 3,
        });
      } catch (err) {
        logger.logVerbose(`Brave search failed for "${searchQuery}": ${(err as Error).message}`);
        continue;
      }

      logger.logDebug(`Brave response for "${searchQuery}": ${searchResults.slice(0, 500)}`);

      // Parse search results — Brave MCP returns plain text
      const resultBlocks = parseSearchResults(searchResults);
      if (resultBlocks.length === 0) {
        logger.logVerbose(`No results for "${searchQuery}"`);
        continue;
      }

      // 3. LLM verification for each result
      for (const result of resultBlocks) {
        try {
          const response = await routeRequest({
            handler: 'people-enrich',
            taskTier: 'classification',
            systemPrompt: 'Determine if the search result matches the person. Respond with JSON only, no markdown: {"matches": true/false, "confidence": 0.0-1.0, "username": "...", "profile_url": "...", "reason": "..."}',
            userMessage: `Person: ${person.display_name}, Email: ${person.emails?.[0] ?? 'unknown'}\nSearch result: ${result.title}\n${result.description}\n${result.url}`,
            maxTokens: 200,
          });

          const verification = parseLlmVerification(response.text);
          if (!verification) {
            logger.logVerbose(`Failed to parse LLM verification for ${platform}`);
            continue;
          }

          logger.logVerbose(`${platform} verification: matches=${verification.matches}, confidence=${verification.confidence}, reason=${verification.reason}`);

          // 4. Store matches above threshold
          if (verification.matches && verification.confidence >= CONFIDENCE_THRESHOLD) {
            const profileUrl = verification.profile_url || result.url;
            try {
              await pool.query(
                `INSERT INTO person_social_profiles (person_id, platform, profile_url, username, confidence, verified_by, verified_at, metadata)
                 VALUES ($1, $2, $3, $4, $5, 'llm-brave-search', NOW(), $6)
                 ON CONFLICT (person_id, platform) DO UPDATE SET
                   profile_url = EXCLUDED.profile_url,
                   username = EXCLUDED.username,
                   confidence = EXCLUDED.confidence,
                   verified_by = EXCLUDED.verified_by,
                   verified_at = EXCLUDED.verified_at,
                   metadata = EXCLUDED.metadata`,
                [
                  person.id,
                  platform,
                  profileUrl,
                  verification.username,
                  verification.confidence,
                  JSON.stringify({
                    search_query: searchQuery,
                    result_title: result.title,
                    llm_reason: verification.reason,
                  }),
                ],
              );
              profilesFound++;
              logger.log(`Found ${platform} profile for ${person.display_name}: ${profileUrl}`);
              break; // One profile per platform is enough
            } catch (err) {
              logger.logMinimal(`Failed to store ${platform} profile: ${(err as Error).message}`);
            }
          }
        } catch (err) {
          logger.logVerbose(`LLM verification error for ${platform}: ${(err as Error).message}`);
        }
      }
    }
  }

  await jobLog(job.id, `Done: ${profilesFound} profiles found from ${searchesRun} searches`);
  logger.log(`Enriched ${person.display_name}: ${profilesFound} profiles found`);

  // Always log the attempt — prevents infinite re-processing of same person
  await pool.query(
    `INSERT INTO person_enrichment_attempts (person_id, searches_run, profiles_found)
     VALUES ($1, $2, $3)
     ON CONFLICT (person_id) DO UPDATE SET
       attempted_at = NOW(), searches_run = EXCLUDED.searches_run, profiles_found = EXCLUDED.profiles_found`,
    [person.id, searchesRun, profilesFound],
  );

  // 5. Self-chain with delay
  try {
    await pool.query(
      `INSERT INTO tempo_jobs (job_type, payload, executor, priority, max_attempts, next_run_at)
       VALUES ('people-enrich', '{}', 'nexus', 1, 3, NOW() + INTERVAL '${SELF_CHAIN_DELAY_SECONDS} seconds')`,
    );
    logger.logVerbose('Self-chained people-enrich for next person');
  } catch (err) {
    logger.logMinimal('Failed to self-chain people-enrich:', (err as Error).message);
  }

  return {
    person_id: person.id,
    person_name: person.display_name,
    profiles_found: profilesFound,
    searches_run: searchesRun,
  };
}

interface SearchResult {
  title: string;
  description: string;
  url: string;
}

/**
 * Parse Brave Search MCP plain-text output into structured results.
 * The MCP tool returns text blocks, not JSON.
 */
function parseSearchResults(text: string): SearchResult[] {
  const results: SearchResult[] = [];

  // Try JSON parse first — some MCP versions return JSON
  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) {
      for (const item of parsed) {
        if (item.url) {
          results.push({
            title: item.title || '',
            description: item.description || item.snippet || '',
            url: item.url,
          });
        }
      }
      return results;
    }
  } catch {
    // Not JSON — parse as text
  }

  // Text parsing: look for URLs and surrounding context
  const urlRegex = /https?:\/\/[^\s)>\]]+/g;
  const lines = text.split('\n');
  let currentTitle = '';

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const urlMatch = trimmed.match(urlRegex);
    if (urlMatch) {
      results.push({
        title: currentTitle || trimmed.replace(urlRegex, '').trim(),
        description: trimmed,
        url: urlMatch[0],
      });
      currentTitle = '';
    } else if (trimmed.length > 10) {
      currentTitle = trimmed;
    }
  }

  return results.slice(0, 5); // Cap at 5 results
}

/**
 * Parse the LLM's JSON verification response.
 */
function parseLlmVerification(text: string): LlmVerification | null {
  try {
    // Strip markdown code fences if present
    const cleaned = text.replace(/```json?\s*/g, '').replace(/```/g, '').trim();
    const parsed = JSON.parse(cleaned);
    return {
      matches: Boolean(parsed.matches),
      confidence: typeof parsed.confidence === 'number' ? parsed.confidence : 0,
      username: parsed.username || null,
      profile_url: parsed.profile_url || null,
      reason: parsed.reason || '',
    };
  } catch {
    logger.logDebug(`LLM verification parse failed: ${text.slice(0, 200)}`);
    return null;
  }
}
