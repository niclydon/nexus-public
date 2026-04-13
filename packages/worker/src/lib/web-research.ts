/**
 * Web research — Brave Search API + URL fetching + LLM synthesis.
 */
import { routeRequest } from './llm/index.js';

const MAX_CONTENT_LENGTH = 30_000;
const MAX_BODY_SIZE = 1_024 * 1_024;
const FETCH_TIMEOUT_MS = 15_000;

export async function fetchPageText(url: string, maxChars?: number): Promise<string> {
  const limit = maxChars ?? MAX_CONTENT_LENGTH;
  let parsed: URL;
  try { parsed = new URL(url); } catch { throw new Error(`Invalid URL: ${url}`); }
  if (!['http:', 'https:'].includes(parsed.protocol)) throw new Error(`Unsupported protocol: ${parsed.protocol}`);

  const response = await fetch(url, {
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    headers: { 'User-Agent': 'ARIA-Assistant/1.0 (Research)', Accept: 'text/html, text/plain, application/json' },
    redirect: 'follow',
  });
  if (!response.ok) throw new Error(`HTTP ${response.status} ${response.statusText}`);

  const body = await response.text();
  const contentType = response.headers.get('content-type') || '';

  let text: string;
  if (contentType.includes('application/json')) {
    try { text = JSON.stringify(JSON.parse(body), null, 2); } catch { text = body; }
  } else if (contentType.includes('text/plain')) {
    text = body;
  } else {
    text = extractTextFromHtml(body);
  }

  return (text.length > limit ? text.slice(0, limit) + '\n\n[Content truncated]' : text).trim() || '[Empty page]';
}

export async function webSearch(query: string, count?: number): Promise<Array<{ title: string; url: string; snippet: string }>> {
  const apiKey = process.env.BRAVE_SEARCH_API_KEY;
  if (!apiKey) return [];

  const params = new URLSearchParams({ q: query, count: String(count ?? 5) });
  const response = await fetch(`https://api.search.brave.com/res/v1/web/search?${params}`, {
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    headers: { Accept: 'application/json', 'X-Subscription-Token': apiKey },
  });
  if (!response.ok) return [];

  const data = await response.json() as { web?: { results?: Array<{ title?: string; url?: string; description?: string }> } };
  return (data.web?.results ?? []).map(r => ({ title: r.title ?? '', url: r.url ?? '', snippet: r.description ?? '' }));
}

export async function researchTopic(params: {
  query: string;
  maxResults?: number;
  handler: string;
}): Promise<{ summary: string; sources: string[]; keyFindings: string[] }> {
  const { query, maxResults = 5, handler } = params;
  const searchResults = await webSearch(query, maxResults);

  if (searchResults.length === 0) {
    return { summary: 'No search results found.', sources: [], keyFindings: [] };
  }

  const pageTexts = await Promise.all(
    searchResults.map(r => fetchPageText(r.url, 5000).catch(() => '')),
  );

  const context = searchResults.map((r, i) =>
    `### Source ${i + 1}: ${r.title}\nURL: ${r.url}\nSnippet: ${r.snippet}\n${pageTexts[i] ? `Content:\n${pageTexts[i]}` : ''}`,
  ).join('\n\n---\n\n');

  const llmResult = await routeRequest({
    handler,
    taskTier: 'generation',
    preferredModel: 'gpt-4o-mini',
    systemPrompt: 'Summarize research into key findings. Return JSON: { "summary": "string", "keyFindings": ["string"] }. Concise, actionable. JSON only.',
    userMessage: `Research query: "${query}"\n\n${context}`,
    maxTokens: 1200,
  });

  const sources = searchResults.map(r => r.url);
  try {
    const cleaned = llmResult.text.replace(/```json\s*/g, '').replace(/```\s*/g, '').trim();
    const parsed = JSON.parse(cleaned) as { summary?: string; keyFindings?: string[] };
    return { summary: parsed.summary ?? llmResult.text, sources, keyFindings: parsed.keyFindings ?? [] };
  } catch {
    return { summary: llmResult.text, sources, keyFindings: [] };
  }
}

function extractTextFromHtml(html: string): string {
  let text = html;
  text = text.replace(/<script[\s\S]*?<\/script>/gi, '');
  text = text.replace(/<style[\s\S]*?<\/style>/gi, '');
  text = text.replace(/<noscript[\s\S]*?<\/noscript>/gi, '');
  text = text.replace(/<!--[\s\S]*?-->/g, '');
  text = text.replace(/<svg[\s\S]*?<\/svg>/gi, '');
  text = text.replace(/<\/?(p|div|br|hr|h[1-6]|li|tr|blockquote|section|article|header|footer|nav|main|aside|pre|table)[^>]*>/gi, '\n');
  text = text.replace(/<[^>]+>/g, '');
  text = text.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&nbsp;/g, ' ');
  text = text.replace(/[ \t]+/g, ' ').replace(/\n\s*\n/g, '\n\n');
  return text.trim();
}
