/**
 * Urgency Triage — reusable LLM-based urgency classification.
 *
 * Quick reasoning pass over incoming data (emails, messages, etc.)
 * to flag anything needing immediate attention. Single batched Forge call,
 * non-blocking on failure.
 */
import { getPool, createLogger } from '@nexus/core';

const logger = createLogger('urgency-triage');

const DEFAULT_THRESHOLD = 4;
const DEFAULT_TIMEOUT_MS = 15_000;

export interface TriageItem {
  from: string;
  subject: string;
  preview: string;
}

export interface TriageConfig {
  /** Category for the proactive insight (e.g. 'email_urgent', 'message_urgent') */
  insightCategory: string;
  /** System prompt override — defaults to generic urgency classifier */
  systemPrompt?: string;
  /** Urgency threshold (1-5) — items at or above this get flagged. Default: 4 */
  threshold?: number;
  /** Confidence score for the proactive insight. Default: 0.9 */
  confidence?: number;
  /** Timeout for the Forge call in ms. Default: 15000 */
  timeoutMs?: number;
}

interface TriageResult {
  index: number;
  urgency: number;
  reason: string;
}

const DEFAULT_SYSTEM_PROMPT = `You are an urgency classifier. For each item, rate urgency 1-5:
1 = Not urgent (newsletters, promotions, FYI, casual chat)
2 = Low (routine correspondence, no deadline)
3 = Medium (needs response within a day)
4 = High (time-sensitive, needs response within hours — travel changes, appointments, deadlines today)
5 = Critical (immediate action needed — security alerts, emergencies, financial issues)

Respond with JSON only: {"results": [{"index": 1, "urgency": 3, "reason": "brief reason"}, ...]}`;

/**
 * Run urgency triage on a batch of items via Forge.
 * Returns the number of items flagged as urgent.
 * Non-blocking — returns 0 on any failure.
 */
export async function triageUrgency(
  items: TriageItem[],
  config: TriageConfig,
): Promise<number> {
  if (items.length === 0) return 0;

  const pool = getPool();
  const forgeUrl = process.env.FORGE_URL ?? 'http://localhost:8642';
  const forgeKey = process.env.FORGE_API_KEY ?? '';
  const threshold = config.threshold ?? DEFAULT_THRESHOLD;
  const timeoutMs = config.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const confidence = config.confidence ?? 0.9;

  const summaries = items.map((item, i) =>
    `[${i + 1}] From: ${item.from}\nSubject: ${item.subject}\nPreview: ${item.preview}`,
  ).join('\n\n');

  const stop = logger.time('urgency-triage');

  try {
    const response = await fetch(`${forgeUrl}/v1/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(forgeKey ? { Authorization: `Bearer ${forgeKey}` } : {}),
      },
      body: JSON.stringify({
        model: 'qwen3-next-chat-80b',
        messages: [
          { role: 'system', content: config.systemPrompt ?? DEFAULT_SYSTEM_PROMPT },
          { role: 'user', content: summaries },
        ],
        max_tokens: 500,
        temperature: 0,
        chat_template_kwargs: { enable_thinking: false },
      }),
      signal: AbortSignal.timeout(timeoutMs),
    });

    stop();

    if (!response.ok) {
      logger.logVerbose('Triage LLM error:', response.status);
      return 0;
    }

    const data = await response.json() as {
      choices: Array<{ message: { content: string } }>;
    };

    let content = data.choices?.[0]?.message?.content?.trim() ?? '';
    if (content.startsWith('```')) {
      const start = content.indexOf('\n') + 1;
      const end = content.lastIndexOf('```');
      content = content.slice(start, end).trim();
    }

    const parsed = JSON.parse(content) as { results: TriageResult[] };
    let urgentCount = 0;

    for (const result of parsed.results ?? []) {
      if (result.urgency >= threshold) {
        const idx = result.index - 1;
        const item = items[idx] ?? items[0];

        await pool.query(
          `INSERT INTO proactive_insights (title, body, category, urgency, confidence, status)
           VALUES ($1, $2, $3, 'urgent', $4, 'pending')`,
          [
            `Urgent: ${item.subject}`,
            `From: ${item.from}\n${result.reason}`,
            config.insightCategory,
            confidence,
          ],
        );

        logger.log(`Urgent flagged [${config.insightCategory}]: ${item.subject} (${result.urgency}/5)`);
        urgentCount++;
      }
    }

    return urgentCount;
  } catch (err) {
    stop();
    logger.logVerbose('Triage failed (non-blocking):', (err as Error).message);
    return 0;
  }
}
