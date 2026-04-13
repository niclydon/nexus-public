import { query } from '../db.js';
import { createLogger } from '../logger.js';

const logger = createLogger('eval-scorer');

/**
 * Eval scoring service for agent response quality assessment.
 *
 * Provides LLM-based scorers that evaluate agent cycle outputs against
 * quality dimensions: task completion, faithfulness, relevancy, hallucination,
 * and coherence.
 *
 * Scores are stored in `agent_evals` and consumed by the Analyst agent
 * for pattern detection (e.g., "agent X has low faithfulness on tool-heavy cycles").
 *
 * Scoring prompts are designed for local Forge inference (qwen3.5-35b).
 * Each scorer returns a 0.0-1.0 score with an explanation.
 */

export interface EvalScore {
  scorer: string;
  score: number;
  explanation: string;
}

export interface EvalInput {
  agentId: string;
  traceId: string;
  input: string;       // The state/prompt the agent received
  output: string;      // The agent's response (summary + actions)
  actions: string[];   // Tool calls made
  results: string[];   // Tool results received
}

// ── Scoring Prompts ────────────────────────────────────────────────────────

const SCORER_PROMPTS: Record<string, string> = {

  task_completion: `You are evaluating whether an AI agent completed its assigned task.

Given the agent's input context and output, score how well the agent addressed what was needed.

Score 0.0-1.0:
- 1.0: Task fully completed, all relevant issues addressed
- 0.7-0.9: Task mostly completed, minor aspects missed
- 0.4-0.6: Task partially completed, significant gaps
- 0.1-0.3: Task barely addressed
- 0.0: Task not attempted or completely wrong

Respond with ONLY valid JSON: {"score": 0.0, "explanation": "brief reason"}`,

  faithfulness: `You are evaluating whether an AI agent's response is faithful to the data it received from tool calls.

Given the agent's tool results and its final output, score whether the output accurately reflects what the tools returned. Penalize any claims not supported by tool results.

Score 0.0-1.0:
- 1.0: Every claim in the output is directly supported by tool results
- 0.7-0.9: Mostly faithful, minor extrapolations but reasonable
- 0.4-0.6: Some claims not supported by evidence
- 0.1-0.3: Significant fabrication or misrepresentation
- 0.0: Output contradicts tool results

Respond with ONLY valid JSON: {"score": 0.0, "explanation": "brief reason"}`,

  relevancy: `You are evaluating whether an AI agent's actions and output are relevant to its mission.

Given the agent's role description (in its input context) and its output, score whether the agent focused on what matters for its domain and avoided unnecessary work.

Score 0.0-1.0:
- 1.0: Every action and statement is directly relevant to the agent's mission
- 0.7-0.9: Mostly relevant, minor tangents
- 0.4-0.6: Mixed relevance, some wasted effort
- 0.1-0.3: Mostly irrelevant to mission
- 0.0: Completely off-topic

Respond with ONLY valid JSON: {"score": 0.0, "explanation": "brief reason"}`,

  hallucination: `You are checking whether an AI agent made claims without evidence.

Given the agent's tool results and its final output, identify any statements that are not grounded in the data the agent actually received. This includes fabricated numbers, invented status reports, and assumed states.

Score 0.0-1.0 (INVERTED — higher = less hallucination):
- 1.0: No hallucination detected, everything grounded
- 0.7-0.9: Minor unsupported details but core claims are grounded
- 0.4-0.6: Some hallucinated content mixed with grounded content
- 0.1-0.3: Mostly hallucinated
- 0.0: Entirely fabricated

Respond with ONLY valid JSON: {"score": 0.0, "explanation": "brief reason"}`,

  coherence: `You are evaluating whether an AI agent's response is internally coherent.

Check: Does the summary match the actions taken? Do the actions make sense given the input? Is the classification (ok/warning/critical) consistent with the evidence?

Score 0.0-1.0:
- 1.0: Fully coherent — summary, actions, status, and evidence all align
- 0.7-0.9: Mostly coherent, minor inconsistencies
- 0.4-0.6: Notable contradictions between summary and actions
- 0.1-0.3: Significant internal contradictions
- 0.0: Incoherent — summary contradicts actions or status

Respond with ONLY valid JSON: {"score": 0.0, "explanation": "brief reason"}`,
};

// ── Scoring Functions ──────────────────────────────────────────────────────

/**
 * Score a single agent cycle against one scorer.
 * Calls the LLM (via Forge) with the scoring prompt and the cycle data.
 * Returns the parsed score or null on failure.
 */
export async function scoreDecision(
  scorer: keyof typeof SCORER_PROMPTS,
  input: EvalInput,
  forgeUrl?: string,
): Promise<EvalScore | null> {
  const prompt = SCORER_PROMPTS[scorer];
  if (!prompt) {
    logger.logMinimal('Unknown scorer:', scorer);
    return null;
  }

  const userMessage = buildScoringInput(scorer, input);
  const url = forgeUrl ?? process.env.FORGE_URL ?? 'http://localhost:8088/v1';

  try {
    const resp = await fetch(`${url}/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'qwen3.5-35b-a3b',
        messages: [
          { role: 'system', content: prompt },
          { role: 'user', content: userMessage },
        ],
        temperature: 0.1,
        max_tokens: 200,
      }),
      signal: AbortSignal.timeout(30_000),
    });

    if (!resp.ok) {
      logger.logMinimal('Scoring LLM call failed:', resp.status, resp.statusText);
      return null;
    }

    const data = await resp.json() as { choices?: Array<{ message?: { content?: string } }> };
    const content = data.choices?.[0]?.message?.content ?? '';

    // Parse JSON from response (handle markdown code blocks)
    const jsonMatch = content.match(/\{[\s\S]*?"score"[\s\S]*?\}/);
    if (!jsonMatch) {
      logger.logVerbose('Could not parse scoring JSON from:', content.slice(0, 200));
      return null;
    }

    const parsed = JSON.parse(jsonMatch[0]) as { score: number; explanation: string };
    return {
      scorer,
      score: Math.max(0, Math.min(1, parsed.score)),
      explanation: parsed.explanation ?? '',
    };
  } catch (err) {
    logger.logMinimal('Scoring error for', scorer, ':', (err as Error).message);
    return null;
  }
}

/**
 * Score a decision against multiple scorers and persist results.
 */
export async function scoreAndPersist(
  scorers: Array<keyof typeof SCORER_PROMPTS>,
  input: EvalInput,
  forgeUrl?: string,
): Promise<EvalScore[]> {
  const results: EvalScore[] = [];

  for (const scorer of scorers) {
    const score = await scoreDecision(scorer, input, forgeUrl);
    if (score) {
      results.push(score);
      await persistScore(input.agentId, input.traceId, score, input);
    }
  }

  return results;
}

/**
 * Get recent eval scores for an agent, optionally filtered by scorer.
 */
export async function getRecentScores(
  agentId: string,
  opts: { scorer?: string; limit?: number; minAge?: string } = {},
): Promise<Array<EvalScore & { trace_id: string; created_at: Date }>> {
  const { scorer, limit = 50, minAge = '7 days' } = opts;

  const conditions = ['agent_id = $1', `created_at >= NOW() - INTERVAL '${minAge}'`];
  const params: unknown[] = [agentId];

  if (scorer) {
    params.push(scorer);
    conditions.push(`scorer = $${params.length}`);
  }

  const result = await query<{ scorer: string; score: number; explanation: string; trace_id: string; created_at: Date }>(
    `SELECT scorer, score, explanation, trace_id, created_at
     FROM agent_evals
     WHERE ${conditions.join(' AND ')}
     ORDER BY created_at DESC
     LIMIT $${params.length + 1}`,
    [...params, limit],
  );

  return result.rows;
}

/**
 * Get aggregate scores for an agent by scorer type.
 */
export async function getScoreSummary(
  agentId: string,
  period = '7 days',
): Promise<Array<{ scorer: string; avg_score: number; min_score: number; count: number }>> {
  const result = await query<{ scorer: string; avg_score: number; min_score: number; count: number }>(
    `SELECT scorer, ROUND(AVG(score)::numeric, 3) as avg_score,
            ROUND(MIN(score)::numeric, 3) as min_score, COUNT(*) as count
     FROM agent_evals
     WHERE agent_id = $1 AND created_at >= NOW() - INTERVAL '${period}'
     GROUP BY scorer
     ORDER BY avg_score ASC`,
    [agentId],
  );

  return result.rows;
}

// ── Internal Helpers ───────────────────────────────────────────────────────

function buildScoringInput(scorer: string, input: EvalInput): string {
  const parts: string[] = [];

  parts.push(`Agent: ${input.agentId}`);
  parts.push(`\nInput context (truncated):\n${input.input.slice(0, 1500)}`);
  parts.push(`\nAgent output:\n${input.output.slice(0, 1000)}`);

  if (['faithfulness', 'hallucination'].includes(scorer) && input.results.length > 0) {
    parts.push(`\nTool results (what the agent actually received):`);
    for (const r of input.results.slice(0, 10)) {
      parts.push(`- ${r.slice(0, 300)}`);
    }
  }

  if (input.actions.length > 0) {
    parts.push(`\nActions taken: ${input.actions.join(', ')}`);
  }

  return parts.join('\n');
}

async function persistScore(
  agentId: string,
  traceId: string,
  score: EvalScore,
  input: EvalInput,
): Promise<void> {
  try {
    await query(
      `INSERT INTO agent_evals (agent_id, trace_id, scorer, score, explanation, input_summary, output_summary)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        agentId,
        traceId,
        score.scorer,
        score.score,
        score.explanation,
        input.input.slice(0, 500),
        input.output.slice(0, 500),
      ],
    );
  } catch (err) {
    logger.logMinimal('Failed to persist eval score:', (err as Error).message);
  }
}
