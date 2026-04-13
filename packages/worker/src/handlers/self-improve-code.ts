/**
 * Self-improve code handler.
 *
 * Picks the highest-priority planned improvement from aria_self_improvement,
 * reads affected files from GitHub, generates minimal code changes via LLM,
 * and creates a PR for human review.
 *
 * Requires: GITHUB_PAT, LLM provider
 */
import type { TempoJob } from '../job-worker.js';
import { getPool, createLogger } from '@nexus/core';
import { logEvent } from '../lib/event-log.js';
import { routeRequest } from '../lib/llm/index.js';
import {
  isGithubConfigured,
  githubReadFile,
  githubCreateBranch,
  githubCreateOrUpdateFile,
  githubCreatePR,
} from '../lib/github.js';

const logger = createLogger('self-improve-code');

interface SelfImprovementItem {
  id: string;
  category: string;
  status: string;
  title: string;
  description: string;
  reasoning: string | null;
  affected_repos: string[] | null;
  affected_files: string[] | null;
  priority: number;
  confidence: number;
  estimated_effort: string | null;
}

interface CodeGenFile {
  path: string;
  content: string;
}

interface CodeGenResult {
  files: CodeGenFile[];
  summary: string;
  commit_message: string;
}

/**
 * Slugify a title for use in branch names.
 */
function slugify(title: string): string {
  return title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 40);
}

/**
 * Find the best target item to work on.
 */
async function findTarget(targetItemId?: string): Promise<SelfImprovementItem | null> {
  const pool = getPool();

  if (targetItemId) {
    const { rows } = await pool.query<SelfImprovementItem>(
      `SELECT id, category, status, title, description, reasoning, affected_repos, affected_files, priority, confidence, estimated_effort
       FROM aria_self_improvement
       WHERE id = $1`,
      [targetItemId]
    );
    return rows[0] ?? null;
  }

  // Find highest priority planned/idea item with sufficient confidence and small effort
  const { rows } = await pool.query<SelfImprovementItem>(
    `SELECT id, category, status, title, description, reasoning, affected_repos, affected_files, priority, confidence, estimated_effort
     FROM aria_self_improvement
     WHERE status IN ('planned', 'idea')
       AND confidence >= 0.7
       AND estimated_effort IN ('trivial', 'small')
     ORDER BY priority DESC, confidence DESC
     LIMIT 1`
  );
  return rows[0] ?? null;
}

/**
 * Run error analysis when no target is found — look for patterns in failures
 * and create a planned improvement.
 */
async function runErrorAnalysis(): Promise<Record<string, unknown>> {
  const pool = getPool();

  // Fetch recent failed jobs
  const { rows: failedJobs } = await pool.query(
    `SELECT job_type, last_error, created_at
     FROM tempo_jobs
     WHERE status = 'failed'
       AND created_at > NOW() - INTERVAL '24 hours'
     ORDER BY created_at DESC
     LIMIT 10`
  );

  // Fetch error events
  const { rows: errorEvents } = await pool.query(
    `SELECT action, component, metadata, created_at
     FROM event_log
     WHERE status = 'error'
       AND created_at > NOW() - INTERVAL '24 hours'
     ORDER BY created_at DESC
     LIMIT 10`
  );

  if (failedJobs.length === 0 && errorEvents.length === 0) {
    return { action: 'no_errors', message: 'No recent errors to analyze' };
  }

  const errorContext = [
    failedJobs.length > 0
      ? `Failed jobs (24h):\n${failedJobs.map((j: { job_type: string; last_error: string }) => `  [${j.job_type}] ${(j.last_error || 'unknown').slice(0, 200)}`).join('\n')}`
      : '',
    errorEvents.length > 0
      ? `Error events (24h):\n${errorEvents.map((e: { component: string; action: string }) => `  [${e.component}] ${e.action}`).join('\n')}`
      : '',
  ]
    .filter(Boolean)
    .join('\n\n');

  const result = await routeRequest({
    handler: 'self-improve-code',
    taskTier: 'generation',
    preferredModel: 'gpt-4o-mini',
    systemPrompt:
      'You analyze error patterns in an AI assistant platform (TypeScript, Node.js, PostgreSQL). ' +
      'Suggest the simplest possible code fix. Return JSON (no markdown fences): ' +
      '{ "title": "string", "description": "string", "category": "string (one of: bug_fix, optimization, feature_idea, code_quality, database_tune, memory_insight, research_finding, proactive_tune, integration_idea, ux_improvement)", "priority": number, "confidence": number, "estimated_effort": "trivial|small" } ' +
      'or { "no_fix": true, "reason": "string" } if no actionable fix is found.',
    userMessage: `Here are recent errors from the ARIA platform:\n\n${errorContext}\n\nWhat's the simplest code fix for these errors?`,
    maxTokens: 800,
    useBatch: true,
  });

  try {
    const cleaned = result.text.replace(/```json\s*/g, '').replace(/```\s*/g, '').trim();
    const parsed = JSON.parse(cleaned);

    if (parsed.no_fix) {
      return { action: 'no_fix', reason: parsed.reason };
    }

    // Insert the fix idea as planned
    const { rows } = await pool.query(
      `INSERT INTO aria_self_improvement
       (category, status, title, description, priority, confidence, estimated_effort, source)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       RETURNING id`,
      [
        ['bug_fix', 'optimization', 'feature_idea', 'code_quality', 'database_tune', 'memory_insight', 'research_finding', 'proactive_tune', 'integration_idea', 'ux_improvement'].includes(parsed.category) ? parsed.category : 'bug_fix',
        'planned',
        parsed.title,
        parsed.description,
        parsed.priority ?? 7,
        parsed.confidence ?? 0.6,
        parsed.estimated_effort || 'small',
        'error_analysis',
      ]
    );

    return { action: 'planned_fix', item_id: rows[0].id, title: parsed.title };
  } catch {
    return { action: 'parse_error', message: 'Could not parse LLM error analysis' };
  }
}

/**
 * Determine which files to read from the target's affected_files,
 * or infer from the description.
 */
function determineAffectedFiles(target: SelfImprovementItem): Array<{ repo: string; path: string }> {
  const files: Array<{ repo: string; path: string }> = [];

  if (target.affected_files && target.affected_files.length > 0) {
    for (const f of target.affected_files) {
      // Expected format: "repo:path" or just "path" (defaults to aria)
      const colonIndex = f.indexOf(':');
      if (colonIndex > 0) {
        files.push({ repo: f.slice(0, colonIndex), path: f.slice(colonIndex + 1) });
      } else {
        files.push({ repo: 'aria', path: f });
      }
    }
  }

  return files.slice(0, 3); // Max 3 files
}

export async function handleSelfImproveCode(job: TempoJob): Promise<Record<string, unknown>> {
  const payload = (job.payload ?? {}) as { target_item_id?: string };
  logger.log(`Starting code improvement job ${job.id}`);

  if (!isGithubConfigured()) {
    throw new Error('GITHUB_PAT is not configured — cannot create PRs');
  }

  // Find target improvement item
  const target = await findTarget(payload.target_item_id);

  if (!target) {
    logger.log('No suitable target found, running error analysis');
    const analysisResult = await runErrorAnalysis();

    logEvent({
      action: `Self-improve code: no target, ran error analysis — ${analysisResult.action}`,
      component: 'self-improvement',
      category: 'self_maintenance',
      metadata: analysisResult,
    });

    return analysisResult;
  }

  logger.log(`Target: "${target.title}" (${target.id})`);

  // Update status to in_progress
  const pool = getPool();
  await pool.query(
    `UPDATE aria_self_improvement SET status = 'in_progress', updated_at = NOW() WHERE id = $1`,
    [target.id]
  );

  try {
    // Determine affected files and read from GitHub
    const affectedFiles = determineAffectedFiles(target);
    const fileContents: Array<{ repo: string; path: string; content: string; sha: string }> = [];

    for (const file of affectedFiles) {
      try {
        const { content, sha } = await githubReadFile(file.repo, file.path);
        fileContents.push({ ...file, content, sha });
      } catch (err) {
        const msg = (err as Error).message;
        logger.logMinimal(`Could not read ${file.repo}/${file.path}: ${msg}`);
      }
    }

    // Build file context for the LLM
    const fileContext = fileContents.length > 0
      ? fileContents
          .map((f) => `### ${f.repo}/${f.path}\n\`\`\`typescript\n${f.content.slice(0, 3000)}\n\`\`\``)
          .join('\n\n')
      : 'No specific files available. Generate the improvement based on the description.';

    // Generate code with Claude Sonnet (reasoning tier)
    const codeResult = await routeRequest({
      handler: 'self-improve-code',
      taskTier: 'reasoning',
      systemPrompt: `You are generating a minimal code change for the ARIA platform. Follow these conventions:
- TypeScript strict mode
- Parameterized SQL queries (never string interpolation for values)
- Server-only code (no client imports)
- No new npm dependencies
- Max 3 files changed, max 100 lines changed per file
- Return ONLY valid JSON (no markdown fences): { "files": [{ "path": "repo/path/to/file.ts", "content": "full file content" }], "summary": "what changed and why", "commit_message": "concise commit message" }
- If the improvement cannot be safely implemented in a minimal change, return: { "files": [], "summary": "reason it cannot be done", "commit_message": "" }`,
      userMessage: `Improvement to implement:
Title: ${target.title}
Description: ${target.description}
Category: ${target.category}
${target.reasoning ? `Reasoning: ${target.reasoning}` : ''}

Current file contents:
${fileContext}

Generate the minimal code change. Return JSON with the complete updated file contents.`,
      maxTokens: 4096,
    });

    logger.log(`Code generated via ${codeResult.model} (${codeResult.provider}, ${codeResult.estimatedCostCents}¢)`);

    // Parse code generation result
    let codeGen: CodeGenResult;
    try {
      const cleaned = codeResult.text.replace(/```json\s*/g, '').replace(/```\s*/g, '').trim();
      codeGen = JSON.parse(cleaned) as CodeGenResult;
    } catch {
      throw new Error('Failed to parse code generation response as JSON');
    }

    if (!codeGen.files || codeGen.files.length === 0) {
      // LLM decided the change can't be safely made
      await pool.query(
        `UPDATE aria_self_improvement
         SET status = 'planned', reasoning = $2, updated_at = NOW()
         WHERE id = $1`,
        [target.id, codeGen.summary || 'LLM could not generate a safe minimal change']
      );
      return { action: 'deferred', reason: codeGen.summary };
    }

    // Enforce limits
    if (codeGen.files.length > 3) {
      codeGen.files = codeGen.files.slice(0, 3);
    }

    // Determine the repo from the first file path
    const firstFilePath = codeGen.files[0].path;
    const repo = firstFilePath.split('/')[0] || 'aria';

    // Create branch and PR
    const dateStr = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const slug = slugify(target.title);
    const branchName = `aria/self-improve-${dateStr}-${slug}`;

    await githubCreateBranch(repo, branchName);

    // Create or update each file
    for (const file of codeGen.files) {
      // Strip the repo prefix from the path if present
      const filePath = file.path.startsWith(`${repo}/`)
        ? file.path.slice(repo.length + 1)
        : file.path;

      // Find existing SHA if we read this file
      const existing = fileContents.find(
        (f) => f.repo === repo && f.path === filePath
      );

      await githubCreateOrUpdateFile(
        repo,
        filePath,
        file.content,
        codeGen.commit_message || `self-improve: ${target.title}`,
        branchName,
        existing?.sha
      );
    }

    // Create PR
    const pr = await githubCreatePR(repo, {
      title: `[ARIA Self-Improve] ${target.title}`,
      body: [
        `## Self-Improvement: ${target.title}`,
        '',
        target.description,
        '',
        `**Category:** ${target.category}`,
        `**Priority:** ${target.priority}`,
        `**Confidence:** ${target.confidence}`,
        `**Effort:** ${target.estimated_effort || 'unknown'}`,
        `**Self-Improvement ID:** \`${target.id}\``,
        '',
        '---',
        '',
        `### Summary`,
        codeGen.summary,
        '',
        '_This PR was autonomously generated by ARIA\'s self-improvement system._',
      ].join('\n'),
      head: branchName,
    });

    logger.log(`Created PR: ${pr.url}`);

    // Update aria_self_improvement
    await pool.query(
      `UPDATE aria_self_improvement
       SET status = 'awaiting_review', pr_url = $2, pr_branch = $3, updated_at = NOW()
       WHERE id = $1`,
      [target.id, pr.url, branchName]
    );

    // Log to event_log
    logEvent({
      action: `Self-improve code: created PR for "${target.title}"`,
      component: 'self-improvement',
      category: 'self_maintenance',
      metadata: {
        item_id: target.id,
        pr_url: pr.url,
        pr_number: pr.number,
        branch: branchName,
        files_changed: codeGen.files.length,
      },
    });

    return {
      action: 'pr_created',
      item_id: target.id,
      pr_url: pr.url,
      pr_number: pr.number,
      branch: branchName,
      files_changed: codeGen.files.length,
      summary: codeGen.summary,
    };
  } catch (err) {
    // If GitHub operations fail, set status back to planned
    const msg = (err as Error).message;
    logger.logMinimal(`Failed for "${target.title}": ${msg}`);

    await pool.query(
      `UPDATE aria_self_improvement
       SET status = 'planned', reasoning = $2, updated_at = NOW()
       WHERE id = $1`,
      [target.id, `Code generation/PR creation failed: ${msg.slice(0, 500)}`]
    );

    logEvent({
      action: `Self-improve code: failed for "${target.title}" — ${msg.slice(0, 100)}`,
      component: 'self-improvement',
      category: 'self_maintenance',
      status: 'error',
      metadata: { item_id: target.id, error: msg },
    });

    throw err;
  }
}
