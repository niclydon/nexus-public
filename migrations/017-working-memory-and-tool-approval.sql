-- Feature 1: Working Memory
-- Short-term structured state that persists across cycles but isn't permanent memory.
-- Agents emit <working_memory> tags in responses; the framework extracts, persists, and
-- re-injects on the next cycle. Cleared when agent reports status=ok with no actions.

ALTER TABLE agent_registry ADD COLUMN IF NOT EXISTS working_memory TEXT;

-- Feature 2: Per-Tool Approval
-- Moves approval gating from agent-level blanket to tool-level granularity.
-- approval_gated agents still queue all non-read_only tools (backward compatible).
-- moderate/high agents now queue only tools where approval_required = true.

ALTER TABLE platform_tools ADD COLUMN IF NOT EXISTS approval_required BOOLEAN DEFAULT false;

-- Mark high-risk tools that should always require approval regardless of agent autonomy
UPDATE platform_tools SET approval_required = true WHERE tool_id IN (
  'restart_service',
  'restart_user_service',
  'write_file',
  'edit_file',
  'git_commit_push',
  'create_pr',
  'run_build',
  'install_mcp_server',
  'install_skill',
  'send_email',
  'update_llm_config'
);

-- Feature 3: Eval Scoring groundwork
-- Table for storing quality scores on agent responses, tool outputs, and conversations.
-- Populated by the Analyst agent or scoring jobs. Consumed by Analyst for pattern detection.

CREATE TABLE IF NOT EXISTS agent_evals (
  id SERIAL PRIMARY KEY,
  agent_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  scorer TEXT NOT NULL,          -- 'faithfulness', 'relevancy', 'hallucination', 'toxicity', 'task_completion'
  score REAL NOT NULL,           -- 0.0 to 1.0
  explanation TEXT,              -- LLM-generated reasoning for the score
  input_summary TEXT,            -- What was being scored (truncated)
  output_summary TEXT,           -- The response being scored (truncated)
  metadata JSONB DEFAULT '{}',  -- Additional context (tool calls, error count, etc.)
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_evals_agent ON agent_evals (agent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_evals_scorer ON agent_evals (scorer, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_evals_trace ON agent_evals (trace_id);
CREATE INDEX IF NOT EXISTS idx_agent_evals_low_scores ON agent_evals (scorer, score) WHERE score < 0.5;
