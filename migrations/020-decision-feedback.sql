-- Human feedback on agent decisions
-- Thumbs up/down ratings to build calibration data for soul package examples

CREATE TABLE IF NOT EXISTS agent_decision_feedback (
  id SERIAL PRIMARY KEY,
  decision_id INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  rating INTEGER NOT NULL CHECK (rating IN (-1, 1)),  -- -1 = thumbs down, 1 = thumbs up
  comment TEXT,
  reviewed_by TEXT NOT NULL DEFAULT 'owner',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_decision_feedback_decision ON agent_decision_feedback (decision_id);
CREATE INDEX IF NOT EXISTS idx_decision_feedback_agent ON agent_decision_feedback (agent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_decision_feedback_rating ON agent_decision_feedback (agent_id, rating);

-- Route review-digest to nexus worker
INSERT INTO nexus_job_routing (job_type) VALUES ('review-digest') ON CONFLICT DO NOTHING;

-- View for unreviewed decisions (recent decisions with actions that haven't been rated)
CREATE OR REPLACE VIEW unreviewed_decisions AS
SELECT
  d.id as decision_id,
  d.agent_id,
  d.trace_id,
  r.display_name as agent_name,
  r.avatar_url,
  d.state_snapshot->>'summary' as summary,
  d.state_snapshot->>'status' as status,
  d.state_snapshot->>'confidence' as confidence,
  d.parsed_actions,
  d.execution_results,
  d.duration_ms,
  d.created_at
FROM agent_decisions d
JOIN agent_registry r ON r.agent_id = d.agent_id
LEFT JOIN agent_decision_feedback f ON f.decision_id = d.id
WHERE f.id IS NULL
  AND d.parsed_actions IS NOT NULL
  AND d.parsed_actions::text != '[]'
  AND d.parsed_actions::text != 'null'
  AND d.created_at >= NOW() - INTERVAL '7 days'
ORDER BY d.created_at DESC;
