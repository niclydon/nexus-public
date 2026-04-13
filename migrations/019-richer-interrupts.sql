-- Richer interrupt schema for approval workflows
-- Inspired by LangChain Agent Inbox: accept / edit / respond / ignore
--
-- Before: binary approve/reject
-- After: four response types with parameter editing and text responses

-- Add response type and modified params to pending actions
ALTER TABLE agent_pending_actions
  ADD COLUMN IF NOT EXISTS response_type TEXT
    CHECK (response_type IN ('accept', 'edit', 'respond', 'ignore')),
  ADD COLUMN IF NOT EXISTS modified_params JSONB,
  ADD COLUMN IF NOT EXISTS response_text TEXT,
  ADD COLUMN IF NOT EXISTS description TEXT,
  ADD COLUMN IF NOT EXISTS allow_edit BOOLEAN DEFAULT false,
  ADD COLUMN IF NOT EXISTS allow_respond BOOLEAN DEFAULT true,
  ADD COLUMN IF NOT EXISTS allow_ignore BOOLEAN DEFAULT false;

-- Backfill: existing approved actions are 'accept' type
UPDATE agent_pending_actions SET response_type = 'accept' WHERE status = 'approved' AND response_type IS NULL;
UPDATE agent_pending_actions SET response_type = 'ignore' WHERE status = 'rejected' AND response_type IS NULL;

-- Update status check to include new statuses
-- 'approved' now means any positive resolution (accept/edit/respond)
-- 'rejected' now means ignore or explicit rejection
