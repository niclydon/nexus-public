-- Migration 022: Allow 'executed' status on agent_pending_actions
-- The tool executor sets status = 'executed' after running an approved action,
-- but the check constraint didn't include it, causing all approval-gated agent
-- cycles to crash.

ALTER TABLE agent_pending_actions DROP CONSTRAINT IF EXISTS agent_pending_actions_status_check;
ALTER TABLE agent_pending_actions ADD CONSTRAINT agent_pending_actions_status_check
  CHECK (status IN ('pending', 'approved', 'rejected', 'expired', 'executed'));

INSERT INTO nexus_schema_version (version, description)
VALUES (22, 'Fix agent_pending_actions status constraint: add executed')
ON CONFLICT (version) DO NOTHING;
