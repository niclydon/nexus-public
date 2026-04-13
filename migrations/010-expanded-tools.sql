-- Nexus: Expand platform tools for v2 agent capabilities

BEGIN;

INSERT INTO platform_tools (tool_id, display_name, description, category, params_schema, risk_level) VALUES
  ('enqueue_job', 'Enqueue Job', 'Submit a new job to the tempo_jobs queue. Used by Collector for backfill orchestration.', 'infrastructure',
   '{"type": "object", "properties": {"job_type": {"type": "string"}, "payload": {"type": "object"}, "priority": {"type": "integer", "default": 0}}, "required": ["job_type"]}',
   'state_changing'),

  ('create_task', 'Create Task', 'Create a task and assign it to an agent. Used for cross-agent work delegation.', 'coordination',
   '{"type": "object", "properties": {"title": {"type": "string"}, "description": {"type": "string"}, "assigned_to": {"type": "string"}, "priority": {"type": "integer", "default": 0}}, "required": ["title", "assigned_to"]}',
   'state_changing'),

  ('update_task', 'Update Task', 'Update task status (open, in_progress, completed, blocked, cancelled).', 'coordination',
   '{"type": "object", "properties": {"task_id": {"type": "integer"}, "status": {"type": "string"}, "result": {"type": "string"}}, "required": ["task_id", "status"]}',
   'state_changing'),

  ('list_agents', 'List Agents', 'List all active agents with their last check-in, schedule, and recent status.', 'coordination',
   '{"type": "object", "properties": {}, "required": []}',
   'read_only'),

  ('get_agent_status', 'Get Agent Status', 'Get the most recent decision summary and status for a specific agent.', 'coordination',
   '{"type": "object", "properties": {"agent_id": {"type": "string"}}, "required": ["agent_id"]}',
   'read_only'),

  ('deliver_report', 'Deliver Report', 'Send a report through the configured delivery channels (email, push, inbox).', 'communication',
   '{"type": "object", "properties": {"report_type": {"type": "string"}, "title": {"type": "string"}, "body": {"type": "string"}, "category": {"type": "string", "default": "general"}}, "required": ["report_type", "title", "body"]}',
   'state_changing'),

  ('forge_health', 'Forge Health', 'Check Forge LLM gateway and backend service health.', 'infrastructure',
   '{"type": "object", "properties": {"check": {"type": "string", "default": "health"}}, "required": []}',
   'read_only')
ON CONFLICT (tool_id) DO NOTHING;

-- Grant tools to appropriate agents
-- Collector: enqueue_job for backfill orchestration
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
  ('collector', 'enqueue_job', 'system')
ON CONFLICT DO NOTHING;

-- ARIA: create/update tasks, list/status agents, deliver reports
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
  ('aria', 'create_task', 'system'),
  ('aria', 'update_task', 'system'),
  ('aria', 'list_agents', 'system'),
  ('aria', 'get_agent_status', 'system'),
  ('aria', 'deliver_report', 'system')
ON CONFLICT DO NOTHING;

-- Monitor: list/status agents (to check agent health)
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
  ('monitor', 'list_agents', 'system'),
  ('monitor', 'get_agent_status', 'system')
ON CONFLICT DO NOTHING;

-- Model Ops: forge_health
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
  ('model-ops', 'forge_health', 'system')
ON CONFLICT DO NOTHING;

-- Analyst: list/status agents (to review decisions)
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
  ('analyst', 'list_agents', 'system'),
  ('analyst', 'get_agent_status', 'system')
ON CONFLICT DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (10, 'Expanded tool catalog: enqueue_job, create/update_task, list/get_agent_status, deliver_report, forge_health')
ON CONFLICT (version) DO NOTHING;

COMMIT;
