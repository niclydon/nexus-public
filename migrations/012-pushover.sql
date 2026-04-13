-- Nexus: Add Pushover as notification channel

BEGIN;

-- Platform tool
INSERT INTO platform_tools (tool_id, display_name, description, category, params_schema, risk_level) VALUES
  ('send_pushover', 'Send Pushover', 'Send a Pushover notification to the owner. For critical alerts and important updates.', 'communication',
   '{"type": "object", "properties": {"title": {"type": "string"}, "message": {"type": "string"}, "priority": {"type": "integer", "default": 0, "description": "-2=lowest, -1=low, 0=normal, 1=high, 2=emergency"}}, "required": ["title", "message"]}',
   'state_changing')
ON CONFLICT (tool_id) DO NOTHING;

-- Grant to ARIA and Monitor (they handle alerts)
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
  ('aria', 'send_pushover', 'system'),
  ('monitor', 'send_pushover', 'system')
ON CONFLICT DO NOTHING;

-- Extend channel check constraint to include pushover
ALTER TABLE notification_delivery_methods DROP CONSTRAINT IF EXISTS notification_delivery_methods_channel_check;
ALTER TABLE notification_delivery_methods ADD CONSTRAINT notification_delivery_methods_channel_check
  CHECK (channel = ANY (ARRAY['email', 'push', 'homepod', 'report', 'pushover']));

-- Add Pushover as a delivery method
INSERT INTO notification_delivery_methods (channel, label, config, is_enabled) VALUES
  ('pushover', 'Pushover', '{}', true)
ON CONFLICT DO NOTHING;

-- Route send-pushover jobs to nexus
INSERT INTO nexus_job_routing (job_type) VALUES ('send-pushover')
ON CONFLICT DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (12, 'Pushover notification channel: tool, job handler, delivery method')
ON CONFLICT (version) DO NOTHING;

COMMIT;
