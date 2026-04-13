-- Synchronous delegation and memory consolidation tools

INSERT INTO platform_tools (tool_id, display_name, description, category, params_schema, risk_level, approval_required) VALUES
  ('delegate_task', 'Delegate Task', 'Synchronously consult another agent within your current cycle. The target agent reasons about your prompt using their soul context and memories, returning a text response in ~5-10s. No tool execution — for quick questions like "Is Forge healthy?" or "What is the photo backlog?"', 'coordination',
   '{"type": "object", "properties": {"agent_id": {"type": "string", "description": "Target agent to consult"}, "prompt": {"type": "string", "description": "What to ask the agent"}}, "required": ["agent_id", "prompt"]}',
   'read_only', false),
  ('consolidate_memories', 'Consolidate Memories', 'Merge redundant agent memories into consolidated entries. Uses LLM to identify clusters of related memories and produces higher-quality merged entries. The originals are deactivated.', 'meta',
   '{"type": "object", "properties": {"agent_id": {"type": "string", "description": "Agent whose memories to consolidate (default: self)"}, "max_entries": {"type": "integer", "description": "Max memories to analyze (default: 50)"}}}',
   'state_changing', false)
ON CONFLICT (tool_id) DO NOTHING;

-- Grant delegate_task to all agents (read_only, everyone can consult peers)
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by)
SELECT agent_id, 'delegate_task', 'system'
FROM agent_registry WHERE is_active = true AND executor = 'nexus'
ON CONFLICT (agent_id, tool_id) DO NOTHING;

-- Grant consolidate_memories to analyst (the quality/memory hygiene agent)
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
  ('analyst', 'consolidate_memories', 'system')
ON CONFLICT (agent_id, tool_id) DO NOTHING;
