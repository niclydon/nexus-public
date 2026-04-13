-- Nexus: Fixer agent tools (approval-gated file/git operations)

BEGIN;

INSERT INTO platform_tools (tool_id, display_name, description, category, params_schema, risk_level) VALUES
  ('read_file', 'Read File', 'Read the contents of a file from the Nexus codebase.', 'code',
   '{"type": "object", "properties": {"path": {"type": "string", "description": "Relative path from repo root"}}, "required": ["path"]}',
   'read_only'),

  ('write_file', 'Write File', 'Write content to a file. Creates the file if it does not exist. APPROVAL REQUIRED.', 'code',
   '{"type": "object", "properties": {"path": {"type": "string"}, "content": {"type": "string"}}, "required": ["path", "content"]}',
   'state_changing'),

  ('edit_file', 'Edit File', 'Replace a string in a file. APPROVAL REQUIRED.', 'code',
   '{"type": "object", "properties": {"path": {"type": "string"}, "old_string": {"type": "string"}, "new_string": {"type": "string"}}, "required": ["path", "old_string", "new_string"]}',
   'state_changing'),

  ('run_build', 'Run Build', 'Run npm run build in the Nexus repo and return success/failure. APPROVAL REQUIRED.', 'code',
   '{"type": "object", "properties": {}, "required": []}',
   'state_changing'),

  ('git_status', 'Git Status', 'Show current git status (modified files, branch).', 'code',
   '{"type": "object", "properties": {}, "required": []}',
   'read_only'),

  ('git_commit_push', 'Git Commit and Push', 'Stage files, commit with message, and push. APPROVAL REQUIRED.', 'code',
   '{"type": "object", "properties": {"files": {"type": "array", "items": {"type": "string"}}, "message": {"type": "string"}}, "required": ["message"]}',
   'state_changing')
ON CONFLICT (tool_id) DO NOTHING;

-- Grant to Fixer
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
  ('fixer', 'read_file', 'system'),
  ('fixer', 'write_file', 'system'),
  ('fixer', 'edit_file', 'system'),
  ('fixer', 'run_build', 'system'),
  ('fixer', 'git_status', 'system'),
  ('fixer', 'git_commit_push', 'system')
ON CONFLICT DO NOTHING;

-- Also grant read_file to Analyst (for documentation review)
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
  ('analyst', 'read_file', 'system')
ON CONFLICT DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (11, 'Fixer tools: read/write/edit file, run build, git status/commit/push. All state_changing tools approval-gated.')
ON CONFLICT (version) DO NOTHING;

COMMIT;
