-- Nexus: MCP client infrastructure — external tool discovery and installation

BEGIN;

CREATE TABLE IF NOT EXISTS mcp_servers (
    id SERIAL PRIMARY KEY,
    server_name TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    install_type TEXT CHECK (install_type IN ('npm', 'pip', 'docker', 'binary', 'builtin')),
    install_package TEXT,
    transport TEXT NOT NULL DEFAULT 'stdio' CHECK (transport IN ('stdio', 'sse', 'streamable_http')),
    command TEXT,
    args TEXT[] DEFAULT '{}',
    url TEXT,
    env_vars JSONB DEFAULT '{}',
    status TEXT DEFAULT 'available' CHECK (status IN ('available', 'installing', 'installed', 'running', 'error', 'disabled')),
    status_message TEXT,
    source TEXT,
    source_url TEXT,
    tools_provided TEXT[] DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Agent tools for MCP discovery
INSERT INTO platform_tools (tool_id, display_name, description, category, params_schema, risk_level) VALUES
  ('list_mcp_servers', 'List MCP Servers', 'List installed and running external MCP servers and their tools.', 'mcp',
   '{"type": "object", "properties": {}, "required": []}',
   'read_only'),
  ('install_mcp_server', 'Install MCP Server', 'Request installation of an external MCP server. Goes to approval queue.', 'mcp',
   '{"type": "object", "properties": {"server_name": {"type": "string"}, "install_package": {"type": "string", "description": "npm package name"}, "description": {"type": "string"}}, "required": ["server_name", "install_package"]}',
   'state_changing')
ON CONFLICT (tool_id) DO NOTHING;

-- Grant discovery to all agents, install to ARIA only
INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by)
SELECT agent_id, 'list_mcp_servers', 'system'
FROM agent_registry WHERE is_active = true AND executor = 'nexus'
ON CONFLICT DO NOTHING;

INSERT INTO agent_tool_grants (agent_id, tool_id, granted_by) VALUES
  ('aria', 'install_mcp_server', 'system')
ON CONFLICT DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (13, 'MCP client infrastructure: mcp_servers table, discovery/install tools, tool executor routing')
ON CONFLICT (version) DO NOTHING;

COMMIT;
