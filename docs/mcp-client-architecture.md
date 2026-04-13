# MCP Client Architecture — External Tool Discovery & Installation

## Problem

Agents need capabilities (Gmail access, calendar sync, Spotify control, etc.) that may already exist as published MCP servers. Today, every capability is a custom-built tool handler. We should be able to install existing MCP servers and expose their tools to agents through the standard tool catalog.

## Goal

An agent can discover a tool it needs, find an MCP server that provides it, request installation, and start using it — all through the existing tool catalog and approval flow.

---

## Architecture

```
Agent needs a capability
  │
  ▼
search_tools → not found
  │
  ▼
search_mcp_registry → finds "google-calendar-mcp"
  │
  ▼
request_mcp_install → queued for approval
  │
  ▼
The owner approves in Chancery
  │
  ▼
Nexus installs MCP server (npm/pip/docker)
  │
  ▼
MCP server starts as managed process
  │
  ▼
Tools from MCP server registered in platform_tools (provider_type='mcp')
  │
  ▼
Agent gets tool grants → uses tools normally
```

## Components

### 1. MCP Server Registry (Discovery)

A `mcp_servers` table tracks available and installed MCP servers:

```sql
CREATE TABLE mcp_servers (
    id SERIAL PRIMARY KEY,
    server_name TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    -- Installation
    install_type TEXT NOT NULL CHECK (install_type IN ('npm', 'pip', 'docker', 'binary', 'builtin')),
    install_package TEXT,           -- e.g. "@anthropic/mcp-server-gmail", "mcp-server-gcal"
    install_args TEXT[] DEFAULT '{}',
    -- Transport
    transport TEXT NOT NULL DEFAULT 'stdio' CHECK (transport IN ('stdio', 'sse', 'streamable_http')),
    command TEXT,                    -- for stdio: the command to run
    args TEXT[] DEFAULT '{}',       -- command arguments
    url TEXT,                       -- for http transports
    -- Auth
    env_vars JSONB DEFAULT '{}',    -- env vars the server needs (keys only, values from secrets)
    -- State
    status TEXT DEFAULT 'available' CHECK (status IN ('available', 'installing', 'installed', 'running', 'error', 'disabled')),
    status_message TEXT,
    pid INTEGER,                    -- process ID if running
    -- Discovery
    source TEXT,                    -- 'registry', 'manual', 'openclaw', 'github'
    source_url TEXT,                -- where it was found
    -- Metadata
    tools_provided TEXT[],          -- list of tool names this server exposes
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 2. MCP Process Manager

A component in the worker that manages MCP server processes:

```
MCP Process Manager
  ├── install(serverName)     — npm install / pip install / docker pull
  ├── start(serverName)       — spawn process, connect via stdio/http
  ├── stop(serverName)        — graceful shutdown
  ├── restart(serverName)     — stop + start
  ├── listTools(serverName)   — discover tools from running server
  └── callTool(serverName, toolName, params)  — execute a tool
```

Installed MCP servers run as child processes of the worker (stdio) or as separate services (http). The process manager handles lifecycle, health checks, and restarts.

### 3. Tool Bridge

When an MCP server is installed and started, its tools are registered in `platform_tools`:

```
MCP server starts
  → listTools() discovers available tools
  → For each tool:
      INSERT INTO platform_tools (
        tool_id = '{server_name}_{tool_name}',
        display_name = tool.name,
        description = tool.description,
        category = 'mcp',
        params_schema = tool.inputSchema,
        risk_level = classify(tool),  -- read_only if GET-like, state_changing otherwise
        provider_type = 'mcp',
        provider_config = {"server": server_name, "tool": tool_name}
      )
```

When an agent calls an MCP-provided tool, the tool executor routes to the MCP process manager:

```typescript
// In tool-executor.ts
if (tool.provider_type === 'mcp') {
  const { server, tool: mcpTool } = tool.provider_config;
  return await mcpProcessManager.callTool(server, mcpTool, action.params);
}
```

### 4. Agent Tools for Discovery & Installation

New platform tools for agents:

```
search_mcp_registry
  Search for MCP servers by keyword/capability.
  Returns: list of available servers with descriptions.
  Risk: read_only

request_mcp_install
  Request installation of an MCP server.
  Goes to approval queue (like tool requests).
  Risk: state_changing

list_mcp_servers
  List installed/running MCP servers and their status.
  Risk: read_only
```

### 5. Skills.md Support

Some capabilities come as SKILLS.md files — instruction sets that tell an agent how to accomplish something using existing tools. These are different from MCP servers (no new process, just new knowledge).

```sql
CREATE TABLE agent_skills (
    id SERIAL PRIMARY KEY,
    skill_name TEXT UNIQUE NOT NULL,
    description TEXT,
    content TEXT NOT NULL,          -- the skill instructions (markdown)
    source TEXT,                    -- 'openclaw', 'manual', 'github'
    source_url TEXT,
    applicable_agents TEXT[],       -- which agents can use this skill
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

Skills are injected into the agent's system prompt when relevant. The agent doesn't need a new tool — it just gets instructions on how to use existing tools to accomplish a task.

---

## Discovery Sources

### MCP Registry (registry.modelcontextprotocol.io)
- Official registry of MCP servers
- Searchable by capability
- Provides install commands

### OpenClaw ClawHub
- Community skills and MCP server integrations
- Searchable via OpenClaw CLI or API
- Skills can be imported as SKILLS.md

### GitHub
- Many MCP servers published as repos
- Search by topic: "mcp-server"
- Manual discovery, then register in mcp_servers

### NPM / PyPI
- MCP servers published as packages
- Search by keyword: "mcp-server-*" or "@modelcontextprotocol/*"

---

## Installation Flow

### Agent-initiated:
```
1. Agent calls search_mcp_registry("gmail")
2. Returns: [{name: "google-gmail-mcp", install: "npm @anthropic/mcp-gmail", tools: ["gmail_search", "gmail_send", ...]}]
3. Agent calls request_mcp_install("google-gmail-mcp", reason: "Need Gmail access for email ingestion")
4. Request goes to agent_pending_actions (approval queue)
5. the owner approves in Chancery
6. System installs: npm install -g @anthropic/mcp-gmail
7. System starts server, discovers tools, registers in platform_tools
8. Agent gets tool grants
9. Agent can now call gmail_search, gmail_send, etc.
```

### Manual:
```
1. Admin adds row to mcp_servers with install details
2. Admin runs: nexus mcp install google-gmail-mcp
3. Tools auto-registered
4. Admin grants tools to agents
```

---

## Example: Gmail via MCP

Instead of our custom `gmail-backfill` handler:

```json
{
  "server_name": "google-gmail",
  "display_name": "Google Gmail MCP",
  "install_type": "npm",
  "install_package": "@anthropic/mcp-server-gmail",
  "transport": "stdio",
  "command": "npx",
  "args": ["@anthropic/mcp-server-gmail"],
  "env_vars": {"GOOGLE_CLIENT_ID": "", "GOOGLE_CLIENT_SECRET": "", "GOOGLE_REFRESH_TOKEN": ""},
  "source": "registry"
}
```

After installation, Collector uses the MCP-provided `gmail_search` tool instead of our custom handler:

```
Collector cycle:
  1. Check gmail source is due
  2. Call gmail_search(query: "after:2026/03/31", maxResults: 100)
  3. Store results in gmail_archive
  4. Update watermark
```

The generic poll pipeline still applies — but the "fetch from API" step is replaced by "call MCP tool."

---

## Example: Google Calendar via MCP

```json
{
  "server_name": "google-calendar",
  "display_name": "Google Calendar MCP",
  "install_type": "npm",
  "install_package": "mcp-server-google-calendar",
  "transport": "stdio",
  "command": "npx",
  "args": ["mcp-server-google-calendar"],
  "env_vars": {"GOOGLE_CLIENT_ID": "", "GOOGLE_CLIENT_SECRET": "", "GOOGLE_REFRESH_TOKEN": ""},
  "source": "registry"
}
```

ARIA can directly call `calendar_list_events` for morning briefings — no custom handler needed.

---

## What Stays Custom

Some sources won't have MCP servers:
- **Looki wearable** — niche device, no MCP server exists
- **Apple Photos via relay** — requires macOS access
- **iMessage via relay** — requires macOS access
- **HealthKit via iOS** — requires iOS app

For these, we use the generic API poll pipeline with custom parsers. The two systems complement each other:
- MCP servers for well-supported APIs (Google, Spotify, GitHub, Slack, etc.)
- Custom poll pipeline for niche/proprietary sources

---

## Implementation Order

1. **MCP Process Manager** — install, start, stop, health check MCP servers
2. **Tool Bridge** — auto-register MCP tools in platform_tools
3. **MCP tool executor routing** — when agent calls an MCP tool, route to the server
4. **Discovery tools** — search_mcp_registry, request_mcp_install
5. **Skills table** — for instruction-based capabilities
6. **First MCP server** — Google Calendar as proof of concept
