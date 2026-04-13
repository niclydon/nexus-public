/**
 * MCP Client Manager — connects to external MCP servers, discovers
 * their tools, and proxies tool calls from the Nexus tool executor.
 *
 * Uses the official @modelcontextprotocol/sdk Client.
 */
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { getPool, createLogger } from '@nexus/core';

const logger = createLogger('mcp-client');

interface ManagedServer {
  name: string;
  client: Client;
  transport: StdioClientTransport;
  tools: Array<{ name: string; description?: string; inputSchema: unknown }>;
}

const servers = new Map<string, ManagedServer>();

/**
 * Connect to an MCP server, discover its tools, and register them
 * in platform_tools with provider_type='mcp'.
 */
export async function connectServer(config: {
  serverName: string;
  command: string;
  args: string[];
  env?: Record<string, string>;
}): Promise<{ tools: string[] }> {
  const { serverName, command, args, env } = config;

  // Disconnect existing connection if any
  if (servers.has(serverName)) {
    await disconnectServer(serverName);
  }

  logger.log(`Connecting to MCP server: ${serverName} (${command} ${args.join(' ')})`);

  const transport = new StdioClientTransport({
    command,
    args,
    env: { ...process.env, ...env } as Record<string, string>,
  });

  const client = new Client({ name: 'nexus-worker', version: '1.0.0' });
  await client.connect(transport);

  // Discover tools
  const toolsResult = await client.listTools();
  const tools = toolsResult.tools;

  logger.log(`Connected to ${serverName}: ${tools.length} tools discovered`);
  tools.forEach(t => logger.logVerbose(`  - ${t.name}: ${t.description ?? '(no description)'}`));

  // Store the connection
  servers.set(serverName, { name: serverName, client, transport, tools });

  // Register tools in platform_tools
  const pool = getPool();
  for (const tool of tools) {
    const toolId = `${serverName}__${tool.name}`;
    await pool.query(
      `INSERT INTO platform_tools (tool_id, display_name, description, category, params_schema, risk_level, provider_type, provider_config, is_active)
       VALUES ($1, $2, $3, 'mcp', $4, 'state_changing', 'mcp', $5, true)
       ON CONFLICT (tool_id) DO UPDATE SET
         description = $3, params_schema = $4, provider_config = $5, is_active = true, created_at = NOW()`,
      [
        toolId,
        tool.name,
        tool.description ?? tool.name,
        JSON.stringify(tool.inputSchema ?? {}),
        JSON.stringify({ server: serverName, tool: tool.name }),
      ],
    );
  }

  // Update mcp_servers status
  await pool.query(
    `UPDATE mcp_servers SET status = 'running', tools_provided = $2, updated_at = NOW()
     WHERE server_name = $1`,
    [serverName, tools.map(t => t.name)],
  );

  logger.log(`Registered ${tools.length} tools from ${serverName} in platform_tools`);
  return { tools: tools.map(t => t.name) };
}

/**
 * Call a tool on a connected MCP server.
 */
export async function callMcpTool(
  serverName: string,
  toolName: string,
  args: Record<string, unknown>,
): Promise<string> {
  const server = servers.get(serverName);
  if (!server) {
    throw new Error(`MCP server '${serverName}' is not connected`);
  }

  logger.logVerbose(`Calling ${serverName}/${toolName}`, args);

  const result = await server.client.callTool({
    name: toolName,
    arguments: args,
  });

  // Extract text content from result
  const content = result.content;
  if (Array.isArray(content)) {
    return content
      .map(c => {
        if (typeof c === 'string') return c;
        if (c.type === 'text') return c.text;
        return JSON.stringify(c);
      })
      .join('\n');
  }

  return typeof content === 'string' ? content : JSON.stringify(content);
}

/**
 * Disconnect from an MCP server and deactivate its tools.
 */
export async function disconnectServer(serverName: string): Promise<void> {
  const server = servers.get(serverName);
  if (!server) return;

  try {
    await server.client.close();
  } catch (err) {
    logger.logVerbose(`Error closing ${serverName}:`, (err as Error).message);
  }

  servers.delete(serverName);

  // Deactivate tools
  const pool = getPool();
  await pool.query(
    `UPDATE platform_tools SET is_active = false WHERE provider_type = 'mcp'
     AND provider_config->>'server' = $1`,
    [serverName],
  );
  await pool.query(
    `UPDATE mcp_servers SET status = 'installed', updated_at = NOW() WHERE server_name = $1`,
    [serverName],
  );

  logger.log(`Disconnected from ${serverName}`);
}

/**
 * List all connected servers and their tools.
 */
export function listConnectedServers(): Array<{ name: string; tools: string[] }> {
  return Array.from(servers.values()).map(s => ({
    name: s.name,
    tools: s.tools.map(t => t.name),
  }));
}

/**
 * Connect to all MCP servers marked as 'installed' or 'running' in the DB.
 * Called at worker startup.
 */
export async function connectAllServers(): Promise<void> {
  const pool = getPool();
  const { rows } = await pool.query<{
    server_name: string;
    command: string;
    args: string[];
    env_vars: Record<string, string>;
  }>(
    `SELECT server_name, command, args, env_vars FROM mcp_servers
     WHERE status IN ('installed', 'running') AND command IS NOT NULL`,
  );

  for (const row of rows) {
    try {
      // Resolve env var values from process.env
      const env: Record<string, string> = {};
      for (const [key, val] of Object.entries(row.env_vars ?? {})) {
        env[key] = val || process.env[key] || '';
      }

      await connectServer({
        serverName: row.server_name,
        command: row.command,
        args: row.args ?? [],
        env,
      });
    } catch (err) {
      logger.logMinimal(`Failed to connect to ${row.server_name}:`, (err as Error).message);
      await pool.query(
        `UPDATE mcp_servers SET status = 'error', status_message = $2, updated_at = NOW()
         WHERE server_name = $1`,
        [row.server_name, (err as Error).message],
      );
    }
  }
}

/**
 * Disconnect all servers. Called at worker shutdown.
 */
export async function disconnectAllServers(): Promise<void> {
  for (const name of servers.keys()) {
    await disconnectServer(name);
  }
}
