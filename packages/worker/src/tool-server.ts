/**
 * Internal HTTP server for tool execution.
 *
 * Exposes the worker's tool executor over HTTP so external clients
 * (chat UIs, API, etc.) can execute tools without reimplementing handlers.
 *
 * Single endpoint: POST /execute
 *   Body: { agent_id, tool_id, params }
 *   Response: { result: string } or { error: string }
 *
 * Listens on TOOL_SERVER_PORT (default 7702), localhost only.
 */
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { createLogger, toolCatalog, agentRegistry } from '@nexus/core';
import { executeAction } from './tool-executor.js';

const logger = createLogger('tool-server');
const PORT = parseInt(process.env.TOOL_SERVER_PORT ?? '7702', 10);

async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString('utf-8');
}

async function handleRequest(req: IncomingMessage, res: ServerResponse) {
  // CORS + JSON headers
  res.setHeader('Content-Type', 'application/json');

  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200);
    res.end(JSON.stringify({ status: 'ok' }));
    return;
  }

  if (req.method !== 'POST' || req.url !== '/execute') {
    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found. Use POST /execute' }));
    return;
  }

  try {
    const body = JSON.parse(await readBody(req));
    const { agent_id, tool_id, params } = body;

    if (!agent_id || !tool_id) {
      res.writeHead(400);
      res.end(JSON.stringify({ error: 'agent_id and tool_id are required' }));
      return;
    }

    // Load agent + granted tools for permission checking
    const agent = await agentRegistry.getAgent(agent_id);
    if (!agent) {
      res.writeHead(404);
      res.end(JSON.stringify({ error: `Agent not found: ${agent_id}` }));
      return;
    }

    const grantedTools = await toolCatalog.getToolsForAgent(agent_id);

    const result = await executeAction(
      agent_id,
      { action: tool_id, params: params ?? {}, reason: 'chat tool execution' },
      grantedTools,
      agent.autonomy_level,
    );

    res.writeHead(200);
    res.end(JSON.stringify({ result }));
  } catch (err) {
    logger.logMinimal('Tool execution error:', (err as Error).message);
    res.writeHead(500);
    res.end(JSON.stringify({ error: (err as Error).message }));
  }
}

export function startToolServer(): Promise<void> {
  return new Promise((resolve) => {
    const server = createServer(handleRequest);
    // Listen on all interfaces so Secondary-Server workers can reach it via Thunderbolt
    server.listen(PORT, '0.0.0.0', () => {
      logger.log(`Tool execution server listening on 127.0.0.1:${PORT}`);
      resolve();
    });
  });
}
