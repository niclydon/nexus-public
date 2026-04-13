/**
 * Event listener — LISTEN/NOTIFY daemon for event-driven agent wake-ups.
 *
 * Listens on the 'nexus_inbox' channel. When an inbox message arrives,
 * triggers an immediate cycle for the recipient agent (if they're not
 * already mid-cycle).
 *
 * Uses a dedicated pg connection (not from the pool) since LISTEN
 * requires a persistent connection.
 */
import pg from 'pg';
import { createLogger } from '@nexus/core';
import { runAgentCycle } from './agent-runner.js';
import { platformMode } from '@nexus/core';

const logger = createLogger('event-listener');

// Track agents currently mid-cycle to prevent double-runs
const agentsInCycle = new Set<string>();

// Debounce: don't wake an agent more than once per interval
// ARIA gets a longer debounce to prevent feedback loops (agents → ARIA → agents → ...)
const lastWake = new Map<string, number>();
const DEFAULT_DEBOUNCE_MS = 30_000;
const AGENT_DEBOUNCE_MS: Record<string, number> = {
  aria: 120_000,     // 2 min — ARIA gets many messages, batch them
  analyst: 90_000,   // 90s — analyst has high empty cycle rate, slow it down
};

let client: pg.Client | null = null;
let reconnectTimer: NodeJS.Timeout | null = null;
let running = true;

/**
 * Register that an agent is currently in a cycle (called by the main worker loop).
 */
export function markAgentBusy(agentId: string): void {
  agentsInCycle.add(agentId);
}

export function markAgentFree(agentId: string): void {
  agentsInCycle.delete(agentId);
}

/**
 * Start listening for inbox notifications.
 */
export async function startEventListener(): Promise<void> {
  if (!running) return;

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    logger.logMinimal('DATABASE_URL not set, event listener disabled');
    return;
  }

  try {
    client = new pg.Client({ connectionString });
    await client.connect();
    await client.query('LISTEN nexus_inbox');
    logger.log('Listening on nexus_inbox channel');

    client.on('notification', (msg) => {
      if (!msg.payload) return;

      try {
        const data = JSON.parse(msg.payload) as {
          id: number;
          to_agent: string;
          from_agent: string;
          priority: number;
        };

        logger.logVerbose(`Inbox notification: ${data.from_agent} → ${data.to_agent} (priority ${data.priority})`);
        handleInboxEvent(data.to_agent, data.priority);
      } catch (err) {
        logger.logDebug('Failed to parse notification:', (err as Error).message);
      }
    });

    client.on('error', (err) => {
      logger.logMinimal('LISTEN connection error:', err.message);
      scheduleReconnect();
    });

    client.on('end', () => {
      logger.logVerbose('LISTEN connection closed');
      if (running) scheduleReconnect();
    });
  } catch (err) {
    logger.logMinimal('Failed to start event listener:', (err as Error).message);
    scheduleReconnect();
  }
}

function scheduleReconnect(): void {
  if (!running || reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    logger.log('Reconnecting event listener...');
    startEventListener();
  }, 5_000);
}

async function handleInboxEvent(toAgent: string, priority: number): Promise<void> {
  // Don't wake if already in a cycle
  if (agentsInCycle.has(toAgent)) {
    logger.logVerbose(`${toAgent} already in cycle, skipping wake`);
    return;
  }

  // Creative Mode: don't wake agents. Inbox messages stay unread and will be
  // picked up when the platform returns to 'normal' mode. Critical-priority
  // messages (priority>=3) bypass this guard so genuine emergencies can still
  // force an attempt (they'll likely fail LLM call but at least we try).
  if (priority < 3) {
    const mode = await platformMode.getPlatformMode();
    if (mode.mode === 'creative') {
      logger.logVerbose(`${toAgent} wake skipped — platform in creative mode`);
      return;
    }
  }

  // Debounce — don't wake more than once per 30s (unless critical priority)
  const now = Date.now();
  const last = lastWake.get(toAgent) ?? 0;
  const debounceMs = AGENT_DEBOUNCE_MS[toAgent] ?? DEFAULT_DEBOUNCE_MS;
  if (priority < 3 && (now - last) < debounceMs) {
    logger.logVerbose(`${toAgent} woken recently, debouncing`);
    return;
  }

  lastWake.set(toAgent, now);
  logger.log(`Waking ${toAgent} (inbox event, priority ${priority})`);

  try {
    markAgentBusy(toAgent);
    const result = await runAgentCycle(toAgent);
    logger.log(`Event-driven cycle: ${result.agentId} | ${result.steps} steps | ${result.actions.length} actions | ${result.durationMs}ms`);
  } catch (err) {
    logger.logMinimal(`Event-driven cycle failed for ${toAgent}:`, (err as Error).message);
  } finally {
    markAgentFree(toAgent);
  }
}

export async function stopEventListener(): Promise<void> {
  running = false;
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (client) {
    try {
      await client.end();
    } catch {
      // Already closed
    }
    client = null;
  }
  logger.log('Event listener stopped');
}
