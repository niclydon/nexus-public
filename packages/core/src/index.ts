// Infrastructure
export { getPool, query, shutdown } from './db.js';
export { createLogger } from './logger.js';
export {
  assertShowcaseDemoEnabled,
  isShowcaseDemoEnabled,
  isShowcaseDangerousToolsAllowed,
} from './showcase.js';

// Middleware
export { authMiddleware, requireCaller } from './middleware/auth.js';

// Services
export * as agentRegistry from './services/agent-registry.js';
export * as toolCatalog from './services/tool-catalog.js';
export * as comms from './services/comms.js';
export * as soulLoader from './services/soul-loader.js';
export * as evalScorer from './services/eval-scorer.js';
export * as entityMerge from './services/entity-merge.js';
export * as semanticSearch from './services/semantic-search.js';
export * as platformMode from './services/platform-mode.js';

// Types
export type {
  Agent,
  AgentMemory,
  AgentDecision,
  AgentTask,
  PendingAction,
  PlatformTool,
  ToolGrant,
  ToolRequest,
  InboxMessage,
  CallerIdentity,
} from './types/index.js';
