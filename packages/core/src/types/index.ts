export type {
  Agent,
  AgentMemory,
  AgentDecision,
  AgentTask,
  PendingAction,
} from './agent.js';

export type {
  PlatformTool,
  ToolGrant,
  ToolRequest,
} from './tool.js';

export type {
  InboxMessage,
} from './comms.js';

export interface CallerIdentity {
  type: 'agent' | 'system' | 'user';
  id: string;
}
