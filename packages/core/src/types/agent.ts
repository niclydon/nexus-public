export interface Agent {
  agent_id: string;
  display_name: string;
  role: string;
  description: string | null;
  avatar_url: string | null;
  system_prompt: string | null;
  schedule: string | null;
  schedule_interval_sec: number | null;
  script: string | null;
  autonomy_level: 'high' | 'moderate' | 'approval_gated';
  personality_mode: 'full' | 'warm' | 'functional';
  is_active: boolean;
  headline: string | null;
  bio: string | null;
  availability: 'working' | 'ooo';
  executor: 'chancery' | 'nexus';
  api_key_hash: string | null;
  api_key_prefix: string | null;
  api_key_created_at: Date | null;
  last_check_in: Date | null;
  // A2A Agent Card fields
  skills: string[];
  // Soul Spec fields
  soul_spec_version: string | null;
  soul_package_path: string | null;
  disclosure_summary: string | null;
  // Working Memory — short-term structured state persisted across cycles
  working_memory: string | null;
  created_at: Date;
  updated_at: Date;
}

export interface AgentMemory {
  id: number;
  agent_id: string;
  memory_type: string;
  category: string | null;
  content: string;
  confidence: number;
  times_reinforced: number;
  source: string | null;
  superseded_by: number | null;
  created_at: Date;
  last_accessed_at: Date;
  is_active: boolean;
}

export interface AgentDecision {
  id: number;
  agent_id: string;
  trace_id: string;
  state_snapshot: Record<string, unknown> | null;
  system_prompt_hash: string | null;
  user_message: string | null;
  llm_response: Record<string, unknown> | null;
  parsed_actions: Record<string, unknown>[] | null;
  execution_results: Record<string, unknown>[] | null;
  duration_ms: number | null;
  created_at: Date;
}

export interface AgentTask {
  id: number;
  title: string;
  description: string | null;
  assigned_to: string;
  assigned_by: string;
  status: 'open' | 'in_progress' | 'completed' | 'blocked' | 'cancelled';
  priority: number;
  parent_task_id: number | null;
  source_inbox_id: number | null;
  trace_id: string | null;
  result: string | null;
  created_at: Date;
  updated_at: Date;
  completed_at: Date | null;
}

export interface PendingAction {
  id: number;
  agent_id: string;
  trace_id: string | null;
  action_type: string;
  params: Record<string, unknown>;
  reason: string | null;
  status: 'pending' | 'approved' | 'rejected' | 'expired';
  // Richer interrupt fields
  response_type: 'accept' | 'edit' | 'respond' | 'ignore' | null;
  modified_params: Record<string, unknown> | null;
  response_text: string | null;
  description: string | null;
  allow_edit: boolean;
  allow_respond: boolean;
  allow_ignore: boolean;
  decided_by: string | null;
  decided_at: Date | null;
  decision_reason: string | null;
  created_at: Date;
}
