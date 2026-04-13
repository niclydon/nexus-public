export interface PlatformTool {
  tool_id: string;
  display_name: string;
  description: string;
  category: string;
  params_schema: Record<string, unknown>;
  risk_level: 'read_only' | 'state_changing' | 'external';
  approval_required: boolean;
  provider_type: 'internal' | 'mcp' | 'external';
  provider_config: Record<string, unknown>;
  is_active: boolean;
  created_at: Date;
}

export interface ToolGrant {
  agent_id: string;
  tool_id: string;
  granted_at: Date;
  granted_by: string;
}

export interface ToolRequest {
  id: string;
  agent_id: string;
  tool_id: string;
  reason: string | null;
  status: 'pending' | 'approved' | 'denied';
  reviewed_by: string | null;
  reviewed_at: Date | null;
  created_at: Date;
}
