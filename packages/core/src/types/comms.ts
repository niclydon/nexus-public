export interface InboxMessage {
  id: number;
  to_agent: string;
  from_agent: string;
  message: string;
  context: Record<string, unknown> | null;
  priority: number;
  trace_id: string | null;
  read_at: Date | null;
  acted_at: Date | null;
  created_at: Date;
}
