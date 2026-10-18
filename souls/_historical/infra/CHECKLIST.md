# Monitor Checklist

## Every Cycle (MANDATORY)

1. **Service health.** `check_system` with `{command: "systemctl is-active nexus-api nexus-worker nexus-mcp nexus-site forge-api chancery-web"}` — verify all active. If any shows "inactive" or "failed", report. For a richer view, `query_db` against `platform_health_state` to see all 26 monitored targets including LLM backends, disk, memory, and postgres-connections. NOTE: The Forge service is `forge-api` (system-level), NOT `forge` (stale user-level unit). The old `nexus-bulk` service was retired — bulk work runs on `secondary-server-bulk` on Secondary-Server, check it via the autoscaler_events table instead.
2. **Job queue.** `get_queue_status` — check for stuck jobs (processing > 20 min). If found, `requeue_stuck_jobs`. Also check for autoscaler thrash: `SELECT job_type, COUNT(*) FROM tempo_jobs WHERE status = 'failed' AND last_error LIKE '%Stuck in processing%' AND failed_at > NOW() - INTERVAL '1 hour' GROUP BY job_type` — if any bulk job type has >10 thrash-failures in an hour, escalate to the owner (likely autoscaler killing workers mid-job).
3. **Agent adherence.** Check peer schedules in your state message. Only flag agents marked ⚠️ OVERDUE.
4. **Disk/memory.** `check_system` with `{command: "df -h / && free -m"}` — if disk > 90% or memory > 95%, report.
5. **Daily checklist.** Call `get_checklist` with `{schedule: "daily"}`. For each incomplete item, do it and call `complete_checklist_item` with `{id: <item_id>, result: "<summary>"}`.
6. **Report status.** If everything is healthy, status "ok" with ZERO actions. Do NOT escalate healthy systems.

## Daily (check with `get_checklist schedule=daily`)

Query `agent_checklists` for daily items.

## Rules

- If everything is healthy, your cycle should take <10 seconds and produce ZERO actions.
- Do NOT check LLM services — that's Inference's job.
- Do NOT check data pipelines — that's Pipeline's job.
- Only escalate if something is actually broken, not degraded or slightly late.
