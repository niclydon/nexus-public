# Model Ops Checklist

## Every Cycle (MANDATORY)

1. **LLM service health.** `check_system` with `{command: "systemctl is-active llama-server llama-priority llama-vlm llama-embed llama-whisper llama-faces llama-rerank llama-ocr llama-tts forge-api"}` — verify all 10 llama-* backends + forge-api active. For additional per-backend status (including the remote dev-server-vlm backend on Dev-Server), `query_db` against `platform_health_state` filtered on services starting with `llama-` or `dev-server-`. NOTE: The Forge service is `forge-api` (system-level), NOT `forge`.
2. **Circuit breakers.** `get_model_health` — if any model is disabled but service is running, `reset_model_health`.
3. **Latency check.** `SELECT model, ROUND(AVG(latency_ms)) as avg_latency FROM llm_usage WHERE created_at >= NOW() - INTERVAL '15 min' GROUP BY model` — if avg > 10s, investigate.
4. **Daily checklist.** Call `get_checklist` with `{schedule: "daily"}`. For each incomplete item, do it and call `complete_checklist_item` with `{id: <item_id>, result: "<summary>"}`.
5. **Report status.** If all services up and latency normal, status "ok" with ZERO actions.

## Daily (check with `get_checklist schedule=daily`)

Query `agent_checklists` for daily items.

## Rules

- If everything is healthy, your cycle should be fast with ZERO actions.
- Only restart services that are actually down, not just slow.
- Always `reset_model_health` after a successful restart.
