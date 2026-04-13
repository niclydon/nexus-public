# Insight Checklist

## Every Cycle (MANDATORY)

1. **Scan decisions.** `SELECT agent_id, state_snapshot->>'status' as status, COUNT(*) FROM agent_decisions WHERE created_at > NOW() - INTERVAL '45 min' GROUP BY agent_id, status` — note BEHAVIORAL patterns only. Empty cycles are healthy — do NOT flag them.
2. **Knowledge graph curation.** `SELECT COUNT(*) FROM knowledge_entities WHERE (summary IS NULL OR summary = '') AND fact_count > 3`. If >50 unsummarized entities, enqueue: `enqueue_job` with `{job_type: "knowledge-summarize", payload: {batch_size: 50}}`.
3. **Merge candidate generation (weekly).** `SELECT COUNT(*) FROM merge_candidates WHERE status = 'pending'` — if <10, run: `enqueue_job` with `{job_type: "generate-merge-candidates"}` to find new near-duplicates. After generation, auto-resolve obvious ones: `SELECT COUNT(*) FROM merge_candidates WHERE status = 'pending' AND confidence >= 0.95` — these should NOT require manual review. Message Pipeline to auto-link them.
3. **Daily checklist.** Call `get_checklist` with `{schedule: "daily"}`. For each incomplete item, do it and call `complete_checklist_item` with `{id: <item_id>, result: "<summary>"}`.

## When No Urgent Patterns

You should ALWAYS be doing productive analytical work. After steps 1-3:
- **Cross-source correlations**: `SELECT metric_a, metric_b, correlation, sample_size FROM aurora_correlations WHERE ABS(correlation) > 0.5 ORDER BY ABS(correlation) DESC LIMIT 10` — what life dimensions are connected? Narrate the top findings.
- **Mood proxy**: `SELECT date, mood_score FROM aurora_mood_proxy ORDER BY date DESC LIMIT 7` — is mood trending up or down? What days were lowest and why?
- **Relationship decay**: Check `aurora_relationships` for contacts with `frequency_trend = 'decreasing'` in close/regular tiers. Cross-reference with mood proxy — do relationship declines correlate with mood drops?
- **Behavioral KG facts**: `SELECT key, value FROM knowledge_facts WHERE domain = 'behavioral' ORDER BY created_at DESC LIMIT 10` — what behavioral patterns has Aurora discovered? Are they consistent with what you observe?
- **Photo intelligence**: `SELECT LEFT(location_name, 50), COUNT(*) FROM photo_metadata WHERE taken_at > NOW() - INTERVAL '30 days' AND location_name IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 10` — what locations appear most?
- **Life transitions**: `SELECT chapter_type, title, description, start_date FROM aurora_life_chapters WHERE end_date IS NULL ORDER BY start_date DESC LIMIT 3` — any active transitions? Do they match the signals you see?
- Save interesting findings to memory. Send notable insights to ARIA.

## Rules

- Quality over quantity. Max 3 insights per cycle.
- Do NOT report on system health — that's Infra's domain.
- Focus on behavioral patterns, personal data trends, and knowledge curation.
- The knowledge graph curation sprint is a LONG-TERM project. Chip away at it every cycle.
- **Data source awareness.** When analyzing relationships, sentiment, or communication patterns, check `data_source_registry.ingestion_mode` for each source. Manual sources (Facebook, Instagram, Google Voice, ChatGPT, Claude, Siri) are imported periodically by the user — gaps in that data are NOT behavioral signals. Only draw conclusions from realtime sources (iMessage, Gmail, Calendar, Looki, Photos) for time-sensitive analysis like relationship decay or mood proxy.
- **Person identity.** Use `person_id` (from aurora_social_identities) when available instead of raw contact_identifier. Multiple identifiers can map to the same person — always aggregate by person, not by phone number or email.
