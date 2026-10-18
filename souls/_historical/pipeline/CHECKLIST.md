# Pipeline Checklist

## Every Cycle (MANDATORY)

1. **Sync handler health (REALTIME SOURCES ONLY).** `SELECT d.source_key, d.ingestion_mode, d.last_sync_at, d.status, d.consecutive_failures FROM data_source_registry d WHERE d.ingestion_mode = 'realtime' AND d.enabled = true ORDER BY d.last_sync_at ASC` — verify all REALTIME sources synced recently. Do NOT alert on manual or scheduled sources being stale — those are user-initiated imports (Facebook, Instagram, Google Voice, ChatGPT, Claude, Siri).
2. **Ingestion flow.** `SELECT source_key, COUNT(*) FROM ingestion_log WHERE ingested_at > NOW() - INTERVAL '1 hour' GROUP BY source_key` — verify data is flowing for realtime sources.
3. **Person identity resolution + auto-linking.** Check for obvious merge candidates and auto-resolve them:
   - `SELECT COUNT(*) FROM merge_candidates WHERE status = 'pending' AND entity_type = 'contact_person' AND confidence >= 0.9` — if >0, these are contacts with matching names AND shared identifiers. Auto-link them by calling `enqueue_job` with `{job_type: "data-hygiene", payload: {target: "auto-merge-contacts"}}`. Do NOT leave these for manual review — exact name + shared phone is a guaranteed match.
   - `SELECT COUNT(*) FROM merge_candidates WHERE status = 'pending' AND entity_type = 'person' AND confidence >= 0.95` — if >0, these are near-identical person records. Auto-merge by enqueuing the same job type.
   - Only flag candidates with confidence 0.6-0.9 for the owner's manual review. Everything above 0.9 should be auto-resolved.
   - Track: `SELECT COUNT(*) FROM aurora_social_identities WHERE is_person = TRUE` and `SELECT COUNT(*) FROM aurora_social_identity_links` — if counts drop unexpectedly, something is wrong.
3. **Enrichment backlogs — FILL THE QUEUE.** Check actual data gaps, not just pending job counts:
   - `SELECT COUNT(*) - COUNT(embedding) as gap FROM photo_metadata` — if gap > 500, enqueue gap/500 embed-backfill jobs (up to 50): `enqueue_job` with `{job_type: "embed-backfill", payload: {table: "photo_metadata", batch_size: 500}}`
   - `SELECT COUNT(*) - COUNT(embedding) as gap FROM knowledge_facts` — same pattern, up to 100 jobs
   - `SELECT COUNT(*) FROM knowledge_entities WHERE (summary IS NULL OR summary = '') AND fact_count > 3` — if >100, enqueue knowledge-summarize jobs with `{job_type: "knowledge-summarize", payload: {batch_size: 50}}`
   - **The autoscaler on Primary-Server and Secondary-Server spawns ephemeral workers when >50 jobs are pending.** Your job is to keep the queue fed so workers have work to do. Enqueue MANY jobs, not just one.
4. **Daily checklist.** Call `get_checklist` with `{schedule: "daily"}`. For each incomplete item, do it and call `complete_checklist_item` with `{id: <item_id>, result: "<summary>"}`.
5. **Report status.** Summarize what you found AND what you did about it.

## Rules

- Do NOT fetch Gmail, Calendar, or iMessage data yourself. The sync handlers do that.
- **Data source types matter.** Check `data_source_registry.ingestion_mode`:
  - `realtime` (imessage, gmail, calendar, looki, photos): Should sync continuously. Alert if stale >15 min.
  - `scheduled` (contacts, health): Syncs periodically. Alert if stale >24 hours.
  - `manual` (facebook, instagram, google_voice, chatgpt, claude, siri): User imports these. NEVER alert on staleness. Only report if asked.
- **Do NOT create anomaly alerts based on data from manual sources.** If Instagram DMs haven't been imported in 30 days, that's normal — not a relationship issue.
- **Flood the queue, let the autoscaler handle the rest.** When you find a backlog (unembedded records, unsummarized entities), enqueue as many jobs as needed to cover the full gap. The autoscaler on Primary-Server and Secondary-Server will automatically spawn ephemeral workers to process them and scale back to zero when done. Don't trickle — batch aggressively.
- Before enqueuing, check how many jobs are already pending: `SELECT COUNT(*) FROM tempo_jobs WHERE status = 'pending' AND job_type = '<type>'`. If already >50 pending, the autoscaler is already working on it — just monitor.
- **Watch for bulk job thrash-failures.** `SELECT job_type, COUNT(*) FROM tempo_jobs WHERE status = 'failed' AND last_error LIKE '%Stuck in processing%' AND failed_at > NOW() - INTERVAL '1 hour' GROUP BY job_type` — if a bulk job type has >10 thrash-failures per hour, the autoscaler may be killing workers mid-job. Escalate to the owner with the failing job type and count.
- Report what you enqueued and the expected backlog reduction.
- When backlogs exist, report what you enqueued, not just the count.
