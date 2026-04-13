# Nexus Database Schema Reference

**Database:** `nexus` on Primary-Server (localhost:5432, user `dbuser`)
**Schema Version:** 54 (migrations 001–054 applied; 014 intentionally skipped)
**Last updated:** 2026-04-11

---

## Migration History

### Phase 1 — Foundation (001-019)

| # | File | What It Does |
|---|---|---|
| 1 | `001-foundation.sql` | 15 core tables: agent_registry, inbox, memory, decisions, tasks, pending_actions, tools, grants, requests, config, audit, prompt_history, journals, llm_log, schema_version |
| 2 | `002-seed-keeper.sql` | Seed Keeper agent + 7 platform tools (query_db, check_system, etc.) |
| 3 | `003-alter-existing-tables.sql` | Add Nexus columns to existing Chancery tables (executor, schedule_interval_sec, API key fields, etc.) |
| 4 | `004-tempo-executor-column.sql` | Add `executor` column to tempo_jobs for dual-worker routing |
| 5 | `005-nexus-executor-routing.sql` | `nexus_job_routing` table + trigger to auto-route jobs |
| 6 | `006-v15-agent-prompts.sql` | Register v1.5 agents (Collector, COS, Relationships, etc.) |
| 7 | `007-v2-agent-consolidation.sql` | Consolidate 12 agents → 7 active (Monitor, Model Ops, Collector, Analyst, Fixer, Relationships, ARIA) |
| 8 | `008-v2-system-prompts.sql` | Load v2 system prompts + platform preamble into agent_config |
| 9 | `009-inbox-notify.sql` | LISTEN/NOTIFY trigger on agent_inbox for event-driven wake-ups |
| 10 | `010-expanded-tools.sql` | 6 new tools (enqueue_job, create/update_task, list/get_agent, deliver_report, forge_health) |
| 11 | `011-fixer-tools.sql` | 6 Fixer tools (read/write/edit_file, git_status, git_commit_push, run_build, create_pr) |
| 12 | `012-pushover.sql` | Pushover notification tool + delivery method |
| 13 | `013-mcp-client.sql` | `mcp_servers` table + list/install MCP server tools |
| — | 014 skipped | |
| 15 | `015-ingestion-pipeline.sql` | `ingestion_log`, `enrichment_config`, `proactive_rules` tables |
| 16 | `016-soul-spec.sql` | Soul Spec columns on agent_registry (soul_spec_version, soul_package_path, disclosure_summary) |
| 17 | `017-working-memory-and-tool-approval.sql` | `working_memory` column, `approval_required` on tools, `agent_evals` table |
| 18 | `018-delegate-and-consolidate.sql` | `delegate_task` + `consolidate_memories` tools |
| 19 | `019-richer-interrupts.sql` | Rich interrupt schema: response_type, modified_params, response_text, allow_edit/respond/ignore |

### Phase 2 — Identity, Chat, Biographical Data (020-036)

| # | File | What It Does |
|---|---|---|
| 20 | `020-decision-feedback.sql` | Human feedback on agent decisions (thumbs up/down + notes) |
| 21 | `021-aria-chat.sql` | ARIA chat infrastructure — `channels` + `channel_messages` tables |
| 22 | `022-fix-pending-actions-status.sql` | Allow 'executed' status on `agent_pending_actions` (was crashing 3 agents for 3 hours) |
| 23 | `023-identity-is-person.sql` | `is_person` flag on `aurora_social_identities` |
| 24 | `024-gmail-kg-ingestion.sql` | `kg_ingested_at` on `aurora_raw_gmail` for knowledge-graph backfill tracking |
| 25 | `025-mood-proxy.sql` | `aurora_mood_proxy` table for behavioral mood estimation |
| 26 | `026-canonical-person.sql` | `aurora_social_identity_links` + `resolve_person_identity()` PL/pgSQL function |
| 27 | `027-merge-candidates.sql` | `merge_candidates` + `merge_audit_log` tables for entity dedup review |
| 28 | `028-voice-metrics.sql` | Per-person voice / speaking stats |
| 29 | `029-communication-style.sql` | Per-person communication style features and cluster labels |
| 30 | `030-apple-notes.sql` | `apple_notes` table |
| 31 | `031-apple-music.sql` | `music_library`, `music_sessions`, `life_event_soundtracks` |
| 32 | `032-biographical-interview.sql` | Extend `life_chapters`, add `bio_inference_*` tables |
| 33 | `033-cross-correlation-analytics.sql` | `aurora_correlations` (cross-metric normalized daily values) |
| 34 | `034-person-social-profiles.sql` | Social profile links per identity |
| 35 | `035-apple-health-records.sql` | `aurora_raw_health_records` schema + indexes |
| 36 | `036-owner-guestbook.sql` | Extract and index 2004–2005 `example-personal.com` guestbook messages |

### Phase 3 — Enrichment, Indexes, Analytics (037-046)

| # | File | What It Does |
|---|---|---|
| 37 | `037-archive-enrichment.sql` | Enrichment columns (sentiment, KG flags) on archive tables |
| 38 | `038-social-enrichment.sql` | Enrichment columns on social/messaging archive tables |
| 39 | `039-music-sessions-soundtracks.sql` | Music session detection + life event soundtracks |
| 40 | `040-enrichment-infrastructure.sql` | `enrichment_config` expansions + 9 unified materialized views including `aurora_unified_communication` |
| 41 | `041-vector-indexes.sql` | HNSW cosine indexes on all embedding columns |
| 42 | `042-life-chapters.sql` | Real life chapters + life arc synthesis |
| 43 | `043-topic-modeling.sql` | `topic_clusters` + `topic_assignments` (k-means, 2-level hierarchy) |
| 44 | `044-apple-music-plays.sql` | `apple_music_plays` — Biome SEGB plays from iOS |
| 45 | `045-platform-health.sql` | Platform health alerting — `platform_health_targets`, `platform_health_state`, `platform_health_checks`, `platform_health_alerts` |
| 46 | `046-creative-mode.sql` | `platform_modes` singleton table for Creative Mode flag |

### Phase 4 — Creative Mode, Observability, Data Hygiene (047-053)

| # | File | What It Does |
|---|---|---|
| 47 | `047-platform-mode-history.sql` | `platform_mode_history` audit table + trigger — logs every creative-mode entry/exit with duration |
| 48 | `048-identifier-normalization.sql` | `normalize_identifier()` IMMUTABLE SQL function, `identifier_normalized` generated column on `aurora_social_identity_links`, aurora_unified_communication matview rewrite that decouples `identity_id` (person-gated) from `contact_name` (best-effort display name) |
| 49 | `049-health-watchdog-v2-targets.sql` | Health watchdog `command` check type + 5 resource targets: `disk-primary-server`, `disk-secondary-server`, `memory-primary-server`, `memory-secondary-server`, `postgres-connections` |
| 50 | `050-autoscaler-events.sql` | `autoscaler_events` table — spawn/exit/scale_up/scale_down/scale_to_zero event log per host for Chancery /health dashboard |
| 51 | `051-email-recipient-cleanup.sql` | `clean_email_recipient()` SQL function + matview rewrite — flips "Last, First" → "First Last" on archived_sent_emails contact_name (1,208 rows gained a readable name) |
| 52 | `052-forge-backend-targets.sql` | 6 new HTTP targets for previously-unmonitored Forge backends: whisper, faces, rerank, ocr, tts, dev-server-vlm (total watchdog targets 21 → 27) |
| 53 | `053-decommission-pgbouncer.sql` | Removes `pgbouncer` target from `platform_health_targets`. Service was stopped + disabled at migration time — was installed but never actually used |
| 54 | `054-drop-aurora-raw-photos-embedding.sql` | Drop `aurora_raw_photos.embedding` column + HNSW index. Table was deprecated during the nexus migration (data lives in `photo_metadata`) but migration 041 still created an index and embed-backfill.ts still had it in TABLE_CONFIGS, causing it to show up as 0% coverage in `/data-quality`. Row data is preserved |

---

## Core Agent Tables

### agent_registry
Primary agent configuration. 32 columns.

Key columns: `agent_id` (PK), `display_name`, `role`, `system_prompt`, `schedule_interval_sec`, `autonomy_level` (high/moderate/approval_gated), `executor` (chancery/nexus), `is_active`, `working_memory`, `soul_spec_version`, `soul_package_path`, `disclosure_summary`, `api_key_hash`, `last_check_in`

### agent_inbox
Agent messaging. LISTEN/NOTIFY trigger fires on INSERT.

Key columns: `to_agent`, `from_agent`, `message`, `context` (JSONB), `priority`, `trace_id`, `read_at`, `acted_at`

### agent_memory
Persistent knowledge with confidence scoring.

Key columns: `agent_id`, `memory_type`, `category`, `content`, `confidence` (0-1), `times_reinforced`, `superseded_by`, `is_active`

### agent_decisions
Full cycle trace logs.

Key columns: `agent_id`, `trace_id`, `state_snapshot` (JSONB), `user_message`, `llm_response` (JSONB), `parsed_actions` (JSONB), `execution_results` (JSONB), `duration_ms`

### agent_tasks
Work assignment and tracking.

Key columns: `title`, `assigned_to`, `assigned_by`, `status` (open/in_progress/completed/blocked/cancelled), `priority`, `parent_task_id`, `trace_id`, `result`

### agent_pending_actions
Approval queue with rich interrupt schema (migration 19).

Key columns: `agent_id`, `action_type`, `params` (JSONB), `status` (pending/approved/rejected/expired), `response_type` (accept/edit/respond/ignore), `modified_params` (JSONB), `response_text`, `allow_edit`, `allow_respond`, `allow_ignore`

### agent_evals
Quality scoring results (migration 17).

Key columns: `agent_id`, `trace_id`, `scorer` (task_completion/faithfulness/relevancy/hallucination/coherence), `score` (0-1), `explanation`

---

## Tool Tables

### platform_tools
Unified tool catalog. ~45 tools registered.

Key columns: `tool_id` (PK), `display_name`, `description`, `category`, `params_schema` (JSONB), `risk_level` (read_only/state_changing/external), `approval_required` (boolean), `provider_type` (internal/mcp/external), `provider_config` (JSONB)

### agent_tool_grants
Per-agent tool permissions. PK: (agent_id, tool_id).

### tool_requests
Tool access request queue with approval workflow.

---

## Infrastructure Tables

### mcp_servers (migration 13)
MCP server registry for external tool providers.

Key columns: `server_name` (UNIQUE), `install_type` (npm/pip/docker/binary/builtin), `transport` (stdio/sse/streamable_http), `command`, `args`, `env_vars` (JSONB), `status`, `tools_provided` (TEXT[])

### ingestion_log (migration 15)
Unified data ingestion index. Dedup: UNIQUE(source_key, source_id).

Key columns: `source_key`, `source_id`, `timestamp`, `category` (email/message/event/photo/health/location/social/document/contact/activity), `summary`, `entities` (JSONB), `proactive_checked`, `enrichments_queued`

### enrichment_config (migration 15)
Category-to-enrichment mapping. 14 seed entries.

### proactive_rules (migration 15)
SQL-based proactive insight triggers. 2 seed rules.

### nexus_job_routing (migration 5)
Auto-routes job types to nexus executor. 30+ job types routed as of phase 3.

---

## Platform Health Tables (migration 45, 49, 52, 53)

### platform_health_targets
Registry of services to monitor. Check types: `http` / `systemd` / `tcp` / `psql` / `command`. Currently 26 enabled targets (up from 12 at migration 45) covering core services, all 10 LLM backends, disk/memory/postgres resource checks, and zombie guards.

Key columns: `service` (PK), `display_name`, `check_type`, `endpoint`, `expected`, `timeout_ms`, `severity_on_down` (critical/warning), `alert_after_failures`, `mute_until`, `enabled`.

### platform_health_state
Current per-service state. One row per service, UPSERT'd every watchdog tick (60s).

Key columns: `service` (PK), `current_status` (ok/down/degraded/unknown), `status_since`, `last_checked_at`, `consecutive_failures`, `last_alert_sent_at`, `detail`.

### platform_health_checks
Append-only history of every check result, indexed for trend queries.

### platform_health_alerts
Audit trail of every Pushover alert sent, with `fingerprint`-based dedup and `resolved_at` auto-fill on recovery.

---

## Platform Mode Tables (migrations 46, 47)

### platform_modes
Singleton (`id=1`) holding the current platform-wide mode flag (`normal` | `creative`). Read by health watchdog, nexus-worker agent loop, log-monitor handler, nexus-api `/health`, and Chancery dashboard banner.

### platform_mode_history
Audit trail of every mode transition. Trigger on `platform_modes` UPDATE closes the previous row and opens a new one. Partial unique index enforces at most one open session.

Key columns: `mode`, `started_at`, `ended_at`, `started_by`, `reason`, `duration_sec` (generated).

---

## Autoscaler Observability (migration 50)

### autoscaler_events
Per-host worker lifecycle log. Rows inserted from `packages/worker/src/autoscaler.ts` on spawn/exit/scale_up/scale_down/scale_to_zero. Powers the Autoscaler section on the Chancery `/health` dashboard.

Key columns: `host` (hostname tag from `os.hostname()`), `event_type`, `worker_pid`, `exit_code`, `alive_count`, `queue_depth`, `detail`, `occurred_at`.

---

## Merge Review (migration 27)

### merge_candidates
Near-duplicate records queued for human review. Entity types: `person`, `knowledge_entity`, `photo_person`, `contact_person`.

Key columns: `entity_type`, `record_a_id`, `record_b_id`, `record_a_name`, `record_b_name`, `confidence`, `match_reason` (JSONB), `status` (pending/merged/dismissed/deferred).

### merge_audit_log
Pre-merge snapshots for undo. 30-day retention.

Key columns: `entity_type`, `keeper_id`, `merged_id`, `keeper_name`, `merged_name`, `snapshot` (JSONB), `merged_at`, `undone_at`.

---

## Topic Modeling (migration 43)

### topic_clusters
Hierarchical k-means clusters. 40 L1 (level=1) clusters, 470 L2 leaf clusters. Centroids indexed with HNSW cosine.

Key columns: `id`, `parent_id`, `level`, `cluster_index`, `label`, `description`, `keywords` (TEXT[]), `centroid` (vector(768)), `member_count`, `time_range` (tstzrange), `source_distribution` (JSONB).

### topic_assignments
569k+ item → cluster assignments. Only populated at leaf level (L2).

Key columns: `cluster_id`, `source_table`, `source_id`, `distance`. UNIQUE on (source_table, source_id).

---

## Canonical Person (migration 26)

### aurora_social_identity_links
Identifiers (phones, emails, usernames) linked to identity rows. `identifier_normalized` generated column (migration 48) holds a canonical-form copy via `normalize_identifier()` for case-insensitive email + E.164-phone matching.

Key columns: `identity_id` (FK to aurora_social_identities), `platform`, `identifier`, `identifier_normalized` (GENERATED, indexed), `identifier_type`, `match_confidence`.

---

## Functions (migrations 26, 48, 51)

| Function | Language | Returns | Purpose |
|---|---|---|---|
| `resolve_person_identity(…)` | PL/pgSQL | integer | Identity resolution at ingestion — deterministic match → cross-platform link → auto-create. Called by sync handlers. |
| `normalize_identifier(text, text)` | SQL IMMUTABLE | text | Canonicalize phones (strip formatting, add +1/+ prefixes) and emails (lowercase, strip display-name wrappers) for cross-source matching. |
| `clean_email_recipient(text)` | SQL IMMUTABLE | text | Parses [Employer] `to_recipients` — flips "Last, First" → "First Last", strips quote wrappers, lowercases emails. |
| `platform_mode_history_log()` | PL/pgSQL | trigger | Closes open history row + opens new one on `platform_modes` UPDATE when mode actually changes. |

---

## Triggers & Functions

| Trigger | Table | Function | Purpose |
|---|---|---|---|
| `trg_inbox_notify` | agent_inbox | `notify_inbox_insert()` | LISTEN/NOTIFY on inbox insert |
| `trg_route_nexus_jobs` | tempo_jobs | `route_nexus_jobs()` | Auto-set executor='nexus' for routed job types |
| `trg_platform_mode_history` | platform_modes | `platform_mode_history_log()` | Log mode transitions to platform_mode_history |

---

## Unified Materialized Views (migration 40)

9 matviews refreshed nightly that normalize data across raw sources:

| View | Rows | Purpose |
|---|---|---|
| `aurora_unified_health` | ~4M | Apple Health records, health_data, summaries, workouts, Strava |
| `aurora_unified_communication` | ~202k | iMessage, Gmail, Instagram, Facebook, Google Voice, BB SMS, archived emails, guestbook (with identity_id + contact_name via aurora_social_identity_links lookup). Rewritten in 048 + 051 to decouple `identity_id` (person-gated) from `contact_name` (best-effort). |
| `aurora_unified_media` | ~157k | Spotify, Last.fm, Netflix, Apple Music |
| `aurora_unified_location` | ~91k | Device GPS, photo GPS, Looki |
| `aurora_unified_social` | ~75k | Instagram, Facebook |
| `aurora_unified_calls` | ~44k | FaceTime, Twilio, Google Voice |
| `aurora_unified_ai_conversations` | ~33k | ChatGPT, Claude, Siri |
| `aurora_unified_financial` | ~6k | life_transactions |
| `aurora_unified_travel` | ~347 | Flights, wallet passes, trips |

---

## Indexes

Performance-critical indexes on: inbox unread messages, active memories (by confidence), decisions (by agent + trace), open tasks, pending approvals, eval scores (low scores), ingestion log (unprocessed items), tempo jobs (nexus polling), `platform_health_checks` (service, checked_at DESC), `autoscaler_events` (host, event_type, occurred_at DESC), `aurora_social_identity_links.identifier_normalized`, HNSW cosine indexes on every `embedding` column across 32 tables (migration 41).
