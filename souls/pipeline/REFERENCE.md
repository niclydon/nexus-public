# Operational Reference

## Backfill Job Payloads

| Job Type | Payload | Self-Chains | Notes |
|---|---|---|---|
| `photo-describe` | `{batch_size: 50}` (backfill) or `{batch_size: 200}` (steady) | No | Backfill when >1000 undescribed, steady when <100 |
| `embed-backfill` | `{table: "table_name", batch_size: 500}` | Yes (via last_id) | Pick largest unembedded table first |
| `sentiment-backfill` | `{chunk_size: 50, triggered_by: "pipeline"}` | Yes | Lowest priority. Aborts at 30% error rate |
| `knowledge-backfill` | `{sources: ["photo_metadata"], batch_size: 200, generate_embeddings: true}` | Yes (between sources) | Modes: core_memory, photo_metadata, entity_geocode |
| `gmail-backfill` | `{batch_size: 50}` | Yes (via page_token) | Auto-triggers knowledge-backfill on completion |

## Source Freshness Thresholds

| Ratio (elapsed/expected) | Status | Action |
|---|---|---|
| < 2.0 | Healthy | None |
| 2.0 - 3.0 | Slightly stale | MONITOR — save to memory |
| 3.0 - 10.0 | Degraded | ACT — investigate cause |
| > 10.0 | Error | ESCALATE |

## Relay Health Red Flags

| Signal | Severity | Meaning |
|---|---|---|
| `daemon.api_unreachable = true` | Critical | Relay can't reach Nexus API |
| `daemon.needs_full_disk_access = true` | Critical | iMessage sync blocked |
| Any service `running = false` | Warning | Service crashed or stopped |
| `attachment-processing.crashed_out = true` | Critical | Toolkit circuit breaker hit |
| Snapshot `created_at > 5 min old` | Critical | Relay itself may be down |

## Embedding Tables (Priority Order)

1. `imessage_incoming` (message column) — most important for search
2. `photo_metadata` (description column) — photo search
3. `gmail_archive` (subject + body) — email search
4. `contacts` (display_name + notes) — contact lookup
5. `conversations`, `proactive_insights`, `looki_moments`, `life_narration`, `aria_journal`
