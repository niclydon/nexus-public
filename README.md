# Nexus

A distributed agent architecture platform that orchestrates autonomous AI agents across infrastructure monitoring, data pipeline management, knowledge graph curation, and personal intelligence.

Built as a solo project to unify multiple existing systems into a single ecosystem with centralized services for tools, jobs, knowledge, communication, agent runtime, and discovery.

> **Snapshot: October 18, 2026.** This repository is a dated public snapshot of a
> private system that keeps moving. It is not a mirror and it is not kept in
> lockstep with the private codebase. Counts, rosters, and architectures below
> describe the code in this snapshot unless a section is explicitly dated
> otherwise. See [SNAPSHOT.md](SNAPSHOT.md) for provenance and scrub method, and
> [docs/what-changed-since-2026-08.md](docs/what-changed-since-2026-08.md) for
> what the private platform has done since the previous snapshot
> (August 25, 2026).

## Public Showcase Safety

This repository is a **public showcase** of a private system and is intentionally **non-production**.

- All three services (`@nexus/api`, `@nexus/worker`, `@nexus/mcp`) refuse to start unless `NEXUS_SHOWCASE_DEMO=true` is set. See `packages/core/src/showcase.ts`.
- When running in showcase mode, the tool executor blocks a curated set of high-impact tools by default:
  `check_system`, `query_db`, `read_file`, `write_file`, `edit_file`, `run_build`, `git_commit_push`, `create_pr`, `restart_service`, `restart_user_service`, `install_mcp_server`, `install_skill`, `send_email`.
- To re-enable those paths for private lab testing only, also set `NEXUS_SHOWCASE_ALLOW_DANGEROUS_TOOLS=true`.

If you are evaluating the architecture, read and inspect the code. Do not treat this repository as deploy-ready software — it was lifted out of a personal lab, stripped of its data, and is not intended to be run end-to-end.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         @nexus/api                               │
│                    Express REST API (:7700)                       │
│          Agents · Tools · Inbox · Approvals · Search             │
└──────────────────────┬───────────────────────────────────────────┘
                       │
┌──────────────────────┴───────────────────────────────────────────┐
│                         @nexus/core                              │
│   Database · Auth · Agent Registry · Tool Catalog · Soul Loader  │
│   Semantic Search · Entity Merge · Eval Scoring · Comms          │
└──────────────────────┬───────────────────────────────────────────┘
                       │
┌──────────────────────┴───────────────────────────────────────────┐
│                        @nexus/worker                             │
│  Agent Cycle Runner · Job Worker · Tool Executor · Autoscaler    │
│  Event Listener · MCP Client Manager · 96 Job Handlers (snapshot)          │
│  LLM Router (multi-provider fallback chain)                      │
└──────────────────────────────────────────────────────────────────┘
                       │
┌──────────────────────┴───────────────────────────────────────────┐
│                         @nexus/mcp                               │
│              MCP Server: 27 tools across 9 domains (snapshot + Oct 2026 delta)              │
│        HTTP + stdio transport for Claude Desktop integration     │
└──────────────────────────────────────────────────────────────────┘
```

## Current Direction

This public repo includes historical artifacts from earlier phases of Nexus, but
the current direction is more constrained:

- agents are bounded reasoning and coordination layers;
- processors, handlers, and migrations do deterministic execution;
- source onboarding and delivery surfaces are treated as explicit contracts.

See `docs/agent-doctrine.md`, `docs/adding-a-source.md`, and
`docs/dispatch-spec.md` for the current public-safe descriptions.

## Key Concepts

### Agent Runtime

Agents are config-driven rows in `agent_registry`, run as short-lived cycles on schedule or event triggers. Each cycle: load config → gather state → assemble prompt → call LLM → parse actions → execute tools (up to 5 rounds) → log decision.

Agent personas use the [ClawSouls Soul Spec v0.5](https://github.com/clawsouls/soulspec/blob/main/soul-spec-v0.5.md) with progressive disclosure (3 levels of context loading based on cycle needs).

### Agents in this snapshot (8)

This roster is historical. Six of these eight personas (Chronicler, Circle,
Coder, Inference, Infra, Pipeline) have since been retired or re-scoped in the
private system; their packages now live under `souls/_historical/`. ARIA and
Insight continue. The current private roster is described in
`docs/what-changed-since-2026-08.md`.

| Agent | Role | Schedule |
|-------|------|----------|
| **ARIA** | Chief of Staff — owner interface, briefings, team coordination | 10 min |
| **Pipeline** | Data flow — sync handlers, ingestion, enrichment backlogs | 5 min |
| **Infra** | Infrastructure — services, jobs, disk, memory, security | 12 min |
| **Inference** | LLM services — model health, circuit breakers, VRAM, latency | 18 min |
| **Coder** | Code quality — fix requests, dependency audits, PRs | 20 min |
| **Insight** | Patterns + knowledge — behavioral analysis, KG curation | 45 min |
| **Circle** | Social intelligence — relationship health, communication patterns | 6 hours |
| **Chronicler** | Biographical intelligence — life story extraction, interviews | 3 hours |

### Three-Layer Job System

- **Layer 1**: Platform jobs — worker polls `tempo_jobs` with `FOR UPDATE SKIP LOCKED`
- **Layer 2**: Agent-registered handlers — agents subscribe to job types, delivered via inbox
- **Layer 3**: Agent tasks — ad-hoc delegated work picked up during next cycle

### Tool Catalog (snapshot era)

At the time of the code in this snapshot, the platform catalog held ~360 tools total: ~50 internal handlers + ~310 MCP tools from 10 connected servers (Gmail, GitHub, Cloudflare, Notion, Google Drive, Apple Photos, iMessage, Brave Search, etc.). Per-agent grants with risk-based approval gating.

### LLM Router

Multi-provider fallback chain with circuit breakers. Routes through local inference (primary) with cloud fallback (Anthropic, OpenAI, Google). Three tiers: reasoning, generation, classification — each with independent provider chains.

### Distributed Autoscaler

Primary worker handles agent cycles and priority work. Bulk processing (embeddings, sentiment analysis, knowledge graph extraction) runs on a satellite node with an autoscaler that monitors queue depth and spawns ephemeral workers on demand. Scale-to-zero when idle.

### Knowledge Pipeline

```
Source → Normalizer → ingestion_log → source table → proactive rules → enrichment queue
                                                           ↓
                                              sentiment · embeddings · KG extraction
```

14 data sources with configurable sync intervals. Proactive Intelligence Engine (PIE) classifies significance and generates anticipatory insights.

## Where the private system is now (October 2026)

The private platform this snapshot was taken from has moved well past it:

- a cross-runtime coordination layer (registry, conditions, stream) now sits
  between the agents, replacing direct seat-to-seat assumptions;
- the knowledge graph was rebuilt in fall 2026, and graph writes now run only
  through governed paths with per-case approval for identity merges;
- work runs through an operator backlog with typed lanes, including a
  no-code data-task lane drained by a dedicated worker;
- the agent roster was re-scoped: a chief-of-staff seat owns triage, routing,
  and follow-through, a platform-operations seat owns estate telemetry, and
  several snapshot-era personas were retired;
- the private MCP surface has grown from the two dozen tools of the original
  snapshot to several hundred, and the private schema is in the thousands
  of migrations. This refresh adds one representative piece of that newer
  work: the agent-bus coordination tools (packages/mcp/src/tools/agent-bus.ts)
  with their schema (migrations/058-agent-bus.sql).

None of that code is in this repository. This snapshot stays useful as a
readable, self-contained picture of the architecture at a point in time, and
future refreshes will follow the same rule: dated snapshots with a provenance
record, never a live mirror.

## Tech Stack

- **Language**: TypeScript (Node.js 20+)
- **API**: Express
- **Database**: PostgreSQL (~250 tables, materialized views, LISTEN/NOTIFY)
- **LLM**: OpenAI-compatible local inference + cloud fallback
- **MCP**: `@modelcontextprotocol/sdk` for tool integration
- **Embeddings**: nomic-embed (768 dimensions) with HNSW indexes
- **Deployment**: systemd services on Linux

## Monorepo Structure

```
packages/
  core/       Shared services, types, database, auth, migrations
  api/        Express REST API server (port 7700)
  mcp/        MCP server (port 7701): 27 tools, HTTP + stdio
  worker/     Agent runner, job worker, tool executor, autoscaler

souls/        Agent persona packages (Soul Spec v0.5); retired personas under souls/_historical/
migrations/   PostgreSQL schema migrations (57 files in this snapshot)
systemd/      Service unit files
scripts/      Deployment, health monitoring, utilities
workers/      Cloudflare Workers (email inbound routing)
docs/         Architecture documentation
```

## Commands

```bash
npm run build          # Build all packages
npm run typecheck      # Type-check all packages
npm run dev:api        # Dev mode for API server
npm run dev:worker     # Dev mode for worker
npm run migrate        # Run database migrations
npm run clean          # Remove all dist/ directories
```

## API Routes

All `/v1/*` routes require Bearer token auth.

| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/health` | Unauthenticated health check |
| GET | `/v1/agents` | List active agents |
| GET | `/v1/agents/:id` | Agent detail |
| GET | `/v1/tools` | Search tools |
| POST | `/v1/inbox/send` | Send agent message |
| GET | `/v1/approvals` | Pending approval queue |
| POST | `/v1/approvals/:id/decide` | Accept/edit/respond/ignore |
| GET | `/v1/search?q=...` | Semantic search across all data |
| POST | `/v1/ingest/email` | Email ingestion endpoint |

## Notable Design Decisions

- **Configuration as Data**: Runtime behavior lives in the database, not config files. Thresholds, whitelists, model selections, and feature flags are all DB-backed and editable through the dashboard.

- **Soul Spec Personas**: Agent behavior is defined in Git-tracked markdown files with progressive disclosure — most cycles load only core behavior (~Level 2), coordination context loads only when the agent has unread messages (~Level 3).

- **Working Memory**: Agents emit `<working_memory>` blocks to persist short-term state across cycles. Auto-clears on clean cycles.

- **Synchronous Delegation**: Agents can query each other's expertise in real-time within a single cycle (~5-10s round-trip) instead of async inbox messaging.

- **Per-Tool Approval Gating**: Two levels — tool-level (`approval_required` flag on high-risk tools) and agent-level autonomy settings. Read-only tools auto-approve.

- **Circuit Breaker Pattern**: LLM models auto-disable after failures with periodic recovery sweeps.

## License

MIT
