# nexus-public — April 2026 project snapshot

A short narrative record of where this repository sits as of 2026-04-24. It exists alongside the README to give a future reader, human or agent, the context the README intentionally leaves out: why this repo exists at all, what it is allowed to do, and what has actually happened to it in the last six weeks.

## What nexus-public is

`nexus-public` is the sanitized public showcase of a private platform. The real system lives in `~/Projects/nexus/` on Furnace, owns the `nexus` Postgres database (forked from the legacy `aria` DB on 2026-03-31), and runs as production on the home lab. This repository is what fell out of that system after the secrets, user data, deployment specifics, and operator-only tooling were stripped.

The README frames it plainly: a distributed agent architecture platform with four packages (`@nexus/core`, `@nexus/api`, `@nexus/mcp`, `@nexus/worker`), eight autonomous agents (ARIA, Pipeline, Infra, Inference, Coder, Insight, Circle, Chronicler), 90+ job handlers, an MCP server exposing 24 tools, a multi-provider LLM router with circuit breakers, a distributed autoscaler, and 54 PostgreSQL migrations. It is intentionally not deploy-ready. Anyone landing here from a link is meant to read the code, understand the architecture, and walk away — not run it end to end.

The relationship to `nexus/` is asymmetric. The private repo is the source of truth, gets the daily commits, and runs against real data. `nexus-public` is a downstream artifact that periodically gets brought in line with the private architecture so the showcase keeps reflecting reality. That asymmetry is why this repo has only five commits total, none of them the day-to-day churn of a working system.

## The showcase guardrails

The single most important defensive change in this repo is commit `19e1245` from 2026-04-14, "Add showcase guardrails to prevent accidental production use." It does two things, and both matter.

First, all three services refuse to start unless `NEXUS_SHOWCASE_DEMO=true` is set. The check lives in `packages/core/src/showcase.ts` as a 22-line module exporting `assertShowcaseDemoEnabled(serviceName)`, called from the entry points of `@nexus/api`, `@nexus/worker`, and `@nexus/mcp`. The error text is explicit:

> Refusing to start: this public repository is showcase-only. Set NEXUS_SHOWCASE_DEMO=true to run a demo instance.

The premise is that a stranger cloning the repo and running `npm run dev:api` should hit a wall before any service binds a port, opens a database connection, or talks to an LLM. Fail fast, fail loud, fail before doing anything.

Second, when running in showcase mode, the worker's tool executor blocks a curated list of high-impact tools by default. The block list lives in `packages/worker/src/tool-executor.ts` as `SHOWCASE_BLOCKED_TOOLS`:

```
check_system, query_db,
read_file, write_file, edit_file,
run_build, git_commit_push, create_pr,
restart_service, restart_user_service,
install_mcp_server, install_skill,
send_email
```

Twelve tool ids. Anything that touches the file system, mutates a database, restarts a systemd unit, opens a PR, installs new capability, or sends mail is off by default. The agent gets a `BLOCKED:` string back instead of a result, which the LLM observes and reasons over. The tool catalog still advertises these tools so an agent's prompt and behavior look identical to the private system; only execution is gated.

There is an escape hatch. Set `NEXUS_SHOWCASE_ALLOW_DANGEROUS_TOOLS=true` and the block lifts. The README and `.env.example` both call out that this is intended for private lab testing only.

The posture is conservative on purpose. A reader can run the services, see agent cycles fire, watch the LLM router fall back across providers, and read tool selection telemetry without giving any agent the ability to modify the host or reach external systems with side effects.

## Current state from code and README

- `VERSION` reads `0.1.1`. The repo has not had a versioned release; this is just the initial bump from `0.1.0`.
- Migrations directory holds 54 SQL files numbered `001-foundation.sql` through `054-drop-aurora-raw-photos-embedding.sql`. The numbering matches the private repo's history, including the pgbouncer decommission (053) and the Aurora-era cleanup (054), which means the showcase was lifted at a point well past the legacy `aria` cutover.
- Eight souls under `souls/` (one directory per agent), aligned with the README's agent table.
- One Cloudflare Worker at `workers/email-inbound/` for inbound mail routing.
- `.github/workflows/codeql.yml` exists from PR #1 (commit `d00f0b9`, 2026-04-13). CodeQL is the only CI configured.
- Stack confirmed by README: TypeScript on Node 20+, Express, PostgreSQL with HNSW vector indexes, `nomic-embed` at 768 dimensions for embeddings, `@modelcontextprotocol/sdk` for tool plumbing, systemd for deployment.
- The README still says `nomic-embed` for embeddings even though the most recent commit renamed those references in code. That is a documentation drift to fix on the next sweep.

There is also a top-level `security_best_practices_report.md` (6.2k, 2026-04-13) sitting next to the README. It predates the showcase guardrails commit by a day and likely informed it.

## Recent git activity

The full log is five commits:

```
f1fff56  2026-04-21  Rename legacy Forge model ids to canonical form across nexus-public
19e1245  2026-04-14  Add showcase guardrails to prevent accidental production use
d8dded5  2026-04-13  Merge pull request #1 from niclydon/niclydon-CodeQL
d00f0b9  2026-04-13  Add CodeQL analysis workflow configuration
501398a  2026-04-13  Nexus: distributed agent architecture platform
```

Three substantive moments, all clustered in the first three weeks of April 2026.

`501398a` is the initial extraction. The whole monorepo lands as one commit on 2026-04-13: four packages, eight souls, 54 migrations, 90+ job handlers, MCP server, autoscaler, watchdog. This is the moment `nexus-public` began.

`19e1245` follows the next day. The guardrails described above. Nine files changed, 93 lines added, 4 removed. README updated in the same commit.

`f1fff56` on 2026-04-21 is the canonical model id sweep. The private homelab/forge convention is `<family>-<role>-<size>[-variant]` (e.g. `qwen3-next-chat-80b`, `qwen3-embed-8b`), enforced by `homelab/scripts/models-lint.py`. The showcase had drifted onto legacy ids, so this commit walked through 18 files and renamed:

- `qwen2.5-72b`, `qwen3.5-35b-a3b`, `furnace-priority` → `qwen3-next-chat-80b`
- `nomic-embed`, `nomic-embed-text-v1.5` → `qwen3-embed-8b`
- `qwen3-coder-moe` → `qwen3-code-30b-a3b`

Seven souls picked up the rename, plus the LLM router chains, the cost map, the eval scorer, the iOS route, and four worker handlers (embed-backfill, knowledge-summarize, memory-summarize, sentiment-backfill). The commit message gives a 7-day sweep window with legacy ids retiring 2026-04-28.

## Unpushed commit posture

`f1fff56` is local-only. `git status` reports the branch is "ahead of 'origin/main' by 1 commit." The commit body itself states the reason:

> NOT PUSHED — nexus-public is the sanitized public showcase and requires explicit PR review before push.

This is deliberate friction. The private `nexus` repo can take direct pushes from the working laptop because it is private and operator-owned. The public showcase is a different audience: anything that lands on `origin/main` is visible to whoever is reading the architecture as a portfolio piece. So the workflow gates that visibility behind a PR review, even for a sweep as mechanical as a model id rename.

The trade-off worth naming: this conflicts with the global GIT.md rule "Push immediately after committing. Never leave unpushed commits sitting on a branch." That rule exists because three machines and several agents touch every other repo and divergent branches cause real pain. `nexus-public` is the explicit exception. It is single-author, single-machine, low-velocity, and the cost of a stale local branch is much smaller than the cost of an unreviewed public push. The carve-out is reasonable, but the unpushed state should not become permanent. As of this snapshot the commit has been local for three days.

## Where to look next

- `packages/core/src/showcase.ts` for the guardrail module itself.
- `packages/worker/src/tool-executor.ts` lines 71-111 for the block list and the per-call gate.
- `docs/agent-roadmap.md`, `docs/unified-ingestion-pipeline.md`, `docs/mcp-client-architecture.md`, `docs/api-poll-pipeline-design.md`, `docs/soulspec-integration.md`, `docs/DATABASE.md` for the architecture deep-dives the README points at.
- `migrations/` for the schema timeline if you want to read the platform's evolution chronologically.

There is no `CHANGES.md` in this repo. The private `nexus` repo carries that record. This snapshot is the closest thing the public showcase has to a chronological log, and it covers the entire history of the repo so far in five commits.
