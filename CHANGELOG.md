# Changelog

Public snapshot editions of Nexus. Each entry is a dated refresh; between
entries, this repository intentionally stays behind the private system.
See SNAPSHOT.md for the provenance record and scrub standard.

## 2026-10-18 — Snapshot refresh (documentation and hygiene)

- README: snapshot banner with the October 18, 2026 date; counts dated to
  the snapshot era; migrations count corrected to 56 files; new section,
  "Where the private system is now (October 2026)".
- Added SNAPSHOT.md (provenance, scrub standard, exclusions, refresh
  policy) and docs/what-changed-since-2026-08.md (coordination layer,
  knowledge-graph rebuild, operator backlog lanes, roster re-scope).
- Retired personas (Chronicler, Circle, Coder, Inference, Infra, Pipeline)
  moved to souls/_historical/ with an explanatory note.
- docs/agent-roadmap.md and docs/DATABASE.md marked historical
  (pre-rebuild design and schema).
- Hygiene: LAN address in a migration comment replaced with a generic
  description; a test-fixture key carrying a person's first name renamed
  to a neutral key.
- Code pass: shell-injection conversions in the tool executor and showcase
  scripts (argument-array process calls, branch/path validation), rate
  limiting on the ingest and approvals API routes, regex-injection fix in
  scripts/add-source.mjs, parameter-type validation in semantic search, a
  parameterized sql template helper in @nexus/core, and HTML sanitization
  fixes (script/style removal until stable, entity decoding with &amp;
  last) across the mail and research helpers.
- Code delta: agent-bus coordination tools (agent_bus_post / _read /
  _hwm) adapted from the private coordination layer, with backing schema
  in migrations/058-agent-bus.sql. MCP surface is now 27 tools.
- Tests and CI: root `npm test` smoke test for the showcase guard, a
  GitHub Actions CI workflow (typecheck + smoke), and a Dependabot
  configuration (monthly npm and actions).
- VERSION set to 0.2.0 to mark the refreshed snapshot edition.

## 2026-08-25 — Previous snapshot

- Last code snapshot prior to this refresh (systemd OnFailure fix,
  2026-09-30 push). Eight-agent roster, 56 migrations, 24-tool MCP server.
