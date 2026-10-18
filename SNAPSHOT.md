# Snapshot record

This repository is a curated public snapshot of a private system. This file is
the provenance record for the current snapshot.

## Snapshot

- Date: October 18, 2026
- Previous snapshot: August 25, 2026 (the code in this repository is
  substantially that snapshot, with the documentation refresh listed below)
- Method: curated selection from the private codebase, scrubbed, and published
  as a public edition. This repository is never produced by changing the
  visibility of the private repository, and its history is not the private
  history.

## What this refresh changed (October 18, 2026)

Documentation and hygiene only. No new private code was lifted in this pass.

- README: added the snapshot banner, dated every count that describes the
  snapshot era, corrected the migrations count (56 files), and added a
  "Where the private system is now" section.
- Added docs/what-changed-since-2026-08.md, a public-safe summary of how the
  private platform evolved after the snapshot.
- Retired personas (Chronicler, Circle, Coder, Inference, Infra, Pipeline)
  moved to souls/_historical/ with a note; ARIA and Insight remain at the top
  level.
- docs/agent-roadmap.md and docs/DATABASE.md are now marked historical: they
  describe the pre-rebuild design and schema.
- Hygiene fixes: a LAN address in a migration comment was replaced with a
  generic description, and a test-fixture key that carried a person's first
  name was renamed to a neutral key. The fixture value was already a scrubbed
  example ("[Person] worked at Acme Corp").
- Code pass (added at the owner's direction, October 18, 2026): fixes for
  the actionable CodeQL findings in the snapshot code (shell-injection
  conversions to argument-array process calls, rate limiting on the ingest
  and approvals routes, regex-injection and parameter-type validation,
  HTML sanitization ordering), a curated code delta (agent-bus coordination
  tools plus migration 058, adapted from the private coordination layer
  with generic seat names), a smoke test and CI, and a Dependabot config.
  The remaining CodeQL dispositions are recorded in the refresh notes:
  the tool executor's free-form command mode is by design and gated by the
  showcase guard, and the generic query wrapper's contract is documented
  in packages/core/src/db.ts with a parameterized sql template helper.

## Scrub standard

Every public edition is checked, before publication, for:

- secrets and tokens anywhere in the tree (automated secret scanning plus a
  manual pass over config and examples);
- photographs and image metadata (this repository contains no photographs);
- real people's full names and photographs. First names appear only in
  clearly fictional or generic examples; one standing rule is absolute: a
  specific close friend of the author is never named in public material;
- internal hostnames, tailnet addresses, and LAN addresses;
- homelab operational detail beyond what the architecture discussion needs;
- licensing fit (this repository is MIT; see LICENSE).

## What is deliberately excluded

Production data of every kind, the current agent persona packages, current
migrations beyond the snapshot set, credentials and configuration, and any
material that identifies private individuals. Where a current capability is
described at all, it is described in prose, not shipped as code.

## Refresh policy

Refreshes are dated snapshots with an entry in this file and in CHANGELOG.md.
Between refreshes, this repository is expected to fall behind the private
system; the snapshot banner in README.md says so, so the staleness is never a
surprise to a reader.
