# What changed since the August 2026 snapshot

A public-safe summary of how the private Nexus platform evolved between the
August 25, 2026 snapshot in this repository and October 18, 2026. This is a
description, not a code drop: none of the systems below are in this repo.

## Coordination became a layer, not a convention

Agents used to coordinate through inboxes and shared tables by convention.
The private platform now runs a dedicated coordination layer with three
parts: a registry (which agents exist, their roles, and whether they are
live), declared conditions (platform facts other agents can rely on), and a
typed stream (intent, ask, flag, done). Multi-step work reads the registry
and announces intent there before it starts.

## The knowledge graph was rebuilt, and its write rules changed

The graph that the snapshot-era ingestion pipelines fed accumulated years of
machine-generated entities, including large numbers of fragment and junk
entities. In fall 2026 the graph was audited and rebuilt, and the write
model changed with it:

- writes run only through governed paths (preview, apply, verify);
- identity merges stay per-case and human-approved, except for clear-cut
  merges backed by two or more independent signals;
- speculative inference is held back pending validation, instead of being
  written as fact;
- a continuous verification lane works the graph's medium-confidence claims
  against primary sources instead of letting them age in place.

## Work became an operator backlog with typed lanes

The snapshot has a three-layer job system for machine work. The private
platform added a human-legible operator backlog on top of it: every unit of
agent work is an item with a lane, an owner, and acceptance criteria. Two
lanes matter most:

- an autonomous-safe code lane, where scoped items are claimed, implemented
  in isolated worktrees, reviewed adversarially, merged, deployed, and
  verified live before they close;
- a no-code data-task lane for graph corrections and data hygiene, drained
  by a dedicated worker under the same evidence rules.

## The roster was re-scoped

Of the eight personas in this snapshot, six (Chronicler, Circle, Coder,
Inference, Infra, Pipeline) were retired or folded into other seats. In
their place, the private system runs a smaller, more accountable roster:

- a chief-of-staff seat that owns triage, routing, verification, and
  follow-through across the whole estate;
- ARIA, the Nexus-native coordinator and the owner's daily interface;
- a platform-operations seat that owns telemetry and hygiene for the whole
  estate and directs a set of read-only scout instruments;
- specialist seats for insight, biography, projects, and data keeping.

## Scale, for orientation

Dated October 18, 2026, and deliberately approximate: the private MCP
surface is in the hundreds of tools (this snapshot ships 24), the private
schema is in the thousands of migrations (this snapshot ships 56), and the
platform also grew a physical-device layer (small displays, sensors, and
consoles on the local network) that the snapshot does not mention at all.

## What did not change

The doctrine in docs/agent-doctrine.md still holds: agents are bounded
reasoning and coordination layers, processors and migrations do
deterministic execution, and delivery to a human is a separate contract
from agent-to-agent coordination. The configuration-as-data principle in
the README is also intact, and has since been extended: managed systems
sync their configuration and state into the platform as queryable data, so
read-only analysis runs against the platform instead of against devices.
