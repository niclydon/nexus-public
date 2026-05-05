# Nexus Agent Doctrine

**Status:** active platform doctrine

Nexus is best understood as a data and memory platform with bounded reasoning
agents on top, not as an unbounded autonomous swarm.

## Core Boundaries

- Processors and handlers do execution.
- The database and migrations own durable state changes.
- Agents reason, explain, coordinate, and request work.
- Tool grants and runtime guardrails define what an agent may actually do.
- Human-facing delivery is a separate contract from agent-to-agent coordination.

## What Agents Should Do

Agents are the interpretation layer. They should:

- synthesize across sources;
- explain what happened and why it matters;
- propose next actions;
- request bounded jobs or tools;
- preserve provenance when they derive conclusions;
- coordinate with other agents when specialization helps.

The durable output of an agent cycle should usually be a decision,
recommendation, report, inbox item, candidate record, or task request. Agents
should not silently own ingestion, infrastructure mutations, or large
background pipelines.

## What Processors Should Do

Processors and job handlers are the execution layer. They should:

- ingest, normalize, classify, enrich, and summarize data;
- be idempotent and replay-safe;
- write structured outputs with source provenance;
- keep ambiguous conclusions reviewable instead of asserting final truth too
  early.

When a workflow is deterministic, it belongs in a handler, migration, or other
explicit runtime surface before it belongs in an open-ended agent prompt.

## Delivery And Approvals

Human delivery lanes, background processors, and approval workflows solve
different problems:

- delivery decides how something reaches a human;
- processors decide how data is transformed;
- approvals decide whether a high-impact action is allowed.

Those concerns should stay separate in code and documentation.

## Documentation Rule

Public docs in this repo should describe agents as bounded reasoning
coordinators layered on top of deterministic runtime systems. If a document
describes agents as owning all ingestion, infrastructure, or model-serving
responsibilities, that document is historical and should be updated or marked
as such.
