# Adding a Data Source

This runbook describes how to onboard a new source into the public Nexus
architecture without coupling that source to every downstream consumer.

## Fast Path

Use the public scaffold first:

```bash
npm run add:source -- \
  --name watch_annotations \
  --pattern push \
  --cadence scheduled \
  --content-types text,location \
  --display-name "Watch annotations"
```

That command creates only public-safe boilerplate:

- source manifest entry in `docs/sources.yaml`
- fixture stub in `tests/fixtures/<source>.json`
- for `push`: raw-ingest migration plus schema doc
- for `pull`: handler skeleton plus routing migration
- for `manual_archive`: importer stub

Use `--dry-run` to preview without writing files.

Current repo examples:

- `watch_annotations`: push-style example with explicit payload schema
- `looki_moments`: pull-style example with a normalized handler stub
- `claude_export`: manual-archive example tied to the official Anthropic export flow

## Principles

- Ingestion writes raw records and stops.
- Downstream enrichment, search, and reporting read from durable tables on
  their own schedule.
- Source naming is part of the contract, not an incidental detail.
- New sources should be documented in a central manifest.

## Source Patterns

Choose the pattern based on who initiates the write:

- `push`: an external producer POSTs data into Nexus.
- `pull`: a Nexus worker fetches data from an external system on a schedule.
- `manual_archive`: an operator imports an export bundle or archive manually.

## Naming

Use a stable lowercase source key that matches `^[a-z][a-z0-9_]{0,30}$`.

That key should line up across:

- the entry in `docs/sources.yaml`,
- raw tables or ingestion tables,
- worker handlers,
- payload schema docs,
- smoke fixtures or sample inputs.

## Push Sources

For a push source, add:

1. a bronze/raw migration,
2. an ingest route or route registration,
3. payload schema documentation,
4. a manifest entry in `docs/sources.yaml`.

The producer should treat retries as idempotent and should not enqueue
downstream enrichment directly.

## Pull Sources

For a pull source, add:

1. a worker handler,
2. job routing or scheduling,
3. raw table ownership,
4. a manifest entry in `docs/sources.yaml`.

The handler should normalize source data into a durable raw form before any
optional downstream enrichment.

## Manual Archive Sources

For a manual archive source, add:

1. an import script,
2. raw table ownership,
3. source documentation and sample fixtures,
4. a manifest entry in `docs/sources.yaml`.

Manual importers should be safe to re-run on the same archive.

## Checklist

- Name chosen and documented.
- Pattern chosen and documented.
- Raw table or ingest target created.
- Handler or importer added.
- Sample payload or fixture added.
- Source manifest updated.
- Public docs updated without private environment details.

## Public Repo Boundary

The public scaffolder and docs in this repo intentionally do not encode:

- private hostnames or IPs
- usernames
- local machine paths
- secret names or tokens
- environment-specific deployment instructions

If a source needs those details in private Nexus, add them there rather than
teaching the public repo about your environment.
