# Claude Data Export

This note documents the public user workflow for obtaining a Claude data export
before a manual importer processes it.

## Official Export Flow

According to Anthropic's help center, individual Claude users and Team Primary
Owners can request an export from the Claude web app or Claude Desktop under:

`Settings -> Privacy -> Export data`

Anthropic also notes:

- exports are available for active Free, Pro, and Max accounts, plus relevant
  workspace owners
- you cannot start the export from the Claude mobile apps
- the export link is delivered by email
- the download link expires after 24 hours

For Enterprise Primary Owners, the export flow is under:

`Settings -> Data management -> Export Data`

Official help article:

- https://support.claude.com/en/articles/9450526-how-can-i-export-my-claude-data

## Public Repo Boundary

This repo documents the acquisition flow, but it does not freeze an exact file
format for Anthropic's export bundle. That format may evolve over time.

When implementing `scripts/ingest-claude_export.ts`, inspect a fresh export and
treat the current archive shape as an input contract to be versioned in code.

## Suggested Normalization Targets

Typical records worth extracting into Nexus-style raw or bronze tables:

- conversations and message turns
- projects or conversation grouping metadata
- timestamps, titles, and lightweight provenance metadata
- artifacts or generated outputs, when present

Keep any public examples synthetic and sanitized.
