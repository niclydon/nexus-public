// Manual archive importer stub for Claude data export.
//
// Public repo posture:
// - this file documents the expected operator workflow
// - it does not assume a private directory layout
// - it does not assume Anthropic's export bundle shape is permanently stable
//
// Official export steps are documented in:
//   docs/source-exports/claude-data-export.md
//
// Suggested implementation plan:
// 1. Inspect the downloaded export bundle and record the current file layout.
// 2. Add a parser that walks conversations/projects/artifacts in that bundle.
// 3. Normalize each record into your chosen bronze/raw format.
// 4. Keep the importer idempotent so re-running the same bundle is safe.
//
// Example invocation shape:
//   npx tsx scripts/ingest-claude_export.ts --input /path/to/export.zip
//
// This remains a stub in nexus-public on purpose.
