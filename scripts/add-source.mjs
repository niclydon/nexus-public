import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const SOURCE_KEY_REGEX = /^[a-z][a-z0-9_]{0,30}$/;
const VALID_PATTERNS = ['push', 'pull', 'manual_archive'];
const VALID_CADENCES = ['realtime', 'scheduled', 'manual'];
const VALID_CONTENT_TYPES = [
  'text', 'photo', 'location', 'audio', 'video', 'health_metric',
  'social_signal', 'document', 'transaction', 'event', 'contact', 'activity',
];

function fail(message) {
  console.error(`ERROR: ${message}`);
  console.error('');
  console.error('Usage: npm run add:source -- --name <source_key> --pattern <push|pull|manual_archive> --cadence <realtime|scheduled|manual> --content-types <comma,separated> [options]');
  console.error('Options:');
  console.error('  --display-name "Human label"');
  console.error('  --category <category>                 default: personal');
  console.error('  --dry-run');
  process.exit(2);
}

function parseArgs() {
  const argv = process.argv.slice(2);
  const get = (flag) => {
    const index = argv.indexOf(flag);
    return index >= 0 ? argv[index + 1] : undefined;
  };
  const has = (flag) => argv.includes(flag);

  const name = get('--name') ?? '';
  const pattern = get('--pattern') ?? '';
  const cadence = get('--cadence') ?? '';
  const contentTypesRaw = get('--content-types') ?? '';
  const displayName = get('--display-name') ?? name;
  const category = get('--category') ?? 'personal';
  const dryRun = has('--dry-run');

  if (!name) fail('--name is required');
  if (!SOURCE_KEY_REGEX.test(name)) fail(`--name '${name}' must match ${SOURCE_KEY_REGEX}`);
  if (!VALID_PATTERNS.includes(pattern)) fail(`--pattern must be one of: ${VALID_PATTERNS.join(', ')}`);
  if (!VALID_CADENCES.includes(cadence)) fail(`--cadence must be one of: ${VALID_CADENCES.join(', ')}`);

  const contentTypes = contentTypesRaw.split(',').map((s) => s.trim()).filter(Boolean);
  if (contentTypes.length === 0) fail('--content-types is required');
  for (const contentType of contentTypes) {
    if (!VALID_CONTENT_TYPES.includes(contentType)) {
      console.warn(`WARN: content type '${contentType}' is not in the canonical list; continuing anyway.`);
    }
  }

  return {
    name,
    pattern,
    cadence,
    contentTypes,
    displayName,
    category,
    dryRun,
  };
}

function repoRoot() {
  const here = dirname(fileURLToPath(import.meta.url));
  return resolve(here, '..');
}

function padMigration(n) {
  return String(n).padStart(3, '0');
}

function nextMigrationNumber(root) {
  const files = readdirSync(join(root, 'migrations'));
  let max = 0;
  for (const file of files) {
    const match = file.match(/^(\d{3})-/);
    if (match) max = Math.max(max, Number(match[1]));
  }
  return max + 1;
}

function ensureNoCollision(root, relativePath) {
  if (existsSync(join(root, relativePath))) {
    fail(`refusing to overwrite existing file: ${relativePath}`);
  }
}

function yamlEntry(args) {
  const bronzeTable = args.pattern === 'push'
    ? `raw_ingest_${args.name}`
    : `aurora_raw_${args.name}`;

  const lines = [
    '  - source_key: ' + args.name,
    '    display_name: ' + args.displayName,
    '    ingestion_pattern: ' + args.pattern,
    '    ingestion_mode: ' + args.cadence,
    '    category: ' + args.category,
    '    content_types:',
    ...args.contentTypes.map((contentType) => `      - ${contentType}`),
    '    bronze_table: ' + bronzeTable,
    '    silver_view: aurora_silver_' + args.name,
    '    handler_path: ' + (args.pattern === 'pull' ? `packages/worker/src/handlers/${args.name}-sync.ts` : 'null'),
    '    import_script_path: ' + (args.pattern === 'manual_archive' ? `scripts/ingest-${args.name}.ts` : 'null'),
    '    payload_schema_doc_path: ' + (args.pattern === 'push' ? `docs/source-schemas/${args.name}.md` : 'null'),
    '    smoke_fixture_path: tests/fixtures/' + args.name + '.json',
    '    lifecycle_state: active',
    '    notes: |',
    '      TODO: describe what this source emits, how often it changes, and any public-safe caveats.',
  ];

  return lines.join('\n');
}

function bronzeMigration(args, number) {
  const filename = `migrations/${padMigration(number)}-raw-ingest-${args.name}.sql`;
  const body = `-- Migration ${padMigration(number)} — raw_ingest_${args.name}
--
-- Public scaffold for ${args.displayName} (${args.pattern} source).

BEGIN;

CREATE TABLE IF NOT EXISTS raw_ingest_${args.name} (
  id                UUID PRIMARY KEY,
  produced_at       TIMESTAMPTZ NOT NULL,
  received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload           JSONB NOT NULL,
  processing_status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (processing_status IN ('pending','processed','error','skipped')),
  processed_at      TIMESTAMPTZ,
  processed_by      TEXT,
  attempts          INTEGER NOT NULL DEFAULT 0,
  error             TEXT
);

CREATE INDEX IF NOT EXISTS raw_ingest_${args.name}_pending_idx
  ON raw_ingest_${args.name} (processing_status, received_at)
  WHERE processing_status = 'pending';

CREATE INDEX IF NOT EXISTS raw_ingest_${args.name}_recent_idx
  ON raw_ingest_${args.name} (received_at DESC);

INSERT INTO nexus_schema_version (version, description)
VALUES (${number}, 'raw_ingest_${args.name} scaffold for ${args.displayName}')
ON CONFLICT DO NOTHING;

COMMIT;
`;
  return { path: filename, body };
}

function schemaMigration(args, number) {
  const kind = `${args.name}.event.v1`;
  const schemaJson = JSON.stringify({
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    $id: `https://example.invalid/schemas/${kind}`,
    type: 'object',
    additionalProperties: false,
    required: ['kind'],
    properties: {
      kind: { const: kind },
    },
  }, null, 2);
  return {
    path: `migrations/${padMigration(number)}-${args.name}-schema-v1.sql`,
    body: `-- Migration ${padMigration(number)} — ${kind} payload schema

BEGIN;

INSERT INTO payload_schemas (kind, source, version, json_schema, description, doc_url)
VALUES (
  '${kind}',
  '${args.name}',
  1,
  $JSON$${schemaJson}$JSON$::jsonb,
  'Initial v1 payload schema for ${args.displayName}',
  'docs/source-schemas/${args.name}.md'
);

INSERT INTO nexus_schema_version (version, description)
VALUES (${number}, '${kind} schema scaffold')
ON CONFLICT DO NOTHING;

COMMIT;
`,
  };
}

function schemaDoc(args) {
  const kind = `${args.name}.event.v1`;
  return {
    path: `docs/source-schemas/${args.name}.md`,
    body: `# \`${args.name}\` Payload Schema

**Source:** ${args.displayName}
**Pattern:** push
**Kind:** \`${kind}\`
**Bronze table:** \`raw_ingest_${args.name}\`

## Envelope

\`\`\`json
{
  "id": "11111111-1111-1111-1111-111111111111",
  "produced_at": "2026-05-04T12:00:00.000Z",
  "payload": {
    "kind": "${kind}"
  }
}
\`\`\`

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| \`kind\` | const | yes | Payload discriminator. |
| ... | ... | ... | TODO |

## Notes

- Keep v1 immutable after real data is written.
- Add v2+ as new schema rows rather than editing the old one in place.
- Keep examples and field descriptions public-safe.
`,
  };
}

function pullHandler(args) {
  const pascal = args.name.split('_').map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join('');
  const firstCategory = args.contentTypes[0] === 'text' ? 'message' : args.contentTypes[0];
  return {
    path: `packages/worker/src/handlers/${args.name}-sync.ts`,
    body: `import { createLogger } from '@nexus/core';
import type { TempoJob } from '../job-worker.js';
import { ingest, registerNormalizer, type IngestRecord } from '../lib/ingestion-pipeline.js';

const logger = createLogger('${args.name}-sync');
const SOURCE_KEY = '${args.name}';

registerNormalizer(SOURCE_KEY, normalize${pascal});

export async function handle${pascal}Sync(_job: TempoJob): Promise<Record<string, unknown>> {
  logger.log('Starting ${args.name} sync');

  const rawItems = await fetch${pascal}Data();
  const result = await ingest(SOURCE_KEY, rawItems);

  logger.log(\`Completed ${args.name} sync: \${result.ingested} ingested, \${result.skipped} skipped\`);
  return result;
}

async function fetch${pascal}Data(): Promise<unknown[]> {
  logger.log('No fetch implementation yet; returning empty set');
  return [];
}

function normalize${pascal}(rawData: unknown): IngestRecord[] {
  const item = rawData as Record<string, unknown>;
  return [{
    source_key: SOURCE_KEY,
    source_id: String(item.id ?? 'TODO-source-id'),
    timestamp: new Date(),
    category: '${firstCategory}',
    summary: String(item.summary ?? 'TODO summary'),
    content: item.content == null ? null : String(item.content),
    entities: [],
    metadata: {},
    raw: item,
  }];
}
`,
  };
}

function pullMigration(args, number) {
  const pascal = args.name.split('_').map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join('');
  return {
    path: `migrations/${padMigration(number)}-${args.name}-sync-routing.sql`,
    body: `-- Migration ${padMigration(number)} — ${args.name}-sync job routing

BEGIN;

INSERT INTO nexus_job_routing (job_type, executor, handler_name, default_priority, default_max_attempts)
VALUES ('${args.name}-sync', 'nexus', 'handle${pascal}Sync', 5, 3)
ON CONFLICT (job_type) DO NOTHING;

INSERT INTO nexus_schema_version (version, description)
VALUES (${number}, 'Route ${args.name}-sync handler')
ON CONFLICT DO NOTHING;

COMMIT;
`,
  };
}

function archiveImporter(args) {
  return {
    path: `scripts/ingest-${args.name}.ts`,
    body: `// TODO: implement manual archive importer for ${args.displayName}.
// Public scaffold: safe to edit freely for your own archive format.
`,
  };
}

function smokeFixture(args) {
  return {
    path: `tests/fixtures/${args.name}.json`,
    body: `{
  "_comment": "Public scaffold fixture for ${args.displayName}",
  "_pattern": "${args.pattern}",
  "_schema_kind": ${args.pattern === 'push' ? `"${args.name}.event.v1"` : 'null'},
  "raw": {},
  "expected_records": []
}
`,
  };
}

function appendSourceEntry(root, args, dryRun) {
  const yamlPath = join(root, 'docs', 'sources.yaml');
  const current = readFileSync(yamlPath, 'utf8');
  const entry = yamlEntry(args);

  if (new RegExp(`\\n\\s*- source_key: ${args.name}(\\n|$)`).test(`\n${current}`)) {
    fail(`docs/sources.yaml already contains source_key '${args.name}'`);
  }

  const normalizedCurrent = current.includes('sources: []')
    ? current.replace('sources: []', 'sources:')
    : current;

  const next = normalizedCurrent.endsWith('\n')
    ? `${normalizedCurrent}${entry}\n`
    : `${normalizedCurrent}\n${entry}\n`;

  if (dryRun) {
    console.log(`  + would append docs/sources.yaml`);
    return;
  }

  writeFileSync(yamlPath, next);
  console.log('  + appended docs/sources.yaml');
}

function writePlannedFile(root, file, dryRun) {
  ensureNoCollision(root, file.path);
  if (dryRun) {
    console.log(`  + would write ${file.path}`);
    return;
  }

  const absolutePath = join(root, file.path);
  mkdirSync(dirname(absolutePath), { recursive: true });
  writeFileSync(absolutePath, file.body);
  console.log(`  + wrote ${file.path}`);
}

function main() {
  const args = parseArgs();
  const root = repoRoot();
  const files = [];

  console.log(`Scaffolding source '${args.name}' (${args.pattern}/${args.cadence})${args.dryRun ? ' [dry-run]' : ''}`);

  if (args.pattern === 'push') {
    const migrationNumber = nextMigrationNumber(root);
    files.push(bronzeMigration(args, migrationNumber));
    files.push(schemaMigration(args, migrationNumber + 1));
    files.push(schemaDoc(args));
  } else if (args.pattern === 'pull') {
    const migrationNumber = nextMigrationNumber(root);
    files.push(pullHandler(args));
    files.push(pullMigration(args, migrationNumber));
  } else {
    files.push(archiveImporter(args));
  }

  files.push(smokeFixture(args));

  for (const file of files) {
    writePlannedFile(root, file, args.dryRun);
  }
  appendSourceEntry(root, args, args.dryRun);

  console.log('');
  console.log('Next steps:');
  if (args.pattern === 'push') {
    console.log(`  1. Flesh out docs/source-schemas/${args.name}.md with real public-safe fields.`);
    console.log('  2. Review the generated migrations before applying them.');
    console.log('  3. Implement the producer and smoke-test the ingest route.');
  } else if (args.pattern === 'pull') {
    console.log(`  1. Implement fetch logic in packages/worker/src/handlers/${args.name}-sync.ts.`);
    console.log('  2. Wire the handler into the handler registry.');
    console.log('  3. Review and apply the generated routing migration.');
  } else {
    console.log(`  1. Replace scripts/ingest-${args.name}.ts with a real importer.`);
    console.log('  2. Add a representative sample fixture and document the archive format.');
  }
  console.log('  4. Replace the TODO notes in docs/sources.yaml.');
}

main();
