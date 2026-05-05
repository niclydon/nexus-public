-- Migration 056 — watch_annotations.event.v1 payload schema

BEGIN;

INSERT INTO payload_schemas (kind, source, version, json_schema, description, doc_url)
VALUES (
  'watch_annotations.event.v1',
  'watch_annotations',
  1,
  $JSON${
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.invalid/schemas/watch_annotations.event.v1",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "kind",
    "note_text",
    "recorded_at"
  ],
  "properties": {
    "kind": {
      "const": "watch_annotations.event.v1"
    },
    "note_text": {
      "type": "string",
      "minLength": 1,
      "maxLength": 2000
    },
    "recorded_at": {
      "type": "string",
      "format": "date-time"
    },
    "latitude": {
      "type": "number",
      "minimum": -90,
      "maximum": 90
    },
    "longitude": {
      "type": "number",
      "minimum": -180,
      "maximum": 180
    },
    "place_label": {
      "type": "string",
      "maxLength": 200
    },
    "tags": {
      "type": "array",
      "items": {
        "type": "string",
        "maxLength": 64
      },
      "maxItems": 20
    }
  }
}$JSON$::jsonb,
  'Initial v1 payload schema for Watch annotations',
  'docs/source-schemas/watch_annotations.md'
);

INSERT INTO nexus_schema_version (version, description)
VALUES (56, 'watch_annotations.event.v1 schema scaffold')
ON CONFLICT DO NOTHING;

COMMIT;
