# `watch_annotations` Payload Schema

**Source:** Watch annotations
**Pattern:** push
**Kind:** `watch_annotations.event.v1`
**Bronze table:** `raw_ingest_watch_annotations`

## Envelope

```json
{
  "id": "11111111-1111-1111-1111-111111111111",
  "produced_at": "2026-05-04T12:00:00.000Z",
  "payload": {
    "kind": "watch_annotations.event.v1",
    "note_text": "Felt focused during the walk to lunch.",
    "recorded_at": "2026-05-04T11:57:00.000Z",
    "latitude": 37.7749,
    "longitude": -122.4194,
    "place_label": "Downtown walk",
    "tags": ["mood", "walking"]
  }
}
```

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | const | yes | Payload discriminator. |
| `note_text` | string | yes | Short free-text annotation captured by the producer. |
| `recorded_at` | ISO timestamp string | yes | When the annotation was actually captured on-device. |
| `latitude` | number | no | Optional location latitude at capture time. |
| `longitude` | number | no | Optional location longitude at capture time. |
| `place_label` | string | no | Human-readable location label supplied by the producer. |
| `tags` | string[] | no | Optional producer-supplied lightweight classification tags. |

## Notes

- Keep v1 immutable after real data is written.
- Add v2+ as new schema rows rather than editing the old one in place.
- Keep examples and field descriptions public-safe.
- This example intentionally uses simple text plus optional location because it
  demonstrates a realistic push source without depending on any private app or
  infrastructure.
