# Unified Ingestion Pipeline

## Problem

Every data source was built separately in ARIA. Gmail goes to `gmail_archive` AND `aurora_raw_gmail`. iMessages go to `imessage_incoming` AND `aurora_raw_imessage`. Photos go to `photo_metadata` AND `aurora_raw_photos`. Each has its own storage format, its own enrichment triggering, and its own proactive checking. There's duplication, inconsistency, and no unified way to process new data.

## Goal

One pipeline that every data source flows through. A new piece of data enters once, gets checked for immediate action, stored once in a normalized format, and queued for all relevant enrichment.

---

## Pipeline

```
         ┌─────────────────────────────────────┐
         │         DATA SOURCES                 │
         │                                      │
         │  Gmail (MCP)     iMessage (relay)     │
         │  Calendar (MCP)  Photos (relay)       │
         │  Instagram (MCP) HealthKit (iOS)      │
         │  Drive (MCP)     Location (iOS)       │
         │  Looki (API)     Music (iOS)          │
         │  Email (webhook) Contacts (relay)     │
         └──────────────┬──────────────────────┘
                        │
                        ▼
         ┌──────────────────────────────────────┐
         │          INGEST                       │
         │  Source-specific: MCP tool call,      │
         │  API fetch, webhook receive, etc.     │
         │  Returns raw source data              │
         └──────────────┬──────────────────────┘
                        │
                        ▼
         ┌──────────────────────────────────────┐
         │          NORMALIZE                    │
         │  Convert to common IngestRecord:      │
         │  {                                    │
         │    source_key,                        │
         │    source_id,     (unique per source) │
         │    timestamp,                         │
         │    category,      (email, message,    │
         │                    event, photo,      │
         │                    health, location,  │
         │                    social, document)  │
         │    summary,       (human-readable)    │
         │    content,       (full text/data)    │
         │    entities,      (people, places)    │
         │    metadata,      (source-specific)   │
         │    raw            (original JSON)     │
         │  }                                    │
         └──────────────┬──────────────────────┘
                        │
                   ┌────┴────┐
                   │         │
                   ▼         ▼
    ┌──────────────────┐  ┌──────────────────────┐
    │  IMMEDIATE CHECK  │  │  STORE RAW            │
    │                   │  │                       │
    │  Is action needed │  │  ingested_records     │
    │  RIGHT NOW?       │  │  (single table,       │
    │                   │  │   all sources)        │
    │  Rules:           │  │                       │
    │  - Email from     │  │  Also write to        │
    │    inner circle   │  │  source-specific      │
    │  - Event in < 2h  │  │  tables for backward  │
    │  - Health anomaly │  │  compat:              │
    │  - Urgent keyword │  │  gmail_archive,       │
    │                   │  │  device_calendar,     │
    │  If yes → create  │  │  aurora_raw_*, etc.   │
    │  proactive_insight│  │                       │
    └──────────────────┘  └───────────┬───────────┘
                                      │
                                      ▼
                        ┌──────────────────────────┐
                        │  ENRICHMENT QUEUE         │
                        │                           │
                        │  For each record, enqueue │
                        │  applicable enrichments:  │
                        │                           │
                        │  - sentiment analysis     │
                        │    (text content)         │
                        │  - embedding generation   │
                        │    (all content)          │
                        │  - knowledge extraction   │
                        │    (entities, facts)      │
                        │  - photo description      │
                        │    (images only)          │
                        │  - relationship scoring   │
                        │    (messages, emails)     │
                        └──────────────────────────┘
```

## Data Model

### ingested_records (new unified table)

```sql
CREATE TABLE ingested_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_key TEXT NOT NULL,          -- 'gmail', 'imessage', 'calendar', etc.
    source_id TEXT NOT NULL,           -- unique ID from the source (message_id, event_id, etc.)
    timestamp TIMESTAMPTZ NOT NULL,    -- when the data was created at source
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    category TEXT NOT NULL,            -- 'email', 'message', 'event', 'photo', 'health', 'location', 'social', 'document'
    summary TEXT,                      -- one-line human-readable summary
    content TEXT,                      -- full text content (for text-based sources)
    entities JSONB DEFAULT '[]',       -- extracted people, places, topics
    metadata JSONB DEFAULT '{}',       -- source-specific fields (from, to, labels, etc.)
    raw JSONB,                         -- original source data
    -- Processing state
    proactive_checked BOOLEAN DEFAULT false,
    enrichment_queued BOOLEAN DEFAULT false,
    -- Dedup
    UNIQUE(source_key, source_id)
);

CREATE INDEX idx_ingested_records_source ON ingested_records(source_key, timestamp DESC);
CREATE INDEX idx_ingested_records_category ON ingested_records(category, timestamp DESC);
CREATE INDEX idx_ingested_records_unprocessed ON ingested_records(proactive_checked, enrichment_queued)
    WHERE proactive_checked = false OR enrichment_queued = false;
```

### How It Relates to Existing Tables

The `ingested_records` table is the single source of truth. But for backward compatibility and specialized queries, we also write to the existing tables:

| Source | ingested_records.category | Also writes to |
|--------|---------------------------|----------------|
| Gmail | email | gmail_archive |
| iMessage | message | aurora_raw_imessage |
| Calendar | event | device_calendar_events |
| Photos | photo | photo_metadata |
| HealthKit | health | health_data |
| Location | location | device_location |
| Instagram | social | aurora_raw_instagram |
| Drive | document | (new) |
| Looki | health | looki_realtime_events |
| Music | social | device_music |
| Contacts | contact | contacts |

Over time, services migrate to reading from `ingested_records` directly, and the duplicate tables become views or are deprecated.

## Normalizers

Each source has a normalizer function that converts raw source data to an IngestRecord:

```typescript
interface IngestRecord {
  source_key: string;
  source_id: string;
  timestamp: Date;
  category: 'email' | 'message' | 'event' | 'photo' | 'health' | 'location' | 'social' | 'document' | 'contact';
  summary: string;
  content: string | null;
  entities: Array<{ name: string; type: string }>;
  metadata: Record<string, unknown>;
  raw: unknown;
}

type Normalizer = (rawData: unknown) => IngestRecord[];
```

Example — Gmail normalizer:
```typescript
function normalizeGmail(email: GmailMessage): IngestRecord {
  return {
    source_key: 'gmail',
    source_id: email.id,
    timestamp: new Date(email.date),
    category: 'email',
    summary: `${email.from} — ${email.subject}`,
    content: email.body,
    entities: [
      { name: email.from, type: 'person' },
      ...email.to.map(t => ({ name: t, type: 'person' })),
    ],
    metadata: {
      from: email.from,
      to: email.to,
      subject: email.subject,
      labels: email.labels,
      thread_id: email.threadId,
      has_attachments: email.attachments?.length > 0,
    },
    raw: email,
  };
}
```

## Immediate Action Check

After normalization, each record is checked against rules:

```typescript
interface ActionRule {
  name: string;
  match: (record: IngestRecord) => boolean;
  action: (record: IngestRecord) => Promise<void>;
}

const rules: ActionRule[] = [
  {
    name: 'inner-circle-message',
    match: (r) => ['email', 'message'].includes(r.category) &&
      isInnerCircle(r.entities.find(e => e.type === 'person')?.name),
    action: async (r) => createProactiveInsight({
      title: `Message from ${r.entities[0]?.name}`,
      body: r.summary,
      urgency: 'standard',
      category: 'social',
    }),
  },
  {
    name: 'upcoming-event-no-prep',
    match: (r) => r.category === 'event' &&
      isWithinHours(r.timestamp, 2) &&
      !hasExistingPrep(r.source_id),
    action: async (r) => createProactiveInsight({
      title: `Event in ${hoursUntil(r.timestamp)}h: ${r.summary}`,
      body: `Attendees: ${r.metadata.attendees?.join(', ')}`,
      urgency: 'urgent',
      category: 'calendar_prep',
    }),
  },
  {
    name: 'health-anomaly',
    match: (r) => r.category === 'health' &&
      isAnomalous(r.metadata),
    action: async (r) => createProactiveInsight({
      title: `Health anomaly: ${r.summary}`,
      body: JSON.stringify(r.metadata),
      urgency: 'standard',
      category: 'health_alert',
    }),
  },
];
```

## Enrichment Queue

After storage, each record is checked for applicable enrichments:

| Enrichment | Applies to | Job type |
|-----------|------------|----------|
| Sentiment analysis | email, message, social | sentiment-backfill |
| Embedding generation | all text content | embed-backfill |
| Knowledge extraction | email, message, document | knowledge-backfill |
| Photo description | photo | photo-describe |
| Relationship scoring | email, message | (new: relationship-score) |

The pipeline enqueues the appropriate jobs:
```typescript
function queueEnrichments(record: IngestRecord): void {
  if (['email', 'message', 'social'].includes(record.category) && record.content) {
    enqueueJob('sentiment-backfill', { record_id: record.id });
  }
  if (record.content) {
    enqueueJob('embed-backfill', { record_id: record.id });
  }
  if (['email', 'message', 'document'].includes(record.category)) {
    enqueueJob('knowledge-backfill', { record_id: record.id });
  }
  if (record.category === 'photo') {
    enqueueJob('photo-describe', { record_id: record.id });
  }
}
```

## How Collector Uses This

Collector's cycle:
1. Check which sources are due
2. For each due source:
   a. Pull data via MCP tool or API
   b. Normalize each record
   c. Run immediate action check
   d. Store in ingested_records + legacy table
   e. Queue enrichments
3. Update data_source_registry

The Collector doesn't need to know the details of each source — it calls the MCP tool, passes the result to the normalizer, and the pipeline handles the rest.

## Implementation Order

1. Create `ingested_records` table
2. Build the pipeline core (normalize → check → store → enqueue)
3. Write Gmail normalizer (first source)
4. Wire Collector to use the pipeline
5. Add normalizers for Calendar, iMessage, etc. one at a time
6. Migrate enrichment jobs to read from `ingested_records` instead of source-specific tables

## What This Replaces

- Custom `gmail-backfill` handler → Collector + Gmail MCP + pipeline
- Custom `calendar-prep` handler → Collector + Calendar MCP + pipeline
- Custom `context-accumulate` handler → pipeline's immediate action check
- Custom `significance-check` handler → pipeline's action rules
- Direct writes to `aurora_raw_*` → pipeline stores in both places during migration
