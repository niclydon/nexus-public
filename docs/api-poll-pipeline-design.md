# API Poll Pipeline — Design

## Problem

14+ data sources need to get data into Nexus. Each was built as a bespoke job handler in aria-tempo with its own auth, polling, pagination, and storage logic. This doesn't scale and is hard to maintain.

## Goal

A generic pipeline where adding a new API source is mostly configuration, not code. Source-specific logic (response parsing) is the only custom part.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Collector Agent                     │
│  Decides WHEN to poll based on schedule + backlog        │
│  Enqueues: api-poll job with source_key                  │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   api-poll Job Handler                    │
│                                                          │
│  1. Load source config from data_source_registry         │
│  2. Get auth (OAuth token, API key, etc.)                │
│  3. Build request (URL, headers, watermark params)       │
│  4. Fetch from API                                       │
│  5. Call source-specific parser                          │
│  6. Write normalized records to target table             │
│  7. Update watermark                                     │
│  8. Update data_source_registry (last_sync_at, status)   │
│  9. If more pages → self-chain with next cursor          │
│ 10. If enrichment needed → enqueue enrichment job        │
└─────────────────────────────────────────────────────────┘
```

## Data Model

### data_source_registry (extend existing table)

New columns needed:
```sql
ALTER TABLE data_source_registry ADD COLUMN IF NOT EXISTS
  poll_config JSONB DEFAULT '{}';
  -- {
  --   "auth_type": "oauth" | "api_key" | "bearer" | "none",
  --   "auth_secret_key": "google_tokens" | "looki_api_key" | etc,
  --   "base_url": "https://www.googleapis.com/calendar/v3",
  --   "endpoint": "/events",
  --   "method": "GET",
  --   "params": {"maxResults": 100, "orderBy": "updated"},
  --   "watermark_field": "updated",
  --   "watermark_param": "updatedMin",
  --   "pagination_type": "token" | "offset" | "cursor" | "none",
  --   "pagination_field": "nextPageToken",
  --   "pagination_param": "pageToken",
  --   "target_table": "device_calendar_events",
  --   "parser": "google_calendar",
  --   "enrichment_jobs": ["knowledge-backfill"],
  --   "rate_limit_ms": 1000,
  --   "max_pages_per_poll": 10
  -- }
```

### poll_watermarks (rename/extend existing context_watermarks)

```sql
CREATE TABLE IF NOT EXISTS poll_watermarks (
  source_key TEXT PRIMARY KEY,
  watermark_value TEXT,  -- last seen timestamp, cursor, or page token
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Source-Specific Parsers

Each source registers a parser function that takes the raw API response and returns normalized records:

```typescript
interface PollParser {
  (response: unknown, sourceConfig: PollConfig): {
    records: Record<string, unknown>[];  // rows to insert
    nextCursor?: string;                  // for pagination
    hasMore: boolean;
  };
}

// Registry of parsers
const parsers: Record<string, PollParser> = {
  google_calendar: parseGoogleCalendar,
  google_gmail: parseGoogleGmail,
  google_voice: parseGoogleVoice,
  looki: parseLooki,
  instagram: parseInstagram,
};
```

The parser is the ONLY source-specific code. Everything else (auth, fetching, pagination, watermarking, storage, error handling) is generic.

## Auth Handling

```typescript
interface AuthProvider {
  getHeaders(sourceConfig: PollConfig): Promise<Record<string, string>>;
  refresh?(sourceConfig: PollConfig): Promise<void>;
}

const authProviders: Record<string, AuthProvider> = {
  oauth: {
    // Read token from google_tokens, refresh if expired
    async getHeaders(config) {
      const token = await getOAuthToken(config.auth_secret_key);
      if (isExpired(token)) await this.refresh(config);
      return { Authorization: `Bearer ${token.access_token}` };
    },
    async refresh(config) {
      // Use refresh_token to get new access_token
      // Update google_tokens table
    },
  },
  api_key: {
    async getHeaders(config) {
      const key = process.env[config.auth_secret_key];
      return { Authorization: `Bearer ${key}` };
    },
  },
  bearer: {
    async getHeaders(config) {
      const token = process.env[config.auth_secret_key];
      return { Authorization: `Bearer ${token}` };
    },
  },
  none: {
    async getHeaders() { return {}; },
  },
};
```

## Job Flow

### Collector's role:
```
Every 5 min cycle:
  1. Query data_source_registry for enabled sources with poll_config
  2. For each source:
     - Check: is it due? (last_sync_at + avg_sync_interval_sec < NOW())
     - Check: is there already an in-flight api-poll job for this source?
     - If due and no in-flight → enqueue_job('api-poll', {source_key: 'google_calendar'})
```

### api-poll handler:
```
Receive job with {source_key, cursor?, page?}
  1. Load config: SELECT poll_config FROM data_source_registry WHERE source_key = $1
  2. Get auth headers
  3. Build URL: base_url + endpoint + params + watermark
  4. Fetch with rate limiting
  5. Parse response with source-specific parser
  6. Upsert records into target_table
  7. Update watermark
  8. If hasMore → self-chain with nextCursor (but respect max_pages_per_poll)
  9. Update data_source_registry: last_sync_at = NOW(), status = 'active'
  10. If enrichment_jobs configured → enqueue each
```

### Error handling:
```
  - Auth expired → attempt refresh → retry once → if still fails, set status = 'auth_expired', escalate to ARIA
  - Rate limited (429) → backoff, retry with delay
  - Server error (5xx) → increment consecutive_failures, retry next poll
  - Parse error → log bad response, continue with what parsed successfully
  - 3+ consecutive failures → set status = 'degraded', Collector escalates
  - 10+ consecutive failures → set status = 'error', Collector escalates to ARIA
```

## Record Storage

Generic upsert into target table. Each parser returns records with a `_pk` field that identifies the upsert key:

```typescript
async function storeRecords(
  targetTable: string,
  records: Record<string, unknown>[],
  pkField: string = 'id',
): Promise<{ inserted: number; updated: number }> {
  // For each record:
  // INSERT INTO {target_table} (...columns...) VALUES (...values...)
  // ON CONFLICT ({pkField}) DO UPDATE SET ...
}
```

## Example: Google Calendar

```json
{
  "source_key": "google_calendar",
  "poll_config": {
    "auth_type": "oauth",
    "auth_secret_key": "google_tokens",
    "base_url": "https://www.googleapis.com/calendar/v3/calendars/primary",
    "endpoint": "/events",
    "method": "GET",
    "params": {
      "maxResults": 100,
      "singleEvents": true,
      "orderBy": "updated"
    },
    "watermark_field": "updated",
    "watermark_param": "updatedMin",
    "pagination_type": "token",
    "pagination_field": "nextPageToken",
    "pagination_param": "pageToken",
    "target_table": "device_calendar_events",
    "parser": "google_calendar",
    "enrichment_jobs": ["knowledge-backfill"],
    "rate_limit_ms": 200,
    "max_pages_per_poll": 10
  }
}
```

Parser:
```typescript
function parseGoogleCalendar(response: any, config: PollConfig) {
  const events = response.items ?? [];
  return {
    records: events.map(e => ({
      _pk: e.id,
      event_id: e.id,
      title: e.summary,
      description: e.description,
      start_time: e.start?.dateTime ?? e.start?.date,
      end_time: e.end?.dateTime ?? e.end?.date,
      location: e.location,
      attendees: JSON.stringify(e.attendees ?? []),
      status: e.status,
      updated_at: e.updated,
      raw: JSON.stringify(e),
    })),
    nextCursor: response.nextPageToken,
    hasMore: !!response.nextPageToken,
  };
}
```

## Adding a New Source

1. Add row to `data_source_registry` with `poll_config`
2. Write a parser function (10-30 lines)
3. Register parser in the parsers map
4. Done — Collector automatically starts polling it

No new job handlers, no new tools, no new routes.

## Migration Path

Existing bespoke handlers (gmail-backfill, looki-realtime-poll, etc.) keep running during the transition. For each source:
1. Add poll_config to data_source_registry
2. Write and register the parser
3. Test with a manual api-poll job
4. Remove the source from the old handler's nexus_job_routing
5. Delete the old handler (or keep as fallback)

## What This Doesn't Cover

- **Pattern 1 (Push/Webhook):** Handled by `/v1/ingest/*` endpoints — separate architecture
- **Pattern 2 (Relay/Local Sync):** Relay pushes to Nexus API — also separate
- **Pattern 4 (Bulk Import):** One-time large file processing — uses data-import handler
- **OAuth initial authorization:** User must complete the OAuth flow in a browser. Nexus stores the tokens. This design only handles token refresh.
