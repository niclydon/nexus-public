-- Ensure aurora_raw_health_records table exists with proper schema and indexes.
-- This table may already exist from the ARIA migration, but CREATE IF NOT EXISTS
-- keeps it idempotent. The unique constraint prevents duplicate records from both
-- the iOS HealthKit sync and the Apple Health XML bulk import.

CREATE TABLE IF NOT EXISTS aurora_raw_health_records (
    id BIGSERIAL PRIMARY KEY,
    record_type TEXT NOT NULL,
    source_name TEXT,
    unit TEXT,
    value REAL,
    start_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ,
    creation_date TIMESTAMPTZ,
    device TEXT,
    metadata JSONB DEFAULT '{}'
);

-- Unique constraint for dedup (type + start_date + source + value covers both iOS daily
-- summaries and per-reading XML records). Using a partial unique index to handle NULLs.
CREATE UNIQUE INDEX IF NOT EXISTS idx_health_records_dedup
    ON aurora_raw_health_records (record_type, start_date, source_name, value)
    WHERE source_name IS NOT NULL AND value IS NOT NULL;

-- Query patterns: filter by type + date range (most common), or scan by date
CREATE INDEX IF NOT EXISTS idx_health_type_date
    ON aurora_raw_health_records (record_type, start_date DESC);

CREATE INDEX IF NOT EXISTS idx_health_date
    ON aurora_raw_health_records (start_date DESC);

-- Track migration
INSERT INTO nexus_schema_version (version, description)
VALUES (35, 'Apple Health records table with dedup and indexes')
ON CONFLICT DO NOTHING;
