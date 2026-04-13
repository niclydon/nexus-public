-- Extract and index guestbook messages from example-personal.com family pages (2004-2005).
-- Form submissions were emailed to user@example.com and stored in aurora_raw_gmail.
-- This migration creates the table, parses all 710 original submissions, and deduplicates.

-- Step 1: Create the guestbook table
CREATE TABLE IF NOT EXISTS site_guestbook (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    gmail_id TEXT UNIQUE,
    recipient_page TEXT NOT NULL,
    recipient_name TEXT,
    poster_name TEXT,
    posted_at TIMESTAMPTZ,
    message_text TEXT,
    raw_body TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_guestbook_recipient ON site_guestbook(recipient_page);
CREATE INDEX IF NOT EXISTS idx_guestbook_poster ON site_guestbook(poster_name);
CREATE INDEX IF NOT EXISTS idx_guestbook_date ON site_guestbook(posted_at);

-- Step 2: Page-to-person mapping
CREATE TABLE IF NOT EXISTS site_guestbook_pages (
    page_key TEXT PRIMARY KEY,
    person_name TEXT NOT NULL,
    section TEXT NOT NULL DEFAULT 'family'
);

-- Populate with page_key → person_name mappings for your site's guestbook pages.
-- Example: INSERT INTO site_guestbook_pages (page_key, person_name, section) VALUES
--     ('person_a', 'Person A', 'family'),
--     ('person_b', 'Person B', 'members');
-- ON CONFLICT (page_key) DO NOTHING;

-- Step 3: Extract and insert guestbook messages from original form submissions only
-- (exclude FW:, Fwd:, Re:, RE: prefixed subjects to avoid duplicates)
INSERT INTO site_guestbook (gmail_id, recipient_page, recipient_name, poster_name, posted_at, message_text, raw_body)
SELECT
    g.id AS gmail_id,
    -- Extract page key from URL (last path segment without .html)
    regexp_replace(
        regexp_replace(g.subject, '.*/', ''),
        '\.html.*', ''
    ) AS recipient_page,
    -- Map page to person name
    p.person_name AS recipient_name,
    -- Extract poster name: "Name: <value>" line
    NULLIF(TRIM(regexp_replace(
        (regexp_match(g.body_text, 'Name:\s*([^\r\n]+)', 'i'))[1],
        '\s+$', '', 'g'
    )), '') AS poster_name,
    -- Build posted_at from Date + Time fields if present, else fall back to email date
    CASE
        WHEN g.body_text ~ 'Date:\s*\d{2}/\d{2}/\d{4}' THEN
            CASE
                WHEN g.body_text ~ 'Time:\s*\d{2}:\d{2}\s*(AM|PM)' THEN
                    to_timestamp(
                        TRIM((regexp_match(g.body_text, 'Date:\s*(\d{2}/\d{2}/\d{4})'))[1])
                        || ' ' ||
                        TRIM((regexp_match(g.body_text, 'Time:\s*(\d{2}:\d{2}\s*(?:AM|PM))'))[1]),
                        'MM/DD/YYYY HH:MI AM'
                    )
                ELSE
                    to_timestamp(
                        TRIM((regexp_match(g.body_text, 'Date:\s*(\d{2}/\d{2}/\d{4})'))[1]),
                        'MM/DD/YYYY'
                    )
            END
        ELSE g.date
    END AS posted_at,
    -- Extract message: everything after "Comments:" line, trimmed
    NULLIF(TRIM(regexp_replace(
        regexp_replace(
            (regexp_match(g.body_text, 'Comments:\s*[\r\n]+(.+)', 'ns'))[1],
            '\r', '', 'g'
        ),
        '^\s+|\s+$', '', 'g'
    )), '') AS message_text,
    g.body_text AS raw_body
FROM aurora_raw_gmail g
LEFT JOIN site_guestbook_pages p ON p.page_key = regexp_replace(
    regexp_replace(g.subject, '.*/', ''),
    '\.html.*', ''
)
WHERE g.subject LIKE '%Data posted to form%example-personal.com%'
  AND g.subject NOT LIKE 'FW:%'
  AND g.subject NOT LIKE 'Fwd:%'
  AND g.subject NOT LIKE 'Re:%'
  AND g.subject NOT LIKE 'RE:%'
ON CONFLICT (gmail_id) DO NOTHING;

-- Step 4: Also extract messages from FW/Fwd emails that DON'T have a matching original
-- (These were forwarded from Hotmail and may contain unique messages)
INSERT INTO site_guestbook (gmail_id, recipient_page, recipient_name, poster_name, posted_at, message_text, raw_body)
SELECT
    g.id AS gmail_id,
    regexp_replace(
        regexp_replace(g.subject, '.*/', ''),
        '\.html.*', ''
    ) AS recipient_page,
    p.person_name AS recipient_name,
    NULLIF(TRIM(regexp_replace(
        (regexp_match(g.body_text, 'Name:\s*([^\r\n]+)', 'i'))[1],
        '\s+$', '', 'g'
    )), '') AS poster_name,
    CASE
        WHEN g.body_text ~ 'Date:\s*\d{2}/\d{2}/\d{4}' THEN
            CASE
                WHEN g.body_text ~ 'Time:\s*\d{2}:\d{2}\s*(AM|PM)' THEN
                    to_timestamp(
                        TRIM((regexp_match(g.body_text, 'Date:\s*(\d{2}/\d{2}/\d{4})'))[1])
                        || ' ' ||
                        TRIM((regexp_match(g.body_text, 'Time:\s*(\d{2}:\d{2}\s*(?:AM|PM))'))[1]),
                        'MM/DD/YYYY HH:MI AM'
                    )
                ELSE
                    to_timestamp(
                        TRIM((regexp_match(g.body_text, 'Date:\s*(\d{2}/\d{2}/\d{4})'))[1]),
                        'MM/DD/YYYY'
                    )
            END
        ELSE g.date
    END AS posted_at,
    NULLIF(TRIM(regexp_replace(
        regexp_replace(
            (regexp_match(g.body_text, 'Comments:\s*[\r\n]+(.+)', 'ns'))[1],
            '\r', '', 'g'
        ),
        '^\s+|\s+$', '', 'g'
    )), '') AS message_text,
    g.body_text AS raw_body
FROM aurora_raw_gmail g
LEFT JOIN site_guestbook_pages p ON p.page_key = regexp_replace(
    regexp_replace(g.subject, '.*/', ''),
    '\.html.*', ''
)
WHERE g.subject LIKE '%Data posted to form%example-personal.com%'
  AND (g.subject LIKE 'FW:%' OR g.subject LIKE 'Fwd:%')
  -- Only include if no original with matching content exists
  AND NOT EXISTS (
      SELECT 1 FROM site_guestbook gb
      WHERE gb.poster_name = NULLIF(TRIM(regexp_replace(
          (regexp_match(g.body_text, 'Name:\s*([^\r\n]+)', 'i'))[1],
          '\s+$', '', 'g'
      )), '')
      AND gb.message_text = NULLIF(TRIM(regexp_replace(
          regexp_replace(
              (regexp_match(g.body_text, 'Comments:\s*[\r\n]+(.+)', 'ns'))[1],
              '\r', '', 'g'
          ),
          '^\s+|\s+$', '', 'g'
      )), '')
      AND gb.recipient_page = regexp_replace(
          regexp_replace(g.subject, '.*/', ''),
          '\.html.*', ''
      )
  )
ON CONFLICT (gmail_id) DO NOTHING;

-- Step 5: Fix anonymous posts where blank Name field caused regex to capture "Comments:"
UPDATE site_guestbook
SET poster_name = NULL
WHERE LOWER(poster_name) = 'comments:';

-- Record migration
INSERT INTO nexus_schema_version (version, description)
VALUES (36, 'Extract example-personal.com guestbook messages from Gmail archive (2004-2005)')
ON CONFLICT DO NOTHING;
