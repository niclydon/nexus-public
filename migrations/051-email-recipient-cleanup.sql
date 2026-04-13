-- [Employer] recipient name cleanup + identity lookup for email-format entries
--
-- archived_sent_emails.to_recipients is a denormalized text field that mostly
-- holds display names in "Last, First" format:
--
--   Smith, Jane                              (1,259 of 1,357 — 93 %)
--   'John A. Doe'                         (6 — quoted)
--   'user@company.com'                    (59 — email addresses)
--   (2 empty)
--
-- Migration 048 left the archived_sent_emails branch of aurora_unified_communication
-- with contact_name = to_recipients, which means rows display the raw ugly
-- form "Smith, Jane" instead of "Jane Smith". Fuzzy matching against
-- aurora_social_identities was considered, but only 4 of 596 unique contacts
-- actually exist as identities — these are 2008-2010 professional contacts
-- that were never ingested.
--
-- This migration does two things:
--
--   (a) Adds a clean_email_recipient() IMMUTABLE SQL function that parses the
--       three common formats and returns a human-readable display name.
--
--   (b) Rewrites the archived_sent_emails branch of the matview to use the
--       cleaned name as contact_name, AND to perform identity lookups for
--       email-format entries (the 59 with '@') via the same normalized
--       identifier-linking logic gmail uses.
--
-- Guestbook is intentionally skipped — poster_name is informal ("auntie",
-- "person_a", "me again") with heavy garbage, not worth cleaning.

CREATE OR REPLACE FUNCTION clean_email_recipient(raw text)
RETURNS text AS $$
  SELECT CASE
    WHEN raw IS NULL OR trim(raw) = '' THEN NULL
    -- Email format: leave as-is (matview does an identity lookup on these)
    WHEN raw LIKE '%@%' THEN
      -- Strip surrounding quotes for cleaner display
      LOWER(trim(regexp_replace(raw, '^''(.+)''$', '\1')))
    -- Quoted name: 'John A. Doe' → John A. Doe
    WHEN raw ~ '^''[^'']+''$' THEN
      regexp_replace(raw, '^''(.+)''$', '\1')
    -- Last, First format → First Last
    WHEN raw ~ '^[A-Z][A-Za-z''.-]+,\s*[A-Z][A-Za-z''.\s-]+$' THEN
      regexp_replace(trim(raw), '^([^,]+),\s*(.+)$', '\2 \1')
    -- Fallback: trim whitespace
    ELSE trim(raw)
  END;
$$ LANGUAGE sql IMMUTABLE;

COMMENT ON FUNCTION clean_email_recipient(text) IS
  'Normalize archived_sent_emails.to_recipients for human display: flips "Last, First" → "First Last", strips quotes, lowercases emails.';

-- Rewrite aurora_unified_communication matview with archived_sent_emails improvements.
-- This is mechanically a copy of migration 048''s matview with JUST the
-- archived_sent_emails branch changed. Everything else stays identical.

DROP MATERIALIZED VIEW IF EXISTS aurora_unified_communication;

CREATE MATERIALIZED VIEW aurora_unified_communication AS
SELECT DISTINCT ON (source_table, source_id)
  timestamp, channel, direction, contact_identifier, identity_id, contact_name,
  content_text, content_length, thread_id, source_table, source_id
FROM (
  -- imessage
  SELECT m.date AS timestamp, 'imessage'::text AS channel,
    CASE WHEN m.is_from_me THEN 'sent' ELSE 'received' END AS direction,
    m.handle_id AS contact_identifier,
    CASE WHEN match.is_person THEN match.identity_id ELSE -1 END AS identity_id,
    COALESCE(match.display_name, m.handle_id) AS contact_name,
    m.text AS content_text,
    LENGTH(m.text) AS content_length,
    m.chat_id AS thread_id,
    'aurora_raw_imessage'::text AS source_table,
    m.guid AS source_id
  FROM aurora_raw_imessage m
  LEFT JOIN LATERAL (
    SELECT l.identity_id, si.display_name, si.is_person
    FROM aurora_social_identity_links l
    JOIN aurora_social_identities si ON si.id = l.identity_id
    WHERE l.identifier = m.handle_id OR l.identifier_normalized = normalize_identifier(m.handle_id)
    ORDER BY si.is_person DESC, (l.identifier = m.handle_id) DESC, l.match_confidence DESC NULLS LAST
    LIMIT 1
  ) match ON true
  WHERE m.date IS NOT NULL

  UNION ALL
  -- gmail
  SELECT g.date, 'gmail'::text,
    CASE WHEN g.is_sent THEN 'sent' ELSE 'received' END,
    CASE WHEN g.is_sent THEN g.to_addresses[1] ELSE g.from_address END AS contact_identifier,
    CASE WHEN match.is_person THEN match.identity_id ELSE -1 END,
    COALESCE(match.display_name, g.from_name, CASE WHEN g.is_sent THEN g.to_addresses[1] ELSE g.from_address END),
    g.body_text,
    LENGTH(g.body_text),
    g.thread_id,
    'aurora_raw_gmail'::text,
    g.id
  FROM aurora_raw_gmail g
  LEFT JOIN LATERAL (
    SELECT l.identity_id, si.display_name, si.is_person
    FROM aurora_social_identity_links l
    JOIN aurora_social_identities si ON si.id = l.identity_id
    WHERE l.identifier = (CASE WHEN g.is_sent THEN g.to_addresses[1] ELSE g.from_address END)
       OR l.identifier_normalized = normalize_identifier(CASE WHEN g.is_sent THEN g.to_addresses[1] ELSE g.from_address END)
    ORDER BY si.is_person DESC,
             (l.identifier = (CASE WHEN g.is_sent THEN g.to_addresses[1] ELSE g.from_address END)) DESC,
             l.match_confidence DESC NULLS LAST
    LIMIT 1
  ) match ON true
  WHERE g.date > '2000-01-01'::timestamptz

  UNION ALL
  -- instagram messages
  SELECT ig.timestamp, 'instagram'::text, 'received'::text,
    ig.sender,
    CASE WHEN match.is_person THEN match.identity_id ELSE -1 END,
    COALESCE(match.display_name, ig.sender),
    ig.content,
    LENGTH(ig.content),
    NULL::text,
    'aurora_raw_instagram'::text,
    ig.id::text
  FROM aurora_raw_instagram ig
  LEFT JOIN LATERAL (
    SELECT l.identity_id, si.display_name, si.is_person
    FROM aurora_social_identity_links l
    JOIN aurora_social_identities si ON si.id = l.identity_id
    WHERE l.identifier = ig.sender OR l.identifier_normalized = normalize_identifier(ig.sender)
    ORDER BY si.is_person DESC, (l.identifier = ig.sender) DESC, l.match_confidence DESC NULLS LAST
    LIMIT 1
  ) match ON true
  WHERE ig.data_type = 'message' AND ig.timestamp IS NOT NULL

  UNION ALL
  -- facebook messages
  SELECT fb.timestamp, 'facebook'::text, 'received'::text,
    fb.sender,
    CASE WHEN match.is_person THEN match.identity_id ELSE -1 END,
    COALESCE(match.display_name, fb.sender),
    fb.content,
    LENGTH(fb.content),
    NULL::text,
    'aurora_raw_facebook'::text,
    fb.id::text
  FROM aurora_raw_facebook fb
  LEFT JOIN LATERAL (
    SELECT l.identity_id, si.display_name, si.is_person
    FROM aurora_social_identity_links l
    JOIN aurora_social_identities si ON si.id = l.identity_id
    WHERE l.identifier = fb.sender OR l.identifier_normalized = normalize_identifier(fb.sender)
    ORDER BY si.is_person DESC, (l.identifier = fb.sender) DESC, l.match_confidence DESC NULLS LAST
    LIMIT 1
  ) match ON true
  WHERE fb.data_type = 'message' AND fb.timestamp IS NOT NULL

  UNION ALL
  -- google voice texts
  SELECT gv.timestamp, 'google_voice'::text,
    CASE WHEN gv.is_incoming THEN 'received' ELSE 'sent' END,
    gv.phone_number,
    CASE WHEN match.is_person THEN match.identity_id ELSE -1 END,
    COALESCE(match.display_name, gv.phone_number),
    gv.message_text,
    LENGTH(gv.message_text),
    NULL::text,
    'aurora_raw_google_voice'::text,
    gv.id::text
  FROM aurora_raw_google_voice gv
  LEFT JOIN LATERAL (
    SELECT l.identity_id, si.display_name, si.is_person
    FROM aurora_social_identity_links l
    JOIN aurora_social_identities si ON si.id = l.identity_id
    WHERE l.identifier = gv.phone_number OR l.identifier_normalized = normalize_identifier(gv.phone_number)
    ORDER BY si.is_person DESC, (l.identifier = gv.phone_number) DESC, l.match_confidence DESC NULLS LAST
    LIMIT 1
  ) match ON true
  WHERE gv.record_type = 'text' AND gv.timestamp IS NOT NULL

  UNION ALL
  -- blackberry sms
  SELECT s.timestamp, 'blackberry_sms'::text AS channel,
    CASE WHEN s.direction = 'sent' THEN 'sent' ELSE 'received' END AS direction,
    s.phone_number AS contact_identifier,
    CASE WHEN match.is_person THEN match.identity_id ELSE -1 END AS identity_id,
    COALESCE(match.display_name, s.phone_number) AS contact_name,
    s.message AS content_text,
    LENGTH(s.message) AS content_length,
    NULL::text AS thread_id,
    'bb_sms_messages'::text AS source_table,
    s.id::text AS source_id
  FROM bb_sms_messages s
  LEFT JOIN LATERAL (
    SELECT l.identity_id, si.display_name, si.is_person
    FROM aurora_social_identity_links l
    JOIN aurora_social_identities si ON si.id = l.identity_id
    WHERE l.identifier = s.phone_number OR l.identifier_normalized = normalize_identifier(s.phone_number)
    ORDER BY si.is_person DESC, (l.identifier = s.phone_number) DESC, l.match_confidence DESC NULLS LAST
    LIMIT 1
  ) match ON true
  WHERE s.timestamp IS NOT NULL

  UNION ALL
  -- employer sent emails — clean display name + identity lookup on email-format entries
  SELECT w.sent_at AS timestamp, 'archived_email'::text AS channel,
    'sent'::text AS direction,
    w.to_recipients AS contact_identifier,
    CASE WHEN match.is_person THEN match.identity_id ELSE -1 END AS identity_id,
    COALESCE(match.display_name, clean_email_recipient(w.to_recipients), w.to_recipients) AS contact_name,
    COALESCE(w.subject, '') || ' — ' || COALESCE(LEFT(w.body_text, 500), '') AS content_text,
    COALESCE(w.body_length, LENGTH(w.body_text)) AS content_length,
    NULL::text AS thread_id,
    'archived_sent_emails'::text AS source_table,
    w.id::text AS source_id
  FROM archived_sent_emails w
  LEFT JOIN LATERAL (
    SELECT l.identity_id, si.display_name, si.is_person
    FROM aurora_social_identity_links l
    JOIN aurora_social_identities si ON si.id = l.identity_id
    -- Only attempt identity lookup when the recipient looks like an email.
    WHERE w.to_recipients LIKE '%@%'
      AND l.identifier_normalized = normalize_identifier(clean_email_recipient(w.to_recipients))
    ORDER BY si.is_person DESC, l.match_confidence DESC NULLS LAST
    LIMIT 1
  ) match ON true
  WHERE w.sent_at IS NOT NULL

  UNION ALL
  -- example-personal.com guestbook — still unlinked (poster_name needs fuzzy name matching; out of scope)
  SELECT g.posted_at AS timestamp, 'guestbook'::text AS channel,
    'received'::text AS direction,
    g.poster_name AS contact_identifier,
    -1 AS identity_id,
    g.poster_name AS contact_name,
    g.message_text AS content_text,
    LENGTH(g.message_text) AS content_length,
    g.recipient_page AS thread_id,
    'site_guestbook'::text AS source_table,
    g.id::text AS source_id
  FROM site_guestbook g
  WHERE g.posted_at IS NOT NULL
) raw;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unified_communication_source
  ON aurora_unified_communication (source_table, source_id);
CREATE INDEX IF NOT EXISTS idx_unified_communication_timestamp
  ON aurora_unified_communication (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_unified_communication_identity
  ON aurora_unified_communication (identity_id) WHERE identity_id != -1;

REFRESH MATERIALIZED VIEW aurora_unified_communication;

INSERT INTO nexus_schema_version (version, description)
VALUES (51, '[Employer] recipient name cleanup — clean_email_recipient() + matview branch rewrite')
ON CONFLICT DO NOTHING;
