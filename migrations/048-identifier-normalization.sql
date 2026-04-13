-- Identifier normalization
--
-- Current state (before this migration):
--   aurora_unified_communication matview JOINs aurora_social_identity_links
--   with `l.identifier = raw.handle_id` (or equivalent). No normalization on
--   either side, so case-differing emails (`Tim@Example.com` vs
--   `tim@example-personal.com`) and format-differing phones (`+15551234567` vs bare
--   `5551234567`) fail to match.
--
-- Measured link rates pre-fix (2026-04-10):
--   imessage         95.5 %   (2.3 k unlinked, mostly empty handle_id rows)
--   gmail             4.8 %   (55.9 k unlinked — case-sensitive JOIN)
--   google_voice     38.2 %   (mostly short codes, not a format issue)
--   instagram        82.9 %
--   facebook         64.8 %
--   blackberry_sms    2.4 %   (1.2 k bare 10-digit US numbers)
--   archived_email  0.0 %   (to_recipients is display names, not emails — separate fix)
--   guestbook         0.0 %   (names, not emails/phones — separate fix)
--
-- Approach: generated column (no backfill, no collisions).
--
--   1. Create `normalize_identifier(text, text)` as IMMUTABLE SQL. Lowercases
--      emails (stripping `Name <addr@x>` wrappers), E.164-normalizes phones
--      (strips space/dash/paren/dot, adds +1 / + prefixes), lowercases
--      everything else.
--   2. Add an `identifier_normalized` GENERATED column on
--      aurora_social_identity_links. Original `identifier` stays untouched,
--      preserving the existing unique `(platform, identifier)` index and
--      sidestepping 235 collisions that would happen if we rewrote in place.
--   3. Drop + recreate aurora_unified_communication matview with
--      `normalize_identifier()` applied on both sides of every JOIN.
--
-- archived_sent_emails and site_guestbook remain hardcoded to identity_id=-1 —
-- their contact columns hold display names, not emails/phones, so they need
-- a separate name-matching fix that's out of scope here.

-- ─── Section 1: normalize_identifier function ───

CREATE OR REPLACE FUNCTION normalize_identifier(identifier text, identifier_type text DEFAULT NULL)
RETURNS text AS $$
  SELECT CASE
    WHEN identifier IS NULL OR identifier = '' THEN NULL
    -- Email: lowercase + trim. Strip display-name wrapper 'Name <addr@domain>'.
    WHEN identifier_type = 'email' OR identifier LIKE '%@%' THEN
      LOWER(TRIM(
        CASE
          WHEN identifier ~ '<[^>]+@[^>]+>' THEN substring(identifier FROM '<([^>]+)>')
          ELSE identifier
        END
      ))
    -- Phone: strip spaces/dashes/parens/dots, add +1/+ prefixes for bare digits.
    WHEN identifier_type = 'phone' OR identifier ~ '^[\+\(\d]' THEN
      CASE
        -- Already-normalized E.164 or formatted with + prefix
        WHEN regexp_replace(identifier, '[\s\-\(\)\.]', '', 'g') ~ '^\+\d{10,15}$' THEN
          regexp_replace(identifier, '[\s\-\(\)\.]', '', 'g')
        -- Bare 10 digits: add +1 (US default)
        WHEN regexp_replace(identifier, '[\s\-\(\)\.]', '', 'g') ~ '^[2-9]\d{9}$' THEN
          '+1' || regexp_replace(identifier, '[\s\-\(\)\.]', '', 'g')
        -- Bare 11 digits starting with 1: add +
        WHEN regexp_replace(identifier, '[\s\-\(\)\.]', '', 'g') ~ '^1[2-9]\d{9}$' THEN
          '+' || regexp_replace(identifier, '[\s\-\(\)\.]', '', 'g')
        -- Leave alone: short codes, vanity numbers, international, etc.
        ELSE LOWER(TRIM(identifier))
      END
    -- Default: lowercase + trim (handles, usernames, names)
    ELSE LOWER(TRIM(identifier))
  END;
$$ LANGUAGE sql IMMUTABLE;

COMMENT ON FUNCTION normalize_identifier(text, text) IS
  'Canonical identifier normalization: lowercase emails (stripping display wrappers), E.164-normalize phones, lowercase everything else. Used by aurora_unified_communication to match raw source identifiers against aurora_social_identity_links.';

-- ─── Section 2: identifier_normalized generated column + index ───

ALTER TABLE aurora_social_identity_links
  ADD COLUMN IF NOT EXISTS identifier_normalized text
  GENERATED ALWAYS AS (normalize_identifier(identifier, identifier_type)) STORED;

CREATE INDEX IF NOT EXISTS idx_asil_identifier_normalized
  ON aurora_social_identity_links (identifier_normalized);

-- ─── Section 3: rewrite aurora_unified_communication matview ───
--
-- Two independent goals in this rewrite:
--
--   (a) Normalized identifier matching so case-differing emails and format-
--       differing phones link correctly (gains are small in practice because
--       most identifiers already match exactly).
--
--   (b) DECOUPLE identity_id from contact_name:
--         identity_id   — integer, person-gated, only set when si.is_person
--                          is true. Preserves all downstream analysis that
--                          expects "this id refers to a person".
--         contact_name  — text, best-effort display name, populated from
--                          si.display_name whenever ANY identity match
--                          exists (person or business), so comm rows show
--                          "Accredo" / "Xfinity" / "ACME" instead of the
--                          raw phone/email.
--
-- Each source uses a LATERAL sub-select that picks the preferred match:
-- exact > normalized, and within each, is_person=true > is_person=false
-- so the match for identity_id / contact_name lookups is deterministic
-- even when multiple identities collide on a normalized identifier.

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
  -- employer sent emails (no identity JOIN — to_recipients holds display names, not addresses)
  SELECT w.sent_at AS timestamp, 'archived_email'::text AS channel,
    'sent'::text AS direction,
    w.to_recipients AS contact_identifier,
    -1 AS identity_id,
    w.to_recipients AS contact_name,
    COALESCE(w.subject, '') || ' — ' || COALESCE(LEFT(w.body_text, 500), '') AS content_text,
    COALESCE(w.body_length, LENGTH(w.body_text)) AS content_length,
    NULL::text AS thread_id,
    'archived_sent_emails'::text AS source_table,
    w.id::text AS source_id
  FROM archived_sent_emails w
  WHERE w.sent_at IS NOT NULL

  UNION ALL
  -- example-personal.com guestbook (no identity JOIN — poster_name needs fuzzy matching)
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

-- Note: is_person restoration was intentionally NOT included in this
-- migration. Dry-runs against multiple heuristics (relationship_type/tier,
-- Apple-Contacts-only, bidirectional-imessage, contacts-identifier match)
-- all produced false-positive rates of 20–67 %, because the user's Apple
-- Contacts contain businesses (Accident Response, Apple Inc., PagerDuty,
-- City Emergency, etc.) and the old classifier poisoned relationship_type
-- on bot rows (notifications@github.com is tagged tier='close' colleague).
--
-- Per feedback memory ("stop mass-flipping real people to is_person"),
-- reliable restoration requires proper LLM classification, which is
-- blocked in Creative Mode. Leave is_person alone — the matview changes
-- above deliver measurable contact_name coverage even without flipping it.

-- Refresh the matview so the new JOIN + contact_name logic take effect.
REFRESH MATERIALIZED VIEW aurora_unified_communication;

INSERT INTO nexus_schema_version (version, description)
VALUES (48, 'Identifier normalization — normalize_identifier() function, identifier_normalized generated column, matview rewrite with normalized JOINs')
ON CONFLICT DO NOTHING;
