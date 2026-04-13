-- Migration 023: Add is_person flag to aurora_social_identities
--
-- Filters non-person entries (noreply, helpdesk, notifications, etc.)
-- from relationship analysis and analytics dashboards.

ALTER TABLE aurora_social_identities
  ADD COLUMN IF NOT EXISTS is_person BOOLEAN DEFAULT TRUE;

-- Backfill: mark known non-person patterns as is_person = false
UPDATE aurora_social_identities
SET is_person = FALSE
WHERE display_name ~* '^(no[_-]?reply|noreply|helpdesk|notifications?|info|support|customercare|kontakt|admin|mailer-daemon|postmaster|abuse|billing|sales|marketing|newsletter|updates?|alerts?|system|donotreply|do-not-reply|feedback|service|automated|robot|bot|daemon)$'
   OR display_name ~* '\.(commissioning|service|system|automated)$'
   OR display_name ~* '^(capitalone|paypal|venmo|chase|amazon|google|apple|microsoft|netflix|spotify|uber|lyft|doordash|grubhub|instacart|costco|walmart|target)$';

-- Index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_asi_is_person ON aurora_social_identities (is_person) WHERE is_person = TRUE;

-- Update schema version
INSERT INTO nexus_schema_version (version, description)
VALUES (23, 'Add is_person flag to aurora_social_identities');
