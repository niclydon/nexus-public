-- Migration 027: Merge candidates and audit log for entity dedup review

CREATE TABLE IF NOT EXISTS merge_candidates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,           -- 'person', 'knowledge_entity', 'photo_person', 'contact_person'
  record_a_id TEXT NOT NULL,
  record_b_id TEXT NOT NULL,
  confidence REAL NOT NULL,
  match_reason JSONB,
  status TEXT DEFAULT 'pending',       -- pending, merged, dismissed, deferred
  decided_by TEXT,
  decided_at TIMESTAMPTZ,
  skip_count INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(entity_type, record_a_id, record_b_id)
);

CREATE INDEX IF NOT EXISTS idx_mc_status_conf ON merge_candidates (status, confidence DESC) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_mc_entity_type ON merge_candidates (entity_type) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS merge_audit_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  merge_candidate_id UUID REFERENCES merge_candidates(id),
  entity_type TEXT NOT NULL,
  keeper_id TEXT NOT NULL,
  merged_id TEXT NOT NULL,
  pre_merge_snapshot JSONB NOT NULL,
  field_choices JSONB,
  merged_by TEXT DEFAULT 'owner',
  merged_at TIMESTAMPTZ DEFAULT NOW(),
  undone_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_mal_recent ON merge_audit_log (merged_at DESC) WHERE undone_at IS NULL;

INSERT INTO nexus_schema_version (version, description)
VALUES (27, 'Merge candidates and audit log for entity dedup review');
