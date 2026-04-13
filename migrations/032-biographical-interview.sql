BEGIN;

CREATE TABLE IF NOT EXISTS bio_inference_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    inference_type TEXT NOT NULL,
    subject_person_id INTEGER REFERENCES aurora_social_identities(id),
    related_person_id INTEGER REFERENCES aurora_social_identities(id),
    hypothesis TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '[]',
    confidence REAL NOT NULL DEFAULT 0.5,
    status TEXT NOT NULL DEFAULT 'pending',
    user_notes TEXT,
    decided_at TIMESTAMPTZ,
    rule_id UUID,
    batch_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_bio_iq_status ON bio_inference_queue(status) WHERE status = 'pending';
CREATE INDEX idx_bio_iq_confidence ON bio_inference_queue(confidence DESC) WHERE status = 'pending';
CREATE INDEX idx_bio_iq_subject ON bio_inference_queue(subject_person_id) WHERE subject_person_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS bio_inference_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_key TEXT UNIQUE NOT NULL,
    description TEXT NOT NULL,
    pattern JSONB NOT NULL DEFAULT '{}',
    total_applied INTEGER NOT NULL DEFAULT 0,
    confirmed_count INTEGER NOT NULL DEFAULT 0,
    corrected_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    confidence_boost REAL NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Extend life chapters
ALTER TABLE aurora_life_chapters ADD COLUMN IF NOT EXISTS narrative TEXT;
ALTER TABLE aurora_life_chapters ADD COLUMN IF NOT EXISTS key_people INTEGER[] DEFAULT '{}';
ALTER TABLE aurora_life_chapters ADD COLUMN IF NOT EXISTS key_places TEXT[] DEFAULT '{}';
ALTER TABLE aurora_life_chapters ADD COLUMN IF NOT EXISTS source_facts UUID[] DEFAULT '{}';
ALTER TABLE aurora_life_chapters ADD COLUMN IF NOT EXISTS is_confirmed BOOLEAN DEFAULT FALSE;

-- Seed rules
INSERT INTO bio_inference_rules (rule_key, description, pattern, confidence_boost) VALUES
    ('contacts_relation_explicit', 'Apple Contacts explicit relation field', '{"source":"contacts","signal":"relation_field"}', 0.3),
    ('same_last_name_family', 'Same last name + family context messages', '{"signal":"name_match","context":"family"}', 0.15),
    ('message_possessive_relation', 'Message says "my brother/sister/etc"', '{"source":"messages","signal":"possessive_relation"}', 0.25),
    ('co_occurrence_high', 'High photo co-occurrence (>10)', '{"source":"photos","signal":"co_occurrence","threshold":10}', 0.1),
    ('calendar_shared_events', 'Shared calendar events', '{"source":"calendar","signal":"shared_events"}', 0.1),
    ('cross_reference_mention', 'Third party mentions person', '{"signal":"third_party_mention"}', 0.15),
    ('geo_temporal_match', 'Same location same time in photos', '{"source":"photos","signal":"geo_temporal_match"}', 0.1),
    ('email_domain_career', 'Email domain matches company', '{"source":"email","signal":"corporate_domain"}', 0.2)
ON CONFLICT (rule_key) DO NOTHING;

INSERT INTO nexus_job_routing (job_type) VALUES ('biographical-interview') ON CONFLICT DO NOTHING;
INSERT INTO nexus_schema_version (version, description) VALUES (32, 'Biographical Interview: inference queue, rules, life chapter enrichment') ON CONFLICT (version) DO NOTHING;

COMMIT;
