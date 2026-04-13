CREATE TABLE IF NOT EXISTS person_social_profiles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    person_id INTEGER NOT NULL REFERENCES aurora_social_identities(id),
    platform TEXT NOT NULL,
    profile_url TEXT NOT NULL,
    username TEXT,
    display_name TEXT,
    bio TEXT,
    confidence REAL DEFAULT 0.5,
    verified_by TEXT,
    verified_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(person_id, platform)
);
CREATE INDEX idx_social_profiles_person ON person_social_profiles(person_id);
INSERT INTO nexus_job_routing (job_type) VALUES ('people-enrich') ON CONFLICT DO NOTHING;
