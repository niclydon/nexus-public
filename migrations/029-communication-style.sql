-- Migration 029: Per-person communication style features and cluster labels

CREATE TABLE IF NOT EXISTS person_communication_style (
  person_id INTEGER PRIMARY KEY,
  contact_name TEXT,
  avg_message_length REAL,
  message_length_stddev REAL,
  emoji_frequency REAL,
  question_frequency REAL,
  primary_contact_hour REAL,
  contact_hour_spread REAL,
  weekend_ratio REAL,
  initiated_ratio REAL,
  avg_sentiment_score REAL,
  style_cluster INTEGER,
  cluster_label TEXT,
  computed_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO nexus_schema_version (version, description)
VALUES (29, 'Per-person communication style features and cluster labels');
