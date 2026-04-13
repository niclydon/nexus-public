-- Migration 028: Voice metrics table for per-person speaking statistics
-- Computed from voice-print diarization data in the aria database

CREATE TABLE IF NOT EXISTS person_voice_metrics (
  person_id INTEGER PRIMARY KEY,
  contact_name TEXT,
  total_segments INTEGER,
  total_speaking_seconds REAL,
  avg_segment_duration REAL,
  conversations_count INTEGER,
  avg_dominance_ratio REAL,
  most_active_hour INTEGER,
  voice_confidence_avg REAL,
  computed_at TIMESTAMPTZ DEFAULT NOW()
);
