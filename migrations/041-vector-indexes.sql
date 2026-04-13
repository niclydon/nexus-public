-- Migration 041: Add HNSW vector indexes (cosine distance) to all embedding columns
-- Tables with existing vector indexes are skipped (gmail_archive, imessage_incoming,
-- knowledge_conversation_chunks, knowledge_entities, knowledge_facts, photo_metadata)

-- Large tables (>50K rows) — custom HNSW parameters
CREATE INDEX IF NOT EXISTS idx_aurora_raw_imessage_embedding_hnsw
  ON aurora_raw_imessage USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_aurora_raw_gmail_embedding_hnsw
  ON aurora_raw_gmail USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64)
  WHERE embedding IS NOT NULL;

-- Remaining tables — default HNSW parameters
CREATE INDEX IF NOT EXISTS idx_apple_notes_embedding_hnsw
  ON apple_notes USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_aria_journal_embedding_hnsw
  ON aria_journal USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_aurora_raw_chatgpt_embedding_hnsw
  ON aurora_raw_chatgpt USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_aurora_raw_claude_embedding_hnsw
  ON aurora_raw_claude USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_aurora_raw_facebook_embedding_hnsw
  ON aurora_raw_facebook USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_aurora_raw_google_chat_embedding_hnsw
  ON aurora_raw_google_chat USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_aurora_raw_google_voice_embedding_hnsw
  ON aurora_raw_google_voice USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_aurora_raw_instagram_embedding_hnsw
  ON aurora_raw_instagram USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_aurora_raw_photos_embedding_hnsw
  ON aurora_raw_photos USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_aurora_raw_siri_interactions_embedding_hnsw
  ON aurora_raw_siri_interactions USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bb_sms_messages_embedding_hnsw
  ON bb_sms_messages USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_blogger_posts_embedding_hnsw
  ON blogger_posts USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_contacts_embedding_hnsw
  ON contacts USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_conversations_embedding_hnsw
  ON conversations USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_data_imports_embedding_hnsw
  ON data_imports USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_life_narration_embedding_hnsw
  ON life_narration USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_life_transactions_embedding_hnsw
  ON life_transactions USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_looki_moments_embedding_hnsw
  ON looki_moments USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_music_library_embedding_hnsw
  ON music_library USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_music_listening_history_embedding_hnsw
  ON music_listening_history USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_site_guestbook_embedding_hnsw
  ON site_guestbook USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_proactive_insights_embedding_hnsw
  ON proactive_insights USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_strava_activities_embedding_hnsw
  ON strava_activities USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_archived_sent_emails_embedding_hnsw
  ON archived_sent_emails USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;

INSERT INTO nexus_schema_version (version) VALUES (41);
