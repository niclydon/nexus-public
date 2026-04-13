-- Migration 021: ARIA Chat Infrastructure
-- Creates channels/channel_messages in nexus DB, seeds aria-direct channel,
-- migrates ARIA's conversation history, memories, journal, and avatar.
-- Also adds a universal conversational-chat skill for all agents.

BEGIN;

-- ============================================================
-- 1. Channels + Channel Messages (ported from chancery migration 011)
-- ============================================================

CREATE TABLE IF NOT EXISTS channels (
    id text PRIMARY KEY,
    name text NOT NULL,
    purpose text,
    channel_type text NOT NULL DEFAULT 'custom',
    agents text[] NOT NULL DEFAULT '{}',
    is_permanent boolean DEFAULT false,
    is_archived boolean DEFAULT false,
    archive_summary text,
    archived_at timestamptz,
    created_by text NOT NULL DEFAULT 'owner',
    created_at timestamptz DEFAULT NOW(),
    updated_at timestamptz DEFAULT NOW(),
    CONSTRAINT valid_channel_type CHECK (channel_type IN ('group', 'direct', 'custom'))
);

CREATE TABLE IF NOT EXISTS channel_messages (
    id serial PRIMARY KEY,
    channel_id text NOT NULL REFERENCES channels(id),
    sender_id text NOT NULL,
    message_type text NOT NULL DEFAULT 'message',
    content text NOT NULL,
    metadata jsonb DEFAULT '{}',
    in_reply_to integer REFERENCES channel_messages(id),
    created_at timestamptz DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_channel_messages_channel
    ON channel_messages(channel_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_channel_messages_sender
    ON channel_messages(sender_id, created_at DESC);

-- Seed the aria-direct channel
INSERT INTO channels (id, name, channel_type, agents, is_permanent)
VALUES ('aria-direct', 'ARIA Direct', 'direct', ARRAY['aria'], true)
ON CONFLICT (id) DO NOTHING;

-- ============================================================
-- 2. Seed conversation history from ARIA-era messages table
-- ============================================================

-- Insert in chronological order (oldest first) so serial IDs match time order
INSERT INTO channel_messages (channel_id, sender_id, content, metadata, created_at)
SELECT 'aria-direct',
       CASE WHEN m.role = 'user' THEN 'owner' ELSE 'aria' END,
       LEFT(m.content, 2000),
       jsonb_build_object('migrated_from', 'aria_messages', 'conversation_id', m.conversation_id::text),
       m.msg_created_at
FROM (
    SELECT m2.role, m2.content, m2.conversation_id, m2.created_at AS msg_created_at
    FROM messages m2
    JOIN conversations c ON c.id = m2.conversation_id
    WHERE c.hidden_at IS NULL
    ORDER BY m2.created_at DESC
    LIMIT 200
) m
ORDER BY m.msg_created_at ASC;

-- ============================================================
-- 3. Seed agent_memory from core_memory (most recent 500)
-- ============================================================

INSERT INTO agent_memory (agent_id, memory_type, category, content, confidence, source, created_at)
SELECT 'aria', 'learned', cm.category,
       cm.key || ': ' || cm.value,
       1.0, 'seeded_from_core_memory', cm.created_at
FROM core_memory cm
WHERE cm.superseded_by IS NULL
ORDER BY cm.updated_at DESC
LIMIT 500;

-- ============================================================
-- 4. Seed agent_journals from aria_journal
-- ============================================================

INSERT INTO agent_journals (agent_id, content, mood, signals_reviewed, created_at)
SELECT 'aria', aj.content, aj.mood,
       COALESCE(aj.sources, '{}'::jsonb),
       aj.created_at
FROM aria_journal aj
WHERE aj.content IS NOT NULL AND aj.content != ''
ORDER BY aj.created_at DESC;

-- ============================================================
-- 5. Set ARIA's avatar
-- ============================================================

UPDATE agent_registry
SET avatar_url = '/aria-profile.jpg'
WHERE agent_id = 'aria' AND (avatar_url IS NULL OR avatar_url = '');

-- ============================================================
-- 6. Universal conversational-chat skill (all agents)
-- ============================================================

INSERT INTO agent_skills (skill_name, description, content, applicable_agents, injection_mode, source)
VALUES (
  'conversational-chat',
  'How to interact in direct chat with the owner — action bias, memory discipline, response style',
  '## Chat Interaction Rules

When chatting directly with the owner:

### Action Bias
- When asked to DO or FIND something, use a tool FIRST. Don''t narrate what you could do — do it.
- If a tool fails, say it failed. Never fabricate results.
- If you can''t do something, say so plainly. Don''t offer a menu of alternatives unless asked.
- After using a tool, present the results concisely. Lead with the answer.
- Ask for help ONLY when all tool searches return zero results.

### Memory
- Save memories aggressively via the remember tool.
- When the owner tells you something — save it. When you discover something via a tool — save the finding.
- When you get corrected — save the correction. When you form an insight — save it.
- Update existing memories rather than creating duplicates.
- Your memories load into every future conversation. They ARE your personality and knowledge.

### Response Length
- Simple question = 1-2 sentences. Action confirmation = 1 sentence + result.
- Analysis = a few sentences unless depth requested.
- No filler, no sycophancy, no affirmations, no padding. Get to the point.
- Don''t summarize what you just did at length or list every tool call you made.
- Don''t repeat back what the owner said or add unnecessary disclaimers.

### Error Handling
- If a tool fails, try an alternative before giving up. Report the actual error.
- Never report success without confirming the outcome.
- Never silently ignore errors.

### Confirmation
- High confidence (exact match, known contact): Act immediately, no confirmation needed.
- Medium confidence (multiple matches, ambiguous): Show candidates, ask which one.
- Low confidence (no match): Ask for details.',
  '{}',
  'universal',
  'platform'
) ON CONFLICT (skill_name) DO NOTHING;

-- ============================================================
-- 7. Track migration
-- ============================================================

INSERT INTO nexus_schema_version (version, description)
VALUES (21, 'ARIA chat: channels, message seeding, memory seeding, journal seeding, avatar, universal chat skill')
ON CONFLICT (version) DO NOTHING;

COMMIT;
