-- Nexus: LISTEN/NOTIFY for inbox messages
-- When a message is inserted into agent_inbox, fire a NOTIFY so the
-- event listener can wake the recipient agent immediately.

BEGIN;

CREATE OR REPLACE FUNCTION notify_inbox_insert() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify('nexus_inbox', json_build_object(
    'id', NEW.id,
    'to_agent', NEW.to_agent,
    'from_agent', NEW.from_agent,
    'priority', NEW.priority
  )::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_inbox_notify ON agent_inbox;
CREATE TRIGGER trg_inbox_notify
  AFTER INSERT ON agent_inbox
  FOR EACH ROW
  EXECUTE FUNCTION notify_inbox_insert();

INSERT INTO nexus_schema_version (version, description)
VALUES (9, 'LISTEN/NOTIFY trigger on agent_inbox for event-driven agent wake-up')
ON CONFLICT (version) DO NOTHING;

COMMIT;
