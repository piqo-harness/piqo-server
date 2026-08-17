CREATE INDEX IF NOT EXISTS events_type_idx ON events (session_id, type, event_id);

CREATE TRIGGER IF NOT EXISTS sessions_integrity_insert
BEFORE INSERT ON sessions
WHEN NEW.revision < 0 OR NEW.last_event_id < 0
  OR NEW.phase NOT IN ('created', 'running', 'interrupted', 'finished', 'failed')
BEGIN
    SELECT RAISE(ABORT, 'invalid session projection cache');
END;

CREATE TRIGGER IF NOT EXISTS sessions_integrity_update
BEFORE UPDATE OF revision, last_event_id, phase ON sessions
WHEN NEW.revision < 0 OR NEW.last_event_id < 0
  OR NEW.phase NOT IN ('created', 'running', 'interrupted', 'finished', 'failed')
BEGIN
    SELECT RAISE(ABORT, 'invalid session projection cache');
END;

CREATE TRIGGER IF NOT EXISTS events_integrity_insert
BEFORE INSERT ON events
WHEN NEW.event_id <= 0 OR NEW.schema_version <= 0
BEGIN
    SELECT RAISE(ABORT, 'invalid event envelope');
END;
