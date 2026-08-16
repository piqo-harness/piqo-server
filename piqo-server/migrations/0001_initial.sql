PRAGMA foreign_keys = ON;

CREATE TABLE sessions (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT,
    parent_session_id TEXT REFERENCES sessions(id),
    forked_at_event_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    phase TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    last_event_id INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE events (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    event_id INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    type TEXT NOT NULL,
    data TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    PRIMARY KEY (session_id, event_id)
);

CREATE INDEX sessions_created_at_idx ON sessions (created_at DESC, id DESC);
CREATE INDEX events_session_range_idx ON events (session_id, event_id);
