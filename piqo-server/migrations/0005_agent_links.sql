CREATE TABLE agent_links (
    instance_id TEXT PRIMARY KEY NOT NULL,
    profile_id TEXT NOT NULL,
    parent_session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    parent_run_id TEXT NOT NULL,
    parent_call_id TEXT NOT NULL UNIQUE,
    child_session_id TEXT NOT NULL UNIQUE REFERENCES sessions(id) ON DELETE CASCADE,
    child_run_id TEXT NOT NULL,
    depth INTEGER NOT NULL CHECK (depth >= 1),
    budget TEXT NOT NULL,
    permission_ceiling TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX agent_links_parent_idx
    ON agent_links (parent_session_id, parent_run_id, created_at, instance_id);
