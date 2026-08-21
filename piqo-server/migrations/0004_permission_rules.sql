CREATE TABLE permission_rules (
    id TEXT PRIMARY KEY NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('session', 'project', 'configuration')),
    session_id TEXT REFERENCES sessions(id) ON DELETE CASCADE,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK (
        (scope = 'session' AND session_id IS NOT NULL AND project_id IS NULL)
        OR (scope = 'project' AND session_id IS NULL AND project_id IS NOT NULL)
        OR (scope = 'configuration' AND session_id IS NULL AND project_id IS NULL)
    )
);

CREATE INDEX permission_rules_match_idx
    ON permission_rules (scope, session_id, project_id, agent_id, tool_name, created_at DESC);
