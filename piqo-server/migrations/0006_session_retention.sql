ALTER TABLE sessions
    ADD COLUMN history_retention TEXT NOT NULL DEFAULT 'forever'
    CHECK (history_retention IN ('forever', 'one_year', 'six_months', 'three_months', 'one_month'));

CREATE INDEX sessions_retention_updated_at_idx
    ON sessions (history_retention, updated_at, id);
