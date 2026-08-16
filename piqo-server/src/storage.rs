use std::{collections::HashMap, path::Path, str::FromStr, sync::Arc, time::Duration};

use chrono::{SecondsFormat, Utc};
use piqo_core::{
    EventId, ProjectionError, RecordedEvent, SemanticEvent, SessionPhase, SessionProjection,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Row, SqlitePool,
};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

pub const EVENT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
    projection_cache: Arc<Mutex<HashMap<String, SessionProjection>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: String,
    pub title: Option<String>,
    pub parent_session_id: Option<String>,
    pub forked_at_event_id: Option<EventId>,
    pub created_at: String,
    pub updated_at: String,
    pub phase: SessionPhase,
    pub revision: u64,
    pub last_event_id: EventId,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("database migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("invalid event JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("session {0} was not found")]
    SessionNotFound(String),
    #[error("event {event_id} was not found in session {session_id}")]
    EventNotFound {
        session_id: String,
        event_id: EventId,
    },
    #[error("event schema version {0} is unsupported")]
    UnsupportedSchemaVersion(u16),
    #[error("session {session_id} has an invalid event log: {reason}")]
    CorruptSession { session_id: String, reason: String },
    #[error("invalid session transition: {0}")]
    InvalidTransition(#[from] ProjectionError),
    #[error("invalid pagination cursor")]
    InvalidCursor,
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("session queue is paused")]
    QueuePaused,
    #[error("session queue is not paused")]
    QueueNotPaused,
    #[error("run {0} was not found")]
    RunNotFound(String),
    #[error("provider {0} was not found")]
    ProviderNotFound(String),
    #[error("provider unavailable: {0}")]
    ProviderUnavailable(String),
    #[error("provider protocol error: {0}")]
    ProviderProtocolError(String),
}

impl SqliteStore {
    pub async fn connect(database_url: &str) -> Result<Self, StoreError> {
        let mut options = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        if !database_url.contains(":memory:") {
            options = options.journal_mode(SqliteJournalMode::Wal);
        }
        // SQLite has a single writer. A single pooled connection makes concurrent
        // append callers serialize cleanly instead of surfacing transient SQLITE_BUSY.
        let max_connections = 1;
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        let store = Self {
            pool,
            projection_cache: Arc::new(Mutex::new(HashMap::new())),
        };
        store.validate_all().await?;
        Ok(store)
    }

    pub async fn connect_file(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_string_lossy();
        Self::connect(&format!("sqlite://{path}")).await
    }

    pub async fn recover_running_sessions(&self) -> Result<Vec<RecordedEvent>, StoreError> {
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT id FROM sessions WHERE phase = 'running'")
                .fetch_all(&self.pool)
                .await?;
        let mut events = Vec::with_capacity(ids.len());
        for session_id in ids {
            let interrupted = self
                .append_event(
                    &session_id,
                    SemanticEvent::SessionInterrupted {
                        reason: "server_restart".to_owned(),
                    },
                )
                .await?;
            if let Some(run) = self.projection(&session_id).await?.active_run().cloned() {
                self.append_event(
                    &session_id,
                    SemanticEvent::RunInterrupted {
                        run_id: run.run_id,
                        reason: "server_restart".to_owned(),
                    },
                )
                .await?;
            }
            events.push(interrupted);
        }
        Ok(events)
    }

    pub async fn create_session(
        &self,
        title: Option<String>,
    ) -> Result<SessionSummary, StoreError> {
        let id = Uuid::now_v7().to_string();
        let now = now();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO sessions (id, title, created_at, updated_at, phase, revision, last_event_id)
             VALUES (?, ?, ?, ?, 'created', 0, 1)",
        )
        .bind(&id)
        .bind(&title)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        self.insert_event_row(
            &mut tx,
            &id,
            1,
            &now,
            &SemanticEvent::SessionCreated { title },
        )
        .await?;
        tx.commit().await?;
        self.get_session(&id).await
    }

    pub async fn get_session(&self, session_id: &str) -> Result<SessionSummary, StoreError> {
        let row = sqlx::query(
            "SELECT id, title, parent_session_id, forked_at_event_id, created_at, updated_at,
                    phase, revision, last_event_id
             FROM sessions WHERE id = ?",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| StoreError::SessionNotFound(session_id.to_owned()))?;
        summary_from_row(&row)
    }

    pub async fn projection(&self, session_id: &str) -> Result<SessionProjection, StoreError> {
        if let Some(projection) = self.projection_cache.lock().await.get(session_id).cloned() {
            return Ok(projection);
        }
        let events = self.events(session_id, 0, u32::MAX).await?;
        let projection = project(session_id, &events)?;
        self.projection_cache
            .lock()
            .await
            .insert(session_id.to_owned(), projection.clone());
        Ok(projection)
    }

    pub async fn list_sessions(
        &self,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<SessionSummary>, Option<String>), StoreError> {
        let limit = limit.clamp(1, 200);
        let rows = if let Some(cursor) = cursor {
            let (created_at, id) = decode_cursor(cursor)?;
            sqlx::query(
                "SELECT id, title, parent_session_id, forked_at_event_id, created_at, updated_at,
                        phase, revision, last_event_id
                 FROM sessions
                 WHERE (created_at, id) < (?, ?)
                 ORDER BY created_at DESC, id DESC LIMIT ?",
            )
            .bind(created_at)
            .bind(id)
            .bind(i64::from(limit) + 1)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT id, title, parent_session_id, forked_at_event_id, created_at, updated_at,
                        phase, revision, last_event_id
                 FROM sessions ORDER BY created_at DESC, id DESC LIMIT ?",
            )
            .bind(i64::from(limit) + 1)
            .fetch_all(&self.pool)
            .await?
        };
        let has_more = rows.len() > limit as usize;
        let rows = rows.into_iter().take(limit as usize);
        let summaries = rows
            .map(|row| summary_from_row(&row))
            .collect::<Result<Vec<_>, _>>()?;
        let next = if has_more {
            summaries
                .last()
                .map(|summary| encode_cursor(&summary.created_at, &summary.id))
        } else {
            None
        };
        Ok((summaries, next))
    }

    pub async fn events(
        &self,
        session_id: &str,
        after: EventId,
        limit: u32,
    ) -> Result<Vec<RecordedEvent>, StoreError> {
        self.ensure_session(session_id).await?;
        let rows = if limit == u32::MAX {
            sqlx::query(
                "SELECT event_id, schema_version, type, data, occurred_at
                 FROM events WHERE session_id = ? AND event_id > ?
                 ORDER BY event_id ASC",
            )
            .bind(session_id)
            .bind(i64::try_from(after).map_err(|_| StoreError::InvalidCursor)?)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT event_id, schema_version, type, data, occurred_at
                 FROM events WHERE session_id = ? AND event_id > ?
                 ORDER BY event_id ASC LIMIT ?",
            )
            .bind(session_id)
            .bind(i64::try_from(after).map_err(|_| StoreError::InvalidCursor)?)
            .bind(i64::from(limit.clamp(1, 200)))
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter()
            .map(|row| recorded_from_row(session_id, &row))
            .collect()
    }

    pub(crate) async fn append_event(
        &self,
        session_id: &str,
        event: SemanticEvent,
    ) -> Result<RecordedEvent, StoreError> {
        self.append_events(session_id, vec![event])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| StoreError::CorruptSession {
                session_id: session_id.to_owned(),
                reason: "empty append result".to_owned(),
            })
    }

    pub(crate) async fn append_events(
        &self,
        session_id: &str,
        events: Vec<SemanticEvent>,
    ) -> Result<Vec<RecordedEvent>, StoreError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let cached_projection = self.projection_cache.lock().await.get(session_id).cloned();
        let mut tx = self.pool.begin().await?;
        let mut projection = if let Some(projection) = cached_projection {
            projection
        } else {
            let previous_rows = sqlx::query(
                "SELECT event_id, schema_version, type, data, occurred_at
                 FROM events WHERE session_id = ? ORDER BY event_id ASC",
            )
            .bind(session_id)
            .fetch_all(&mut *tx)
            .await?;
            let previous_events = previous_rows
                .iter()
                .map(|row| recorded_from_row(session_id, row))
                .collect::<Result<Vec<_>, _>>()?;
            project(session_id, &previous_events)?
        };
        let mut recorded = Vec::with_capacity(events.len());
        for event in events {
            let row = sqlx::query(
                "UPDATE sessions SET last_event_id = last_event_id + 1
                 WHERE id = ?
                 RETURNING last_event_id",
            )
            .bind(session_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| StoreError::SessionNotFound(session_id.to_owned()))?;
            let event_id =
                u64::try_from(row.try_get::<i64, _>("last_event_id")?).map_err(|_| {
                    StoreError::CorruptSession {
                        session_id: session_id.to_owned(),
                        reason: "negative event id".to_owned(),
                    }
                })?;
            projection.apply(event_id, &event)?;
            let occurred_at = now();
            self.insert_event_row(&mut tx, session_id, event_id, &occurred_at, &event)
                .await?;
            sqlx::query("UPDATE sessions SET phase = ?, revision = ?, updated_at = ? WHERE id = ?")
                .bind(phase_name(projection.state.phase))
                .bind(
                    i64::try_from(projection.state.revision)
                        .map_err(|_| StoreError::InvalidCursor)?,
                )
                .bind(&occurred_at)
                .bind(session_id)
                .execute(&mut *tx)
                .await?;
            recorded.push(RecordedEvent {
                id: event_id,
                session_id: session_id.to_owned(),
                schema_version: EVENT_SCHEMA_VERSION,
                occurred_at,
                event,
                raw_data: None,
            });
        }
        tx.commit().await?;
        self.projection_cache
            .lock()
            .await
            .insert(session_id.to_owned(), projection);
        Ok(recorded)
    }

    pub async fn fork_session(
        &self,
        parent_session_id: &str,
        at_event_id: EventId,
        title: Option<String>,
    ) -> Result<SessionSummary, StoreError> {
        let mut tx = self.pool.begin().await?;
        let parent_exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM sessions WHERE id = ?")
            .bind(parent_session_id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
        if !parent_exists {
            return Err(StoreError::SessionNotFound(parent_session_id.to_owned()));
        }
        let rows = sqlx::query(
            "SELECT event_id, schema_version, type, data, occurred_at FROM events
             WHERE session_id = ? AND event_id <= ? ORDER BY event_id ASC",
        )
        .bind(parent_session_id)
        .bind(i64::try_from(at_event_id).map_err(|_| StoreError::InvalidCursor)?)
        .fetch_all(&mut *tx)
        .await?;
        if rows.is_empty()
            || rows
                .last()
                .and_then(|row| row.try_get::<i64, _>("event_id").ok())
                != Some(i64::try_from(at_event_id).map_err(|_| StoreError::InvalidCursor)?)
        {
            return Err(StoreError::EventNotFound {
                session_id: parent_session_id.to_owned(),
                event_id: at_event_id,
            });
        }
        let parent_events = rows
            .iter()
            .map(|row| recorded_from_row(parent_session_id, row))
            .collect::<Result<Vec<_>, _>>()?;
        let parent_state = project(parent_session_id, &parent_events)?;
        let id = Uuid::now_v7().to_string();
        let created_at = now();
        sqlx::query(
            "INSERT INTO sessions
             (id, title, parent_session_id, forked_at_event_id, created_at, updated_at, phase, revision, last_event_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(title)
        .bind(parent_session_id)
        .bind(i64::try_from(at_event_id).map_err(|_| StoreError::InvalidCursor)?)
        .bind(&created_at)
        .bind(&created_at)
        .bind(phase_name(parent_state.state.phase))
        .bind(i64::try_from(parent_state.state.revision).map_err(|_| StoreError::InvalidCursor)?)
        .bind(i64::try_from(at_event_id).map_err(|_| StoreError::InvalidCursor)?)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO events (session_id, event_id, schema_version, type, data, occurred_at)
             SELECT ?, event_id, schema_version, type, data, occurred_at
             FROM events WHERE session_id = ? AND event_id <= ?
             ORDER BY event_id ASC",
        )
        .bind(&id)
        .bind(parent_session_id)
        .bind(i64::try_from(at_event_id).map_err(|_| StoreError::InvalidCursor)?)
        .execute(&mut *tx)
        .await?;
        let mut branch_projection = parent_state;
        let mut branch_events = vec![SemanticEvent::SessionForked {
            parent_session_id: parent_session_id.to_owned(),
            at_event_id,
        }];
        if let Some(run) = branch_projection.active_run().cloned() {
            branch_events.push(SemanticEvent::RunInterrupted {
                run_id: run.run_id,
                reason: "forked_branch".to_owned(),
            });
        }
        branch_events.push(SemanticEvent::QueuePaused);
        let mut next_id = at_event_id
            .checked_add(1)
            .ok_or(StoreError::InvalidCursor)?;
        for event in branch_events {
            branch_projection.apply(next_id, &event)?;
            self.insert_event_row(&mut tx, &id, next_id, &created_at, &event)
                .await?;
            sqlx::query("UPDATE sessions SET phase = ?, revision = ?, last_event_id = ?, updated_at = ? WHERE id = ?")
                .bind(phase_name(branch_projection.state.phase))
                .bind(i64::try_from(branch_projection.state.revision).map_err(|_| StoreError::InvalidCursor)?)
                .bind(i64::try_from(next_id).map_err(|_| StoreError::InvalidCursor)?)
                .bind(&created_at)
                .bind(&id)
                .execute(&mut *tx)
                .await?;
            next_id = next_id.checked_add(1).ok_or(StoreError::InvalidCursor)?;
        }
        tx.commit().await?;
        self.get_session(&id).await
    }

    async fn insert_event_row(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        session_id: &str,
        event_id: EventId,
        occurred_at: &str,
        event: &SemanticEvent,
    ) -> Result<(), StoreError> {
        let value = serde_json::to_value(event)?;
        let event_type = value.get("type").and_then(Value::as_str).ok_or_else(|| {
            StoreError::CorruptSession {
                session_id: session_id.to_owned(),
                reason: "event has no type".to_owned(),
            }
        })?;
        let data = value.get("data").cloned().unwrap_or(Value::Null);
        sqlx::query(
            "INSERT INTO events (session_id, event_id, schema_version, type, data, occurred_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(session_id)
        .bind(i64::try_from(event_id).map_err(|_| StoreError::InvalidCursor)?)
        .bind(i64::from(EVENT_SCHEMA_VERSION))
        .bind(event_type)
        .bind(serde_json::to_string(&data)?)
        .bind(occurred_at)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn ensure_session(&self, session_id: &str) -> Result<(), StoreError> {
        let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM sessions WHERE id = ?")
            .bind(session_id)
            .fetch_optional(&self.pool)
            .await?
            .is_some();
        if exists {
            Ok(())
        } else {
            Err(StoreError::SessionNotFound(session_id.to_owned()))
        }
    }

    async fn validate_all(&self) -> Result<(), StoreError> {
        let rows = sqlx::query("SELECT id, phase, revision, last_event_id FROM sessions")
            .fetch_all(&self.pool)
            .await?;
        for row in rows {
            let id: String = row.try_get("id")?;
            let events = self.events(&id, 0, u32::MAX).await?;
            let state = project(&id, &events)?;
            let cached_phase = parse_phase(row.try_get::<String, _>("phase")?)?;
            let cached_revision =
                u64::try_from(row.try_get::<i64, _>("revision")?).map_err(|_| {
                    StoreError::CorruptSession {
                        session_id: id.clone(),
                        reason: "negative revision".to_owned(),
                    }
                })?;
            let cached_last =
                u64::try_from(row.try_get::<i64, _>("last_event_id")?).map_err(|_| {
                    StoreError::CorruptSession {
                        session_id: id.clone(),
                        reason: "negative event id".to_owned(),
                    }
                })?;
            if state.state.phase != cached_phase
                || state.state.revision != cached_revision
                || state.state.last_event_id.unwrap_or_default() != cached_last
            {
                return Err(StoreError::CorruptSession {
                    session_id: id,
                    reason: "projection cache does not match event log".to_owned(),
                });
            }
        }
        Ok(())
    }
}

pub(crate) fn project(
    session_id: &str,
    events: &[RecordedEvent],
) -> Result<SessionProjection, StoreError> {
    let mut state = SessionProjection::new(session_id);
    for (expected, event) in events.iter().enumerate() {
        let expected_id = (expected + 1) as EventId;
        if event.id != expected_id || event.schema_version != EVENT_SCHEMA_VERSION {
            return Err(StoreError::CorruptSession {
                session_id: session_id.to_owned(),
                reason: format!("non-contiguous or unsupported event {}", event.id),
            });
        }
        state.apply(event.id, &event.event)?;
    }
    if events.is_empty() {
        return Err(StoreError::CorruptSession {
            session_id: session_id.to_owned(),
            reason: "session has no creation event".to_owned(),
        });
    }
    Ok(state)
}

fn recorded_from_row(
    session_id: &str,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<RecordedEvent, StoreError> {
    let schema_version = u16::try_from(row.try_get::<i64, _>("schema_version")?)
        .map_err(|_| StoreError::UnsupportedSchemaVersion(u16::MAX))?;
    if schema_version != EVENT_SCHEMA_VERSION {
        return Err(StoreError::UnsupportedSchemaVersion(schema_version));
    }
    let event_type: String = row.try_get("type")?;
    let data: Value = serde_json::from_str(&row.try_get::<String, _>("data")?)?;
    let event: SemanticEvent = serde_json::from_value(json!({
        "type": event_type,
        "data": data,
    }))?;
    Ok(RecordedEvent {
        id: u64::try_from(row.try_get::<i64, _>("event_id")?).map_err(|_| {
            StoreError::CorruptSession {
                session_id: session_id.to_owned(),
                reason: "negative event id".to_owned(),
            }
        })?,
        session_id: session_id.to_owned(),
        schema_version,
        occurred_at: row.try_get("occurred_at")?,
        event,
        raw_data: Some(data),
    })
}

fn summary_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<SessionSummary, StoreError> {
    Ok(SessionSummary {
        id: row.try_get("id")?,
        title: row.try_get("title")?,
        parent_session_id: row.try_get("parent_session_id")?,
        forked_at_event_id: row
            .try_get::<Option<i64>, _>("forked_at_event_id")?
            .map(|v| {
                u64::try_from(v).map_err(|_| sqlx::Error::Protocol("negative fork event id".into()))
            })
            .transpose()?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        phase: parse_phase(row.try_get("phase")?)?,
        revision: u64::try_from(row.try_get::<i64, _>("revision")?)
            .map_err(|_| sqlx::Error::Protocol("negative revision".into()))?,
        last_event_id: u64::try_from(row.try_get::<i64, _>("last_event_id")?)
            .map_err(|_| sqlx::Error::Protocol("negative event id".into()))?,
    })
}

fn parse_phase(value: String) -> Result<SessionPhase, StoreError> {
    serde_json::from_str(&format!("\"{value}\"")).map_err(|error| StoreError::CorruptSession {
        session_id: "unknown".to_owned(),
        reason: format!("invalid phase: {error}"),
    })
}

fn phase_name(phase: SessionPhase) -> &'static str {
    match phase {
        SessionPhase::Created => "created",
        SessionPhase::Running => "running",
        SessionPhase::Interrupted => "interrupted",
        SessionPhase::Finished => "finished",
        SessionPhase::Failed => "failed",
    }
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn encode_cursor(created_at: &str, id: &str) -> String {
    format!("{created_at}|{id}")
}

fn decode_cursor(cursor: &str) -> Result<(String, String), StoreError> {
    let (created_at, id) = cursor.split_once('|').ok_or(StoreError::InvalidCursor)?;
    if created_at.is_empty() || id.is_empty() {
        return Err(StoreError::InvalidCursor);
    }
    Ok((created_at.to_owned(), id.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    async fn store() -> (SqliteStore, NamedTempFile) {
        let file = NamedTempFile::new().expect("temporary sqlite file");
        let store = SqliteStore::connect_file(file.path())
            .await
            .expect("store opens");
        (store, file)
    }

    #[tokio::test]
    async fn persists_events_and_reopens_with_the_same_projection() {
        let (store, file) = store().await;
        let session = store
            .create_session(Some("demo".into()))
            .await
            .expect("session creates");
        store
            .append_event(
                &session.id,
                SemanticEvent::SessionPhaseChanged {
                    from: SessionPhase::Created,
                    to: SessionPhase::Running,
                    reason: None,
                },
            )
            .await
            .expect("phase change appends");
        drop(store);
        let reopened = SqliteStore::connect_file(file.path())
            .await
            .expect("store reopens");
        let loaded = reopened
            .get_session(&session.id)
            .await
            .expect("session loads");
        assert_eq!(loaded.phase, SessionPhase::Running);
        assert_eq!(loaded.last_event_id, 2);
    }

    #[tokio::test]
    async fn forks_an_autonomous_prefix_and_marks_it_interrupted() {
        let (store, _file) = store().await;
        let parent = store.create_session(None).await.expect("session creates");
        let child = store
            .fork_session(&parent.id, 1, Some("branch".into()))
            .await
            .expect("fork creates");
        assert_ne!(parent.id, child.id);
        assert_eq!(child.phase, SessionPhase::Interrupted);
        assert_eq!(child.last_event_id, 3);
        assert_eq!(
            store
                .events(&child.id, 0, 10)
                .await
                .expect("events load")
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn fork_interrupts_an_active_run_and_suspends_the_branch() {
        let (store, _file) = store().await;
        let parent = store.create_session(None).await.expect("session creates");
        store
            .append_event(
                &parent.id,
                SemanticEvent::SessionPhaseChanged {
                    from: SessionPhase::Created,
                    to: SessionPhase::Running,
                    reason: None,
                },
            )
            .await
            .expect("session starts");
        store
            .append_event(
                &parent.id,
                SemanticEvent::RunQueued {
                    run_id: "run-1".into(),
                    retry_of: None,
                    provider: "provider".into(),
                    model: "model".into(),
                    request: serde_json::json!({"stream": true}),
                },
            )
            .await
            .expect("run queues");
        store
            .append_event(
                &parent.id,
                SemanticEvent::RunStarted {
                    run_id: "run-1".into(),
                    attempt_id: "attempt-1".into(),
                    attempt: 1,
                },
            )
            .await
            .expect("run starts");

        let child = store
            .fork_session(&parent.id, 4, None)
            .await
            .expect("fork creates");
        let projection = store.projection(&child.id).await.expect("projection loads");
        assert!(projection.active_run().is_none());
        assert!(projection.queue_paused);
        assert_eq!(
            projection.runs["run-1"].status,
            piqo_core::RunStatus::Interrupted
        );
    }

    #[tokio::test]
    async fn allocates_unique_monotonic_ids_for_concurrent_appends() {
        let (store, _file) = store().await;
        let session = store.create_session(None).await.expect("session creates");
        let store = Arc::new(store);
        let mut tasks = Vec::new();
        for index in 0..8 {
            let store = Arc::clone(&store);
            let session_id = session.id.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .append_event(
                        &session_id,
                        SemanticEvent::MessageStarted {
                            message_id: format!("m{index}"),
                            agent_id: "agent".into(),
                            role: piqo_core::MessageRole::User,
                            author: piqo_core::MessageAuthor::User,
                        },
                    )
                    .await
            }));
        }
        for task in tasks {
            task.await
                .expect("append task joins")
                .expect("append succeeds");
        }
        let events = store
            .events(&session.id, 0, u32::MAX)
            .await
            .expect("events load");
        let ids: Vec<_> = events.iter().map(|event| event.id).collect();
        assert_eq!(ids, (1..=9).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn recovery_marks_a_running_session_once() {
        let (store, file) = store().await;
        let session = store.create_session(None).await.expect("session creates");
        store
            .append_event(
                &session.id,
                SemanticEvent::SessionPhaseChanged {
                    from: SessionPhase::Created,
                    to: SessionPhase::Running,
                    reason: None,
                },
            )
            .await
            .expect("session starts");
        drop(store);

        let reopened = SqliteStore::connect_file(file.path())
            .await
            .expect("store reopens");
        assert_eq!(
            reopened
                .recover_running_sessions()
                .await
                .expect("recovery runs")
                .len(),
            1
        );
        assert_eq!(
            reopened
                .recover_running_sessions()
                .await
                .expect("recovery is idempotent")
                .len(),
            0
        );
        assert_eq!(
            reopened
                .get_session(&session.id)
                .await
                .expect("session loads")
                .phase,
            SessionPhase::Interrupted
        );
    }

    #[tokio::test]
    async fn paginates_sessions_with_a_stable_cursor() {
        let (store, _file) = store().await;
        let first = store.create_session(None).await.expect("session creates");
        let second = store.create_session(None).await.expect("session creates");
        let (page, cursor) = store
            .list_sessions(None, 1)
            .await
            .expect("first page loads");
        assert_eq!(page.len(), 1);
        let cursor = cursor.expect("second page exists");
        let (next, _) = store
            .list_sessions(Some(&cursor), 1)
            .await
            .expect("second page loads");
        assert_eq!(next.len(), 1);
        assert_ne!(page[0].id, next[0].id);
        assert!(page[0].id == first.id || page[0].id == second.id);
    }

    #[tokio::test]
    async fn supports_an_in_memory_database_for_embedded_tests() {
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("in-memory store opens");
        let session = store.create_session(None).await.expect("session creates");
        assert_eq!(session.last_event_id, 1);
    }

    #[tokio::test]
    async fn fork_copies_unknown_additive_event_fields_verbatim() {
        let (store, _file) = store().await;
        let parent = store.create_session(None).await.expect("session creates");
        store
            .append_event(
                &parent.id,
                SemanticEvent::MessageStarted {
                    message_id: "m1".into(),
                    agent_id: String::new(),
                    role: piqo_core::MessageRole::User,
                    author: piqo_core::MessageAuthor::User,
                },
            )
            .await
            .expect("message starts");
        sqlx::query("UPDATE events SET data = ? WHERE session_id = ? AND event_id = 2")
            .bind(r#"{"message_id":"m1","agent_id":"","role":"user","future_field":{"kept":true}}"#)
            .bind(&parent.id)
            .execute(&store.pool)
            .await
            .expect("future field updates");
        let child = store
            .fork_session(&parent.id, 2, None)
            .await
            .expect("fork creates");
        let events = store
            .events(&child.id, 0, u32::MAX)
            .await
            .expect("events load");
        assert_eq!(
            events[1].raw_data.as_ref().expect("raw data"),
            &serde_json::json!({
                "message_id": "m1",
                "agent_id": "",
                "role": "user",
                "future_field": {"kept": true}
            })
        );
    }
}
