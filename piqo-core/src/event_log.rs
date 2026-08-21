use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{
    AgentPhase, ContentBlock, MessageAuthor, MessageRole, PermissionDecision, SessionPhase,
};

/// Monotonically increasing identifier assigned to a recorded event.
pub type EventId = u64;

/// State changes that make up a session's durable history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum SemanticEvent {
    SessionCreated {
        title: Option<String>,
    },
    SessionPhaseChanged {
        from: SessionPhase,
        to: SessionPhase,
        reason: Option<String>,
    },
    SessionInterrupted {
        reason: String,
    },
    SessionForked {
        parent_session_id: String,
        at_event_id: EventId,
    },
    MessageStarted {
        message_id: String,
        agent_id: String,
        role: MessageRole,
        #[serde(default = "default_message_author")]
        author: MessageAuthor,
    },
    MessageContentAppended {
        message_id: String,
        block: ContentBlock,
    },
    MessageCompleted {
        message_id: String,
    },
    MessageInterrupted {
        message_id: String,
    },
    RunQueued {
        run_id: String,
        retry_of: Option<String>,
        provider: String,
        model: String,
        request: Value,
    },
    RunStarted {
        run_id: String,
        attempt_id: String,
        attempt: u32,
    },
    RunAttemptStarted {
        run_id: String,
        attempt_id: String,
        attempt: u32,
    },
    RunAttemptFailed {
        run_id: String,
        attempt_id: String,
        error: String,
        retryable: bool,
    },
    RunCompleted {
        run_id: String,
        usage: Option<Value>,
    },
    RunFailed {
        run_id: String,
        error: String,
    },
    RunCancelled {
        run_id: String,
        reason: Option<String>,
    },
    RunInterrupted {
        run_id: String,
        reason: String,
    },
    RunRequiresAction {
        run_id: String,
        call_ids: Vec<String>,
    },
    QueuePaused,
    QueueResumed,
    ToolCallEmitted {
        #[serde(default)]
        run_id: String,
        #[serde(default)]
        assistant_message_id: String,
        call_id: String,
        agent_id: String,
        tool_name: String,
        arguments: Value,
        #[serde(default)]
        raw_arguments: String,
    },
    ToolResult {
        #[serde(default)]
        run_id: String,
        call_id: String,
        agent_id: String,
        tool_name: String,
        result: Value,
    },
    AgentPhaseChanged {
        agent_id: String,
        phase: AgentPhase,
    },
    PermissionRequested {
        request_id: String,
        call_id: Option<String>,
        agent_id: String,
        tool_name: String,
        arguments: Value,
    },
    PermissionResolved {
        request_id: String,
        decision: PermissionDecision,
    },
    AgentSpawned {
        agent_id: String,
        parent_id: Option<String>,
    },
    AgentFinished {
        agent_id: String,
    },
}

fn default_message_author() -> MessageAuthor {
    MessageAuthor::System
}

/// An event together with the durable sequence number assigned to it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedEvent {
    pub id: EventId,
    pub session_id: String,
    pub schema_version: u16,
    pub occurred_at: String,
    #[serde(flatten)]
    pub event: SemanticEvent,
    #[serde(skip)]
    pub raw_data: Option<Value>,
}

/// Append-only in-memory representation of a session event log.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EventLog {
    events: Vec<RecordedEvent>,
}

impl EventLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an event and return its newly assigned id.
    pub fn append(&mut self, event: SemanticEvent) -> EventId {
        let id = self.last_id().map_or(1, |last| last + 1);
        self.events.push(RecordedEvent {
            id,
            session_id: String::new(),
            schema_version: 1,
            occurred_at: String::new(),
            event,
            raw_data: None,
        });
        id
    }

    pub fn append_for_session(
        &mut self,
        session_id: impl Into<String>,
        occurred_at: impl Into<String>,
        event: SemanticEvent,
    ) -> EventId {
        let id = self.last_id().map_or(1, |last| last + 1);
        self.events.push(RecordedEvent {
            id,
            session_id: session_id.into(),
            schema_version: 1,
            occurred_at: occurred_at.into(),
            event,
            raw_data: None,
        });
        id
    }

    pub fn last_id(&self) -> Option<EventId> {
        self.events.last().map(|event| event.id)
    }

    /// Replay events strictly after `after`, as required by SSE resume.
    pub fn replay_from(&self, after: EventId) -> impl Iterator<Item = &RecordedEvent> {
        self.events.iter().filter(move |event| event.id > after)
    }

    pub fn iter(&self) -> impl Iterator<Item = &RecordedEvent> {
        self.events.iter()
    }

    /// Create a branch containing the history through `event_id`.
    pub fn fork_at(&self, event_id: EventId) -> Result<Self, EventLogError> {
        if !self.events.iter().any(|event| event.id == event_id) {
            return Err(EventLogError::UnknownEvent(event_id));
        }

        Ok(Self {
            events: self
                .events
                .iter()
                .filter(|event| event.id <= event_id)
                .cloned()
                .collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EventLogError {
    #[error("cannot fork at unknown event {0}")]
    UnknownEvent(EventId),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assigns_monotonic_ids_and_replays_after_an_offset() {
        let mut log = EventLog::new();
        assert_eq!(
            log.append(SemanticEvent::AgentFinished {
                agent_id: "a".into()
            }),
            1
        );
        assert_eq!(
            log.append(SemanticEvent::AgentFinished {
                agent_id: "b".into()
            }),
            2
        );

        let ids: Vec<_> = log.replay_from(1).map(|event| event.id).collect();
        assert_eq!(ids, vec![2]);
    }

    #[test]
    fn forks_the_log_at_an_existing_event() {
        let mut log = EventLog::new();
        log.append(SemanticEvent::AgentFinished {
            agent_id: "a".into(),
        });
        log.append(SemanticEvent::AgentFinished {
            agent_id: "b".into(),
        });

        let branch = log.fork_at(1).expect("event 1 was appended above");
        assert_eq!(branch.last_id(), Some(1));
        assert!(log.fork_at(3).is_err());
    }

    #[test]
    fn serializes_versioned_events_with_stable_type_and_data_fields() {
        let event = SemanticEvent::MessageContentAppended {
            message_id: "m1".into(),
            block: ContentBlock::Json(serde_json::json!({"vendor": "opaque"})),
        };
        let value = serde_json::to_value(event).expect("event serializes");
        assert_eq!(value["type"], "message_content_appended");
        assert_eq!(value["data"]["message_id"], "m1");
        assert_eq!(value["data"]["block"]["kind"], "json");

        let with_future_field = serde_json::json!({
            "type": "message_completed",
            "data": {"message_id": "m1", "future_field": true}
        });
        let decoded: SemanticEvent = serde_json::from_value(with_future_field)
            .expect("unknown additive fields remain compatible");
        assert_eq!(
            decoded,
            SemanticEvent::MessageCompleted {
                message_id: "m1".into()
            }
        );
    }
}
