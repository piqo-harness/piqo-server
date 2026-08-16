use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{AgentPhase, PermissionDecision};

/// Monotonically increasing identifier assigned to a recorded event.
pub type EventId = u64;

/// State changes that make up a session's durable history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SemanticEvent {
    MessageStarted {
        agent_id: String,
    },
    ToolCallEmitted {
        agent_id: String,
        tool_name: String,
        arguments: Value,
    },
    ToolResult {
        agent_id: String,
        tool_name: String,
        result: Value,
    },
    PhaseChanged {
        agent_id: String,
        phase: AgentPhase,
    },
    PermissionRequested {
        request_id: String,
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

/// An event together with the durable sequence number assigned to it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedEvent {
    pub id: EventId,
    pub event: SemanticEvent,
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
        self.events.push(RecordedEvent { id, event });
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
}
