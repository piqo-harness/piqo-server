//! Pure domain types for sessions, permissions, and replayable event logs.

mod event_log;
mod permissions;
mod session;

pub use event_log::{EventId, EventLog, EventLogError, RecordedEvent, SemanticEvent};
pub use permissions::{PermissionDecision, PermissionPolicy, PermissionRule, ToolRequest};
pub use session::{
    AgentPhase, ContentBlock, MessageAuthor, MessageProjection, MessageRole, PermissionProjection,
    ProjectionError, RunProjection, RunStatus, SessionPhase, SessionProjection, SessionState,
    SessionTransitionError,
};
