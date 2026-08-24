//! Pure domain types for sessions, permissions, and replayable event logs.

mod context;
mod event_log;
mod orchestration;
mod permissions;
mod session;

pub use context::{
    estimate_tokens, CompactionStrategy, ContextArtifact, ContextFact, ContextProjection,
    ToolCorrelation, CONTEXT_ESTIMATOR_VERSION,
};
pub use event_log::{EventId, EventLog, EventLogError, RecordedEvent, SemanticEvent};
pub use orchestration::{
    AgentBudget, AgentLink, AgentResult, AgentTerminalStatus, DelegatedContextRef,
};
pub use permissions::{
    PermissionDecision, PermissionDecisionSource, PermissionEvaluation, PermissionPolicy,
    PermissionRule, PermissionScope, ToolRequest,
};
pub use session::{
    AgentPhase, ContentBlock, MessageAuthor, MessageProjection, MessageRole, PermissionProjection,
    ProjectionError, RunProjection, RunStatus, SessionPhase, SessionProjection, SessionState,
    SessionTransitionError,
};
