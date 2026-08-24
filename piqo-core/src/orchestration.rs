use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Immutable limits captured when a child agent is admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudget {
    pub max_model_turns: u32,
    pub max_duration_seconds: u64,
    pub max_tree_duration_seconds: u64,
    pub max_tree_tokens: u64,
    pub max_context_bytes: u64,
    pub max_result_bytes: u64,
}

/// A durable relationship between a managed delegation and its child run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLink {
    pub instance_id: String,
    pub profile_id: String,
    pub parent_session_id: String,
    pub parent_run_id: String,
    pub parent_call_id: String,
    pub child_session_id: String,
    pub child_run_id: String,
    pub depth: u8,
    pub budget: AgentBudget,
    /// Serialized parent capability ceiling, interpreted by the server policy edge.
    pub permission_ceiling: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTerminalStatus {
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

/// Bounded envelope delivered to the parent as the result of `delegate`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentResult {
    pub instance_id: String,
    pub profile_id: String,
    pub child_session_id: String,
    pub child_run_id: String,
    pub status: AgentTerminalStatus,
    pub output: Option<Value>,
    pub output_truncated: bool,
    pub usage: Option<Value>,
    pub error: Option<String>,
}

/// A frozen source message copied into a child session with provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegatedContextRef {
    pub source_session_id: String,
    pub message_id: String,
    pub last_event_id: u64,
}
