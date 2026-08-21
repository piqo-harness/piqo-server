use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::PermissionDecision;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentPhase {
    Created,
    Running,
    WaitingForPermission,
    Finished,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPhase {
    Created,
    Running,
    Interrupted,
    Finished,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum MessageAuthor {
    System,
    User,
    Agent(String),
    Tool(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ContentBlock {
    Text(String),
    Json(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionState {
    pub session_id: String,
    pub phase: SessionPhase,
    pub revision: u64,
    pub last_event_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    RequiresAction,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageProjection {
    pub message_id: String,
    pub role: MessageRole,
    pub agent_id: Option<String>,
    pub author: MessageAuthor,
    pub blocks: Vec<ContentBlock>,
    pub completed: bool,
    pub interrupted: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunProjection {
    pub run_id: String,
    pub retry_of: Option<String>,
    pub provider: String,
    pub model: String,
    pub request: serde_json::Value,
    pub status: RunStatus,
    pub attempt_id: Option<String>,
    pub attempts: u32,
    pub error: Option<String>,
    pub queue_priority: i64,
    #[serde(default)]
    pub tool_calls: BTreeMap<String, ToolCallProjection>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallProjection {
    pub call_id: String,
    pub assistant_message_id: String,
    pub agent_id: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
    pub raw_arguments: String,
    #[serde(default)]
    pub native: bool,
    #[serde(default)]
    pub execution_id: Option<String>,
    pub result: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PermissionProjection {
    pub request_id: String,
    pub run_id: String,
    pub call_id: Option<String>,
    pub agent_id: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
    pub decision: Option<PermissionDecision>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionProjection {
    pub state: SessionState,
    pub messages: Vec<MessageProjection>,
    pub agents: BTreeMap<String, AgentPhase>,
    pub pending_permissions: BTreeMap<String, PermissionProjection>,
    pub runs: BTreeMap<String, RunProjection>,
    pub queue_paused: bool,
}

impl SessionState {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            phase: SessionPhase::Created,
            revision: 0,
            last_event_id: None,
        }
    }

    pub fn start(&mut self) -> Result<(), SessionTransitionError> {
        self.transition(
            SessionPhase::Running,
            &[
                SessionPhase::Created,
                SessionPhase::Finished,
                SessionPhase::Interrupted,
                SessionPhase::Failed,
            ],
        )
    }

    pub fn finish(&mut self) -> Result<(), SessionTransitionError> {
        self.transition(SessionPhase::Finished, &[SessionPhase::Running])
    }

    pub fn interrupt(&mut self) -> Result<(), SessionTransitionError> {
        self.transition(SessionPhase::Interrupted, &[SessionPhase::Running])
    }

    pub fn resume(&mut self) -> Result<(), SessionTransitionError> {
        self.start()
    }

    pub fn fail(&mut self) -> Result<(), SessionTransitionError> {
        self.transition(
            SessionPhase::Failed,
            &[
                SessionPhase::Created,
                SessionPhase::Running,
                SessionPhase::Interrupted,
            ],
        )
    }

    fn transition(
        &mut self,
        next: SessionPhase,
        allowed_from: &[SessionPhase],
    ) -> Result<(), SessionTransitionError> {
        if !allowed_from.contains(&self.phase) {
            return Err(SessionTransitionError {
                from: self.phase,
                to: next,
            });
        }
        self.phase = next;
        self.revision += 1;
        Ok(())
    }

    pub fn apply(
        &mut self,
        event_id: u64,
        event: &crate::SemanticEvent,
    ) -> Result<(), ProjectionError> {
        match event {
            crate::SemanticEvent::SessionCreated { .. } => {
                if self.revision != 0 || self.last_event_id.is_some() {
                    return Err(ProjectionError::DuplicateCreation);
                }
            }
            crate::SemanticEvent::SessionPhaseChanged { from, to, .. } => {
                if self.phase != *from {
                    return Err(ProjectionError::InvalidTransition(SessionTransitionError {
                        from: self.phase,
                        to: *to,
                    }));
                }
                self.transition(*to, allowed_sources(*to))?;
            }
            crate::SemanticEvent::SessionInterrupted { .. } => self.interrupt()?,
            crate::SemanticEvent::SessionForked { .. } => {
                self.phase = SessionPhase::Interrupted;
                self.revision += 1;
            }
            _ => {}
        }
        self.last_event_id = Some(event_id);
        Ok(())
    }
}

impl SessionProjection {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            state: SessionState::new(session_id),
            messages: Vec::new(),
            agents: BTreeMap::new(),
            pending_permissions: BTreeMap::new(),
            runs: BTreeMap::new(),
            queue_paused: false,
        }
    }

    pub fn apply(
        &mut self,
        event_id: u64,
        event: &crate::SemanticEvent,
    ) -> Result<(), ProjectionError> {
        self.state.apply(event_id, event)?;
        match event {
            crate::SemanticEvent::MessageStarted {
                message_id,
                agent_id,
                role,
                author,
            } => {
                if self
                    .messages
                    .iter()
                    .any(|message| message.message_id == *message_id)
                {
                    return Err(ProjectionError::DuplicateMessage(message_id.clone()));
                }
                let projected_author = match (author, role, agent_id.is_empty()) {
                    (MessageAuthor::System, MessageRole::User, _) => MessageAuthor::User,
                    (MessageAuthor::System, MessageRole::Assistant, false) => {
                        MessageAuthor::Agent(agent_id.clone())
                    }
                    (MessageAuthor::System, MessageRole::Tool, false) => {
                        MessageAuthor::Tool(agent_id.clone())
                    }
                    _ => author.clone(),
                };
                self.messages.push(MessageProjection {
                    message_id: message_id.clone(),
                    role: *role,
                    agent_id: (!agent_id.is_empty()).then(|| agent_id.clone()),
                    author: projected_author,
                    blocks: Vec::new(),
                    completed: false,
                    interrupted: false,
                });
            }
            crate::SemanticEvent::MessageContentAppended { message_id, block } => {
                let message = self
                    .messages
                    .iter_mut()
                    .find(|message| message.message_id == *message_id)
                    .ok_or_else(|| ProjectionError::UnknownMessage(message_id.clone()))?;
                if message.completed || message.interrupted {
                    return Err(ProjectionError::MessageAlreadyClosed(message_id.clone()));
                }
                message.blocks.push(block.clone());
            }
            crate::SemanticEvent::MessageCompleted { message_id } => {
                let message = self
                    .messages
                    .iter_mut()
                    .find(|message| message.message_id == *message_id)
                    .ok_or_else(|| ProjectionError::UnknownMessage(message_id.clone()))?;
                if message.completed || message.interrupted {
                    return Err(ProjectionError::MessageAlreadyClosed(message_id.clone()));
                }
                message.completed = true;
            }
            crate::SemanticEvent::MessageInterrupted { message_id } => {
                let message = self
                    .messages
                    .iter_mut()
                    .find(|message| message.message_id == *message_id)
                    .ok_or_else(|| ProjectionError::UnknownMessage(message_id.clone()))?;
                if message.completed || message.interrupted {
                    return Err(ProjectionError::MessageAlreadyClosed(message_id.clone()));
                }
                message.interrupted = true;
            }
            crate::SemanticEvent::AgentSpawned { agent_id, .. } => {
                if self
                    .agents
                    .insert(agent_id.clone(), AgentPhase::Created)
                    .is_some()
                {
                    return Err(ProjectionError::DuplicateAgent(agent_id.clone()));
                }
            }
            crate::SemanticEvent::AgentPhaseChanged { agent_id, phase } => {
                self.agents.insert(agent_id.clone(), *phase);
            }
            crate::SemanticEvent::AgentFinished { agent_id } => {
                self.agents.insert(agent_id.clone(), AgentPhase::Finished);
            }
            crate::SemanticEvent::PermissionRequested {
                request_id,
                run_id,
                call_id,
                agent_id,
                tool_name,
                arguments,
            } => {
                if self.pending_permissions.contains_key(request_id) {
                    return Err(ProjectionError::DuplicatePermissionRequest(
                        request_id.clone(),
                    ));
                }
                self.pending_permissions.insert(
                    request_id.clone(),
                    PermissionProjection {
                        request_id: request_id.clone(),
                        run_id: run_id.clone(),
                        call_id: call_id.clone(),
                        agent_id: agent_id.clone(),
                        tool_name: tool_name.clone(),
                        arguments: arguments.clone(),
                        decision: None,
                    },
                );
            }
            crate::SemanticEvent::PermissionResolved {
                request_id,
                decision,
                ..
            } => {
                let pending = self
                    .pending_permissions
                    .get_mut(request_id)
                    .ok_or_else(|| ProjectionError::UnknownPermissionRequest(request_id.clone()))?;
                match pending.decision {
                    None => pending.decision = Some(*decision),
                    Some(existing) if existing == *decision => {}
                    Some(_) => {
                        return Err(ProjectionError::ConflictingPermissionResolution(
                            request_id.clone(),
                        ))
                    }
                }
            }
            crate::SemanticEvent::RunQueued {
                run_id,
                retry_of,
                provider,
                model,
                request,
            } => {
                if self.runs.contains_key(run_id) {
                    return Err(ProjectionError::DuplicateRun(run_id.clone()));
                }
                self.runs.insert(
                    run_id.clone(),
                    RunProjection {
                        run_id: run_id.clone(),
                        retry_of: retry_of.clone(),
                        provider: provider.clone(),
                        model: model.clone(),
                        request: request.clone(),
                        status: RunStatus::Queued,
                        attempt_id: None,
                        attempts: 0,
                        error: None,
                        queue_priority: if retry_of.is_some() { -1 } else { 0 },
                        tool_calls: BTreeMap::new(),
                    },
                );
            }
            crate::SemanticEvent::RunStarted {
                run_id,
                attempt_id,
                attempt,
            } => {
                let run = self
                    .runs
                    .get_mut(run_id)
                    .ok_or_else(|| ProjectionError::UnknownRun(run_id.clone()))?;
                if !matches!(run.status, RunStatus::Queued | RunStatus::RequiresAction) {
                    return Err(ProjectionError::InvalidRunTransition {
                        run_id: run_id.clone(),
                        from: run.status,
                        to: RunStatus::Running,
                    });
                }
                run.status = RunStatus::Running;
                run.attempt_id = Some(attempt_id.clone());
                run.attempts = *attempt;
                self.queue_paused = false;
            }
            crate::SemanticEvent::RunCompleted { run_id, .. } => {
                self.set_run_terminal(run_id, RunStatus::Completed, None)?;
            }
            crate::SemanticEvent::RunAttemptStarted {
                run_id,
                attempt_id,
                attempt,
            } => {
                let run = self
                    .runs
                    .get_mut(run_id)
                    .ok_or_else(|| ProjectionError::UnknownRun(run_id.clone()))?;
                if run.status != RunStatus::Running {
                    return Err(ProjectionError::InvalidRunTransition {
                        run_id: run_id.clone(),
                        from: run.status,
                        to: RunStatus::Running,
                    });
                }
                run.attempt_id = Some(attempt_id.clone());
                run.attempts = *attempt;
            }
            crate::SemanticEvent::RunFailed { run_id, error } => {
                self.set_run_terminal(run_id, RunStatus::Failed, Some(error.clone()))?;
                self.queue_paused = true;
            }
            crate::SemanticEvent::RunCancelled { run_id, .. } => {
                self.set_run_terminal(run_id, RunStatus::Cancelled, None)?;
                self.queue_paused = true;
            }
            crate::SemanticEvent::RunInterrupted { run_id, reason } => {
                self.set_run_terminal(run_id, RunStatus::Interrupted, Some(reason.clone()))?;
                self.queue_paused = true;
            }
            crate::SemanticEvent::RunRequiresAction { run_id, .. } => {
                let run = self
                    .runs
                    .get_mut(run_id)
                    .ok_or_else(|| ProjectionError::UnknownRun(run_id.clone()))?;
                if run.status != RunStatus::Running {
                    return Err(ProjectionError::InvalidRunTransition {
                        run_id: run_id.clone(),
                        from: run.status,
                        to: RunStatus::RequiresAction,
                    });
                }
                run.status = RunStatus::RequiresAction;
                self.queue_paused = true;
            }
            crate::SemanticEvent::ToolCallEmitted {
                run_id,
                assistant_message_id,
                call_id,
                agent_id,
                tool_name,
                arguments,
                raw_arguments,
                native,
            } => {
                let run = self
                    .runs
                    .get_mut(run_id)
                    .ok_or_else(|| ProjectionError::UnknownRun(run_id.clone()))?;
                if run.tool_calls.contains_key(call_id) {
                    return Err(ProjectionError::DuplicateToolCall(call_id.clone()));
                }
                run.tool_calls.insert(
                    call_id.clone(),
                    ToolCallProjection {
                        call_id: call_id.clone(),
                        assistant_message_id: assistant_message_id.clone(),
                        agent_id: agent_id.clone(),
                        tool_name: tool_name.clone(),
                        arguments: arguments.clone(),
                        raw_arguments: raw_arguments.clone(),
                        native: *native,
                        execution_id: None,
                        result: None,
                    },
                );
            }
            crate::SemanticEvent::ToolExecutionStarted {
                run_id,
                call_id,
                execution_id,
            } => {
                let run = self
                    .runs
                    .get_mut(run_id)
                    .ok_or_else(|| ProjectionError::UnknownRun(run_id.clone()))?;
                let call = run
                    .tool_calls
                    .get_mut(call_id)
                    .ok_or_else(|| ProjectionError::UnknownToolCall(call_id.clone()))?;
                if !call.native {
                    return Err(ProjectionError::ExternalToolExecution(call_id.clone()));
                }
                match &call.execution_id {
                    None => call.execution_id = Some(execution_id.clone()),
                    Some(existing) if existing == execution_id => {}
                    Some(_) => {
                        return Err(ProjectionError::ConflictingToolExecution(call_id.clone()))
                    }
                }
            }
            crate::SemanticEvent::ToolResult {
                run_id,
                call_id,
                result,
                ..
            } => {
                let run = self
                    .runs
                    .get_mut(run_id)
                    .ok_or_else(|| ProjectionError::UnknownRun(run_id.clone()))?;
                let call = run
                    .tool_calls
                    .get_mut(call_id)
                    .ok_or_else(|| ProjectionError::UnknownToolCall(call_id.clone()))?;
                match &call.result {
                    None => call.result = Some(result.clone()),
                    Some(existing) if existing == result => {}
                    Some(_) => return Err(ProjectionError::ConflictingToolResult(call_id.clone())),
                }
            }
            crate::SemanticEvent::RunAttemptFailed { run_id, error, .. } => {
                let run = self
                    .runs
                    .get_mut(run_id)
                    .ok_or_else(|| ProjectionError::UnknownRun(run_id.clone()))?;
                run.error = Some(error.clone());
            }
            crate::SemanticEvent::QueuePaused => self.queue_paused = true,
            crate::SemanticEvent::QueueResumed => self.queue_paused = false,
            crate::SemanticEvent::SessionForked { .. } => self.queue_paused = true,
            _ => {}
        }
        Ok(())
    }

    fn set_run_terminal(
        &mut self,
        run_id: &str,
        status: RunStatus,
        error: Option<String>,
    ) -> Result<(), ProjectionError> {
        let run = self
            .runs
            .get_mut(run_id)
            .ok_or_else(|| ProjectionError::UnknownRun(run_id.to_owned()))?;
        if !matches!(
            run.status,
            RunStatus::Queued | RunStatus::Running | RunStatus::RequiresAction
        ) {
            return Err(ProjectionError::InvalidRunTransition {
                run_id: run_id.to_owned(),
                from: run.status,
                to: status,
            });
        }
        run.status = status;
        run.error = error;
        Ok(())
    }

    pub fn active_run(&self) -> Option<&RunProjection> {
        self.runs
            .values()
            .find(|run| run.status == RunStatus::Running)
    }

    pub fn queued_runs(&self) -> impl Iterator<Item = &RunProjection> {
        let mut runs: Vec<_> = self
            .runs
            .values()
            .filter(|run| run.status == RunStatus::Queued)
            .collect();
        runs.sort_by(|left, right| {
            left.queue_priority
                .cmp(&right.queue_priority)
                .then_with(|| left.run_id.cmp(&right.run_id))
        });
        runs.into_iter()
    }

    pub fn ready_action_run(&self) -> Option<&RunProjection> {
        self.runs.values().find(|run| {
            run.status == RunStatus::RequiresAction
                && !run.tool_calls.is_empty()
                && run.tool_calls.values().all(|call| call.result.is_some())
        })
    }
}

fn allowed_sources(next: SessionPhase) -> &'static [SessionPhase] {
    match next {
        SessionPhase::Created => &[],
        SessionPhase::Running => &[
            SessionPhase::Created,
            SessionPhase::Finished,
            SessionPhase::Interrupted,
            SessionPhase::Failed,
        ],
        SessionPhase::Interrupted => &[SessionPhase::Running],
        SessionPhase::Finished => &[SessionPhase::Running],
        SessionPhase::Failed => &[
            SessionPhase::Created,
            SessionPhase::Running,
            SessionPhase::Interrupted,
        ],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid session transition from {from:?} to {to:?}")]
pub struct SessionTransitionError {
    pub from: SessionPhase,
    pub to: SessionPhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProjectionError {
    #[error("session creation event was duplicated")]
    DuplicateCreation,
    #[error("invalid transition while projecting session: {0}")]
    InvalidTransition(#[from] SessionTransitionError),
    #[error("message {0} was started more than once")]
    DuplicateMessage(String),
    #[error("message {0} is unknown")]
    UnknownMessage(String),
    #[error("tool call {0} was emitted more than once")]
    DuplicateToolCall(String),
    #[error("tool call {0} is unknown")]
    UnknownToolCall(String),
    #[error("permission request {0} was emitted more than once")]
    DuplicatePermissionRequest(String),
    #[error("permission request {0} is unknown")]
    UnknownPermissionRequest(String),
    #[error("permission request {0} already has a different resolution")]
    ConflictingPermissionResolution(String),
    #[error("tool call {0} already has a different result")]
    ConflictingToolResult(String),
    #[error("external tool call {0} cannot be executed natively")]
    ExternalToolExecution(String),
    #[error("tool call {0} already has a different execution claim")]
    ConflictingToolExecution(String),
    #[error("message {0} is already closed")]
    MessageAlreadyClosed(String),
    #[error("agent {0} was spawned more than once")]
    DuplicateAgent(String),
    #[error("run {0} was queued more than once")]
    DuplicateRun(String),
    #[error("run {0} is unknown")]
    UnknownRun(String),
    #[error("invalid run {run_id} transition from {from:?} to {to:?}")]
    InvalidRunTransition {
        run_id: String,
        from: RunStatus,
        to: RunStatus,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_resumable_session_phases() {
        let mut state = SessionState::new("session-1");
        state.start().expect("created sessions can start");
        state.finish().expect("running sessions can finish");
        state
            .start()
            .expect("finished sessions can receive a new run");
        state.fail().expect("running sessions can fail");
        state.start().expect("failed sessions can be resumed");
        assert_eq!(state.phase, SessionPhase::Running);
    }

    #[test]
    fn rejects_transitions_from_created_to_finished() {
        let mut state = SessionState::new("session-1");
        assert!(state.finish().is_err());
    }

    #[test]
    fn projects_messages_runs_and_permissions() {
        let mut projection = SessionProjection::new("s");
        projection
            .apply(1, &crate::SemanticEvent::SessionCreated { title: None })
            .expect("creation projects");
        projection
            .apply(
                2,
                &crate::SemanticEvent::RunQueued {
                    run_id: "r".into(),
                    retry_of: None,
                    provider: "local".into(),
                    model: "model".into(),
                    request: serde_json::json!({"stream": true}),
                },
            )
            .expect("run queues");
        projection
            .apply(
                3,
                &crate::SemanticEvent::MessageStarted {
                    message_id: "m".into(),
                    agent_id: String::new(),
                    role: MessageRole::User,
                    author: MessageAuthor::User,
                },
            )
            .expect("message starts");
        projection
            .apply(
                4,
                &crate::SemanticEvent::MessageContentAppended {
                    message_id: "m".into(),
                    block: ContentBlock::Text("hello".into()),
                },
            )
            .expect("content appends");
        projection
            .apply(
                5,
                &crate::SemanticEvent::MessageCompleted {
                    message_id: "m".into(),
                },
            )
            .expect("message completes");
        assert_eq!(projection.messages[0].blocks.len(), 1);
        assert_eq!(projection.runs["r"].status, RunStatus::Queued);
    }

    #[test]
    fn retries_are_selected_before_existing_queued_runs() {
        let mut projection = SessionProjection::new("s");
        projection
            .apply(1, &crate::SemanticEvent::SessionCreated { title: None })
            .expect("creation projects");
        for id in ["normal", "retry"] {
            projection
                .apply(
                    if id == "normal" { 2 } else { 3 },
                    &crate::SemanticEvent::RunQueued {
                        run_id: id.into(),
                        retry_of: (id == "retry").then(|| "failed".into()),
                        provider: "provider".into(),
                        model: "model".into(),
                        request: serde_json::json!({}),
                    },
                )
                .expect("run queues");
        }
        assert_eq!(
            projection
                .queued_runs()
                .next()
                .map(|run| run.run_id.as_str()),
            Some("retry")
        );
    }

    #[test]
    fn projects_tool_results_idempotently_and_detects_conflicts() {
        let mut projection = SessionProjection::new("s");
        projection
            .apply(1, &crate::SemanticEvent::SessionCreated { title: None })
            .expect("creation projects");
        projection
            .apply(
                2,
                &crate::SemanticEvent::RunQueued {
                    run_id: "r".into(),
                    retry_of: None,
                    provider: "p".into(),
                    model: "m".into(),
                    request: serde_json::json!({}),
                },
            )
            .expect("run queues");
        projection
            .apply(
                3,
                &crate::SemanticEvent::RunStarted {
                    run_id: "r".into(),
                    attempt_id: "a".into(),
                    attempt: 1,
                },
            )
            .expect("run starts");
        projection
            .apply(
                4,
                &crate::SemanticEvent::ToolCallEmitted {
                    run_id: "r".into(),
                    assistant_message_id: "m".into(),
                    call_id: "c".into(),
                    agent_id: "assistant".into(),
                    tool_name: "lookup".into(),
                    arguments: serde_json::json!({"q":"x"}),
                    raw_arguments: "{\"q\":\"x\"}".into(),
                    native: false,
                },
            )
            .expect("call emits");
        projection
            .apply(
                5,
                &crate::SemanticEvent::RunRequiresAction {
                    run_id: "r".into(),
                    call_ids: vec!["c".into()],
                },
            )
            .expect("run pauses");
        let result = crate::SemanticEvent::ToolResult {
            run_id: "r".into(),
            call_id: "c".into(),
            agent_id: "assistant".into(),
            tool_name: "lookup".into(),
            result: serde_json::json!({"answer": 1}),
        };
        projection.apply(6, &result).expect("result projects");
        projection
            .apply(7, &result)
            .expect("same result is idempotent");
        assert_eq!(
            projection.ready_action_run().map(|run| run.run_id.as_str()),
            Some("r")
        );
        let conflict = crate::SemanticEvent::ToolResult {
            run_id: "r".into(),
            call_id: "c".into(),
            agent_id: "assistant".into(),
            tool_name: "lookup".into(),
            result: serde_json::json!({"answer": 2}),
        };
        assert!(matches!(
            projection.apply(8, &conflict),
            Err(ProjectionError::ConflictingToolResult(_))
        ));
    }
}
