use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use piqo_core::{
    ContentBlock, MessageRole, PermissionDecision, PermissionDecisionSource, PermissionScope,
    RecordedEvent, RunProjection, RunStatus, SemanticEvent, SessionPhase,
};
use piqo_provider::{merge_request_bodies, ProviderDelta, ProviderProtocol, ProviderTransport};
use piqo_tools::{NativeExecutor, NativeTool, ShellProgram};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    config::{ConfigManager, PermissionSetting, PiqoConfig, ResolvedProvider},
    storage::StoreError,
    SqliteStore,
};

#[derive(Clone)]
pub(crate) struct EventHub {
    channels: Arc<Mutex<HashMap<String, broadcast::Sender<RecordedEvent>>>>,
}

impl EventHub {
    pub(crate) fn new() -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn subscribe(&self, session_id: &str) -> broadcast::Receiver<RecordedEvent> {
        let mut channels = self.channels.lock().await;
        channels.retain(|_, sender| sender.receiver_count() > 0);
        channels
            .entry(session_id.to_owned())
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    pub(crate) async fn publish(&self, event: RecordedEvent) {
        let mut channels = self.channels.lock().await;
        let should_remove = channels
            .get(&event.session_id)
            .map(|sender| sender.send(event.clone()).is_err() || sender.receiver_count() == 0)
            .unwrap_or(false);
        if should_remove {
            channels.remove(&event.session_id);
        }
        channels.retain(|_, sender| sender.receiver_count() > 0);
    }

    pub(crate) async fn remove_if_unused(&self, session_id: &str) {
        let mut channels = self.channels.lock().await;
        if channels
            .get(session_id)
            .is_some_and(|sender| sender.receiver_count() == 0)
        {
            channels.remove(session_id);
        }
    }

    pub(crate) async fn close_sessions(&self, session_ids: &[String]) {
        let mut channels = self.channels.lock().await;
        for session_id in session_ids {
            channels.remove(session_id);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRequest {
    pub provider: String,
    pub model: String,
    pub input: Value,
    pub agent: Option<String>,
    pub variant: Option<String>,
    #[serde(default)]
    pub body: Value,
}

#[derive(Clone)]
pub struct SessionSupervisor {
    store: SqliteStore,
    config: ConfigManager,
    transport: ProviderTransport,
    hub: EventHub,
    locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    cancellations: Arc<Mutex<HashMap<String, CancellationToken>>>,
    deleting_projects: Arc<Mutex<HashSet<String>>>,
    dump_dir: Option<PathBuf>,
    shutdown: CancellationToken,
    workers: Arc<Mutex<JoinSet<()>>>,
}

impl SessionSupervisor {
    #[cfg(test)]
    pub(crate) fn with_dump_dir(
        store: SqliteStore,
        config: ConfigManager,
        hub: EventHub,
        dump_dir: Option<PathBuf>,
    ) -> Self {
        Self::with_dump_dir_and_shutdown(store, config, hub, dump_dir, CancellationToken::new())
    }

    pub(crate) fn with_dump_dir_and_shutdown(
        store: SqliteStore,
        config: ConfigManager,
        hub: EventHub,
        dump_dir: Option<PathBuf>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            store,
            config,
            transport: ProviderTransport::new(),
            hub,
            locks: Arc::new(Mutex::new(HashMap::new())),
            cancellations: Arc::new(Mutex::new(HashMap::new())),
            deleting_projects: Arc::new(Mutex::new(HashSet::new())),
            dump_dir,
            shutdown,
            workers: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    pub async fn shutdown(&self, grace: Duration) -> Result<(), StoreError> {
        self.shutdown.cancel();
        let mut workers = {
            let mut tracked = self.workers.lock().await;
            std::mem::take(&mut *tracked)
        };
        let workers_finished = timeout(grace, async {
            while let Some(result) = workers.join_next().await {
                if let Err(error) = result {
                    tracing::warn!(%error, "piqo worker task stopped during shutdown");
                }
            }
        })
        .await
        .is_ok();
        if !workers_finished {
            workers.abort_all();
            while let Some(result) = workers.join_next().await {
                if let Err(error) = result {
                    tracing::debug!(%error, "aborted piqo worker task");
                }
            }
        }
        self.interrupt_remaining_runs("server_shutdown").await?;
        if !workers_finished {
            return Err(StoreError::ShutdownTimeout);
        }
        Ok(())
    }

    pub async fn resume_ready_actions_after_restart(&self) -> Result<(), StoreError> {
        for session_id in self.store.session_ids().await? {
            let projection = self.store.projection(&session_id).await?;
            let interrupted_native_runs = projection
                .runs
                .values()
                .filter(|run| {
                    run.status == RunStatus::RequiresAction
                        && run.tool_calls.values().any(|call| {
                            call.native && call.execution_id.is_some() && call.result.is_none()
                        })
                })
                .map(|run| run.run_id.clone())
                .collect::<Vec<_>>();
            if !interrupted_native_runs.is_empty() {
                self.append_many(
                    &session_id,
                    interrupted_native_runs
                        .into_iter()
                        .map(|run_id| SemanticEvent::RunInterrupted {
                            run_id,
                            reason: "native_execution_interrupted_by_restart".to_owned(),
                        })
                        .collect(),
                )
                .await?;
                continue;
            }
            if projection.ready_action_run().is_some() {
                self.spawn_worker(session_id).await;
            }
        }
        Ok(())
    }

    async fn interrupt_remaining_runs(&self, reason: &str) -> Result<(), StoreError> {
        let sessions = self.store.session_ids().await?;
        for session_id in sessions {
            let projection = self.store.projection(&session_id).await?;
            let mut events = Vec::new();
            for run in projection.runs.values().filter(|run| {
                matches!(
                    run.status,
                    RunStatus::Queued | RunStatus::Running | RunStatus::RequiresAction
                )
            }) {
                events.push(SemanticEvent::RunInterrupted {
                    run_id: run.run_id.clone(),
                    reason: reason.to_owned(),
                });
            }
            if projection.state.phase == SessionPhase::Running {
                events.push(SemanticEvent::SessionPhaseChanged {
                    from: SessionPhase::Running,
                    to: SessionPhase::Interrupted,
                    reason: Some(reason.to_owned()),
                });
            }
            if !events.is_empty() {
                self.append_many(&session_id, events).await?;
            }
        }
        Ok(())
    }

    pub async fn queue_run(
        &self,
        session_id: &str,
        request: RunRequest,
    ) -> Result<String, StoreError> {
        if self.shutdown.is_cancelled() {
            return Err(StoreError::ShuttingDown);
        }
        self.ensure_session_mutable(session_id).await?;
        if self.store.projection(session_id).await?.queue_paused {
            return Err(StoreError::QueuePaused);
        }
        if request.provider.is_empty() || request.model.is_empty() {
            return Err(StoreError::InvalidRequest(
                "provider and model are required".into(),
            ));
        }
        self.config
            .resolve_provider(&request.provider)
            .map_err(|error| match error {
                crate::config::ConfigError::ProviderNotFound(name) => {
                    StoreError::ProviderNotFound(name)
                }
                error @ crate::config::ConfigError::InvalidProtocol { .. } => {
                    StoreError::ProviderProtocolError(error.to_string())
                }
                other => StoreError::ProviderUnavailable(other.to_string()),
            })?;
        let run_id = Uuid::now_v7().to_string();
        let payload = serde_json::to_value(&request).map_err(StoreError::Json)?;
        self.append(
            session_id,
            SemanticEvent::RunQueued {
                run_id: run_id.clone(),
                retry_of: None,
                provider: request.provider,
                model: request.model,
                request: payload,
            },
        )
        .await?;
        self.spawn_worker(session_id.to_owned()).await;
        Ok(run_id)
    }

    pub async fn resume(&self, session_id: &str) -> Result<(), StoreError> {
        if self.shutdown.is_cancelled() {
            return Err(StoreError::ShuttingDown);
        }
        self.ensure_session_mutable(session_id).await?;
        let projection = self.store.projection(session_id).await?;
        if !projection.queue_paused {
            return Err(StoreError::QueueNotPaused);
        }
        self.append(session_id, SemanticEvent::QueueResumed).await?;
        self.spawn_worker(session_id.to_owned()).await;
        Ok(())
    }

    pub async fn submit_tool_result(
        &self,
        session_id: &str,
        run_id: &str,
        call_id: &str,
        result: Value,
    ) -> Result<(), StoreError> {
        if self.shutdown.is_cancelled() {
            return Err(StoreError::ShuttingDown);
        }
        self.ensure_session_mutable(session_id).await?;
        let lock = {
            let mut locks = self.locks.lock().await;
            locks
                .entry(session_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let guard = lock.lock().await;
        let outcome = async {
            let projection = self.store.projection(session_id).await?;
            let run = projection
                .runs
                .get(run_id)
                .ok_or_else(|| StoreError::RunNotFound(run_id.to_owned()))?;
            if !matches!(run.status, RunStatus::RequiresAction) {
                return Err(StoreError::Conflict(
                    "run is not awaiting tool results".into(),
                ));
            }
            let request: RunRequest =
                serde_json::from_value(run.request.clone()).map_err(StoreError::Json)?;
            if request.body.get("messages").is_some() || request.body.get("input").is_some() {
                return Err(StoreError::CallerOwnedTranscript);
            }
            let call = match run.tool_calls.get(call_id) {
                Some(call) => call,
                None if projection
                    .runs
                    .values()
                    .any(|other| other.tool_calls.contains_key(call_id)) =>
                {
                    return Err(StoreError::ToolCallWrongRun {
                        call_id: call_id.to_owned(),
                        run_id: run_id.to_owned(),
                    });
                }
                None => return Err(StoreError::ToolCallNotFound(call_id.to_owned())),
            };
            if call.native {
                return Err(StoreError::NativeToolManaged(call_id.to_owned()));
            }
            let permission = projection.pending_permissions.values().find(|permission| {
                permission.run_id == run_id && permission.call_id.as_deref() == Some(call_id)
            });
            match permission.and_then(|permission| permission.decision) {
                Some(PermissionDecision::Allow) => {}
                Some(PermissionDecision::Deny) => {
                    return Err(StoreError::Conflict("tool permission was denied".into()))
                }
                _ => {
                    return Err(StoreError::Conflict(
                        "tool permission has not been approved".into(),
                    ))
                }
            }
            if let Some(existing) = &call.result {
                return if existing == &result {
                    Ok(false)
                } else {
                    Err(StoreError::ToolResultConflict(call_id.to_owned()))
                };
            }
            let all_ready = run
                .tool_calls
                .values()
                .all(|candidate| candidate.call_id == call_id || candidate.result.is_some());
            let mut events = vec![SemanticEvent::ToolResult {
                run_id: run_id.to_owned(),
                call_id: call_id.to_owned(),
                agent_id: call.agent_id.clone(),
                tool_name: call.tool_name.clone(),
                result,
            }];
            if all_ready {
                events.push(SemanticEvent::QueueResumed);
            }
            self.append_many(session_id, events).await?;
            Ok(all_ready)
        }
        .await;
        drop(guard);
        self.release_session_lock(session_id, &lock).await;
        if matches!(outcome, Ok(true)) {
            self.spawn_worker(session_id.to_owned()).await;
        }
        outcome.map(|_| ())
    }

    pub async fn approve_permission(
        &self,
        session_id: &str,
        run_id: &str,
        request_id: &str,
        scope: PermissionScope,
    ) -> Result<(), StoreError> {
        self.resolve_permission(
            session_id,
            run_id,
            request_id,
            PermissionDecision::Allow,
            Some(scope),
        )
        .await
    }

    pub async fn deny_permission(
        &self,
        session_id: &str,
        run_id: &str,
        request_id: &str,
    ) -> Result<(), StoreError> {
        self.resolve_permission(
            session_id,
            run_id,
            request_id,
            PermissionDecision::Deny,
            None,
        )
        .await
    }

    async fn resolve_permission(
        &self,
        session_id: &str,
        run_id: &str,
        request_id: &str,
        decision: PermissionDecision,
        scope: Option<PermissionScope>,
    ) -> Result<(), StoreError> {
        if self.shutdown.is_cancelled() {
            return Err(StoreError::ShuttingDown);
        }
        self.ensure_session_mutable(session_id).await?;
        let projection = self.store.projection(session_id).await?;
        let request = projection
            .pending_permissions
            .get(request_id)
            .ok_or_else(|| StoreError::InvalidRequest("permission request was not found".into()))?;
        if request.run_id != run_id {
            return Err(StoreError::InvalidRequest(
                "permission request does not belong to run".into(),
            ));
        }
        if let Some(existing) = request.decision {
            return if existing == decision {
                Ok(())
            } else {
                Err(StoreError::Conflict(
                    "permission resolution conflicts with the recorded decision".into(),
                ))
            };
        }
        if decision == PermissionDecision::Deny && scope.is_some() {
            return Err(StoreError::InvalidPermissionScope);
        }
        let session = self.store.get_session(session_id).await?;
        let rule = match scope {
            Some(PermissionScope::Once) | None => None,
            Some(PermissionScope::Session) => Some(
                self.store
                    .create_permission_rule(
                        PermissionScope::Session,
                        Some(session_id),
                        None,
                        &request.agent_id,
                        &request.tool_name,
                    )
                    .await?,
            ),
            Some(PermissionScope::Project) => {
                let project_id = session
                    .project_id
                    .as_deref()
                    .ok_or(StoreError::InvalidPermissionScope)?;
                Some(
                    self.store
                        .create_permission_rule(
                            PermissionScope::Project,
                            None,
                            Some(project_id),
                            &request.agent_id,
                            &request.tool_name,
                        )
                        .await?,
                )
            }
            Some(PermissionScope::Configuration) => Some(
                self.store
                    .create_permission_rule(
                        PermissionScope::Configuration,
                        None,
                        None,
                        &request.agent_id,
                        &request.tool_name,
                    )
                    .await?,
            ),
        };
        let mut events = vec![SemanticEvent::PermissionResolved {
            request_id: request_id.to_owned(),
            decision,
            source: Some(PermissionDecisionSource::RequestApproval),
            scope,
            rule_id: rule.as_ref().map(|rule| rule.id.clone()),
            reason: None,
        }];
        if decision == PermissionDecision::Deny {
            let call_id = request.call_id.clone().ok_or_else(|| {
                StoreError::InvalidRequest("permission request has no tool call".into())
            })?;
            events.push(SemanticEvent::ToolResult { run_id: run_id.to_owned(), call_id, agent_id: request.agent_id.clone(), tool_name: request.tool_name.clone(), result: json!({"error":{"code":"permission_denied","message":"tool invocation was denied by permission policy"}}) });
        }
        self.append_many(session_id, events).await?;
        if decision == PermissionDecision::Allow {
            if self
                .execute_approved_native_calls(session_id, run_id)
                .await?
            {
                self.spawn_worker(session_id.to_owned()).await;
            }
        } else if decision == PermissionDecision::Deny
            && self
                .resume_if_tool_results_ready(session_id, run_id)
                .await?
        {
            self.spawn_worker(session_id.to_owned()).await;
        }
        Ok(())
    }

    pub async fn cancel(&self, session_id: &str, run_id: &str) -> Result<(), StoreError> {
        if self.shutdown.is_cancelled() {
            return Err(StoreError::ShuttingDown);
        }
        self.ensure_session_mutable(session_id).await?;
        let projection = self.store.projection(session_id).await?;
        let run = projection
            .runs
            .get(run_id)
            .ok_or_else(|| StoreError::RunNotFound(run_id.to_owned()))?;
        match run.status {
            RunStatus::Queued | RunStatus::RequiresAction => {
                self.append(
                    session_id,
                    SemanticEvent::RunCancelled {
                        run_id: run_id.to_owned(),
                        reason: Some("cancelled_by_user".into()),
                    },
                )
                .await?;
                Ok(())
            }
            RunStatus::Running => {
                let cancellations = self.cancellations.lock().await;
                if let Some(token) = cancellations.get(run_id) {
                    token.cancel();
                    Ok(())
                } else {
                    Err(StoreError::Conflict("run is stopping".into()))
                }
            }
            _ => Err(StoreError::Conflict("run is already terminal".into())),
        }
    }

    pub async fn retry(&self, session_id: &str, run_id: &str) -> Result<String, StoreError> {
        if self.shutdown.is_cancelled() {
            return Err(StoreError::ShuttingDown);
        }
        self.ensure_session_mutable(session_id).await?;
        let projection = self.store.projection(session_id).await?;
        let run = projection
            .runs
            .get(run_id)
            .ok_or_else(|| StoreError::RunNotFound(run_id.to_owned()))?
            .clone();
        if !matches!(
            run.status,
            RunStatus::Failed | RunStatus::Interrupted | RunStatus::Cancelled
        ) {
            return Err(StoreError::Conflict(
                "only failed, interrupted or cancelled runs can be retried".into(),
            ));
        }
        let request: RunRequest =
            serde_json::from_value(run.request.clone()).map_err(StoreError::Json)?;
        let new_id = Uuid::now_v7().to_string();
        let request_value = serde_json::to_value(&request).map_err(StoreError::Json)?;
        let mut events = vec![SemanticEvent::RunQueued {
            run_id: new_id.clone(),
            retry_of: Some(run_id.to_owned()),
            provider: request.provider.clone(),
            model: request.model.clone(),
            request: request_value,
        }];
        if self.store.projection(session_id).await?.queue_paused {
            events.push(SemanticEvent::QueueResumed);
        }
        self.append_many(session_id, events).await?;
        self.spawn_worker(session_id.to_owned()).await;
        Ok(new_id)
    }

    pub async fn project_is_deleting(&self, project_id: &str) -> bool {
        self.deleting_projects.lock().await.contains(project_id)
    }

    pub async fn delete_project(&self, project_id: &str) -> Result<(), StoreError> {
        {
            let mut deleting = self.deleting_projects.lock().await;
            if !deleting.insert(project_id.to_owned()) {
                return Err(StoreError::ProjectDeleting(project_id.to_owned()));
            }
        }

        let result = async {
            let session_ids = self.store.project_session_ids(project_id).await?;
            for session_id in &session_ids {
                self.cancel_session_for_project_deletion(session_id).await?;
            }
            for session_id in &session_ids {
                self.wait_for_session_idle(session_id).await;
            }
            self.hub.close_sessions(&session_ids).await;
            self.store.delete_project(project_id).await
        }
        .await;

        self.deleting_projects.lock().await.remove(project_id);
        result
    }

    async fn ensure_session_mutable(&self, session_id: &str) -> Result<(), StoreError> {
        let session = self.store.get_session(session_id).await?;
        if let Some(project_id) = session.project_id {
            if self.project_is_deleting(&project_id).await {
                return Err(StoreError::ProjectDeleting(project_id));
            }
        }
        Ok(())
    }

    async fn cancel_session_for_project_deletion(
        &self,
        session_id: &str,
    ) -> Result<(), StoreError> {
        let projection = self.store.projection(session_id).await?;
        let mut events = Vec::new();
        for run in projection.runs.values() {
            match run.status {
                RunStatus::Queued | RunStatus::RequiresAction => {
                    events.push(SemanticEvent::RunCancelled {
                        run_id: run.run_id.clone(),
                        reason: Some("project_deleted".to_owned()),
                    })
                }
                RunStatus::Running => {
                    if let Some(token) = self.cancellations.lock().await.get(&run.run_id).cloned() {
                        token.cancel();
                    }
                }
                _ => {}
            }
        }
        if !events.is_empty() {
            self.append_many(session_id, events).await?;
        }
        Ok(())
    }

    async fn wait_for_session_idle(&self, session_id: &str) {
        let lock = {
            let mut locks = self.locks.lock().await;
            locks
                .entry(session_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let guard = lock.lock().await;
        drop(guard);
        self.release_session_lock(session_id, &lock).await;
    }

    async fn spawn_worker(&self, session_id: String) {
        if self.shutdown.is_cancelled() {
            return;
        }
        let this = self.clone();
        let mut workers = self.workers.lock().await;
        if self.shutdown.is_cancelled() {
            return;
        }
        workers.spawn(async move {
            if let Err(error) = this.process_session(&session_id).await {
                tracing::error!(%session_id, %error, "session worker stopped");
            }
        });
    }

    async fn process_session(&self, session_id: &str) -> Result<(), StoreError> {
        let lock = {
            let mut locks = self.locks.lock().await;
            locks
                .entry(session_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let guard = lock.lock().await;
        let result = async {
            loop {
                if self.shutdown.is_cancelled() {
                    return Ok(());
                }
                let projection = self.store.projection(session_id).await?;
                if projection.active_run().is_some() {
                    return Ok(());
                }
                if let Some(run) = projection.ready_action_run().cloned() {
                    let max_turns = self
                        .config
                        .snapshot()
                        .map_err(|error| StoreError::ProviderUnavailable(error.to_string()))?
                        .defaults
                        .max_model_turns;
                    if run.attempts >= max_turns {
                        self.append(
                            session_id,
                            SemanticEvent::RunFailed {
                                run_id: run.run_id.clone(),
                                error: format!("maximum model turns ({max_turns}) exceeded"),
                            },
                        )
                        .await?;
                        continue;
                    }
                    self.append(session_id, SemanticEvent::QueueResumed).await?;
                    self.append(
                        session_id,
                        SemanticEvent::RunStarted {
                            run_id: run.run_id.clone(),
                            attempt_id: Uuid::now_v7().to_string(),
                            attempt: run.attempts + 1,
                        },
                    )
                    .await?;
                    let token = CancellationToken::new();
                    self.cancellations
                        .lock()
                        .await
                        .insert(run.run_id.clone(), token.clone());
                    let result = self
                        .execute_run(session_id, &run, token, None, None, 0)
                        .await;
                    self.cancellations.lock().await.remove(&run.run_id);
                    self.finish_run(session_id, &run, result).await?;
                    continue;
                }
                if projection.queue_paused {
                    return Ok(());
                }
                let Some(mut run) = projection.queued_runs().next().cloned() else {
                    if projection.state.phase == SessionPhase::Running {
                        self.append(
                            session_id,
                            SemanticEvent::SessionPhaseChanged {
                                from: SessionPhase::Running,
                                to: SessionPhase::Finished,
                                reason: Some("queue_empty".into()),
                            },
                        )
                        .await?;
                    }
                    return Ok(());
                };
                if projection.state.phase != SessionPhase::Running {
                    self.append(
                        session_id,
                        SemanticEvent::SessionPhaseChanged {
                            from: projection.state.phase,
                            to: SessionPhase::Running,
                            reason: Some("run_started".into()),
                        },
                    )
                    .await?;
                }
                let attempt_id = Uuid::now_v7().to_string();
                let token = CancellationToken::new();
                self.cancellations
                    .lock()
                    .await
                    .insert(run.run_id.clone(), token.clone());
                if let Err(error) = self
                    .append(
                        session_id,
                        SemanticEvent::RunStarted {
                            run_id: run.run_id.clone(),
                            attempt_id: attempt_id.clone(),
                            attempt: run.attempts + 1,
                        },
                    )
                    .await
                {
                    self.cancellations.lock().await.remove(&run.run_id);
                    return Err(error);
                }
                run.attempt_id = Some(attempt_id);
                run.attempts += 1;
                let result = self
                    .execute_run(session_id, &run, token, None, None, 0)
                    .await;
                self.cancellations.lock().await.remove(&run.run_id);
                self.finish_run(session_id, &run, result).await?;
            }
        }
        .await;
        drop(guard);
        self.release_session_lock(session_id, &lock).await;
        result
    }

    async fn release_session_lock(&self, session_id: &str, lock: &Arc<Mutex<()>>) {
        let mut locks = self.locks.lock().await;
        if Arc::strong_count(lock) == 2
            && locks
                .get(session_id)
                .is_some_and(|current| Arc::ptr_eq(current, lock))
        {
            locks.remove(session_id);
        }
    }

    async fn execute_run(
        &self,
        session_id: &str,
        run: &RunProjection,
        cancellation: CancellationToken,
        execution: Option<Arc<RunExecutionConfig>>,
        existing_assistant_message: Option<String>,
        retry_count: u8,
    ) -> ExecutionResult {
        let request: RunRequest = match serde_json::from_value(run.request.clone()) {
            Ok(request) => request,
            Err(error) => return ExecutionResult::Failed(error.to_string(), false),
        };
        let initial_config = if execution.is_none() {
            let config = match self.config.snapshot() {
                Ok(config) => config,
                Err(error) => return ExecutionResult::Failed(error.to_string(), false),
            };
            let provider = match config.resolve_provider(&request.provider) {
                Ok(provider) => provider,
                Err(error) => return ExecutionResult::Failed(error.to_string(), false),
            };
            Some((config, provider))
        } else {
            None
        };
        let mut attempt_id = if retry_count == 0 {
            run.attempt_id
                .clone()
                .unwrap_or_else(|| Uuid::now_v7().to_string())
        } else {
            let attempt_id = Uuid::now_v7().to_string();
            if let Err(error) = self
                .append(
                    session_id,
                    SemanticEvent::RunAttemptStarted {
                        run_id: run.run_id.clone(),
                        attempt_id: attempt_id.clone(),
                        attempt: run.attempts + u32::from(retry_count),
                    },
                )
                .await
            {
                return ExecutionResult::Failed(error.to_string(), false);
            }
            attempt_id
        };
        if run.retry_of.is_none() && existing_assistant_message.is_none() {
            let user_message_id = Uuid::now_v7().to_string();
            if let Err(error) = self
                .append(
                    session_id,
                    SemanticEvent::MessageStarted {
                        message_id: user_message_id.clone(),
                        agent_id: String::new(),
                        role: MessageRole::User,
                        author: piqo_core::MessageAuthor::User,
                    },
                )
                .await
            {
                return ExecutionResult::Failed(error.to_string(), false);
            }
            let input_block = if let Some(text) = request.input.as_str() {
                ContentBlock::Text(text.to_owned())
            } else {
                ContentBlock::Json(request.input.clone())
            };
            if let Err(error) = self
                .append(
                    session_id,
                    SemanticEvent::MessageContentAppended {
                        message_id: user_message_id.clone(),
                        block: input_block,
                    },
                )
                .await
            {
                return ExecutionResult::Failed(error.to_string(), false);
            }
            if let Err(error) = self
                .append(
                    session_id,
                    SemanticEvent::MessageCompleted {
                        message_id: user_message_id,
                    },
                )
                .await
            {
                return ExecutionResult::Failed(error.to_string(), false);
            }
        }
        let execution = match (execution, initial_config) {
            (Some(execution), None) => execution,
            (None, Some((config, provider))) => {
                let projection = match self.store.projection(session_id).await {
                    Ok(projection) => projection,
                    Err(error) => return ExecutionResult::Failed(error.to_string(), false),
                };
                let native_tools = match self.store.get_session(session_id).await {
                    Ok(session) if session.project_id.is_some() => {
                        configured_native_tools(&config, &request)
                    }
                    Ok(_) => Vec::new(),
                    Err(error) => return ExecutionResult::Failed(error.to_string(), false),
                };
                let body = match build_body(
                    &config,
                    &provider.protocol,
                    &request,
                    &projection,
                    &native_tools,
                ) {
                    Ok(body) => body,
                    Err(error) => return ExecutionResult::Failed(error.to_string(), false),
                };
                Arc::new(RunExecutionConfig { provider, body })
            }
            _ => return ExecutionResult::Failed("invalid execution snapshot".to_owned(), false),
        };
        let provider = &execution.provider;
        let body = &execution.body;
        let is_existing_assistant = existing_assistant_message.is_some();
        let assistant_message_id =
            existing_assistant_message.unwrap_or_else(|| Uuid::now_v7().to_string());
        if !is_existing_assistant {
            if let Err(error) = self
                .append(
                    session_id,
                    SemanticEvent::MessageStarted {
                        message_id: assistant_message_id.clone(),
                        agent_id: "assistant".into(),
                        role: MessageRole::Assistant,
                        author: piqo_core::MessageAuthor::Agent("assistant".into()),
                    },
                )
                .await
            {
                return ExecutionResult::Failed(error.to_string(), false);
            }
        }
        let request = match self.transport.build_request_with_headers(
            &provider.endpoint,
            body.clone(),
            &provider.headers,
        ) {
            Ok(request) => request,
            Err(error) => return ExecutionResult::Failed(error.to_string(), false),
        };
        let mut retries = retry_count;
        let response = loop {
            if let Some(directory) = &self.dump_dir {
                if let Err(error) = dump_request(
                    directory,
                    &run.run_id,
                    retries.saturating_add(1),
                    provider,
                    body,
                )
                .await
                {
                    tracing::warn!(run_id = %run.run_id, %error, "unable to dump provider request");
                }
            }
            let result = tokio::select! {
                _ = cancellation.cancelled() => return ExecutionResult::Cancelled,
                _ = self.shutdown.cancelled() => return ExecutionResult::Interrupted,
                response = self.transport.send_with_connect_timeout(
                    request.try_clone().expect("JSON request is cloneable"),
                    Duration::from_secs(provider.connect_timeout_seconds),
                ) => response,
            };
            match result {
                Ok(response) => break response,
                Err(error) if retries < 5 => {
                    let _ = self
                        .append(
                            session_id,
                            SemanticEvent::RunAttemptFailed {
                                run_id: run.run_id.clone(),
                                attempt_id: attempt_id.clone(),
                                error: error.to_string(),
                                retryable: true,
                            },
                        )
                        .await;
                    retries += 1;
                    let next_attempt_id = Uuid::now_v7().to_string();
                    if let Err(start_error) = self
                        .append(
                            session_id,
                            SemanticEvent::RunAttemptStarted {
                                run_id: run.run_id.clone(),
                                attempt_id: next_attempt_id.clone(),
                                attempt: run.attempts + u32::from(retries),
                            },
                        )
                        .await
                    {
                        return ExecutionResult::Failed(start_error.to_string(), false);
                    }
                    attempt_id = next_attempt_id;
                    tokio::select! {
                        _ = self.shutdown.cancelled() => return ExecutionResult::Interrupted,
                        _ = tokio::time::sleep(
                            std::time::Duration::from_millis(
                                250u64.saturating_mul(1u64 << (retries - 1)),
                            )
                            .min(std::time::Duration::from_secs(4)),
                        ) => {}
                    }
                }
                Err(error) => return ExecutionResult::Failed(error.to_string(), false),
            }
        };
        if !response.status().is_success() {
            return ExecutionResult::Failed(
                format!("provider returned {}", response.status()),
                false,
            );
        }
        let stream_mode = body_stream_mode(body);
        let mut saw_delta = false;
        let mut usage = None;
        let mut text_buffer = String::new();
        let mut last_text_flush = Instant::now();
        let mut tool_buffers: HashMap<String, ToolCallBuffer> = HashMap::new();
        let result = if stream_mode {
            let mut bytes = response.bytes_stream();
            let mut buffer = Vec::new();
            let mut outcome = ExecutionResult::Completed;
            let mut saw_completed = false;
            let mut flush_tick = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return ExecutionResult::Cancelled,
                    _ = self.shutdown.cancelled() => return ExecutionResult::Interrupted,
                    _ = flush_tick.tick() => {
                        if !text_buffer.is_empty() {
                            let buffered = std::mem::take(&mut text_buffer);
                            last_text_flush = Instant::now();
                            if let Err(error) = self
                                .persist_delta(
                                    session_id,
                                    &run.run_id,
                                    &assistant_message_id,
                                    ProviderDelta::Text(buffered),
                                )
                                .await
                            {
                                outcome = ExecutionResult::Failed(error.to_string(), false);
                                break;
                            }
                        }
                        continue;
                    }
                    chunk = bytes.next() => {
                        let Some(chunk) = chunk else { break };
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        outcome = ExecutionResult::Failed(error.to_string(), !saw_delta);
                        break;
                    }
                };
                buffer.extend_from_slice(&chunk);
                while let Some((index, delimiter_len)) = find_sse_frame_end(&buffer) {
                    let frame = buffer[..index].to_vec();
                    buffer.drain(..index + delimiter_len);
                    let frame = match std::str::from_utf8(&frame) {
                        Ok(frame) => frame,
                        Err(error) => {
                            outcome = ExecutionResult::Failed(
                                format!("provider returned invalid UTF-8 SSE data: {error}"),
                                false,
                            );
                            break;
                        }
                    };
                    let (event_type, data) = parse_sse_frame(frame);
                    if data.trim().is_empty() {
                        continue;
                    }
                    match piqo_provider::parse_sse_event(
                        provider.protocol,
                        event_type.as_deref(),
                        &data,
                    ) {
                        Ok(deltas) => {
                            for delta in deltas {
                                if matches!(
                                    &delta,
                                    ProviderDelta::Text(_)
                                        | ProviderDelta::ToolCall { .. }
                                        | ProviderDelta::ToolCallDelta { .. }
                                ) {
                                    saw_delta = true;
                                }
                                if let ProviderDelta::ToolCallDelta {
                                    index,
                                    call_id,
                                    name,
                                    arguments,
                                } = &delta
                                {
                                    let key = index
                                        .map(|index| format!("stream-index-{index}"))
                                        .or_else(|| call_id.clone())
                                        .unwrap_or_else(|| {
                                            format!("anonymous-tool-{}", tool_buffers.len())
                                        });
                                    let entry = tool_buffers.entry(key.clone()).or_insert_with(|| {
                                        ToolCallBuffer {
                                            call_id: call_id.clone(),
                                            name: name.clone(),
                                            arguments: String::new(),
                                        }
                                    });
                                    if entry.name.is_none() {
                                        entry.name = name.clone();
                                    }
                                    if entry.call_id.is_none() {
                                        entry.call_id = call_id.clone();
                                    }
                                    entry.arguments.push_str(arguments);
                                    continue;
                                }
                                if matches!(&delta, ProviderDelta::Completed) {
                                    saw_completed = true;
                                    for buffer in std::mem::take(&mut tool_buffers).into_values() {
                                        match self
                                            .persist_delta(
                                                session_id,
                                                &run.run_id,
                                                &assistant_message_id,
                                                ProviderDelta::ToolCall {
                                                    call_id: buffer.call_id,
                                                    name: buffer.name,
                                                    arguments: buffer.arguments,
                                                },
                                            )
                                            .await
                                        {
                                            Ok(DeltaResult::RequiresAction) => {
                                                outcome = ExecutionResult::RequiresAction;
                                                break;
                                            }
                                            Ok(DeltaResult::Usage(_)) | Ok(DeltaResult::None) => {}
                                            Err(error) => {
                                                outcome = ExecutionResult::Failed(
                                                    error.to_string(),
                                                    false,
                                                );
                                                break;
                                            }
                                        }
                                    }
                                }
                                if let ProviderDelta::Text(text) = delta {
                                    text_buffer.push_str(&text);
                                    if text_buffer.len() < 4096
                                        && last_text_flush.elapsed() < Duration::from_millis(100)
                                    {
                                        continue;
                                    }
                                    let buffered = std::mem::take(&mut text_buffer);
                                    last_text_flush = Instant::now();
                                    if let Err(error) = self
                                        .persist_delta(
                                            session_id,
                                            &run.run_id,
                                            &assistant_message_id,
                                            ProviderDelta::Text(buffered),
                                        )
                                        .await
                                    {
                                        outcome = ExecutionResult::Failed(error.to_string(), false);
                                        break;
                                    }
                                    continue;
                                }
                                match self
                                    .persist_delta(
                                        session_id,
                                        &run.run_id,
                                        &assistant_message_id,
                                        delta,
                                    )
                                    .await
                                {
                                    Ok(DeltaResult::Usage(value)) => usage = Some(value),
                                    Ok(DeltaResult::None) => {}
                                    Ok(DeltaResult::RequiresAction) => {
                                        outcome = ExecutionResult::RequiresAction;
                                        break;
                                    }
                                    Err(error) => {
                                        outcome = ExecutionResult::Failed(error.to_string(), false);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            outcome = ExecutionResult::Failed(error.to_string(), false);
                            break;
                        }
                    }
                    if matches!(
                        outcome,
                        ExecutionResult::Failed(_, _) | ExecutionResult::RequiresAction
                    ) {
                        break;
                    }
                }
                if matches!(
                    outcome,
                    ExecutionResult::Failed(_, _) | ExecutionResult::RequiresAction
                ) {
                    break;
                }
                    }
                }
            }
            if matches!(outcome, ExecutionResult::Completed) && !saw_completed {
                outcome = ExecutionResult::Failed(
                    "provider stream ended before a completion event".to_owned(),
                    false,
                );
            }
            outcome
        } else {
            let text_result = tokio::select! {
                _ = cancellation.cancelled() => return ExecutionResult::Cancelled,
                _ = self.shutdown.cancelled() => return ExecutionResult::Interrupted,
                result = response.text() => result,
            };
            match text_result {
                Ok(text) => {
                    match piqo_provider::parse_non_stream_response(provider.protocol, &text) {
                        Ok(deltas) => {
                            let mut outcome = ExecutionResult::Completed;
                            for delta in deltas {
                                let delta = match delta {
                                    ProviderDelta::Text(text) => {
                                        text_buffer.push_str(&text);
                                        continue;
                                    }
                                    other => other,
                                };
                                match self
                                    .persist_delta(
                                        session_id,
                                        &run.run_id,
                                        &assistant_message_id,
                                        delta,
                                    )
                                    .await
                                {
                                    Ok(DeltaResult::Usage(value)) => usage = Some(value),
                                    Ok(DeltaResult::None) => {}
                                    Ok(DeltaResult::RequiresAction) => {
                                        outcome = ExecutionResult::RequiresAction;
                                        break;
                                    }
                                    Err(error) => {
                                        outcome = ExecutionResult::Failed(error.to_string(), false);
                                        break;
                                    }
                                }
                            }
                            outcome
                        }
                        Err(error) => ExecutionResult::Failed(error.to_string(), false),
                    }
                }
                Err(error) => ExecutionResult::Failed(error.to_string(), !saw_delta),
            }
        };
        if !text_buffer.is_empty() {
            if let Err(error) = self
                .persist_delta(
                    session_id,
                    &run.run_id,
                    &assistant_message_id,
                    ProviderDelta::Text(text_buffer),
                )
                .await
            {
                return ExecutionResult::Failed(error.to_string(), saw_delta);
            }
        }
        if let ExecutionResult::Failed(error, true) = &result {
            if self.shutdown.is_cancelled() {
                return ExecutionResult::Interrupted;
            }
            if retry_count < 5 {
                let _ = self
                    .append(
                        session_id,
                        SemanticEvent::RunAttemptFailed {
                            run_id: run.run_id.clone(),
                            attempt_id: attempt_id.clone(),
                            error: error.clone(),
                            retryable: true,
                        },
                    )
                    .await;
                tokio::select! {
                    _ = self.shutdown.cancelled() => return ExecutionResult::Interrupted,
                    _ = tokio::time::sleep(
                        Duration::from_millis(250u64.saturating_mul(1u64 << retry_count))
                            .min(Duration::from_secs(4)),
                    ) => {}
                }
                return Box::pin(self.execute_run(
                    session_id,
                    run,
                    cancellation,
                    Some(execution),
                    Some(assistant_message_id),
                    retry_count + 1,
                ))
                .await;
            }
        }
        match result {
            ExecutionResult::Completed => ExecutionResult::CompletedWithUsage(usage),
            other => other,
        }
    }

    async fn persist_delta(
        &self,
        session_id: &str,
        run_id: &str,
        assistant_message_id: &str,
        delta: ProviderDelta,
    ) -> Result<DeltaResult, StoreError> {
        match delta {
            ProviderDelta::Text(text) => {
                self.append(
                    session_id,
                    SemanticEvent::MessageContentAppended {
                        message_id: assistant_message_id.to_owned(),
                        block: ContentBlock::Text(text),
                    },
                )
                .await?;
                Ok(DeltaResult::None)
            }
            ProviderDelta::ToolCall {
                call_id,
                name,
                arguments,
            } => {
                let call_id = call_id.unwrap_or_else(|| Uuid::now_v7().to_string());
                let tool_name = name.unwrap_or_else(|| "function".into());
                let native = self
                    .is_native_call(session_id, run_id, &tool_name)
                    .await
                    .unwrap_or(false);
                self.append(
                    session_id,
                    SemanticEvent::ToolCallEmitted {
                        run_id: run_id.to_owned(),
                        assistant_message_id: assistant_message_id.to_owned(),
                        call_id: call_id.clone(),
                        agent_id: "assistant".into(),
                        tool_name,
                        arguments: serde_json::from_str(&arguments)
                            .unwrap_or_else(|_| json!({"raw": arguments})),
                        raw_arguments: arguments,
                        native,
                    },
                )
                .await?;
                Ok(DeltaResult::None)
            }
            ProviderDelta::ToolCallDelta { .. } => Err(StoreError::InvalidRequest(
                "incomplete tool call reached persistence".into(),
            )),
            ProviderDelta::Usage(value) => Ok(DeltaResult::Usage(value)),
            ProviderDelta::Completed => Ok(DeltaResult::None),
            ProviderDelta::RequiresAction => {
                self.append(
                    session_id,
                    SemanticEvent::RunRequiresAction {
                        run_id: run_id.to_owned(),
                        call_ids: Vec::new(),
                    },
                )
                .await?;
                Ok(DeltaResult::RequiresAction)
            }
        }
    }

    async fn evaluate_tool_permissions(
        &self,
        session_id: &str,
        run: &RunProjection,
    ) -> Result<(), StoreError> {
        let request: RunRequest =
            serde_json::from_value(run.request.clone()).map_err(StoreError::Json)?;
        let agent_id = request.agent.unwrap_or_else(|| "assistant".to_owned());
        let configured = self.config.agent(&agent_id).ok();
        let session = self.store.get_session(session_id).await?;
        let projection = self.store.projection(session_id).await?;
        let run = projection
            .runs
            .get(&run.run_id)
            .ok_or_else(|| StoreError::RunNotFound(run.run_id.clone()))?;
        let mut events = Vec::new();
        let mut has_pending_approval = false;
        for call in run.tool_calls.values().filter(|call| call.result.is_none()) {
            let request_id = Uuid::now_v7().to_string();
            let configured_decision =
                configured
                    .as_ref()
                    .and_then(|agent| match call.tool_name.as_str() {
                        "read" => agent.permissions.read,
                        "write" | "edit" => agent.permissions.write,
                        "bash" => agent.permissions.bash,
                        _ => None,
                    });
            let (decision, source, rule_id) =
                if configured_decision == Some(PermissionSetting::Deny) {
                    (
                        PermissionDecision::Deny,
                        PermissionDecisionSource::Configuration,
                        None,
                    )
                } else if let Some(rule) = self
                    .store
                    .matching_permission_rule(
                        session_id,
                        session.project_id.as_deref(),
                        &agent_id,
                        NativeTool::parse(&call.tool_name)
                            .map(NativeTool::permission_name)
                            .unwrap_or(&call.tool_name),
                    )
                    .await?
                {
                    let source = match rule.scope {
                        PermissionScope::Session => PermissionDecisionSource::SessionRule,
                        PermissionScope::Project => PermissionDecisionSource::ProjectRule,
                        PermissionScope::Configuration => {
                            PermissionDecisionSource::InteractiveConfiguration
                        }
                        PermissionScope::Once => PermissionDecisionSource::RequestApproval,
                    };
                    (PermissionDecision::Allow, source, Some(rule.id))
                } else {
                    match configured_decision {
                        Some(PermissionSetting::Allow) => (
                            PermissionDecision::Allow,
                            PermissionDecisionSource::Configuration,
                            None,
                        ),
                        Some(PermissionSetting::Ask) => {
                            has_pending_approval = true;
                            (
                                PermissionDecision::Ask,
                                PermissionDecisionSource::Configuration,
                                None,
                            )
                        }
                        _ => (
                            PermissionDecision::Deny,
                            PermissionDecisionSource::Default,
                            None,
                        ),
                    }
                };
            events.push(SemanticEvent::PermissionRequested {
                request_id: request_id.clone(),
                run_id: run.run_id.clone(),
                call_id: Some(call.call_id.clone()),
                agent_id: agent_id.clone(),
                tool_name: call.tool_name.clone(),
                arguments: call.arguments.clone(),
            });
            if decision != PermissionDecision::Ask {
                events.push(SemanticEvent::PermissionResolved {
                    request_id,
                    decision,
                    source: Some(source),
                    scope: None,
                    rule_id,
                    reason: None,
                });
                if decision == PermissionDecision::Deny {
                    events.push(SemanticEvent::ToolResult { run_id: run.run_id.clone(), call_id: call.call_id.clone(), agent_id: call.agent_id.clone(), tool_name: call.tool_name.clone(), result: json!({"error":{"code":"permission_denied","message":"tool invocation was denied by permission policy"}}) });
                }
            }
        }
        self.append_many(session_id, events).await?;
        if !has_pending_approval {
            self.execute_approved_native_calls(session_id, &run.run_id)
                .await?;
        }
        Ok(())
    }

    async fn is_native_call(
        &self,
        session_id: &str,
        run_id: &str,
        tool_name: &str,
    ) -> Result<bool, StoreError> {
        let Some(tool) = NativeTool::parse(tool_name) else {
            return Ok(false);
        };
        let session = self.store.get_session(session_id).await?;
        if session.project_id.is_none() {
            return Ok(false);
        }
        let projection = self.store.projection(session_id).await?;
        let run = projection
            .runs
            .get(run_id)
            .ok_or_else(|| StoreError::RunNotFound(run_id.to_owned()))?;
        let request: RunRequest =
            serde_json::from_value(run.request.clone()).map_err(StoreError::Json)?;
        if request.body.get("tools").is_some() {
            return Ok(false);
        }
        let agent_id = request.agent.unwrap_or_else(|| "assistant".to_owned());
        let configured = self.config.agent(&agent_id).ok();
        let decision = configured.and_then(|agent| match tool {
            NativeTool::Read => agent.permissions.read,
            NativeTool::Write | NativeTool::Edit => agent.permissions.write,
            NativeTool::Bash => agent.permissions.bash,
        });
        Ok(matches!(
            decision,
            Some(PermissionSetting::Allow | PermissionSetting::Ask)
        ))
    }

    async fn execute_approved_native_calls(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> Result<bool, StoreError> {
        let session = self.store.get_session(session_id).await?;
        let Some(project_id) = session.project_id else {
            return Ok(false);
        };
        let project = self.store.get_project(&project_id).await?;
        loop {
            let projection = self.store.projection(session_id).await?;
            let run = projection
                .runs
                .get(run_id)
                .ok_or_else(|| StoreError::RunNotFound(run_id.to_owned()))?;
            let Some(call) = run.tool_calls.values().find(|call| {
                call.native
                    && call.result.is_none()
                    && call.execution_id.is_none()
                    && projection.pending_permissions.values().any(|permission| {
                        permission.run_id == run_id
                            && permission.call_id.as_deref() == Some(call.call_id.as_str())
                            && permission.decision == Some(PermissionDecision::Allow)
                    })
            }) else {
                break;
            };
            let tool = NativeTool::parse(&call.tool_name)
                .ok_or_else(|| StoreError::InvalidRequest("unknown native tool".into()))?;
            let call_id = call.call_id.clone();
            let agent_id = call.agent_id.clone();
            let tool_name = call.tool_name.clone();
            let arguments = call.arguments.clone();
            self.append(
                session_id,
                SemanticEvent::ToolExecutionStarted {
                    run_id: run_id.to_owned(),
                    call_id: call_id.clone(),
                    execution_id: Uuid::now_v7().to_string(),
                },
            )
            .await?;
            let config = self
                .config
                .snapshot()
                .map_err(|error| StoreError::ProviderUnavailable(error.to_string()))?;
            let shell = ShellProgram::discover(config.native_tools.shell.as_deref())
                .map_err(|error| StoreError::InvalidRequest(error.to_string()))?;
            let executor = NativeExecutor::new(
                PathBuf::from(&project.path),
                config.native_tools.limits(),
                shell,
            )
            .map_err(|error| StoreError::InvalidRequest(error.to_string()))?;
            let cancellation = self
                .cancellations
                .lock()
                .await
                .get(run_id)
                .cloned()
                .unwrap_or_else(CancellationToken::new);
            let result = executor.execute(tool, &arguments, cancellation).await;
            self.append(
                session_id,
                SemanticEvent::ToolResult {
                    run_id: run_id.to_owned(),
                    call_id,
                    agent_id,
                    tool_name,
                    result,
                },
            )
            .await?;
        }
        self.resume_if_tool_results_ready(session_id, run_id).await
    }

    async fn resume_if_tool_results_ready(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> Result<bool, StoreError> {
        let projection = self.store.projection(session_id).await?;
        let run = projection
            .runs
            .get(run_id)
            .ok_or_else(|| StoreError::RunNotFound(run_id.to_owned()))?;
        if run.status == RunStatus::RequiresAction
            && run.tool_calls.values().all(|call| call.result.is_some())
        {
            self.append(session_id, SemanticEvent::QueueResumed).await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn finish_run(
        &self,
        session_id: &str,
        run: &RunProjection,
        result: ExecutionResult,
    ) -> Result<(), StoreError> {
        let result = if matches!(result, ExecutionResult::Completed) {
            match self.store.projection(session_id).await {
                Ok(projection) => {
                    let call_ids = projection
                        .runs
                        .get(&run.run_id)
                        .map(|run| {
                            run.tool_calls
                                .values()
                                .filter(|call| call.result.is_none())
                                .map(|call| call.call_id.clone())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    if call_ids.is_empty() {
                        result
                    } else if self
                        .append(
                            session_id,
                            SemanticEvent::RunRequiresAction {
                                run_id: run.run_id.clone(),
                                call_ids,
                            },
                        )
                        .await
                        .is_ok()
                    {
                        match self.evaluate_tool_permissions(session_id, run).await {
                            Ok(()) => ExecutionResult::RequiresAction,
                            Err(error) => ExecutionResult::Failed(error.to_string(), false),
                        }
                    } else {
                        ExecutionResult::Failed("unable to persist required action".into(), false)
                    }
                }
                Err(error) => ExecutionResult::Failed(error.to_string(), false),
            }
        } else {
            result
        };
        match result {
            ExecutionResult::CompletedWithUsage(usage) => {
                let projection = self.store.projection(session_id).await?;
                if let Some(message) =
                    projection.messages.iter().rev().find(|message| {
                        message.role == MessageRole::Assistant && !message.completed
                    })
                {
                    self.append(
                        session_id,
                        SemanticEvent::MessageCompleted {
                            message_id: message.message_id.clone(),
                        },
                    )
                    .await?;
                }
                self.append(
                    session_id,
                    SemanticEvent::RunCompleted {
                        run_id: run.run_id.clone(),
                        usage,
                    },
                )
                .await?;
            }
            ExecutionResult::Cancelled => {
                self.interrupt_message(session_id).await?;
                self.append(
                    session_id,
                    SemanticEvent::RunCancelled {
                        run_id: run.run_id.clone(),
                        reason: Some("cancelled_by_user".into()),
                    },
                )
                .await?;
                self.append(
                    session_id,
                    SemanticEvent::SessionPhaseChanged {
                        from: SessionPhase::Running,
                        to: SessionPhase::Interrupted,
                        reason: Some("run_cancelled".into()),
                    },
                )
                .await?;
            }
            ExecutionResult::Interrupted => {
                self.interrupt_message(session_id).await?;
                self.append(
                    session_id,
                    SemanticEvent::RunInterrupted {
                        run_id: run.run_id.clone(),
                        reason: "server_shutdown".into(),
                    },
                )
                .await?;
                self.append(
                    session_id,
                    SemanticEvent::SessionPhaseChanged {
                        from: SessionPhase::Running,
                        to: SessionPhase::Interrupted,
                        reason: Some("server_shutdown".into()),
                    },
                )
                .await?;
            }
            ExecutionResult::RequiresAction => {
                let projection = self.store.projection(session_id).await?;
                if let Some(message) = projection.messages.iter().rev().find(|message| {
                    message.role == MessageRole::Assistant
                        && !message.completed
                        && !message.interrupted
                }) {
                    self.append(
                        session_id,
                        SemanticEvent::MessageCompleted {
                            message_id: message.message_id.clone(),
                        },
                    )
                    .await?;
                }
                self.append(
                    session_id,
                    SemanticEvent::SessionPhaseChanged {
                        from: SessionPhase::Running,
                        to: SessionPhase::Interrupted,
                        reason: Some("requires_action".into()),
                    },
                )
                .await?;
            }
            ExecutionResult::Failed(error, _retryable) => {
                self.interrupt_message(session_id).await?;
                self.append(
                    session_id,
                    SemanticEvent::RunFailed {
                        run_id: run.run_id.clone(),
                        error,
                    },
                )
                .await?;
                self.append(
                    session_id,
                    SemanticEvent::SessionPhaseChanged {
                        from: SessionPhase::Running,
                        to: SessionPhase::Failed,
                        reason: Some("run_failed".into()),
                    },
                )
                .await?;
            }
            ExecutionResult::Completed => {}
        }
        Ok(())
    }

    async fn interrupt_message(&self, session_id: &str) -> Result<(), StoreError> {
        let projection = self.store.projection(session_id).await?;
        if let Some(message) = projection.messages.iter().rev().find(|message| {
            message.role == MessageRole::Assistant && !message.completed && !message.interrupted
        }) {
            self.append(
                session_id,
                SemanticEvent::MessageInterrupted {
                    message_id: message.message_id.clone(),
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn append(
        &self,
        session_id: &str,
        event: SemanticEvent,
    ) -> Result<RecordedEvent, StoreError> {
        let recorded = self.store.append_event(session_id, event).await?;
        self.hub.publish(recorded.clone()).await;
        Ok(recorded)
    }

    async fn append_many(
        &self,
        session_id: &str,
        events: Vec<SemanticEvent>,
    ) -> Result<Vec<RecordedEvent>, StoreError> {
        let recorded = self.store.append_events(session_id, events).await?;
        for event in &recorded {
            self.hub.publish(event.clone()).await;
        }
        Ok(recorded)
    }
}

async fn dump_request(
    directory: &PathBuf,
    run_id: &str,
    attempt: u8,
    provider: &crate::config::ResolvedProvider,
    body: &Value,
) -> Result<(), std::io::Error> {
    tokio::fs::create_dir_all(directory).await?;
    let body_path = directory.join(format!("{run_id}-{attempt}.json"));
    let metadata_path = directory.join(format!("{run_id}-{attempt}.meta.json"));
    tokio::fs::write(
        body_path,
        serde_json::to_vec_pretty(body).map_err(std::io::Error::other)?,
    )
    .await?;
    let metadata = json!({
        "run_id": run_id,
        "attempt": attempt,
        "provider": provider.name,
        "protocol": match provider.protocol {
            ProviderProtocol::ChatCompletions => "chat_completions",
            ProviderProtocol::Responses => "responses",
        },
    });
    tokio::fs::write(
        metadata_path,
        serde_json::to_vec_pretty(&metadata).map_err(std::io::Error::other)?,
    )
    .await
}

#[derive(Debug)]
enum ExecutionResult {
    Completed,
    CompletedWithUsage(Option<Value>),
    Failed(String, bool),
    Cancelled,
    Interrupted,
    RequiresAction,
}

struct RunExecutionConfig {
    provider: ResolvedProvider,
    body: Value,
}

struct ToolCallBuffer {
    call_id: Option<String>,
    name: Option<String>,
    arguments: String,
}

enum DeltaResult {
    None,
    Usage(Value),
    RequiresAction,
}

fn build_body(
    config: &PiqoConfig,
    protocol: &ProviderProtocol,
    request: &RunRequest,
    projection: &piqo_core::SessionProjection,
    native_tools: &[NativeTool],
) -> Result<Value, StoreError> {
    let layers = config.body_layers(
        &request.model,
        request.agent.as_deref(),
        request.variant.as_deref(),
        request.body.clone(),
    );
    let mut body = merge_request_bodies(layers)
        .map_err(|error| StoreError::InvalidRequest(error.to_string()))?;
    let object = body
        .as_object_mut()
        .ok_or_else(|| StoreError::InvalidRequest("request body must be an object".into()))?;
    object
        .entry("model")
        .or_insert_with(|| Value::String(request.model.clone()));
    object.entry("stream").or_insert(Value::Bool(true));
    if !native_tools.is_empty() {
        let definitions = native_tool_definitions(protocol, native_tools);
        object.entry("tools").or_insert(Value::Array(definitions));
    }
    let mut transcript = projection
        .messages
        .iter()
        .filter(|message| message.completed || message.interrupted)
        .map(|message| {
            let content = if message.blocks.len() == 1 {
                match &message.blocks[0] {
                    ContentBlock::Text(text) => Value::String(text.clone()),
                    ContentBlock::Json(value) => value.clone(),
                }
            } else {
                Value::Array(
                    message
                        .blocks
                        .iter()
                        .map(|block| match block {
                            ContentBlock::Text(text) => json!({"type": "text", "text": text}),
                            ContentBlock::Json(value) => value.clone(),
                        })
                        .collect(),
                )
            };
            json!({"role": role_name(message.role), "content": content})
        })
        .collect::<Vec<_>>();
    if let Some(instructions) = request
        .agent
        .as_deref()
        .map(|name| config.agent(name))
        .transpose()
        .map_err(|error| StoreError::InvalidRequest(error.to_string()))?
        .and_then(|agent| agent.instructions)
    {
        transcript.insert(0, json!({"role": "system", "content": instructions}));
    }
    match protocol {
        ProviderProtocol::ChatCompletions => {
            let completed_messages = projection
                .messages
                .iter()
                .filter(|message| message.completed || message.interrupted)
                .collect::<Vec<_>>();
            let offset = transcript.len().saturating_sub(completed_messages.len());
            for (index, message) in completed_messages.iter().enumerate() {
                if message.role != MessageRole::Assistant {
                    continue;
                }
                let calls = projection.runs.values().flat_map(|run| run.tool_calls.values())
                    .filter(|call| call.assistant_message_id == message.message_id)
                    .map(|call| json!({"id": call.call_id, "type": "function", "function": {"name": call.tool_name, "arguments": call.raw_arguments}}))
                    .collect::<Vec<_>>();
                if !calls.is_empty() {
                    transcript[offset + index]
                        .as_object_mut()
                        .expect("transcript messages are objects")
                        .insert("tool_calls".into(), Value::Array(calls));
                }
            }
            for call in projection
                .runs
                .values()
                .flat_map(|run| run.tool_calls.values())
            {
                if let Some(result) = &call.result {
                    transcript.push(json!({"role": "tool", "tool_call_id": call.call_id, "content": result.to_string()}));
                }
            }
            object.entry("messages").or_insert(Value::Array(transcript));
        }
        ProviderProtocol::Responses => {
            for call in projection
                .runs
                .values()
                .flat_map(|run| run.tool_calls.values())
            {
                transcript.push(json!({"type": "function_call", "call_id": call.call_id, "name": call.tool_name, "arguments": call.raw_arguments}));
                if let Some(result) = &call.result {
                    transcript.push(json!({"type": "function_call_output", "call_id": call.call_id, "output": result}));
                }
            }
            object.entry("input").or_insert(Value::Array(transcript));
        }
    }
    Ok(body)
}

fn configured_native_tools(config: &PiqoConfig, request: &RunRequest) -> Vec<NativeTool> {
    if request.body.get("tools").is_some() {
        return Vec::new();
    }
    let Some(agent_name) = request.agent.as_deref() else {
        return Vec::new();
    };
    let Ok(agent) = config.agent(agent_name) else {
        return Vec::new();
    };
    [
        (NativeTool::Read, agent.permissions.read),
        (NativeTool::Write, agent.permissions.write),
        (NativeTool::Edit, agent.permissions.write),
        (NativeTool::Bash, agent.permissions.bash),
    ]
    .into_iter()
    .filter_map(|(tool, permission)| {
        matches!(
            permission,
            Some(PermissionSetting::Allow | PermissionSetting::Ask)
        )
        .then_some(tool)
    })
    .collect()
}

fn native_tool_definitions(protocol: &ProviderProtocol, tools: &[NativeTool]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            let (description, parameters) = match tool {
                NativeTool::Read => (
                    "Read a UTF-8 regular file inside the project workspace.",
                    json!({"type":"object","additionalProperties":false,"required":["filePath"],"properties":{"filePath":{"type":"string"},"offset":{"type":"integer","minimum":1},"limit":{"type":"integer","minimum":1}}}),
                ),
                NativeTool::Write => (
                    "Create or atomically overwrite a UTF-8 file inside the project workspace.",
                    json!({"type":"object","additionalProperties":false,"required":["filePath","content","mode"],"properties":{"filePath":{"type":"string"},"content":{"type":"string"},"mode":{"type":"string","enum":["create","overwrite"]}}}),
                ),
                NativeTool::Edit => (
                    "Replace exact text in an existing UTF-8 file inside the project workspace.",
                    json!({"type":"object","additionalProperties":false,"required":["filePath","oldString","newString"],"properties":{"filePath":{"type":"string"},"oldString":{"type":"string"},"newString":{"type":"string"},"replaceAll":{"type":"boolean"}}}),
                ),
                NativeTool::Bash => (
                    "Execute one command with a project workspace working directory.",
                    json!({"type":"object","additionalProperties":false,"required":["command"],"properties":{"command":{"type":"string"},"cwd":{"type":"string"}}}),
                ),
            };
            match protocol {
                ProviderProtocol::ChatCompletions => json!({"type":"function","function":{"name":tool.name(),"description":description,"parameters":parameters}}),
                ProviderProtocol::Responses => json!({"type":"function","name":tool.name(),"description":description,"parameters":parameters}),
            }
        })
        .collect()
}

fn role_name(role: MessageRole) -> &'static str {
    match role {
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
        MessageRole::Tool => "tool",
    }
}

fn body_stream_mode(body: &Value) -> bool {
    body.get("stream").and_then(Value::as_bool).unwrap_or(true)
}

fn parse_sse_frame(frame: &str) -> (Option<String>, String) {
    let mut event_type = None;
    let mut data = String::new();
    for line in frame.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            event_type = Some(value.trim().to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(value.trim_start());
        }
    }
    (event_type, data)
}

fn find_sse_frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(lf), Some(crlf)) if crlf < lf => Some((crlf, 4)),
        (Some(lf), _) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PiqoConfig;
    use serde_json::json;
    use tempfile::{tempdir, NamedTempFile};
    use tokio::{net::TcpListener, sync::oneshot};

    async fn send_raw_response(stream: tokio::net::TcpStream, response: &str) {
        let mut remaining = response.as_bytes();
        while !remaining.is_empty() {
            stream.writable().await.expect("provider socket writable");
            match stream.try_write(remaining) {
                Ok(written) => remaining = &remaining[written..],
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("provider response writes: {error}"),
            }
        }
    }

    async fn send_http_response(stream: tokio::net::TcpStream, body: &str, length: usize) {
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {length}\r\nconnection: close\r\n\r\n{body}"
        );
        send_raw_response(stream, &response).await;
    }

    fn completed_sse() -> String {
        concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        )
        .to_owned()
    }

    async fn wait_for_run_status(
        store: &SqliteStore,
        session_id: &str,
        run_id: &str,
        status: RunStatus,
    ) {
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let projection = store
                    .projection(session_id)
                    .await
                    .expect("projection loads");
                if projection.runs[run_id].status == status {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        if result.is_err() {
            let projection = store
                .projection(session_id)
                .await
                .expect("projection loads after timeout");
            panic!(
                "run did not reach {status:?}: {:?}",
                projection.runs[run_id]
            );
        }
    }

    #[tokio::test]
    async fn queued_run_uses_the_latest_config_when_execution_starts() {
        let database = NamedTempFile::new().expect("temporary sqlite file");
        let store = SqliteStore::connect_file(database.path())
            .await
            .expect("store opens");
        let old_provider = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("old provider binds");
        let old_address = old_provider.local_addr().expect("old provider address");
        let new_provider = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("new provider binds");
        let new_address = new_provider.local_addr().expect("new provider address");
        let directory = tempdir().expect("config directory creates");
        let config_path = directory.path().join("piqo.toml");
        let initial: PiqoConfig = toml::from_str(&format!(
            "[providers.local]\nbase_url = \"http://{old_address}\"\n"
        ))
        .expect("initial config parses");
        let manager = ConfigManager::file(&config_path, initial);
        let supervisor = SessionSupervisor::with_dump_dir_and_shutdown(
            store.clone(),
            manager.clone(),
            EventHub::new(),
            None,
            CancellationToken::new(),
        );
        let session = store
            .create_session(None, None)
            .await
            .expect("session creates");
        let session_lock = Arc::new(Mutex::new(()));
        let guard = session_lock.lock().await;
        supervisor
            .locks
            .lock()
            .await
            .insert(session.id.clone(), session_lock.clone());
        let run_id = supervisor
            .queue_run(
                &session.id,
                RunRequest {
                    provider: "local".into(),
                    model: "test-model".into(),
                    input: json!("hello"),
                    agent: None,
                    variant: None,
                    body: json!({}),
                },
            )
            .await
            .expect("run queues");

        std::fs::write(
            &config_path,
            format!("[providers.local]\nbase_url = \"http://{new_address}\"\n"),
        )
        .expect("new config writes");
        manager.reload().await.expect("config reloads");
        let provider_task = tokio::spawn(async move {
            let (stream, _) = new_provider.accept().await.expect("new provider accepts");
            let body = completed_sse();
            send_http_response(stream, &body, body.len()).await;
        });
        drop(guard);

        wait_for_run_status(&store, &session.id, &run_id, RunStatus::Completed).await;
        provider_task.await.expect("new provider task joins");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), old_provider.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn active_run_retries_with_its_original_config_snapshot() {
        let database = NamedTempFile::new().expect("temporary sqlite file");
        let store = SqliteStore::connect_file(database.path())
            .await
            .expect("store opens");
        let old_provider = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("old provider binds");
        let old_address = old_provider.local_addr().expect("old provider address");
        let new_provider = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("new provider binds");
        let new_address = new_provider.local_addr().expect("new provider address");
        let directory = tempdir().expect("config directory creates");
        let config_path = directory.path().join("piqo.toml");
        let initial: PiqoConfig = toml::from_str(&format!(
            "[providers.local]\nbase_url = \"http://{old_address}\"\n"
        ))
        .expect("initial config parses");
        let manager = ConfigManager::file(&config_path, initial);
        let supervisor = SessionSupervisor::with_dump_dir_and_shutdown(
            store.clone(),
            manager.clone(),
            EventHub::new(),
            None,
            CancellationToken::new(),
        );
        let (first_response_sent, first_response_received) = oneshot::channel();
        let provider_task = tokio::spawn(async move {
            let (stream, _) = old_provider.accept().await.expect("first request accepts");
            send_raw_response(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n20\r\ndata: {}\n\n",
            )
            .await;
            first_response_sent
                .send(())
                .expect("test receives first response signal");
            let (stream, _) = old_provider.accept().await.expect("retry accepts");
            let body = completed_sse();
            send_http_response(stream, &body, body.len()).await;
        });
        let session = store
            .create_session(None, None)
            .await
            .expect("session creates");
        let run_id = supervisor
            .queue_run(
                &session.id,
                RunRequest {
                    provider: "local".into(),
                    model: "test-model".into(),
                    input: json!("hello"),
                    agent: None,
                    variant: None,
                    body: json!({}),
                },
            )
            .await
            .expect("run queues");
        first_response_received
            .await
            .expect("first response is sent");
        std::fs::write(
            &config_path,
            format!("[providers.local]\nbase_url = \"http://{new_address}\"\n"),
        )
        .expect("new config writes");
        manager.reload().await.expect("config reloads");

        wait_for_run_status(&store, &session.id, &run_id, RunStatus::Completed).await;
        provider_task.await.expect("old provider task joins");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), new_provider.accept())
                .await
                .is_err()
        );
    }

    #[test]
    fn injects_agent_instructions_without_overriding_a_raw_transcript() {
        let config: PiqoConfig = toml::from_str(
            r#"
                [agents.reviewer]
                instructions = "Review carefully."
            "#,
        )
        .expect("config parses");
        let request = RunRequest {
            provider: "local".to_owned(),
            model: "model".to_owned(),
            input: Value::Null,
            agent: Some("reviewer".to_owned()),
            variant: None,
            body: Value::Null,
        };
        let projection = piqo_core::SessionProjection::new("session");
        let body = build_body(
            &config,
            &ProviderProtocol::ChatCompletions,
            &request,
            &projection,
            &[],
        )
        .expect("body builds");
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][0]["content"], "Review carefully.");

        let request = RunRequest {
            body: json!({"messages": [{"role": "user", "content": "raw"}]}),
            ..request
        };
        let body = build_body(
            &config,
            &ProviderProtocol::ChatCompletions,
            &request,
            &projection,
            &[],
        )
        .expect("body builds");
        assert_eq!(
            body["messages"],
            json!([{"role": "user", "content": "raw"}])
        );
    }

    #[test]
    fn advertises_only_native_tools_enabled_by_the_named_agent() {
        let config: PiqoConfig = toml::from_str(
            r#"
                [agents.worker.permissions]
                read = "allow"
                write = "ask"
                bash = "deny"
            "#,
        )
        .expect("config parses");
        let request = RunRequest {
            provider: "local".to_owned(),
            model: "model".to_owned(),
            input: Value::Null,
            agent: Some("worker".to_owned()),
            variant: None,
            body: json!({}),
        };
        let tools = configured_native_tools(&config, &request);
        assert_eq!(
            tools,
            vec![NativeTool::Read, NativeTool::Write, NativeTool::Edit]
        );
        let body = build_body(
            &config,
            &ProviderProtocol::ChatCompletions,
            &request,
            &piqo_core::SessionProjection::new("session"),
            &tools,
        )
        .expect("body builds");
        assert_eq!(body["tools"].as_array().unwrap().len(), 3);

        let caller_owned = RunRequest {
            body: json!({"tools": []}),
            ..request
        };
        assert!(configured_native_tools(&config, &caller_owned).is_empty());
    }

    #[tokio::test]
    async fn shutdown_interrupts_an_active_provider_run_and_persists_the_reason() {
        let file = NamedTempFile::new().expect("temporary sqlite file");
        let store = SqliteStore::connect_file(file.path())
            .await
            .expect("store opens");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock provider binds");
        let address = listener.local_addr().expect("mock provider address");
        let provider_task = tokio::spawn(async move {
            let (_connection, _) = listener.accept().await.expect("provider accepts");
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let config: PiqoConfig = toml::from_str(&format!(
            "[providers.local]\nbase_url = \"http://{address}\"\n"
        ))
        .expect("provider config parses");
        let hub = EventHub::new();
        let supervisor = SessionSupervisor::with_dump_dir_and_shutdown(
            store.clone(),
            ConfigManager::memory(config),
            hub,
            None,
            CancellationToken::new(),
        );
        let session = store
            .create_session(None, None)
            .await
            .expect("session creates");
        let run_id = supervisor
            .queue_run(
                &session.id,
                RunRequest {
                    provider: "local".into(),
                    model: "test-model".into(),
                    input: json!("hello"),
                    agent: None,
                    variant: None,
                    body: json!({}),
                },
            )
            .await
            .expect("run queues");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let projection = store
                    .projection(&session.id)
                    .await
                    .expect("projection loads");
                if projection
                    .runs
                    .get(&run_id)
                    .is_some_and(|run| run.status == RunStatus::Running)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("run starts");

        supervisor
            .shutdown(Duration::from_secs(2))
            .await
            .expect("shutdown completes");
        let projection = store
            .projection(&session.id)
            .await
            .expect("projection loads");
        assert_eq!(projection.runs[&run_id].status, RunStatus::Interrupted);
        assert_eq!(
            projection.runs[&run_id].error.as_deref(),
            Some("server_shutdown")
        );
        assert!(store
            .events(&session.id, 0, u32::MAX)
            .await
            .expect("events load")
            .iter()
            .any(|event| matches!(
                &event.event,
                SemanticEvent::RunInterrupted { run_id: id, reason }
                    if id == &run_id && reason == "server_shutdown"
            )));
        provider_task.abort();
    }

    #[tokio::test]
    async fn shutdown_interrupts_queued_and_requires_action_runs() {
        let file = NamedTempFile::new().expect("temporary sqlite file");
        let store = SqliteStore::connect_file(file.path())
            .await
            .expect("store opens");
        let session = store
            .create_session(None, None)
            .await
            .expect("session creates");
        store
            .append_events(
                &session.id,
                vec![
                    SemanticEvent::RunQueued {
                        run_id: "queued".into(),
                        retry_of: None,
                        provider: "local".into(),
                        model: "test".into(),
                        request: json!({}),
                    },
                    SemanticEvent::RunQueued {
                        run_id: "requires-action".into(),
                        retry_of: None,
                        provider: "local".into(),
                        model: "test".into(),
                        request: json!({}),
                    },
                    SemanticEvent::RunStarted {
                        run_id: "requires-action".into(),
                        attempt_id: "attempt".into(),
                        attempt: 1,
                    },
                    SemanticEvent::RunRequiresAction {
                        run_id: "requires-action".into(),
                        call_ids: vec!["call".into()],
                    },
                ],
            )
            .await
            .expect("events append");
        let supervisor = SessionSupervisor::with_dump_dir(
            store.clone(),
            ConfigManager::memory(PiqoConfig::default()),
            EventHub::new(),
            None,
        );

        supervisor
            .shutdown(Duration::from_secs(1))
            .await
            .expect("shutdown completes");
        let projection = store
            .projection(&session.id)
            .await
            .expect("projection loads");
        assert_eq!(projection.runs["queued"].status, RunStatus::Interrupted);
        assert_eq!(
            projection.runs["requires-action"].status,
            RunStatus::Interrupted
        );
        assert!(projection
            .runs
            .values()
            .all(|run| run.error.as_deref() == Some("server_shutdown")));
    }

    #[tokio::test]
    async fn deleting_a_project_cancels_active_work_and_closes_its_streams() {
        let file = NamedTempFile::new().expect("temporary sqlite file");
        let store = SqliteStore::connect_file(file.path())
            .await
            .expect("store opens");
        let project = store
            .create_project("demo".into(), "/workspace/demo".into())
            .await
            .expect("project creates");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock provider binds");
        let address = listener.local_addr().expect("mock provider address");
        let provider_task = tokio::spawn(async move {
            let (_connection, _) = listener.accept().await.expect("provider accepts");
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let config: PiqoConfig = toml::from_str(&format!(
            "[providers.local]\nbase_url = \"http://{address}\"\n"
        ))
        .expect("provider config parses");
        let hub = EventHub::new();
        let session = store
            .create_session(None, Some(project.id.clone()))
            .await
            .expect("session creates");
        let mut stream = hub.subscribe(&session.id).await;
        let supervisor = SessionSupervisor::with_dump_dir_and_shutdown(
            store.clone(),
            ConfigManager::memory(config),
            hub,
            None,
            CancellationToken::new(),
        );
        let run_id = supervisor
            .queue_run(
                &session.id,
                RunRequest {
                    provider: "local".into(),
                    model: "test-model".into(),
                    input: json!("hello"),
                    agent: None,
                    variant: None,
                    body: json!({}),
                },
            )
            .await
            .expect("run queues");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let projection = store
                    .projection(&session.id)
                    .await
                    .expect("projection loads");
                if projection
                    .runs
                    .get(&run_id)
                    .is_some_and(|run| run.status == RunStatus::Running)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("run starts");

        supervisor
            .delete_project(&project.id)
            .await
            .expect("project deletes");
        assert!(matches!(
            store.get_session(&session.id).await,
            Err(StoreError::SessionNotFound(_))
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(
                    stream.recv().await,
                    Err(broadcast::error::RecvError::Closed)
                ) {
                    break;
                }
            }
        })
        .await
        .expect("session stream closes");
        provider_task.abort();
    }
}
