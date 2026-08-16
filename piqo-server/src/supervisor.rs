use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use piqo_core::{
    ContentBlock, MessageRole, RecordedEvent, RunProjection, RunStatus, SemanticEvent, SessionPhase,
};
use piqo_provider::{merge_request_bodies, ProviderDelta, ProviderProtocol, ProviderTransport};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{config::PiqoConfig, storage::StoreError, SqliteStore};

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
    config: Arc<PiqoConfig>,
    transport: ProviderTransport,
    hub: EventHub,
    locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    cancellations: Arc<Mutex<HashMap<String, CancellationToken>>>,
    dump_dir: Option<PathBuf>,
}

impl SessionSupervisor {
    pub(crate) fn with_dump_dir(
        store: SqliteStore,
        config: Arc<PiqoConfig>,
        hub: EventHub,
        dump_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            store,
            config,
            transport: ProviderTransport::new(),
            hub,
            locks: Arc::new(Mutex::new(HashMap::new())),
            cancellations: Arc::new(Mutex::new(HashMap::new())),
            dump_dir,
        }
    }

    pub async fn queue_run(
        &self,
        session_id: &str,
        request: RunRequest,
    ) -> Result<String, StoreError> {
        self.store.get_session(session_id).await?;
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
        self.spawn_worker(session_id.to_owned());
        Ok(run_id)
    }

    pub async fn resume(&self, session_id: &str) -> Result<(), StoreError> {
        let projection = self.store.projection(session_id).await?;
        if !projection.queue_paused {
            return Err(StoreError::QueueNotPaused);
        }
        self.append(session_id, SemanticEvent::QueueResumed).await?;
        self.spawn_worker(session_id.to_owned());
        Ok(())
    }

    pub async fn cancel(&self, session_id: &str, run_id: &str) -> Result<(), StoreError> {
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
        self.spawn_worker(session_id.to_owned());
        Ok(new_id)
    }

    fn spawn_worker(&self, session_id: String) {
        let this = self.clone();
        tokio::spawn(async move {
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
        let _guard = lock.lock().await;
        loop {
            let projection = self.store.projection(session_id).await?;
            if projection.queue_paused || projection.active_run().is_some() {
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
            let result = self.execute_run(session_id, &run, token, None, 0).await;
            self.cancellations.lock().await.remove(&run.run_id);
            self.finish_run(session_id, &run, result).await?;
        }
    }

    async fn execute_run(
        &self,
        session_id: &str,
        run: &RunProjection,
        cancellation: CancellationToken,
        existing_assistant_message: Option<String>,
        retry_count: u8,
    ) -> ExecutionResult {
        let request: RunRequest = match serde_json::from_value(run.request.clone()) {
            Ok(request) => request,
            Err(error) => return ExecutionResult::Failed(error.to_string(), false),
        };
        let provider = match self.config.resolve_provider(&request.provider) {
            Ok(provider) => provider,
            Err(error) => return ExecutionResult::Failed(error.to_string(), false),
        };
        let attempt_id = if retry_count == 0 {
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
        let projection = match self.store.projection(session_id).await {
            Ok(projection) => projection,
            Err(error) => return ExecutionResult::Failed(error.to_string(), false),
        };
        let body = match build_body(&self.config, &provider.protocol, &request, &projection) {
            Ok(body) => body,
            Err(error) => return ExecutionResult::Failed(error.to_string(), false),
        };
        let is_existing_assistant = existing_assistant_message.is_some();
        let assistant_message_id =
            existing_assistant_message.unwrap_or_else(|| Uuid::now_v7().to_string());
        if run.retry_of.is_none() || !is_existing_assistant {
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
                    &provider,
                    &body,
                )
                .await
                {
                    tracing::warn!(run_id = %run.run_id, %error, "unable to dump provider request");
                }
            }
            let result = tokio::select! {
                _ = cancellation.cancelled() => return ExecutionResult::Cancelled,
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
                    tokio::time::sleep(
                        std::time::Duration::from_millis(
                            250u64.saturating_mul(1u64 << (retries - 1)),
                        )
                        .min(std::time::Duration::from_secs(4)),
                    )
                    .await;
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
        let stream_mode = body_stream_mode(&body);
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
                                    call_id,
                                    name,
                                    arguments,
                                } = &delta
                                {
                                    let key = call_id.clone().unwrap_or_else(|| {
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
            match response.text().await {
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
                tokio::time::sleep(
                    Duration::from_millis(250u64.saturating_mul(1u64 << retry_count))
                        .min(Duration::from_secs(4)),
                )
                .await;
                return Box::pin(self.execute_run(
                    session_id,
                    run,
                    cancellation,
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
                self.append(
                    session_id,
                    SemanticEvent::ToolCallEmitted {
                        call_id: call_id.clone(),
                        agent_id: "assistant".into(),
                        tool_name: name.unwrap_or_else(|| "function".into()),
                        arguments: serde_json::from_str(&arguments)
                            .unwrap_or_else(|_| json!({"raw": arguments})),
                    },
                )
                .await?;
                self.append(
                    session_id,
                    SemanticEvent::RunRequiresAction {
                        run_id: run_id.to_owned(),
                        call_ids: vec![call_id],
                    },
                )
                .await?;
                Ok(DeltaResult::RequiresAction)
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

    async fn finish_run(
        &self,
        session_id: &str,
        run: &RunProjection,
        result: ExecutionResult,
    ) -> Result<(), StoreError> {
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
            ExecutionResult::RequiresAction => {
                self.interrupt_message(session_id).await?;
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
    RequiresAction,
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
) -> Result<Value, StoreError> {
    let mut body = merge_request_bodies(config.body_layers(
        &request.model,
        request.agent.as_deref(),
        request.variant.as_deref(),
        request.body.clone(),
    ))
    .map_err(|error| StoreError::InvalidRequest(error.to_string()))?;
    let object = body
        .as_object_mut()
        .ok_or_else(|| StoreError::InvalidRequest("request body must be an object".into()))?;
    object
        .entry("model")
        .or_insert_with(|| Value::String(request.model.clone()));
    object.entry("stream").or_insert(Value::Bool(true));
    let transcript = projection
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
    match protocol {
        ProviderProtocol::ChatCompletions => {
            object.entry("messages").or_insert(Value::Array(transcript));
        }
        ProviderProtocol::Responses => {
            object.entry("input").or_insert(Value::Array(transcript));
        }
    }
    Ok(body)
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
