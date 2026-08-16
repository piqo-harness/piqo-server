//! HTTP/SSE API, session supervision, and durable SQLite storage.

mod config;
mod storage;
mod supervisor;

use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use async_stream::stream;
use axum::{
    extract::{
        FromRequest, FromRequestParts, Json as AxumJson, Path as AxumPath, Query as AxumQuery,
        Request, State,
    },
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Json,
    },
    routing::{get, post},
    Router,
};
#[cfg(test)]
use piqo_core::SemanticEvent;
use piqo_core::{EventId, RecordedEvent, RunProjection, RunStatus, SessionPhase};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::broadcast;
use tower_http::trace::TraceLayer;
use utoipa::{OpenApi, ToSchema};

pub use config::{ConfigError, PiqoConfig, ProviderCatalogEntry, ProviderConfig};
pub use storage::{SessionSummary, SqliteStore, StoreError, EVENT_SCHEMA_VERSION};
use supervisor::EventHub;
pub use supervisor::{RunRequest, SessionSupervisor};

#[derive(Debug, Error)]
#[error("piqo refuses non-loopback bind address {0} until authentication is configured")]
pub struct BindAddressError(pub SocketAddr);

pub fn validate_bind_address(bind: SocketAddr) -> Result<(), BindAddressError> {
    if bind.ip().is_loopback() {
        Ok(())
    } else {
        Err(BindAddressError(bind))
    }
}

#[derive(Clone)]
pub struct AppState {
    store: SqliteStore,
    hub: EventHub,
    supervisor: SessionSupervisor,
    config: Arc<PiqoConfig>,
}

impl AppState {
    pub fn new(store: SqliteStore) -> Self {
        Self::with_config(store, Arc::new(PiqoConfig::default()))
    }

    pub fn with_config(store: SqliteStore, config: Arc<PiqoConfig>) -> Self {
        Self::with_config_and_dump(store, config, None)
    }

    pub fn with_config_and_dump(
        store: SqliteStore,
        config: Arc<PiqoConfig>,
        dump_dir: Option<std::path::PathBuf>,
    ) -> Self {
        let hub = EventHub::new();
        let supervisor =
            SessionSupervisor::with_dump_dir(store.clone(), config.clone(), hub.clone(), dump_dir);
        Self {
            store,
            hub,
            supervisor,
            config,
        }
    }

    pub fn store(&self) -> &SqliteStore {
        &self.store
    }

    pub fn supervisor(&self) -> &SessionSupervisor {
        &self.supervisor
    }

    pub fn config(&self) -> &PiqoConfig {
        &self.config
    }

    #[cfg(test)]
    pub(crate) async fn append_event(
        &self,
        session_id: &str,
        event: SemanticEvent,
    ) -> Result<RecordedEvent, StoreError> {
        let recorded = self.store.append_event(session_id, event).await?;
        self.hub.publish(recorded.clone()).await;
        Ok(recorded)
    }

    async fn subscribe(&self, session_id: &str) -> tokio::sync::broadcast::Receiver<RecordedEvent> {
        self.hub.subscribe(session_id).await
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateSessionRequest {
    pub title: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SessionListResponse {
    pub sessions: Vec<ApiSessionSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApiSessionSummary {
    pub id: String,
    pub title: Option<String>,
    pub parent_session_id: Option<String>,
    pub forked_at_event_id: Option<EventId>,
    pub created_at: String,
    pub updated_at: String,
    pub phase: String,
    pub revision: u64,
    pub last_event_id: EventId,
    pub projection: Value,
}

impl From<SessionSummary> for ApiSessionSummary {
    fn from(summary: SessionSummary) -> Self {
        Self {
            id: summary.id,
            title: summary.title,
            parent_session_id: summary.parent_session_id,
            forked_at_event_id: summary.forked_at_event_id,
            created_at: summary.created_at,
            updated_at: summary.updated_at,
            phase: session_phase_name(summary.phase).to_owned(),
            revision: summary.revision,
            last_event_id: summary.last_event_id,
            projection: Value::Null,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ForkSessionRequest {
    pub at_event_id: EventId,
    pub title: Option<String>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CreateRunRequest {
    pub provider: String,
    pub model: String,
    pub input: Value,
    pub agent: Option<String>,
    pub variant: Option<String>,
    #[serde(default)]
    pub body: Value,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct RunAcceptedResponse {
    pub session_id: String,
    pub run_id: String,
    pub status: &'static str,
    pub events_url: String,
    pub stream_url: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct RunResponse {
    pub session_id: String,
    pub run: ApiRun,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ApiRun {
    pub run_id: String,
    pub retry_of: Option<String>,
    pub provider: String,
    pub model: String,
    pub request: Value,
    pub status: String,
    pub attempt_id: Option<String>,
    pub attempts: u32,
    pub error: Option<String>,
}

impl From<&RunProjection> for ApiRun {
    fn from(run: &RunProjection) -> Self {
        Self {
            run_id: run.run_id.clone(),
            retry_of: run.retry_of.clone(),
            provider: run.provider.clone(),
            model: run.model.clone(),
            request: run.request.clone(),
            status: run_status_name(run.status).to_owned(),
            attempt_id: run.attempt_id.clone(),
            attempts: run.attempts,
            error: run.error.clone(),
        }
    }
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ProviderCatalogResponse {
    pub providers: Vec<ProviderCatalogEntry>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiEvent {
    pub id: EventId,
    pub session_id: String,
    pub schema_version: u16,
    pub occurred_at: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: Value,
}

impl TryFrom<RecordedEvent> for ApiEvent {
    type Error = StoreError;

    fn try_from(event: RecordedEvent) -> Result<Self, Self::Error> {
        let value = serde_json::to_value(&event.event)?;
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| StoreError::CorruptSession {
                session_id: event.session_id.clone(),
                reason: "event has no type".to_owned(),
            })?
            .to_owned();
        Ok(Self {
            id: event.id,
            session_id: event.session_id,
            schema_version: event.schema_version,
            occurred_at: event.occurred_at,
            event_type,
            data: event
                .raw_data
                .unwrap_or_else(|| value.get("data").cloned().unwrap_or(Value::Null)),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    after: Option<EventId>,
    limit: Option<u32>,
}

#[derive(Debug)]
pub enum ApiError {
    Store(StoreError),
    BadRequest { code: &'static str, message: String },
}

struct ApiJson<T>(T);

struct ApiPath<T>(T);
struct ApiQuery<T>(T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        AxumJson::<T>::from_request(req, state)
            .await
            .map(|AxumJson(value)| Self(value))
            .map_err(|error| ApiError::BadRequest {
                code: "invalid_request",
                message: error.to_string(),
            })
    }
}

impl<S, T> FromRequestParts<S> for ApiPath<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        AxumPath::<T>::from_request_parts(parts, state)
            .await
            .map(|AxumPath(value)| Self(value))
            .map_err(|error| ApiError::BadRequest {
                code: "invalid_request",
                message: error.to_string(),
            })
    }
}

impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        AxumQuery::<T>::from_request_parts(parts, state)
            .await
            .map(|AxumQuery(value)| Self(value))
            .map_err(|error| ApiError::BadRequest {
                code: "invalid_request",
                message: error.to_string(),
            })
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, code, message) = match self {
            Self::BadRequest { code, message } => (StatusCode::BAD_REQUEST, code, message),
            Self::Store(StoreError::SessionNotFound(id)) => (
                StatusCode::NOT_FOUND,
                "session_not_found",
                format!("session {id} was not found"),
            ),
            Self::Store(StoreError::EventNotFound {
                session_id,
                event_id,
            }) => (
                StatusCode::NOT_FOUND,
                "event_not_found",
                format!("event {event_id} was not found in session {session_id}"),
            ),
            Self::Store(StoreError::InvalidCursor) => (
                StatusCode::BAD_REQUEST,
                "invalid_cursor",
                "invalid pagination cursor".to_owned(),
            ),
            Self::Store(StoreError::InvalidTransition(error)) => (
                StatusCode::CONFLICT,
                "invalid_transition",
                error.to_string(),
            ),
            Self::Store(StoreError::InvalidRequest(message)) => {
                (StatusCode::BAD_REQUEST, "invalid_request", message)
            }
            Self::Store(StoreError::Conflict(message)) => {
                (StatusCode::CONFLICT, "conflict", message)
            }
            Self::Store(StoreError::QueuePaused) => (
                StatusCode::CONFLICT,
                "queue_paused",
                "session queue is paused".to_owned(),
            ),
            Self::Store(StoreError::QueueNotPaused) => (
                StatusCode::CONFLICT,
                "conflict",
                "session queue is not paused".to_owned(),
            ),
            Self::Store(StoreError::RunNotFound(id)) => (
                StatusCode::NOT_FOUND,
                "run_not_found",
                format!("run {id} was not found"),
            ),
            Self::Store(StoreError::ProviderNotFound(name)) => (
                StatusCode::NOT_FOUND,
                "provider_not_found",
                format!("provider {name} was not found"),
            ),
            Self::Store(StoreError::ProviderUnavailable(message)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "provider_unavailable",
                message,
            ),
            Self::Store(StoreError::ProviderProtocolError(message)) => {
                (StatusCode::BAD_GATEWAY, "provider_protocol_error", message)
            }
            Self::Store(_error) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "storage_unavailable",
                "storage is temporarily unavailable".to_owned(),
            ),
        };
        (
            status,
            Json(ErrorResponse {
                error: ErrorBody {
                    code: code.to_owned(),
                    message,
                },
            }),
        )
            .into_response()
    }
}

/// Build the complete versioned HTTP surface around a durable store.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/openapi.json", get(openapi))
        .route("/api/v1/sessions", post(create_session).get(list_sessions))
        .route("/api/v1/sessions/{session_id}", get(get_session))
        .route("/api/v1/sessions/{session_id}/events", get(get_events))
        .route(
            "/api/v1/sessions/{session_id}/events/stream",
            get(stream_events),
        )
        .route("/api/v1/sessions/{session_id}/forks", post(fork_session))
        .route("/api/v1/providers", get(list_providers))
        .route("/api/v1/sessions/{session_id}/runs", post(create_run))
        .route("/api/v1/sessions/{session_id}/runs/{run_id}", get(get_run))
        .route(
            "/api/v1/sessions/{session_id}/runs/{run_id}/cancel",
            post(cancel_run),
        )
        .route(
            "/api/v1/sessions/{session_id}/runs/{run_id}/retries",
            post(retry_run),
        )
        .route(
            "/api/v1/sessions/{session_id}/queue/resume",
            post(resume_queue),
        )
        .layer(TraceLayer::new_for_http())
        .method_not_allowed_fallback(api_method_not_allowed)
        .fallback(api_not_found)
        .with_state(state)
}

async fn api_not_found() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: ErrorBody {
                code: "not_found".to_owned(),
                message: "route not found".to_owned(),
            },
        }),
    )
}

async fn api_method_not_allowed() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(ErrorResponse {
            error: ErrorBody {
                code: "invalid_request".to_owned(),
                message: "method not allowed".to_owned(),
            },
        }),
    )
}

#[utoipa::path(get, path = "/api/v1/health", responses((status = 200, body = HealthResponse)))]
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

#[utoipa::path(
    post,
    path = "/api/v1/sessions",
    request_body = CreateSessionRequest,
    responses((status = 201, body = ApiSessionSummary), (status = 503, body = ErrorResponse))
)]
async fn create_session(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<CreateSessionRequest>,
) -> Result<(StatusCode, Json<ApiSessionSummary>), ApiError> {
    Ok((
        StatusCode::CREATED,
        Json(ApiSessionSummary::from(
            state.store.create_session(request.title).await?,
        )),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/sessions",
    params(("cursor" = Option<String>, Query), ("limit" = Option<u32>, Query)),
    responses((status = 200, body = SessionListResponse), (status = 400, body = ErrorResponse))
)]
async fn list_sessions(
    State(state): State<AppState>,
    ApiQuery(query): ApiQuery<ListQuery>,
) -> Result<Json<SessionListResponse>, ApiError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let (sessions, next_cursor) = state
        .store
        .list_sessions(query.cursor.as_deref(), limit)
        .await?;
    Ok(Json(SessionListResponse {
        sessions: sessions.into_iter().map(ApiSessionSummary::from).collect(),
        next_cursor,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/sessions/{session_id}",
    params(("session_id" = String, Path)),
    responses((status = 200, body = ApiSessionSummary), (status = 404, body = ErrorResponse))
)]
async fn get_session(
    State(state): State<AppState>,
    ApiPath(session_id): ApiPath<String>,
) -> Result<Json<ApiSessionSummary>, ApiError> {
    let summary = state.store.get_session(&session_id).await?;
    let projection = state.store.projection(&session_id).await?;
    let mut response = ApiSessionSummary::from(summary);
    response.projection =
        serde_json::to_value(projection).map_err(|error| ApiError::BadRequest {
            code: "invalid_request",
            message: error.to_string(),
        })?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/api/v1/sessions/{session_id}/events",
    params(("session_id" = String, Path), ("after" = Option<u64>, Query), ("limit" = Option<u32>, Query)),
    responses((status = 200, body = [ApiEvent]), (status = 404, body = ErrorResponse))
)]
async fn get_events(
    State(state): State<AppState>,
    ApiPath(session_id): ApiPath<String>,
    ApiQuery(query): ApiQuery<EventsQuery>,
) -> Result<Json<Vec<ApiEvent>>, ApiError> {
    let events = state
        .store
        .events(
            &session_id,
            query.after.unwrap_or_default(),
            query.limit.unwrap_or(200),
        )
        .await?;
    let events = events
        .into_iter()
        .map(ApiEvent::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(events))
}

#[utoipa::path(
    get,
    path = "/api/v1/sessions/{session_id}/events/stream",
    params(("session_id" = String, Path), ("Last-Event-ID" = Option<String>, Header)),
    responses((status = 200, description = "Server-sent event stream"), (status = 404, body = ErrorResponse))
)]
async fn stream_events(
    State(state): State<AppState>,
    ApiPath(session_id): ApiPath<String>,
    headers: HeaderMap,
) -> Result<Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    state.store.get_session(&session_id).await?;
    let after = parse_last_event_id(&headers)?;
    let mut receiver = state.subscribe(&session_id).await;
    let replay = state.store.events(&session_id, after, u32::MAX).await?;
    let stream = stream! {
        let mut last_id = after;
        for recorded in replay {
            if recorded.id > last_id {
                last_id = recorded.id;
                yield Ok(event_for_sse(recorded));
            }
        }
        loop {
            match receiver.recv().await {
                Ok(recorded) if recorded.id > last_id => {
                    last_id = recorded.id;
                    yield Ok(event_for_sse(recorded));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    match state.store.events(&session_id, last_id, u32::MAX).await {
                        Ok(events) => {
                            for recorded in events {
                                if recorded.id > last_id {
                                    last_id = recorded.id;
                                    yield Ok(event_for_sse(recorded));
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
        state.hub.remove_if_unused(&session_id).await;
    };
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/sessions/{session_id}/forks",
    params(("session_id" = String, Path)),
    request_body = ForkSessionRequest,
    responses((status = 201, body = ApiSessionSummary), (status = 404, body = ErrorResponse))
)]
async fn fork_session(
    State(state): State<AppState>,
    ApiPath(session_id): ApiPath<String>,
    ApiJson(request): ApiJson<ForkSessionRequest>,
) -> Result<(StatusCode, Json<ApiSessionSummary>), ApiError> {
    Ok((
        StatusCode::CREATED,
        Json(ApiSessionSummary::from(
            state
                .store
                .fork_session(&session_id, request.at_event_id, request.title)
                .await?,
        )),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/providers",
    responses((status = 200, body = ProviderCatalogResponse))
)]
async fn list_providers(State(state): State<AppState>) -> Json<ProviderCatalogResponse> {
    Json(ProviderCatalogResponse {
        providers: state.config().catalog(),
    })
}

#[utoipa::path(
    post,
    path = "/api/v1/sessions/{session_id}/runs",
    params(("session_id" = String, Path)),
    request_body = CreateRunRequest,
    responses((status = 202, body = RunAcceptedResponse), (status = 404, body = ErrorResponse))
)]
async fn create_run(
    State(state): State<AppState>,
    ApiPath(session_id): ApiPath<String>,
    ApiJson(request): ApiJson<CreateRunRequest>,
) -> Result<(StatusCode, Json<RunAcceptedResponse>), ApiError> {
    let run_request = RunRequest {
        provider: request.provider,
        model: request.model,
        input: request.input,
        agent: request.agent,
        variant: request.variant,
        body: request.body,
    };
    let run_id = state
        .supervisor()
        .queue_run(&session_id, run_request)
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(RunAcceptedResponse {
            session_id: session_id.clone(),
            run_id,
            status: "queued",
            events_url: format!("/api/v1/sessions/{session_id}/events"),
            stream_url: format!("/api/v1/sessions/{session_id}/events/stream"),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/sessions/{session_id}/runs/{run_id}",
    params(("session_id" = String, Path), ("run_id" = String, Path)),
    responses((status = 200, body = RunResponse), (status = 404, body = ErrorResponse))
)]
async fn get_run(
    State(state): State<AppState>,
    ApiPath((session_id, run_id)): ApiPath<(String, String)>,
) -> Result<Json<RunResponse>, ApiError> {
    let projection = state.store().projection(&session_id).await?;
    let run = projection
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::Store(StoreError::RunNotFound(run_id.clone())))?;
    Ok(Json(RunResponse {
        session_id,
        run: ApiRun::from(run),
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/sessions/{session_id}/runs/{run_id}/cancel",
    params(("session_id" = String, Path), ("run_id" = String, Path)),
    responses((status = 202), (status = 404, body = ErrorResponse))
)]
async fn cancel_run(
    State(state): State<AppState>,
    ApiPath((session_id, run_id)): ApiPath<(String, String)>,
) -> Result<StatusCode, ApiError> {
    state.supervisor().cancel(&session_id, &run_id).await?;
    Ok(StatusCode::ACCEPTED)
}

#[utoipa::path(
    post,
    path = "/api/v1/sessions/{session_id}/runs/{run_id}/retries",
    params(("session_id" = String, Path), ("run_id" = String, Path)),
    responses((status = 202, body = RunAcceptedResponse), (status = 409, body = ErrorResponse))
)]
async fn retry_run(
    State(state): State<AppState>,
    ApiPath((session_id, run_id)): ApiPath<(String, String)>,
) -> Result<(StatusCode, Json<RunAcceptedResponse>), ApiError> {
    let new_id = state.supervisor().retry(&session_id, &run_id).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(RunAcceptedResponse {
            session_id: session_id.clone(),
            run_id: new_id,
            status: "queued",
            events_url: format!("/api/v1/sessions/{session_id}/events"),
            stream_url: format!("/api/v1/sessions/{session_id}/events/stream"),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/sessions/{session_id}/queue/resume",
    params(("session_id" = String, Path)),
    responses((status = 202), (status = 409, body = ErrorResponse))
)]
async fn resume_queue(
    State(state): State<AppState>,
    ApiPath(session_id): ApiPath<String>,
) -> Result<StatusCode, ApiError> {
    state.supervisor().resume(&session_id).await?;
    Ok(StatusCode::ACCEPTED)
}

fn parse_last_event_id(headers: &HeaderMap) -> Result<EventId, ApiError> {
    let Some(value) = headers.get("Last-Event-ID") else {
        return Ok(0);
    };
    let value = value.to_str().map_err(|_| ApiError::BadRequest {
        code: "invalid_cursor",
        message: "Last-Event-ID is not valid UTF-8".to_owned(),
    })?;
    value.parse::<EventId>().map_err(|_| ApiError::BadRequest {
        code: "invalid_cursor",
        message: "Last-Event-ID must be an integer".to_owned(),
    })
}

fn session_phase_name(phase: SessionPhase) -> &'static str {
    match phase {
        SessionPhase::Created => "created",
        SessionPhase::Running => "running",
        SessionPhase::Interrupted => "interrupted",
        SessionPhase::Finished => "finished",
        SessionPhase::Failed => "failed",
    }
}

fn run_status_name(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "queued",
        RunStatus::Running => "running",
        RunStatus::RequiresAction => "requires_action",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
        RunStatus::Interrupted => "interrupted",
    }
}

fn event_for_sse(event: RecordedEvent) -> Event {
    match ApiEvent::try_from(event) {
        Ok(api_event) => {
            let event_name = api_event.event_type.clone();
            match serde_json::to_string(&api_event) {
                Ok(data) => Event::default()
                    .id(api_event.id.to_string())
                    .event(event_name)
                    .data(data),
                Err(error) => Event::default()
                    .event("error")
                    .data(format!("{{\"error\":\"{error}\"}}")),
            }
        }
        Err(error) => Event::default().event("error").data(error.to_string()),
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        health,
        create_session,
        list_sessions,
        get_session,
        get_events,
        stream_events,
        fork_session,
        list_providers,
        create_run,
        get_run,
        cancel_run,
        retry_run,
        resume_queue
    ),
    components(schemas(
        HealthResponse,
        CreateSessionRequest,
        ApiSessionSummary,
        SessionListResponse,
        ForkSessionRequest,
        ApiEvent,
        ErrorResponse,
        ErrorBody,
        CreateRunRequest,
        RunAcceptedResponse,
        RunResponse,
        ApiRun,
        ProviderCatalogResponse
    )),
    tags((name = "sessions", description = "Durable session and event-log API"))
)]
pub struct ApiDoc;

async fn openapi() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use piqo_core::SemanticEvent;
    use tempfile::NamedTempFile;
    use tower::ServiceExt;

    async fn app() -> (Router, AppState, NamedTempFile) {
        let file = NamedTempFile::new().expect("temporary sqlite file");
        let store = SqliteStore::connect_file(file.path())
            .await
            .expect("store opens");
        let state = AppState::new(store);
        (router(state.clone()), state, file)
    }

    #[tokio::test]
    async fn creates_and_reads_a_session_over_http() {
        let (app, _state, _file) = app().await;
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"title":"demo"}"#))
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body reads")
            .to_bytes();
        let session: ApiSessionSummary = serde_json::from_slice(&body).expect("response decodes");
        assert_eq!(session.title.as_deref(), Some("demo"));
    }

    #[tokio::test]
    async fn returns_openapi_document() {
        let (app, _state, _file) = app().await;
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/openapi.json")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body reads")
            .to_bytes();
        let document: Value = serde_json::from_slice(&body).expect("openapi decodes");
        assert!(document["paths"]["/api/v1/sessions"].is_object());
    }

    #[tokio::test]
    async fn replays_events_after_last_event_id_over_sse() {
        let (app, state, _file) = app().await;
        let session = state
            .store
            .create_session(None)
            .await
            .expect("session creates");
        state
            .append_event(
                &session.id,
                SemanticEvent::MessageStarted {
                    message_id: "m1".into(),
                    agent_id: "agent".into(),
                    role: piqo_core::MessageRole::User,
                    author: piqo_core::MessageAuthor::User,
                },
            )
            .await
            .expect("event appends");
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/v1/sessions/{}/events/stream", session.id))
                    .header("Last-Event-ID", "1")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body();
        let frame = body
            .frame()
            .await
            .expect("first SSE frame")
            .expect("frame exists");
        let data = frame.into_data().expect("SSE frame is data");
        let text = String::from_utf8(data.to_vec()).expect("SSE data is UTF-8");
        assert!(text.contains("id: 2"));
        assert!(text.contains("message_started"));
    }

    #[tokio::test]
    async fn streams_an_event_appended_after_connection() {
        let (app, state, _file) = app().await;
        let session = state
            .store
            .create_session(None)
            .await
            .expect("session creates");
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/v1/sessions/{}/events/stream", session.id))
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        let mut body = response.into_body();
        state
            .append_event(
                &session.id,
                SemanticEvent::MessageStarted {
                    message_id: "live-message".into(),
                    agent_id: String::new(),
                    role: piqo_core::MessageRole::User,
                    author: piqo_core::MessageAuthor::User,
                },
            )
            .await
            .expect("live event appends");
        let mut saw_live = false;
        for _ in 0..3 {
            let frame = body
                .frame()
                .await
                .expect("live SSE frame")
                .expect("frame exists");
            let data = frame.into_data().expect("SSE frame is data");
            let text = String::from_utf8(data.to_vec()).expect("SSE data is UTF-8");
            if text.contains("id: 2") && text.contains("live-message") {
                saw_live = true;
                break;
            }
        }
        assert!(saw_live, "live event was not received");
    }

    #[tokio::test]
    async fn returns_a_stable_error_for_unknown_sessions() {
        let (app, _state, _file) = app().await;
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/sessions/missing")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body reads")
            .to_bytes();
        let error: ErrorResponse = serde_json::from_slice(&body).expect("error decodes");
        assert_eq!(error.error.code, "session_not_found");
    }

    #[tokio::test]
    async fn rejects_malformed_json_with_the_common_error_envelope() {
        let (app, _state, _file) = app().await;
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from("not-json"))
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body reads")
            .to_bytes();
        let error: ErrorResponse = serde_json::from_slice(&body).expect("error decodes");
        assert_eq!(error.error.code, "invalid_request");
    }

    #[test]
    fn only_loopback_addresses_are_allowed() {
        assert!(validate_bind_address("127.0.0.1:8080".parse().expect("address parses")).is_ok());
        assert!(validate_bind_address("[::1]:8080".parse().expect("address parses")).is_ok());
        assert!(validate_bind_address("0.0.0.0:8080".parse().expect("address parses")).is_err());
    }
}
