//! HTTP/SSE API, session supervision, and durable SQLite storage.

mod config;
mod runtime;
mod storage;
mod supervisor;

use std::{
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_stream::stream;
use axum::{
    extract::{
        FromRequest, FromRequestParts, Json as AxumJson, Path as AxumPath, Query as AxumQuery,
        Request, State,
    },
    http::{HeaderMap, StatusCode},
    middleware,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Json, Response,
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
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi, ToSchema};

pub use config::{
    ConfigError, ConfigManager, ConfigSnapshot, CreateProviderRequest, DiscoveryStatus,
    ModelDiscovery, ModelSource, PiqoConfig, ProviderCatalogEntry, ProviderConfig,
    ProviderCredentialInput, ProviderCredentialSummary, ProviderModelsResponse,
    ReplaceProviderModelsRequest, UpdateProviderRequest,
};
pub use runtime::{
    ensure_private_directory, prepare_server, PreparedServer, ServerError, ServerOptions,
};
pub use storage::{Project, SessionSummary, SqliteStore, StoreError, EVENT_SCHEMA_VERSION};
use supervisor::EventHub;
pub use supervisor::{RunRequest, SessionSupervisor};

pub const API_VERSION: &str = "v1";
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

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
    config: ConfigManager,
    lifecycle: Arc<LifecycleState>,
    shutdown: CancellationToken,
    fatal_reload_error: Arc<Mutex<Option<String>>>,
}

#[derive(Debug, Default)]
pub(crate) struct LifecycleState {
    shutting_down: AtomicBool,
    streams_closed: CancellationToken,
}

impl LifecycleState {
    fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
    }

    fn close_streams(&self) {
        self.streams_closed.cancel();
    }

    fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }
}

impl AppState {
    pub fn new(store: SqliteStore) -> Self {
        Self::with_config(store, Arc::new(PiqoConfig::default()))
    }

    pub fn with_config(store: SqliteStore, config: Arc<PiqoConfig>) -> Self {
        Self::with_config_manager_and_dump(store, ConfigManager::memory((*config).clone()), None)
    }

    pub fn with_config_file(
        store: SqliteStore,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, ConfigError> {
        Ok(Self::with_config_manager_and_dump(
            store,
            ConfigManager::load(path)?,
            None,
        ))
    }

    pub fn with_config_and_dump(
        store: SqliteStore,
        config: Arc<PiqoConfig>,
        dump_dir: Option<std::path::PathBuf>,
    ) -> Self {
        Self::with_config_manager_and_dump(
            store,
            ConfigManager::memory((*config).clone()),
            dump_dir,
        )
    }

    pub fn with_config_path_and_dump(
        store: SqliteStore,
        config: Arc<PiqoConfig>,
        config_path: PathBuf,
        dump_dir: Option<std::path::PathBuf>,
    ) -> Self {
        Self::with_config_manager_and_dump(
            store,
            ConfigManager::file(config_path, (*config).clone()),
            dump_dir,
        )
    }

    fn with_config_manager_and_dump(
        store: SqliteStore,
        config: ConfigManager,
        dump_dir: Option<std::path::PathBuf>,
    ) -> Self {
        let shutdown = CancellationToken::new();
        Self::with_config_manager_and_shutdown(store, config, dump_dir, shutdown)
    }

    fn with_config_manager_and_shutdown(
        store: SqliteStore,
        config: ConfigManager,
        dump_dir: Option<std::path::PathBuf>,
        shutdown: CancellationToken,
    ) -> Self {
        let hub = EventHub::new();
        let supervisor = SessionSupervisor::with_dump_dir_and_shutdown(
            store.clone(),
            config.clone(),
            hub.clone(),
            dump_dir,
            shutdown.clone(),
        );
        Self {
            store,
            hub,
            supervisor,
            config,
            lifecycle: Arc::new(LifecycleState::default()),
            shutdown,
            fatal_reload_error: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn with_config_and_dump_and_shutdown(
        store: SqliteStore,
        config: ConfigManager,
        dump_dir: Option<std::path::PathBuf>,
        shutdown: CancellationToken,
    ) -> Self {
        Self::with_config_manager_and_shutdown(store, config, dump_dir, shutdown)
    }

    pub fn store(&self) -> &SqliteStore {
        &self.store
    }

    pub fn supervisor(&self) -> &SessionSupervisor {
        &self.supervisor
    }

    pub fn config(&self) -> &ConfigManager {
        &self.config
    }

    async fn reload_config(&self) -> Result<ConfigSnapshot, ConfigError> {
        self.config.reload().await
    }

    async fn request_fatal_reload_shutdown(&self, message: String) {
        let mut fatal_error = self.fatal_reload_error.lock().await;
        if fatal_error.is_none() {
            *fatal_error = Some(message);
        }
        drop(fatal_error);
        self.shutdown.cancel();
    }

    pub(crate) async fn take_fatal_reload_error(&self) -> Option<String> {
        self.fatal_reload_error.lock().await.take()
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

    pub(crate) fn lifecycle(&self) -> Arc<LifecycleState> {
        self.lifecycle.clone()
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateSessionRequest {
    pub title: Option<String>,
    pub project_id: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateProjectRequest {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateProjectRequest {
    pub name: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiProject {
    pub id: String,
    pub name: String,
    pub path: String,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Project> for ApiProject {
    fn from(project: Project) -> Self {
        Self {
            id: project.id,
            name: project.name,
            path: project.path,
            created_at: project.created_at,
            updated_at: project.updated_at,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProjectListResponse {
    pub projects: Vec<ApiProject>,
    pub next_cursor: Option<String>,
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
    pub project_id: Option<String>,
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
            project_id: summary.project_id,
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

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ConfigReloadResponse {
    pub revision: u64,
    pub loaded_at: String,
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
    server_version: &'static str,
    api_version: &'static str,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    cursor: Option<String>,
    limit: Option<u32>,
    unassigned: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    after: Option<EventId>,
    limit: Option<u32>,
}

#[derive(Debug)]
pub enum ApiError {
    Store(StoreError),
    Config(ConfigError),
    BadRequest { code: &'static str, message: String },
    InvalidConfig(String),
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

impl From<ConfigError> for ApiError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, code, message) = match self {
            Self::BadRequest { code, message } => (StatusCode::BAD_REQUEST, code, message),
            Self::InvalidConfig(message) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "config_invalid", message)
            }
            Self::Config(ConfigError::ProviderNotFound(name)) => (
                StatusCode::NOT_FOUND,
                "provider_not_found",
                format!("provider {name} was not found"),
            ),
            Self::Config(ConfigError::ProviderAlreadyExists(name)) => (
                StatusCode::CONFLICT,
                "provider_already_exists",
                format!("provider {name} already exists"),
            ),
            Self::Config(ConfigError::ManualModelOverride(name)) => (
                StatusCode::CONFLICT,
                "manual_model_override",
                format!("provider {name} has a manual model override"),
            ),
            Self::Config(ConfigError::InvalidProvider(message)) => {
                (StatusCode::BAD_REQUEST, "invalid_request", message)
            }
            Self::Config(ConfigError::InvalidProtocol { source, .. }) => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                source.to_string(),
            ),
            Self::Config(ConfigError::ConflictingCredentials(name)) => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                format!("provider {name} has conflicting credentials"),
            ),
            Self::Config(ConfigError::ReadOnly) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "configuration_read_only",
                "provider configuration is read-only".to_owned(),
            ),
            Self::Config(_error) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "configuration_unavailable",
                "provider configuration is temporarily unavailable".to_owned(),
            ),
            Self::Store(StoreError::SessionNotFound(id)) => (
                StatusCode::NOT_FOUND,
                "session_not_found",
                format!("session {id} was not found"),
            ),
            Self::Store(StoreError::ProjectNotFound(id)) => (
                StatusCode::NOT_FOUND,
                "project_not_found",
                format!("project {id} was not found"),
            ),
            Self::Store(StoreError::ProjectPathConflict(path)) => (
                StatusCode::CONFLICT,
                "project_path_conflict",
                format!("a project already uses path {path}"),
            ),
            Self::Store(StoreError::ProjectDeleting(id)) => (
                StatusCode::CONFLICT,
                "project_deleting",
                format!("project {id} is being deleted"),
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
            Self::Store(StoreError::ShuttingDown) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "server_shutting_down",
                "server is shutting down".to_owned(),
            ),
            Self::Store(StoreError::ShutdownTimeout) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "server_shutdown_timeout",
                "server shutdown timed out".to_owned(),
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
    router_with_token(state, None)
}

pub fn router_with_token(state: AppState, token: Option<String>) -> Router {
    let lifecycle = state.lifecycle();
    let mut router = Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/openapi.json", get(openapi))
        .route("/api/v1/projects", post(create_project).get(list_projects))
        .route(
            "/api/v1/projects/{project_id}",
            get(get_project)
                .patch(update_project)
                .delete(delete_project),
        )
        .route(
            "/api/v1/projects/{project_id}/sessions",
            get(list_project_sessions),
        )
        .route("/api/v1/sessions", post(create_session).get(list_sessions))
        .route("/api/v1/sessions/{session_id}", get(get_session))
        .route("/api/v1/sessions/{session_id}/events", get(get_events))
        .route(
            "/api/v1/sessions/{session_id}/events/stream",
            get(stream_events),
        )
        .route("/api/v1/sessions/{session_id}/forks", post(fork_session))
        .route(
            "/api/v1/providers",
            get(list_providers).post(create_provider),
        )
        .route(
            "/api/v1/providers/{provider}",
            get(get_provider)
                .patch(update_provider)
                .delete(delete_provider),
        )
        .route(
            "/api/v1/providers/{provider}/models",
            get(list_provider_models)
                .put(replace_provider_models)
                .delete(clear_provider_models),
        )
        .route(
            "/api/v1/providers/{provider}/models/refresh",
            post(refresh_provider_models),
        )
        .route("/api/v1/config/reload", post(reload_config))
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
        .layer(middleware::from_fn(
            move |request: Request, next: middleware::Next| {
                let lifecycle = lifecycle.clone();
                async move {
                    if lifecycle.is_shutting_down() {
                        return shutting_down_response();
                    }
                    next.run(request).await
                }
            },
        ));
    if let Some(token) = token {
        let expected = Arc::<str>::from(token);
        router = router.layer(middleware::from_fn(
            move |request: Request, next: middleware::Next| {
                let expected = expected.clone();
                async move { authenticate_request(request, next, expected).await }
            },
        ));
    }
    router
}

fn shutting_down_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse {
            error: ErrorBody {
                code: "server_shutting_down".to_owned(),
                message: "server is shutting down".to_owned(),
            },
        }),
    )
        .into_response()
}

async fn authenticate_request(
    request: Request,
    next: middleware::Next,
    expected: Arc<str>,
) -> Response {
    let authorized = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|candidate| constant_time_equal(candidate.as_bytes(), expected.as_bytes()));
    if authorized {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, "Bearer")],
            Json(ErrorResponse {
                error: ErrorBody {
                    code: "unauthorized".to_owned(),
                    message: "a valid bearer token is required".to_owned(),
                },
            }),
        )
            .into_response()
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let length = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..length {
        difference |= usize::from(left.get(index).copied().unwrap_or_default())
            ^ usize::from(right.get(index).copied().unwrap_or_default());
    }
    difference == 0
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

fn validated_project_name(name: String) -> Result<String, ApiError> {
    let name = name.trim().to_owned();
    if name.is_empty() {
        return Err(ApiError::BadRequest {
            code: "invalid_request",
            message: "project name must not be empty".to_owned(),
        });
    }
    Ok(name)
}

async fn canonical_project_path(path: String) -> Result<String, ApiError> {
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        return Err(ApiError::BadRequest {
            code: "invalid_request",
            message: "project path must be absolute".to_owned(),
        });
    }
    let canonical = tokio::fs::canonicalize(&path)
        .await
        .map_err(|error| ApiError::BadRequest {
            code: "invalid_request",
            message: format!("project path cannot be resolved: {error}"),
        })?;
    let metadata = tokio::fs::metadata(&canonical)
        .await
        .map_err(|error| ApiError::BadRequest {
            code: "invalid_request",
            message: format!("project path cannot be inspected: {error}"),
        })?;
    if !metadata.is_dir() {
        return Err(ApiError::BadRequest {
            code: "invalid_request",
            message: "project path must be a directory".to_owned(),
        });
    }
    Ok(canonical.to_string_lossy().into_owned())
}

#[utoipa::path(get, path = "/api/v1/health", responses((status = 200, body = HealthResponse)))]
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        server_version: SERVER_VERSION,
        api_version: API_VERSION,
    })
}

#[utoipa::path(
    post,
    path = "/api/v1/projects",
    request_body = CreateProjectRequest,
    responses((status = 201, body = ApiProject), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse))
)]
async fn create_project(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<CreateProjectRequest>,
) -> Result<(StatusCode, Json<ApiProject>), ApiError> {
    let name = validated_project_name(request.name)?;
    let path = canonical_project_path(request.path).await?;
    let project = state.store.create_project(name, path).await?;
    Ok((StatusCode::CREATED, Json(ApiProject::from(project))))
}

#[utoipa::path(
    get,
    path = "/api/v1/projects",
    params(("cursor" = Option<String>, Query), ("limit" = Option<u32>, Query)),
    responses((status = 200, body = ProjectListResponse), (status = 400, body = ErrorResponse))
)]
async fn list_projects(
    State(state): State<AppState>,
    ApiQuery(query): ApiQuery<ListQuery>,
) -> Result<Json<ProjectListResponse>, ApiError> {
    let (projects, next_cursor) = state
        .store
        .list_projects(query.cursor.as_deref(), query.limit.unwrap_or(50))
        .await?;
    Ok(Json(ProjectListResponse {
        projects: projects.into_iter().map(ApiProject::from).collect(),
        next_cursor,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/projects/{project_id}",
    params(("project_id" = String, Path)),
    responses((status = 200, body = ApiProject), (status = 404, body = ErrorResponse))
)]
async fn get_project(
    State(state): State<AppState>,
    ApiPath(project_id): ApiPath<String>,
) -> Result<Json<ApiProject>, ApiError> {
    Ok(Json(ApiProject::from(
        state.store.get_project(&project_id).await?,
    )))
}

#[utoipa::path(
    patch,
    path = "/api/v1/projects/{project_id}",
    params(("project_id" = String, Path)),
    request_body = UpdateProjectRequest,
    responses((status = 200, body = ApiProject), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse))
)]
async fn update_project(
    State(state): State<AppState>,
    ApiPath(project_id): ApiPath<String>,
    ApiJson(request): ApiJson<UpdateProjectRequest>,
) -> Result<Json<ApiProject>, ApiError> {
    if request.name.is_none() && request.path.is_none() {
        return Err(ApiError::BadRequest {
            code: "invalid_request",
            message: "at least one of name or path is required".to_owned(),
        });
    }
    let name = request.name.map(validated_project_name).transpose()?;
    let path = match request.path {
        Some(path) => Some(canonical_project_path(path).await?),
        None => None,
    };
    Ok(Json(ApiProject::from(
        state.store.update_project(&project_id, name, path).await?,
    )))
}

#[utoipa::path(
    delete,
    path = "/api/v1/projects/{project_id}",
    params(("project_id" = String, Path)),
    responses((status = 204), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse))
)]
async fn delete_project(
    State(state): State<AppState>,
    ApiPath(project_id): ApiPath<String>,
) -> Result<StatusCode, ApiError> {
    state.supervisor().delete_project(&project_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/api/v1/projects/{project_id}/sessions",
    params(("project_id" = String, Path), ("cursor" = Option<String>, Query), ("limit" = Option<u32>, Query)),
    responses((status = 200, body = SessionListResponse), (status = 404, body = ErrorResponse))
)]
async fn list_project_sessions(
    State(state): State<AppState>,
    ApiPath(project_id): ApiPath<String>,
    ApiQuery(query): ApiQuery<ListQuery>,
) -> Result<Json<SessionListResponse>, ApiError> {
    let (sessions, next_cursor) = state
        .store
        .list_project_sessions(
            &project_id,
            query.cursor.as_deref(),
            query.limit.unwrap_or(50),
        )
        .await?;
    Ok(Json(SessionListResponse {
        sessions: sessions.into_iter().map(ApiSessionSummary::from).collect(),
        next_cursor,
    }))
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
    if let Some(project_id) = request.project_id.as_deref() {
        if state.supervisor().project_is_deleting(project_id).await {
            return Err(ApiError::Store(StoreError::ProjectDeleting(
                project_id.to_owned(),
            )));
        }
    }
    Ok((
        StatusCode::CREATED,
        Json(ApiSessionSummary::from(
            state
                .store
                .create_session(request.title, request.project_id)
                .await?,
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
    let (sessions, next_cursor) = if query.unassigned.unwrap_or(false) {
        state
            .store
            .list_unassigned_sessions(query.cursor.as_deref(), limit)
            .await?
    } else {
        state
            .store
            .list_sessions(query.cursor.as_deref(), limit)
            .await?
    };
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
            tokio::select! {
                _ = state.lifecycle.streams_closed.cancelled() => break,
                result = receiver.recv() => match result {
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
    if let Some(project_id) = state.store.get_session(&session_id).await?.project_id {
        if state.supervisor().project_is_deleting(&project_id).await {
            return Err(ApiError::Store(StoreError::ProjectDeleting(project_id)));
        }
    }
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
    tag = "providers",
    responses((status = 200, body = ProviderCatalogResponse), (status = 503, body = ErrorResponse))
)]
async fn list_providers(
    State(state): State<AppState>,
) -> Result<Json<ProviderCatalogResponse>, ApiError> {
    Ok(Json(ProviderCatalogResponse {
        providers: state.config().catalog()?,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/config/reload",
    tag = "configuration",
    responses(
        (status = 200, body = ConfigReloadResponse),
        (status = 422, body = ErrorResponse)
    )
)]
async fn reload_config(
    State(state): State<AppState>,
) -> Result<Json<ConfigReloadResponse>, ApiError> {
    match state.reload_config().await {
        Ok(snapshot) => {
            let providers = state.config().catalog()?;
            let manager = state.config().clone();
            tokio::spawn(async move {
                manager.discover_all().await;
            });
            Ok(Json(ConfigReloadResponse {
                revision: snapshot.revision,
                loaded_at: snapshot.loaded_at,
                providers,
            }))
        }
        Err(error) => {
            let message = error.to_string();
            state.request_fatal_reload_shutdown(message.clone()).await;
            Err(ApiError::InvalidConfig(message))
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/providers",
    tag = "providers",
    request_body = CreateProviderRequest,
    responses((status = 201, body = ProviderCatalogEntry), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 503, body = ErrorResponse))
)]
async fn create_provider(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<CreateProviderRequest>,
) -> Result<(StatusCode, Json<ProviderCatalogEntry>), ApiError> {
    let provider = state.config().create_provider(request).await?;
    Ok((StatusCode::CREATED, Json(provider)))
}

#[utoipa::path(
    get,
    path = "/api/v1/providers/{provider}",
    tag = "providers",
    params(("provider" = String, Path)),
    responses((status = 200, body = ProviderCatalogEntry), (status = 404, body = ErrorResponse), (status = 503, body = ErrorResponse))
)]
async fn get_provider(
    State(state): State<AppState>,
    ApiPath(provider): ApiPath<String>,
) -> Result<Json<ProviderCatalogEntry>, ApiError> {
    Ok(Json(state.config().provider(&provider)?))
}

#[utoipa::path(
    patch,
    path = "/api/v1/providers/{provider}",
    tag = "providers",
    params(("provider" = String, Path)),
    request_body = UpdateProviderRequest,
    responses((status = 200, body = ProviderCatalogEntry), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 503, body = ErrorResponse))
)]
async fn update_provider(
    State(state): State<AppState>,
    ApiPath(provider): ApiPath<String>,
    ApiJson(request): ApiJson<UpdateProviderRequest>,
) -> Result<Json<ProviderCatalogEntry>, ApiError> {
    Ok(Json(
        state.config().update_provider(&provider, request).await?,
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/providers/{provider}",
    tag = "providers",
    params(("provider" = String, Path)),
    responses((status = 204), (status = 404, body = ErrorResponse), (status = 503, body = ErrorResponse))
)]
async fn delete_provider(
    State(state): State<AppState>,
    ApiPath(provider): ApiPath<String>,
) -> Result<StatusCode, ApiError> {
    state.config().delete_provider(&provider).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/api/v1/providers/{provider}/models",
    tag = "models",
    params(("provider" = String, Path)),
    responses((status = 200, body = ProviderModelsResponse), (status = 404, body = ErrorResponse), (status = 503, body = ErrorResponse))
)]
async fn list_provider_models(
    State(state): State<AppState>,
    ApiPath(provider): ApiPath<String>,
) -> Result<Json<ProviderModelsResponse>, ApiError> {
    Ok(Json(state.config().models(&provider)?))
}

#[utoipa::path(
    put,
    path = "/api/v1/providers/{provider}/models",
    tag = "models",
    params(("provider" = String, Path)),
    request_body = ReplaceProviderModelsRequest,
    responses((status = 200, body = ProviderModelsResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 503, body = ErrorResponse))
)]
async fn replace_provider_models(
    State(state): State<AppState>,
    ApiPath(provider): ApiPath<String>,
    ApiJson(request): ApiJson<ReplaceProviderModelsRequest>,
) -> Result<Json<ProviderModelsResponse>, ApiError> {
    Ok(Json(
        state
            .config()
            .replace_models(&provider, request.models)
            .await?,
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/providers/{provider}/models",
    tag = "models",
    params(("provider" = String, Path)),
    responses((status = 200, body = ProviderModelsResponse), (status = 404, body = ErrorResponse), (status = 503, body = ErrorResponse))
)]
async fn clear_provider_models(
    State(state): State<AppState>,
    ApiPath(provider): ApiPath<String>,
) -> Result<Json<ProviderModelsResponse>, ApiError> {
    Ok(Json(state.config().clear_models(&provider).await?))
}

#[utoipa::path(
    post,
    path = "/api/v1/providers/{provider}/models/refresh",
    tag = "models",
    params(("provider" = String, Path)),
    responses((status = 200, body = ProviderModelsResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 503, body = ErrorResponse))
)]
async fn refresh_provider_models(
    State(state): State<AppState>,
    ApiPath(provider): ApiPath<String>,
) -> Result<Json<ProviderModelsResponse>, ApiError> {
    Ok(Json(state.config().refresh_models(&provider).await?))
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
    modifiers(&SecurityAddon),
    security(("bearerAuth" = [])),
    paths(
        openapi,
        health,
        create_project,
        list_projects,
        get_project,
        update_project,
        delete_project,
        list_project_sessions,
        create_session,
        list_sessions,
        get_session,
        get_events,
        stream_events,
        fork_session,
        list_providers,
        create_provider,
        get_provider,
        update_provider,
        delete_provider,
        list_provider_models,
        replace_provider_models,
        clear_provider_models,
        refresh_provider_models,
        reload_config,
        create_run,
        get_run,
        cancel_run,
        retry_run,
        resume_queue
    ),
    components(schemas(
        HealthResponse,
        CreateProjectRequest,
        UpdateProjectRequest,
        ApiProject,
        ProjectListResponse,
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
        ProviderCatalogResponse,
        ConfigReloadResponse,
        ProviderCatalogEntry,
        CreateProviderRequest,
        UpdateProviderRequest,
        ReplaceProviderModelsRequest,
        ProviderModelsResponse,
        ProviderCredentialInput,
        ProviderCredentialSummary,
        ModelSource,
        ModelDiscovery,
        DiscoveryStatus
    )),
    tags(
        (name = "projects", description = "Persistent project grouping API"),
        (name = "sessions", description = "Durable session and event-log API"),
        (name = "configuration", description = "Runtime configuration API"),
        (name = "providers", description = "Provider configuration API"),
        (name = "models", description = "Provider model catalog API")
    )
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearerAuth",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("base64url")
                        .build(),
                ),
            );
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/openapi.json",
    responses((status = 200, description = "OpenAPI 3 document"))
)]
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

    async fn authenticated_app() -> (Router, NamedTempFile) {
        let file = NamedTempFile::new().expect("temporary sqlite file");
        let store = SqliteStore::connect_file(file.path())
            .await
            .expect("store opens");
        let state = AppState::new(store);
        (router_with_token(state, Some("secret-token".into())), file)
    }

    #[tokio::test]
    async fn protects_every_route_with_the_bearer_token() {
        let (app, _file) = authenticated_app().await;
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()["www-authenticate"], "Bearer");

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/openapi.json")
                    .header("authorization", "Bearer wrong-token")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/health")
                    .header("authorization", "Bearer secret-token")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), StatusCode::OK);
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
        assert!(document["components"]["securitySchemes"]["bearerAuth"].is_object());
        assert_eq!(document["security"][0]["bearerAuth"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn replays_events_after_last_event_id_over_sse() {
        let (app, state, _file) = app().await;
        let session = state
            .store
            .create_session(None, None)
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
            .create_session(None, None)
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
