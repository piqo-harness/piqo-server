//! HTTP/SSE API and session supervision.

use axum::{routing::get, Json, Router};
use serde::Serialize;
use tower_http::trace::TraceLayer;
use utoipa::ToSchema;

/// Build the versioned HTTP surface. Session routes will be added around the
/// same router as the event-log and supervision layers are implemented.
pub fn router() -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .layer(TraceLayer::new_for_http())
}

#[derive(Debug, Serialize, ToSchema)]
struct HealthResponse {
    status: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
