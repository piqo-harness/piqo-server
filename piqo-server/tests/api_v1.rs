use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use piqo_server::{router, AppState, SqliteStore};
use serde_json::{json, Value};
use tempfile::{tempdir, NamedTempFile};
use tower::ServiceExt;

async fn app() -> (axum::Router, NamedTempFile) {
    let file = NamedTempFile::new().expect("temporary sqlite file");
    let store = SqliteStore::connect_file(file.path())
        .await
        .expect("store opens");
    (router(AppState::new(store)), file)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body reads")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("JSON response")
}

#[tokio::test]
async fn lists_sessions_with_the_default_page_size() {
    let (app, _file) = app().await;
    for title in ["first", "second"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"title":"{title}"}}"#)))
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), 201);
    }

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/sessions")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 200);
    let value = json_body(response).await;
    assert_eq!(
        value["sessions"].as_array().expect("sessions array").len(),
        2
    );
}

#[tokio::test]
async fn rejects_malformed_query_and_methods_with_the_common_envelope() {
    let (app, _file) = app().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/sessions?limit=invalid")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 400);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "invalid_request"
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/sessions")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 405);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "invalid_request"
    );
}

#[tokio::test]
async fn openapi_documents_run_and_queue_routes() {
    let (app, _file) = app().await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/openapi.json")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    let value = json_body(response).await;
    assert!(value["paths"]["/api/v1/sessions/{session_id}/runs"].is_object());
    assert!(value["paths"]["/api/v1/sessions/{session_id}/queue/resume"].is_object());
    assert!(value["components"]["schemas"]["ApiRun"].is_object());
    assert!(value["paths"]["/api/v1/projects"].is_object());
    assert!(value["paths"]["/api/v1/projects/{project_id}/sessions"].is_object());
}

#[tokio::test]
async fn manages_projects_and_lists_their_sessions_separately_from_unassigned() {
    let (app, _file) = app().await;
    let directory = tempdir().expect("project directory creates");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/projects")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name": "workspace", "path": directory.path()}).to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 201);
    let project = json_body(response).await;
    let project_id = project["id"].as_str().expect("project id");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/sessions")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"title": "grouped", "project_id": project_id}).to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 201);
    let session = json_body(response).await;
    assert_eq!(session["project_id"], project_id);
    let session_id = session["id"].as_str().expect("session id").to_owned();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/sessions")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"title":"unassigned"}"#))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 201);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/projects/{project_id}/sessions"))
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    let grouped = json_body(response).await;
    assert_eq!(grouped["sessions"].as_array().expect("sessions").len(), 1);
    assert_eq!(grouped["sessions"][0]["id"], session_id);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/sessions?unassigned=true")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    let unassigned = json_body(response).await;
    assert_eq!(
        unassigned["sessions"].as_array().expect("sessions").len(),
        1
    );
    assert!(unassigned["sessions"][0]["project_id"].is_null());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/projects/{project_id}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"renamed"}"#))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 200);
    assert_eq!(json_body(response).await["name"], "renamed");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/projects/{project_id}"))
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 204);

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/sessions/{session_id}"))
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn validates_project_paths_and_session_project_references() {
    let (app, _file) = app().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/projects")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"bad","path":"relative"}"#))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 400);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/sessions")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"project_id":"missing"}"#))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 404);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "project_not_found"
    );
}
