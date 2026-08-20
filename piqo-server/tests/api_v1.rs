use axum::{
    body::Body,
    http::{HeaderMap, Request, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use http_body_util::BodyExt;
use piqo_server::{router, AppState, ConfigManager, SqliteStore};
use serde_json::{json, Value};
use std::time::Duration;
use tempfile::{tempdir, NamedTempFile, TempDir};
use tokio::{net::TcpListener, sync::mpsc};
use tower::ServiceExt;

async fn app() -> (axum::Router, NamedTempFile) {
    let file = NamedTempFile::new().expect("temporary sqlite file");
    let store = SqliteStore::connect_file(file.path())
        .await
        .expect("store opens");
    (router(AppState::new(store)), file)
}

async fn configurable_app() -> (axum::Router, NamedTempFile, TempDir) {
    let database = NamedTempFile::new().expect("temporary sqlite file");
    let directory = tempdir().expect("temporary config directory");
    let config = directory.path().join("piqo.toml");
    std::fs::write(&config, "# retained configuration comment\n")
        .expect("configuration fixture writes");
    let store = SqliteStore::connect_file(database.path())
        .await
        .expect("store opens");
    let state = AppState::with_config_file(store, config).expect("configuration manager loads");
    (piqo_server::router(state), database, directory)
}

async fn mock_provider() -> (String, mpsc::UnboundedReceiver<HeaderMap>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock provider binds");
    let address = listener.local_addr().expect("mock provider address");
    let (headers_tx, headers_rx) = mpsc::unbounded_channel();
    let models = get(move |headers: HeaderMap| {
        let headers_tx = headers_tx.clone();
        async move {
            let _ = headers_tx.send(headers);
            Json(json!({
                "data": [
                    {"id": "zeta/model"},
                    {"id": "alpha-model"},
                    {"id": "zeta/model"}
                ]
            }))
        }
    });
    let app = Router::new().route("/v1/models", models).route(
        "/v1/chat/completions",
        post(|| async {
            Json(json!({
                "choices": [{"message": {"content": "ok"}}]
            }))
        }),
    );
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            panic!("mock provider failed: {error}");
        }
    });
    (format!("http://{address}"), headers_rx)
}

async fn mock_catalog(status: StatusCode, body: Value, delay: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock catalog binds");
    let address = listener.local_addr().expect("mock catalog address");
    let app = Router::new().route(
        "/v1/models",
        get(move || {
            let body = body.clone();
            async move {
                tokio::time::sleep(delay).await;
                (status, Json(body))
            }
        }),
    );
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            panic!("mock catalog failed: {error}");
        }
    });
    format!("http://{address}")
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
        .clone()
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
    let operations = [
        ("/api/v1/openapi.json", "get"),
        ("/api/v1/health", "get"),
        ("/api/v1/projects", "get"),
        ("/api/v1/projects", "post"),
        ("/api/v1/projects/{project_id}", "get"),
        ("/api/v1/projects/{project_id}", "patch"),
        ("/api/v1/projects/{project_id}", "delete"),
        ("/api/v1/projects/{project_id}/sessions", "get"),
        ("/api/v1/sessions", "get"),
        ("/api/v1/sessions", "post"),
        ("/api/v1/sessions/{session_id}", "get"),
        ("/api/v1/sessions/{session_id}/events", "get"),
        ("/api/v1/sessions/{session_id}/events/stream", "get"),
        ("/api/v1/sessions/{session_id}/forks", "post"),
        ("/api/v1/providers", "get"),
        ("/api/v1/providers", "post"),
        ("/api/v1/agents", "get"),
        ("/api/v1/providers/{provider}", "get"),
        ("/api/v1/providers/{provider}", "patch"),
        ("/api/v1/providers/{provider}", "delete"),
        ("/api/v1/providers/{provider}/models", "get"),
        ("/api/v1/providers/{provider}/models", "put"),
        ("/api/v1/providers/{provider}/models", "delete"),
        ("/api/v1/providers/{provider}/models/refresh", "post"),
        ("/api/v1/config/reload", "post"),
        ("/api/v1/sessions/{session_id}/runs", "post"),
        ("/api/v1/sessions/{session_id}/runs/{run_id}", "get"),
        ("/api/v1/sessions/{session_id}/runs/{run_id}/cancel", "post"),
        (
            "/api/v1/sessions/{session_id}/runs/{run_id}/retries",
            "post",
        ),
        ("/api/v1/sessions/{session_id}/queue/resume", "post"),
    ];
    for (path, method) in operations {
        assert!(
            value["paths"][path][method].is_object(),
            "OpenAPI is missing {method} {path}"
        );
    }
    assert!(value["components"]["schemas"]["ApiRun"].is_object());
    assert!(value["components"]["schemas"]["ProviderModelsResponse"].is_object());
    assert_eq!(
        value["components"]["schemas"]["ProviderCredentialInput"]["oneOf"][1]["properties"]
            ["value"]["writeOnly"],
        true
    );
}

#[tokio::test]
async fn reloads_configuration_and_updates_the_provider_catalog() {
    let (app, _database, config_directory) = configurable_app().await;
    let config_path = config_directory.path().join("piqo.toml");
    std::fs::write(
        &config_path,
        r#"[providers.second]
base_url = "https://example.com"
models = ["vendor/model"]

[models."vendor/model".body]
temperature = 0.5
"#,
    )
    .expect("replacement configuration writes");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/config/reload")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
    let reloaded = json_body(response).await;
    assert_eq!(reloaded["revision"], 2);
    assert!(reloaded["loaded_at"].as_str().is_some());
    assert_eq!(reloaded["providers"][0]["name"], "second");
    assert_eq!(reloaded["providers"][0]["models"][0], "vendor/model");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/providers")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        json_body(response).await["providers"],
        reloaded["providers"]
    );
}

#[tokio::test]
async fn manages_providers_models_and_redacts_secrets() {
    let (base_url, mut discovered_headers) = mock_provider().await;
    let (app, _database, config_directory) = configurable_app().await;
    let create = json!({
        "name": "local",
        "base_url": base_url,
        "credentials": {"type": "api_key", "value": "provider-secret"},
        "headers": {"x-custom-auth": "header-secret"},
        "connect_timeout_seconds": 2
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/providers")
                .header("content-type", "application/json")
                .body(Body::from(create.to_string()))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 201);
    let provider = json_body(response).await;
    assert_eq!(provider["credentials"]["type"], "api_key");
    assert_eq!(provider["discovery"]["status"], "succeeded");
    assert_eq!(provider["models"], json!(["alpha-model", "zeta/model"]));
    let serialized = provider.to_string();
    assert!(!serialized.contains("provider-secret"));
    assert!(!serialized.contains("header-secret"));
    let headers = discovered_headers
        .recv()
        .await
        .expect("discovery request is observed");
    assert_eq!(headers["authorization"], "Bearer provider-secret");
    assert_eq!(headers["x-custom-auth"], "header-secret");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/providers/local")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"connect_timeout_seconds":3}"#))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 200);
    let updated = json_body(response).await;
    assert_eq!(updated["connect_timeout_seconds"], 3);
    assert_eq!(updated["credentials"]["type"], "api_key");
    discovered_headers
        .recv()
        .await
        .expect("provider update triggers discovery");

    let config = std::fs::read_to_string(config_directory.path().join("piqo.toml"))
        .expect("configuration reads");
    assert!(config.contains("# retained configuration comment"));
    assert!(config.contains("provider-secret"));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/providers/local/models")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"models": [" custom/model ", "first", "custom/model"]}).to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 200);
    let models = json_body(response).await;
    assert_eq!(models["source"], "manual");
    assert_eq!(models["models"], json!(["custom/model", "first"]));
    assert_eq!(models["discovery"]["status"], "not_applicable");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/providers/local/models/refresh")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 409);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "manual_model_override"
    );

    let session_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/sessions")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    let session = json_body(session_response).await;
    let session_id = session["id"].as_str().expect("session id");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/sessions/{session_id}/runs"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "provider": "local",
                        "model": "not-in-manual-list",
                        "input": "hello",
                        "body": {"stream": false}
                    })
                    .to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 202);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/providers/local/models")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 200);
    assert_eq!(json_body(response).await["source"], "discovery");
    discovered_headers
        .recv()
        .await
        .expect("discovery runs after clearing override");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/providers/local")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 204);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/providers/local")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn retains_provider_when_discovery_fails() {
    let (app, _database, _directory) = configurable_app().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/providers")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "name": "offline",
                        "base_url": "http://127.0.0.1:9",
                        "connect_timeout_seconds": 1
                    })
                    .to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 201);
    assert_eq!(json_body(response).await["discovery"]["status"], "failed");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/providers/offline")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 200);
    assert_eq!(json_body(response).await["name"], "offline");
}

#[tokio::test]
async fn reports_http_malformed_and_timeout_discovery_failures() {
    let failing = mock_catalog(
        StatusCode::BAD_GATEWAY,
        json!({"error": "must not be exposed"}),
        Duration::ZERO,
    )
    .await;
    let malformed = mock_catalog(
        StatusCode::OK,
        json!({"data": [{"name": "missing-id"}]}),
        Duration::ZERO,
    )
    .await;
    let slow = mock_catalog(
        StatusCode::OK,
        json!({"data": [{"id": "too-late"}]}),
        Duration::from_millis(1500),
    )
    .await;
    let (app, _database, _directory) = configurable_app().await;
    for (name, base_url) in [
        ("http-error", failing),
        ("malformed", malformed),
        ("timeout", slow),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/providers")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "name": name,
                            "base_url": base_url,
                            "connect_timeout_seconds": 1
                        })
                        .to_string(),
                    ))
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), 201);
        let provider = json_body(response).await;
        assert_eq!(provider["discovery"]["status"], "failed");
        assert!(!provider.to_string().contains("must not be exposed"));
    }
}

#[tokio::test]
async fn discovers_configured_providers_during_startup_pass() {
    let (base_url, mut headers) = mock_provider().await;
    let directory = tempdir().expect("temporary config directory");
    let path = directory.path().join("piqo.toml");
    std::fs::write(
        &path,
        format!("[providers.local]\nbase_url = \"{base_url}\"\n"),
    )
    .expect("configuration writes");
    let manager = ConfigManager::load(path).expect("manager loads");
    assert_eq!(manager.provider("local").expect("provider").models.len(), 0);
    manager.discover_all().await;
    headers.recv().await.expect("startup discovery is observed");
    let provider = manager.provider("local").expect("provider loads");
    assert_eq!(
        provider.discovery.status,
        piqo_server::DiscoveryStatus::Succeeded
    );
    assert_eq!(provider.models, vec!["alpha-model", "zeta/model"]);
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
        .clone()
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

#[tokio::test]
async fn lists_markdown_agents_and_uses_their_provider_and_model_defaults() {
    let database = NamedTempFile::new().expect("temporary sqlite file");
    let directory = tempdir().expect("temporary configuration directory");
    let agents = directory.path().join("agents");
    std::fs::create_dir(&agents).expect("agent directory creates");
    std::fs::write(
        agents.join("reviewer.md"),
        r#"---
description: Read-only reviewer
provider: local
model: reviewer-model
permissions:
  read: allow
  write: deny
  bash: ask
---
Review the change carefully.
"#,
    )
    .expect("agent fixture writes");
    let config = directory.path().join("piqo.toml");
    std::fs::write(
        &config,
        r#"[providers.local]
base_url = "http://127.0.0.1:9"
"#,
    )
    .expect("configuration fixture writes");
    let store = SqliteStore::connect_file(database.path())
        .await
        .expect("store opens");
    let app = router(AppState::with_config_file(store, config).expect("configuration loads"));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/agents")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 200);
    let agents = json_body(response).await;
    assert_eq!(agents["agents"][0]["id"], "reviewer");
    assert_eq!(agents["agents"][0]["permissions"]["write"], "deny");
    assert!(agents["agents"][0].get("instructions").is_none());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/sessions")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    let session_id = json_body(response).await["id"]
        .as_str()
        .expect("session id")
        .to_owned();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/sessions/{session_id}/runs"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"agent": "reviewer", "input": "Review this"}).to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 202);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/sessions/{session_id}/runs"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"agent": "missing", "input": "Review this"}).to_string(),
                ))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), 400);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "agent_not_found"
    );
}
