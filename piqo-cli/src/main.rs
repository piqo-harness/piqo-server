use std::{
    collections::HashSet,
    io::{self, Write},
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use piqo_server::{AppState, PiqoConfig, RunRequest, SqliteStore};
use reqwest::Client;
use serde_json::{json, Value};
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(name = "piqo", about = "Headless agent harness server")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the HTTP/SSE server.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: SocketAddr,
        #[arg(long, default_value = "sqlite://piqo.db")]
        database: String,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        dump_requests: Option<PathBuf>,
    },
    /// Attach a client to an existing session.
    Attach {
        session_id: String,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        server: String,
        #[arg(long)]
        json: bool,
    },
    /// Run a prompt against an existing daemon.
    Run {
        prompt: String,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        server: String,
        #[arg(long)]
        session: Option<String>,
        #[arg(long, default_value = "omlx")]
        provider: String,
        #[arg(long)]
        model: String,
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    match Cli::parse().command {
        Command::Serve {
            bind,
            database,
            config,
            dump_requests,
        } => {
            piqo_server::validate_bind_address(bind)?;
            let config_path = config.unwrap_or_else(default_config_path);
            let config = PiqoConfig::load(&config_path)
                .with_context(|| format!("loading configuration {}", config_path.display()))?;
            if let Some(directory) = &dump_requests {
                tracing::warn!(path = %directory.display(), "request dumps may contain sensitive prompts");
            }
            let store = SqliteStore::connect(&database).await?;
            let recovered = store.recover_running_sessions().await?;
            if !recovered.is_empty() {
                tracing::info!(
                    sessions = recovered.len(),
                    "marked sessions interrupted after restart"
                );
            }
            let listener = TcpListener::bind(bind).await?;
            tracing::info!(address = %bind, config = %config_path.display(), "piqo server listening");
            axum::serve(
                listener,
                piqo_server::router(AppState::with_config_and_dump(
                    store,
                    std::sync::Arc::new(config),
                    dump_requests,
                )),
            )
            .await?;
        }
        Command::Attach {
            session_id,
            server,
            json,
        } => {
            follow_events(&Client::new(), &server, &session_id, 0, json, None).await?;
        }
        Command::Run {
            prompt,
            server,
            session,
            provider,
            model,
            json: json_output,
        } => {
            let client = Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .build()?;
            let session_id = match session {
                Some(id) => id,
                None => {
                    let response = client
                        .post(format!("{server}/api/v1/sessions"))
                        .json(&json!({}))
                        .send()
                        .await
                        .context("connecting to piqo daemon")?
                        .error_for_status()
                        .context("creating session")?;
                    response.json::<Value>().await?["id"]
                        .as_str()
                        .context("daemon response has no session id")?
                        .to_owned()
                }
            };
            let response = client
                .post(format!("{server}/api/v1/sessions/{session_id}/runs"))
                .json(&RunRequest {
                    provider,
                    model,
                    input: Value::String(prompt),
                    agent: None,
                    variant: None,
                    body: Value::Object(Default::default()),
                })
                .send()
                .await
                .context("connecting to piqo daemon")?
                .error_for_status()
                .context("creating run")?;
            let accepted = response.json::<Value>().await?;
            let run_id = accepted["run_id"]
                .as_str()
                .context("daemon response has no run id")?;
            follow_events(&client, &server, &session_id, 0, json_output, Some(run_id)).await?;
        }
    }
    Ok(())
}

async fn follow_events(
    client: &Client,
    server: &str,
    session_id: &str,
    after: u64,
    json_output: bool,
    until_run: Option<&str>,
) -> Result<()> {
    let mut last_seen = after;
    let mut assistant_messages = HashSet::new();
    let mut active_run_seen = until_run.is_none();
    loop {
        let response = client
            .get(format!(
                "{server}/api/v1/sessions/{session_id}/events/stream"
            ))
            .header("Last-Event-ID", last_seen.to_string())
            .send()
            .await
            .context("connecting to piqo event stream")?
            .error_for_status()
            .context("opening piqo event stream")?;
        let mut stream = response.bytes_stream();
        let mut buffer = String::new();
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);
        while let Some(chunk) = tokio::select! {
            _ = &mut ctrl_c => return Ok(()),
            chunk = stream.next() => chunk,
        } {
            let chunk = chunk.context("reading piqo event stream")?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(index) = buffer.find("\n\n") {
                let frame = buffer[..index].to_owned();
                buffer.drain(..index + 2);
                let (id, data) = parse_frame(&frame);
                if data.is_empty() {
                    continue;
                }
                let event: Value = match serde_json::from_str(&data) {
                    Ok(event) => event,
                    Err(_) => continue,
                };
                if let Some(event_id) = id {
                    last_seen = last_seen.max(event_id);
                }
                if event["type"] == "run_started"
                    && until_run.is_some_and(|run_id| event["data"]["run_id"] == run_id)
                {
                    active_run_seen = true;
                }
                if event["type"] == "message_started"
                    && (event["data"]["role"] == "assistant"
                        || event["data"]["author"]["kind"] == "agent")
                {
                    if let Some(message_id) = event["data"]["message_id"].as_str() {
                        assistant_messages.insert(message_id.to_owned());
                    }
                }
                if json_output {
                    println!("{}", serde_json::to_string(&event)?);
                } else if active_run_seen
                    && event["type"] == "message_content_appended"
                    && event["data"]["block"]["kind"] == "text"
                    && assistant_messages
                        .contains(event["data"]["message_id"].as_str().unwrap_or_default())
                {
                    print!(
                        "{}",
                        event["data"]["block"]["value"].as_str().unwrap_or_default()
                    );
                    io::stdout().flush()?;
                }
                if let Some(run_id) = until_run {
                    if event["data"]["run_id"] == run_id
                        && matches!(
                            event["type"].as_str(),
                            Some(
                                "run_completed"
                                    | "run_failed"
                                    | "run_cancelled"
                                    | "run_interrupted"
                                    | "run_requires_action",
                            )
                        )
                    {
                        println!();
                        return Ok(());
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn parse_frame(frame: &str) -> (Option<u64>, String) {
    let mut id = None;
    let mut data = String::new();
    for line in frame.lines() {
        if let Some(value) = line.strip_prefix("id:") {
            id = value.trim().parse().ok();
        } else if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(value.trim_start());
        }
    }
    (id, data)
}

fn default_config_path() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(|home| PathBuf::from(home).join(".config"))
                .unwrap_or_else(|| PathBuf::from("."))
        })
        .join("piqo/piqo.toml")
}
