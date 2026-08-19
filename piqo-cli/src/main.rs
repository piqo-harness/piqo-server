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
use piqo_server::{prepare_server, RunRequest, ServerOptions};
use reqwest::Client;
use serde_json::{json, Value};

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
        #[arg(long, env = "PIQO_SERVER_TOKEN")]
        token: Option<String>,
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
        /// Project to associate with a newly created session.
        #[arg(long)]
        project: Option<String>,
        #[arg(long, env = "PIQO_SERVER_TOKEN")]
        token: Option<String>,
        #[arg(long, default_value = "omlx")]
        provider: String,
        #[arg(long)]
        model: String,
        #[arg(long)]
        json: bool,
    },
    /// Manage durable project groups.
    Project {
        #[command(subcommand)]
        command: ProjectCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ProjectCommand {
    /// Create a project from an existing local directory.
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        server: String,
        #[arg(long, env = "PIQO_SERVER_TOKEN")]
        token: Option<String>,
    },
    /// List projects.
    List {
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        server: String,
        #[arg(long, env = "PIQO_SERVER_TOKEN")]
        token: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Show a project.
    Get {
        project_id: String,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        server: String,
        #[arg(long, env = "PIQO_SERVER_TOKEN")]
        token: Option<String>,
    },
    /// Rename a project or point it at a different directory.
    Update {
        project_id: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        server: String,
        #[arg(long, env = "PIQO_SERVER_TOKEN")]
        token: Option<String>,
    },
    /// Delete a project and all of its sessions.
    Delete {
        project_id: String,
        #[arg(long)]
        yes: bool,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        server: String,
        #[arg(long, env = "PIQO_SERVER_TOKEN")]
        token: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    match Cli::parse().command {
        Command::Serve {
            bind,
            database,
            config,
            dump_requests,
        } => {
            let config_path = config.unwrap_or_else(default_config_path);
            if let Some(directory) = &dump_requests {
                tracing::warn!(path = %directory.display(), "request dumps may contain sensitive prompts");
            }
            let prepared = prepare_server(ServerOptions {
                bind,
                database,
                config: config_path.clone(),
                dump_requests,
                auth_token: None,
                instance_lock: None,
                shutdown_timeout: Duration::from_secs(10),
            })
            .await
            .with_context(|| {
                format!(
                    "starting server with configuration {}",
                    config_path.display()
                )
            })?;
            tracing::info!(address = %prepared.local_addr()?, config = %config_path.display(), "piqo server listening");
            let shutdown = prepared.shutdown_token();
            let run = prepared.run();
            tokio::pin!(run);
            tokio::select! {
                result = &mut run => result?,
                _ = tokio::signal::ctrl_c() => {
                    shutdown.cancel();
                    run.await?;
                }
            }
        }
        Command::Attach {
            session_id,
            server,
            token,
            json,
        } => {
            let token = token.or_else(|| std::env::var("PIQO_SERVER_TOKEN").ok());
            follow_events(
                &Client::new(),
                &server,
                &session_id,
                0,
                json,
                None,
                token.as_deref(),
            )
            .await?;
        }
        Command::Run {
            prompt,
            server,
            session,
            project,
            token,
            provider,
            model,
            json: json_output,
        } => {
            validate_run_options(session.as_deref(), project.as_deref())?;
            let client = Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .build()?;
            let token = token.or_else(|| std::env::var("PIQO_SERVER_TOKEN").ok());
            let session_id = match session {
                Some(id) => id,
                None => {
                    let mut request = client.post(format!("{server}/api/v1/sessions"));
                    if let Some(token) = token.as_deref() {
                        request = request.bearer_auth(token);
                    }
                    let response = request
                        .json(&json!({"project_id": project}))
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
            let mut request = client.post(format!("{server}/api/v1/sessions/{session_id}/runs"));
            if let Some(token) = token.as_deref() {
                request = request.bearer_auth(token);
            }
            let response = request
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
            follow_events(
                &client,
                &server,
                &session_id,
                0,
                json_output,
                Some(run_id),
                token.as_deref(),
            )
            .await?;
        }
        Command::Project { command } => run_project_command(command).await?,
    }
    Ok(())
}

async fn run_project_command(command: ProjectCommand) -> Result<()> {
    let client = Client::new();
    match command {
        ProjectCommand::Create {
            name,
            path,
            server,
            token,
        } => {
            let response = authenticated(client.post(format!("{server}/api/v1/projects")), token)
                .json(&json!({"name": name, "path": path}))
                .send()
                .await?
                .error_for_status()?;
            print_json(response).await
        }
        ProjectCommand::List {
            server,
            token,
            limit,
            cursor,
        } => {
            let mut request = client.get(format!("{server}/api/v1/projects"));
            if let Some(limit) = limit {
                request = request.query(&[("limit", limit.to_string())]);
            }
            if let Some(cursor) = cursor {
                request = request.query(&[("cursor", cursor)]);
            }
            let response = authenticated(request, token)
                .send()
                .await?
                .error_for_status()?;
            print_json(response).await
        }
        ProjectCommand::Get {
            project_id,
            server,
            token,
        } => {
            let response = authenticated(
                client.get(format!("{server}/api/v1/projects/{project_id}")),
                token,
            )
            .send()
            .await?
            .error_for_status()?;
            print_json(response).await
        }
        ProjectCommand::Update {
            project_id,
            name,
            path,
            server,
            token,
        } => {
            if name.is_none() && path.is_none() {
                anyhow::bail!("provide at least one of --name or --path");
            }
            let response = authenticated(
                client.patch(format!("{server}/api/v1/projects/{project_id}")),
                token,
            )
            .json(&json!({"name": name, "path": path}))
            .send()
            .await?
            .error_for_status()?;
            print_json(response).await
        }
        ProjectCommand::Delete {
            project_id,
            yes,
            server,
            token,
        } => {
            if !yes {
                anyhow::bail!("project deletion is destructive; repeat with --yes");
            }
            authenticated(
                client.delete(format!("{server}/api/v1/projects/{project_id}")),
                token,
            )
            .send()
            .await?
            .error_for_status()?;
            Ok(())
        }
    }
}

fn authenticated(
    request: reqwest::RequestBuilder,
    token: Option<String>,
) -> reqwest::RequestBuilder {
    match token.or_else(|| std::env::var("PIQO_SERVER_TOKEN").ok()) {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

async fn print_json(response: reqwest::Response) -> Result<()> {
    let value: Value = response.json().await?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn validate_run_options(session: Option<&str>, project: Option<&str>) -> Result<()> {
    if session.is_some() && project.is_some() {
        anyhow::bail!("--project cannot be used with --session");
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
    token: Option<&str>,
) -> Result<()> {
    let mut last_seen = after;
    let mut assistant_messages = HashSet::new();
    let mut active_run_seen = until_run.is_none();
    loop {
        let mut request = client
            .get(format!(
                "{server}/api/v1/sessions/{session_id}/events/stream"
            ))
            .header("Last-Event-ID", last_seen.to_string());
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_a_project_when_reusing_a_session() {
        assert!(validate_run_options(Some("session"), Some("project")).is_err());
        assert!(validate_run_options(None, Some("project")).is_ok());
    }

    #[test]
    fn parses_the_destructive_project_delete_confirmation() {
        let cli = Cli::try_parse_from(["piqo", "project", "delete", "project-id", "--yes"])
            .expect("CLI parses");
        assert!(matches!(
            cli.command,
            Command::Project {
                command: ProjectCommand::Delete { yes: true, .. }
            }
        ));
    }
}
