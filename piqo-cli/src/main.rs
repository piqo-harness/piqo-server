use std::net::SocketAddr;

use anyhow::Result;
use clap::{Parser, Subcommand};
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
    },
    /// Attach a client to an existing session (not implemented yet).
    Attach { session_id: String },
    /// Run a one-shot prompt (not implemented yet).
    Run { prompt: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    match Cli::parse().command {
        Command::Serve { bind } => {
            let listener = TcpListener::bind(bind).await?;
            tracing::info!(address = %bind, "piqo server listening");
            axum::serve(listener, piqo_server::router()).await?;
        }
        Command::Attach { session_id } => {
            anyhow::bail!("attaching to session {session_id} is not implemented yet");
        }
        Command::Run { prompt } => {
            anyhow::bail!("one-shot run for prompt {prompt:?} is not implemented yet");
        }
    }

    Ok(())
}
