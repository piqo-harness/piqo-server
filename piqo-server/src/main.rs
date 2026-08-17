use std::{io::Write, net::SocketAddr, path::PathBuf, process::ExitCode, time::Duration};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use clap::Parser;
use piqo_server::{
    ensure_private_directory, prepare_server, ServerError, ServerOptions, API_VERSION,
    SERVER_VERSION,
};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(name = "piqo-server", version, about = "Private piqo sidecar server")]
struct Cli {}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let _ = Cli::parse();

    let data_dir = match home_data_dir() {
        Ok(path) => path,
        Err(message) => return fatal("home_unavailable", message),
    };
    if let Err(error) = ensure_private_directory(&data_dir) {
        return fatal(
            "storage_unavailable",
            format!("unable to prepare {}: {error}", data_dir.display()),
        );
    }
    let token = match generate_token() {
        Ok(token) => token,
        Err(error) => return fatal("storage_unavailable", error),
    };
    let bind: SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("sidecar bind address is a valid constant");
    let options = ServerOptions {
        bind,
        database: format!("sqlite://{}", data_dir.join("piqo.db").display()),
        config: data_dir.join("piqo.toml"),
        dump_requests: None,
        auth_token: Some(token.clone()),
        instance_lock: Some(data_dir.join("piqo.lock")),
        shutdown_timeout: Duration::from_secs(10),
    };
    let prepared = match prepare_server(options).await {
        Ok(prepared) => prepared,
        Err(error) => return fatal(error.startup_code(), error.to_string()),
    };
    let local_addr = match prepared.local_addr() {
        Ok(address) => address,
        Err(error) => return fatal("bind_failed", error.to_string()),
    };
    let ready = json!({
        "type": "ready",
        "protocol_version": 1,
        "server_version": SERVER_VERSION,
        "api_version": API_VERSION,
        "pid": std::process::id(),
        "base_url": format!("http://{local_addr}"),
        "token": token,
    });
    println!("{ready}");
    let _ = std::io::stdout().flush();

    let shutdown = prepared.shutdown_token();
    let run = prepared.run();
    tokio::pin!(run);
    tokio::select! {
        result = &mut run => finish_run(result),
        _ = wait_for_shutdown() => {
            shutdown.cancel();
            finish_run(run.await)
        }
    }
}

fn finish_run(result: Result<(), ServerError>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "piqo sidecar stopped with an error");
            ExitCode::from(1)
        }
    }
}

fn home_data_dir() -> Result<PathBuf, String> {
    let home = std::env::var_os("HOME").ok_or_else(|| "HOME is not set".to_owned())?;
    Ok(PathBuf::from(home).join(".config/piqo"))
}

fn generate_token() -> Result<String, String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| format!("unable to generate auth token: {error}"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn fatal(code: &str, message: impl Into<String>) -> ExitCode {
    println!(
        "{}",
        json!({
            "type": "fatal",
            "protocol_version": 1,
            "code": code,
            "message": message.into(),
        })
    );
    let _ = std::io::stdout().flush();
    ExitCode::from(2)
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = terminate.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_a_32_byte_url_safe_token() {
        let first = generate_token().expect("token generates");
        let second = generate_token().expect("token generates");
        assert_eq!(
            URL_SAFE_NO_PAD
                .decode(first.as_bytes())
                .expect("token decodes")
                .len(),
            32
        );
        assert_ne!(first, second);
    }
}
