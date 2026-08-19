use std::{
    fs::{File, OpenOptions},
    future::IntoFuture,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use axum::Router;
use fs2::FileExt;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{
    router_with_token, AppState, BindAddressError, ConfigError, ConfigManager, PiqoConfig,
    SqliteStore, StoreError,
};

#[derive(Debug, Clone)]
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub database: String,
    pub config: PathBuf,
    pub dump_requests: Option<PathBuf>,
    pub auth_token: Option<String>,
    pub instance_lock: Option<PathBuf>,
    pub shutdown_timeout: Duration,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),
    #[error("storage error: {0}")]
    Store(#[from] StoreError),
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unable to bind server listener: {0}")]
    BindIo(std::io::Error),
    #[error("bind address error: {0}")]
    Bind(#[from] BindAddressError),
    #[error("another piqo sidecar already owns the instance lock {0}")]
    InstanceLocked(PathBuf),
    #[error("server shutdown timed out")]
    ShutdownTimeout,
    #[error("configuration reload failed: {0}")]
    ConfigReload(String),
}

impl ServerError {
    pub fn startup_code(&self) -> &'static str {
        match self {
            Self::InstanceLocked(_) => "instance_already_running",
            Self::Config(_) => "config_invalid",
            Self::Store(_) => "storage_unavailable",
            Self::Bind(_) | Self::BindIo(_) => "bind_failed",
            Self::Io(_) => "storage_unavailable",
            Self::ShutdownTimeout => "storage_unavailable",
            Self::ConfigReload(_) => "config_invalid",
        }
    }
}

struct InstanceLock {
    _file: File,
}

impl InstanceLock {
    fn acquire(path: &Path) -> Result<Self, ServerError> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        if let Err(error) = file.try_lock_exclusive() {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                return Err(ServerError::InstanceLocked(path.to_owned()));
            }
            return Err(ServerError::Io(error));
        }
        file.set_len(0)?;
        writeln!(file, "{}", std::process::id())?;
        file.sync_data()?;
        Ok(Self { _file: file })
    }
}

pub fn ensure_private_directory(path: &Path) -> Result<(), std::io::Error> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub struct PreparedServer {
    listener: TcpListener,
    router: Router,
    state: AppState,
    shutdown: CancellationToken,
    shutdown_timeout: Duration,
    _instance_lock: Option<InstanceLock>,
}

impl PreparedServer {
    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub async fn run(self) -> Result<(), ServerError> {
        let PreparedServer {
            listener,
            router,
            state,
            shutdown,
            shutdown_timeout,
            _instance_lock,
        } = self;
        let lifecycle = state.lifecycle();
        let supervisor = state.supervisor().clone();
        let server_shutdown = shutdown.clone();
        let cleanup_shutdown = shutdown.clone();
        let mut server = Box::pin(
            (axum::serve(listener, router)
                .with_graceful_shutdown(server_shutdown.cancelled_owned()))
            .into_future(),
        );
        let mut cleanup = Box::pin(async move {
            cleanup_shutdown.cancelled().await;
            lifecycle.begin_shutdown();
            let result = supervisor.shutdown(shutdown_timeout).await;
            lifecycle.close_streams();
            result
        });

        tokio::select! {
            server_result = &mut server => {
                shutdown.cancel();
                let cleanup_result = (&mut cleanup).await;
                cleanup_result.map_err(map_shutdown_error)?;
                server_result.map_err(ServerError::Io)?;
            }
            cleanup_result = &mut cleanup => {
                cleanup_result.map_err(map_shutdown_error)?;
                server.await.map_err(ServerError::Io)?;
            }
        }
        if let Some(message) = state.take_fatal_reload_error().await {
            return Err(ServerError::ConfigReload(message));
        }
        Ok(())
    }
}

fn map_shutdown_error(error: StoreError) -> ServerError {
    if matches!(&error, StoreError::ShutdownTimeout) {
        ServerError::ShutdownTimeout
    } else {
        ServerError::Store(error)
    }
}

pub async fn prepare_server(options: ServerOptions) -> Result<PreparedServer, ServerError> {
    crate::validate_bind_address(options.bind)?;
    let _instance_lock = options
        .instance_lock
        .as_deref()
        .map(InstanceLock::acquire)
        .transpose()?;
    let config = PiqoConfig::load(&options.config)?;
    config.validate()?;
    let config = ConfigManager::new(options.config.clone(), Arc::new(config));
    let store = SqliteStore::connect(&options.database).await?;
    let recovered = store.recover_running_sessions().await?;
    if !recovered.is_empty() {
        tracing::info!(
            sessions = recovered.len(),
            "marked sessions interrupted after restart"
        );
    }
    let listener = TcpListener::bind(options.bind)
        .await
        .map_err(ServerError::BindIo)?;
    let shutdown = CancellationToken::new();
    let state = AppState::with_config_manager_and_dump_and_shutdown(
        store,
        config,
        options.dump_requests,
        shutdown.clone(),
    );
    let router = router_with_token(state.clone(), options.auth_token);
    Ok(PreparedServer {
        listener,
        router,
        state,
        shutdown,
        shutdown_timeout: options.shutdown_timeout,
        _instance_lock,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, NamedTempFile};

    #[tokio::test]
    async fn invalid_reload_responds_then_stops_the_server_with_an_error() {
        let directory = tempdir().expect("config directory creates");
        let config_path = directory.path().join("piqo.toml");
        std::fs::write(&config_path, "").expect("initial config writes");
        let database = NamedTempFile::new().expect("temporary sqlite file");
        let prepared = prepare_server(ServerOptions {
            bind: "127.0.0.1:0".parse().expect("loopback address parses"),
            database: format!("sqlite://{}", database.path().display()),
            config: config_path.clone(),
            dump_requests: None,
            auth_token: None,
            instance_lock: None,
            shutdown_timeout: Duration::from_secs(2),
        })
        .await
        .expect("server prepares");
        let address = prepared.local_addr().expect("server address");
        std::fs::write(&config_path, "invalid = [").expect("invalid config writes");

        let server = tokio::spawn(prepared.run());
        let response = reqwest::Client::new()
            .post(format!("http://{address}/api/v1/config/reload"))
            .send()
            .await
            .expect("reload request completes");
        assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        let body: serde_json::Value = response.json().await.expect("error body decodes");
        assert_eq!(body["error"]["code"], "config_invalid");

        let result = tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .expect("server stops")
            .expect("server task joins");
        assert!(matches!(result, Err(ServerError::ConfigReload(_))));
    }
}
