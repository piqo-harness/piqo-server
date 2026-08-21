//! Permission-gated native tools and MCP client integration.

use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use piqo_core::{PermissionDecision, PermissionPolicy, ToolRequest};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::{
    io::AsyncReadExt,
    process::Command,
    sync::{Mutex, RwLock},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// The runtime boundary through which every tool invocation is authorized.
#[derive(Debug, Clone)]
pub struct ToolRuntime {
    policy: PermissionPolicy,
}

impl ToolRuntime {
    pub fn new(policy: PermissionPolicy) -> Self {
        Self { policy }
    }

    pub fn authorize(&self, request: &ToolRequest) -> PermissionDecision {
        self.policy.evaluate(request)
    }
}

/// Configuration for an MCP server launched as a child process over stdio.
/// The actual rmcp client session is intentionally kept at this IO edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default = "default_startup_timeout")]
    pub startup_timeout_seconds: u64,
    #[serde(default = "default_call_timeout")]
    pub call_timeout_seconds: u64,
    #[serde(default = "default_result_limit")]
    pub max_result_bytes: usize,
    #[serde(default = "default_termination_grace")]
    pub termination_grace_millis: u64,
    #[serde(default = "default_restart_limit")]
    pub max_restart_attempts: u8,
}

fn default_enabled() -> bool {
    true
}
fn default_startup_timeout() -> u64 {
    10
}
fn default_call_timeout() -> u64 {
    30
}
fn default_result_limit() -> usize {
    50 * 1024
}
fn default_termination_grace() -> u64 {
    1_000
}
fn default_restart_limit() -> u8 {
    3
}

impl McpServerConfig {
    pub fn new(
        command: impl Into<String>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            command: command.into(),
            args: args.into_iter().map(Into::into).collect(),
            cwd: None,
            enabled: true,
            environment: BTreeMap::new(),
            startup_timeout_seconds: default_startup_timeout(),
            call_timeout_seconds: default_call_timeout(),
            max_result_bytes: default_result_limit(),
            termination_grace_millis: default_termination_grace(),
            max_restart_attempts: default_restart_limit(),
        }
    }

    pub fn validate(&self) -> Result<(), McpError> {
        if self.command.trim().is_empty()
            || self.startup_timeout_seconds == 0
            || self.call_timeout_seconds == 0
            || self.max_result_bytes == 0
            || self.termination_grace_millis == 0
            || self.cwd.as_ref().is_some_and(|path| !path.is_absolute())
            || self.environment.iter().any(|(child, host)| {
                child.is_empty() || host.is_empty() || child.contains('=') || host.contains('=')
            })
        {
            return Err(McpError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpServerState {
    Starting,
    Healthy,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, Serialize)]
pub struct McpToolDefinition {
    pub name: String,
    pub server_id: String,
    pub server_tool_name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct McpServerDiagnostics {
    pub id: String,
    pub enabled: bool,
    pub state: McpServerState,
    pub tools: Vec<McpToolDefinition>,
    pub last_error: Option<String>,
    pub restart_attempts: u8,
}

#[derive(Debug, Error)]
pub enum McpError {
    #[error("invalid MCP configuration")]
    InvalidConfiguration,
    #[error("MCP server is unavailable")]
    Unavailable,
    #[error("MCP tool is unavailable")]
    ToolNotFound,
    #[error("MCP tool arguments are invalid")]
    InvalidArguments,
    #[error("MCP call result exceeded the configured limit")]
    ResultTooLarge,
    #[error("MCP execution outcome is uncertain")]
    Uncertain,
}

#[derive(Clone)]
struct McpClientHandler {
    catalog_dirty: Arc<AtomicBool>,
}

impl rmcp::ClientHandler for McpClientHandler {
    fn on_tool_list_changed(
        &self,
        _context: rmcp::service::NotificationContext<rmcp::RoleClient>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        self.catalog_dirty.store(true, Ordering::Release);
        std::future::ready(())
    }
}

type McpService = rmcp::service::RunningService<rmcp::RoleClient, McpClientHandler>;

struct McpConnection {
    service: McpService,
    child: Mutex<tokio::process::Child>,
    config: McpServerConfig,
}

struct McpEntry {
    id: String,
    config: McpServerConfig,
    state: RwLock<McpServerState>,
    tools: RwLock<Vec<McpToolDefinition>>,
    last_error: RwLock<Option<String>>,
    restart_attempts: Mutex<u8>,
    connection: Mutex<Option<Arc<McpConnection>>>,
    catalog_dirty: Arc<AtomicBool>,
}

/// Supervises configured stdio MCP servers. Configuration is trusted local
/// policy; discovered metadata and all subprocess output are treated as
/// untrusted and are never logged verbatim.
#[derive(Clone, Default)]
pub struct McpManager {
    entries: Arc<Mutex<BTreeMap<String, Arc<McpEntry>>>>,
}

impl McpManager {
    pub fn new(configs: BTreeMap<String, McpServerConfig>) -> Result<Self, McpError> {
        for config in configs.values() {
            config.validate()?;
        }
        let entries = configs
            .into_iter()
            .map(|(id, config)| {
                (
                    id.clone(),
                    Arc::new(McpEntry {
                        id: id.clone(),
                        config,
                        state: RwLock::new(McpServerState::Stopped),
                        tools: RwLock::new(Vec::new()),
                        last_error: RwLock::new(None),
                        restart_attempts: Mutex::new(0),
                        connection: Mutex::new(None),
                        catalog_dirty: Arc::new(AtomicBool::new(false)),
                    }),
                )
            })
            .collect();
        Ok(Self {
            entries: Arc::new(Mutex::new(entries)),
        })
    }

    pub async fn reconcile(
        &self,
        configs: BTreeMap<String, McpServerConfig>,
    ) -> Result<(), McpError> {
        let replacement = Self::new(configs)?;
        let previous = {
            let mut entries = self.entries.lock().await;
            std::mem::replace(&mut *entries, replacement.entries.lock().await.clone())
        };
        for entry in previous.into_values() {
            Self::stop_entry(&entry).await;
        }
        self.start_enabled().await;
        Ok(())
    }

    /// Eagerly attempts each enabled server once. A failed child is observable
    /// in diagnostics but does not invalidate the otherwise valid config.
    pub async fn start_enabled(&self) {
        let entries = self
            .entries
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries.into_iter().filter(|entry| entry.config.enabled) {
            let _ = Self::ensure_entry(entry).await;
        }
    }

    pub async fn diagnostics(&self) -> Vec<McpServerDiagnostics> {
        let entries = self.entries.lock().await.clone();
        let mut diagnostics = Vec::with_capacity(entries.len());
        for (id, entry) in entries {
            diagnostics.push(McpServerDiagnostics {
                id,
                enabled: entry.config.enabled,
                state: *entry.state.read().await,
                tools: entry.tools.read().await.clone(),
                last_error: entry.last_error.read().await.clone(),
                restart_attempts: *entry.restart_attempts.lock().await,
            });
        }
        diagnostics
    }

    pub async fn catalog(&self) -> Vec<McpToolDefinition> {
        self.start_enabled().await;
        let entries = self
            .entries
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut tools = entries;
        tools.sort_by(|left, right| left.id.cmp(&right.id));
        let mut catalog = Vec::new();
        for entry in tools {
            catalog.extend(entry.tools.read().await.clone());
        }
        let mut counts = BTreeMap::<String, usize>::new();
        for tool in &catalog {
            *counts.entry(tool.name.clone()).or_default() += 1;
        }
        catalog.retain(|tool| counts[&tool.name] == 1);
        catalog.sort_by(|left, right| left.name.cmp(&right.name));
        catalog
    }

    pub async fn call(
        &self,
        public_name: &str,
        arguments: &Value,
        cancellation: CancellationToken,
    ) -> Result<Value, McpError> {
        let entries = self.entries.lock().await.clone();
        let Some((entry, definition)) = entries.values().find_map(|entry| {
            entry.tools.try_read().ok().and_then(|tools| {
                tools
                    .iter()
                    .find(|tool| tool.name == public_name)
                    .cloned()
                    .map(|tool| (entry.clone(), tool))
            })
        }) else {
            return Err(McpError::ToolNotFound);
        };
        let connection = Self::ensure_entry(entry.clone()).await?;
        let validator = jsonschema::validator_for(&definition.input_schema)
            .map_err(|_| McpError::InvalidArguments)?;
        if !validator.is_valid(arguments) || !arguments.is_object() {
            return Err(McpError::InvalidArguments);
        }
        let params = rmcp::model::CallToolRequestParams::new(definition.server_tool_name)
            .with_arguments(arguments.as_object().cloned().expect("validated object"));
        let result = tokio::select! {
            _ = cancellation.cancelled() => {
                Self::stop_entry(&entry).await;
                return Err(McpError::Uncertain);
            },
            result = timeout(Duration::from_secs(connection.config.call_timeout_seconds), connection.service.peer().call_tool_once(params)) => result,
        };
        let result = match result {
            Ok(Ok(result)) => result,
            _ => {
                Self::stop_entry(&entry).await;
                return Err(McpError::Uncertain);
            }
        };
        let rmcp::model::CallToolResponse::Complete(result) = result else {
            Self::stop_entry(&entry).await;
            return Err(McpError::Uncertain);
        };
        let value = serde_json::to_value(result).map_err(|_| McpError::Uncertain)?;
        if serde_json::to_vec(&value)
            .map_err(|_| McpError::Uncertain)?
            .len()
            > connection.config.max_result_bytes
        {
            return Err(McpError::ResultTooLarge);
        }
        Ok(value)
    }

    pub async fn shutdown(&self) {
        let entries = self
            .entries
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            Self::stop_entry(&entry).await;
        }
    }

    async fn ensure_entry(entry: Arc<McpEntry>) -> Result<Arc<McpConnection>, McpError> {
        if !entry.config.enabled {
            return Err(McpError::Unavailable);
        }
        if let Some(connection) = entry.connection.lock().await.clone() {
            if entry.catalog_dirty.swap(false, Ordering::AcqRel) {
                Self::refresh_catalog(&entry, &connection).await?;
            }
            return Ok(connection);
        }
        let mut attempts = entry.restart_attempts.lock().await;
        if *attempts >= entry.config.max_restart_attempts && *attempts != 0 {
            return Err(McpError::Unavailable);
        }
        *entry.state.write().await = McpServerState::Starting;
        let mut command = Command::new(&entry.config.command);
        command
            .args(&entry.config.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear();
        for key in ["PATH", "HOME", "TMPDIR", "TEMP", "TMP"] {
            if let Some(value) = env::var_os(key) {
                command.env(key, value);
            }
        }
        for (child, host) in &entry.config.environment {
            if let Some(value) = env::var_os(host) {
                command.env(child, value);
            }
        }
        if let Some(cwd) = &entry.config.cwd {
            command.current_dir(cwd);
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(_) => return Self::fail(&entry, &mut attempts).await,
        };
        let stdout = child.stdout.take().ok_or(McpError::Unavailable)?;
        let stdin = child.stdin.take().ok_or(McpError::Unavailable)?;
        if let Some(mut stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut buffer = [0u8; 8192];
                while stderr
                    .read(&mut buffer)
                    .await
                    .ok()
                    .filter(|count| *count > 0)
                    .is_some()
                {}
            });
        }
        let service = match timeout(
            Duration::from_secs(entry.config.startup_timeout_seconds),
            rmcp::serve_client(
                McpClientHandler {
                    catalog_dirty: entry.catalog_dirty.clone(),
                },
                (stdout, stdin),
            ),
        )
        .await
        {
            Ok(Ok(service)) => service,
            _ => {
                let _ = child.start_kill();
                return Self::fail(&entry, &mut attempts).await;
            }
        };
        let connection = Arc::new(McpConnection {
            service,
            child: Mutex::new(child),
            config: entry.config.clone(),
        });
        if Self::refresh_catalog(&entry, &connection).await.is_err() {
            Self::stop_connection(&connection).await;
            return Self::fail(&entry, &mut attempts).await;
        }
        *entry.last_error.write().await = None;
        *entry.state.write().await = McpServerState::Healthy;
        *entry.connection.lock().await = Some(connection.clone());
        *attempts = 0;
        Ok(connection)
    }

    async fn fail<T>(entry: &Arc<McpEntry>, attempts: &mut u8) -> Result<T, McpError> {
        *attempts = attempts.saturating_add(1);
        *entry.state.write().await = McpServerState::Failed;
        *entry.last_error.write().await =
            Some("MCP server startup, handshake, or discovery failed".to_owned());
        Err(McpError::Unavailable)
    }

    async fn refresh_catalog(
        entry: &Arc<McpEntry>,
        connection: &Arc<McpConnection>,
    ) -> Result<(), McpError> {
        let discovered = connection
            .service
            .peer()
            .list_all_tools()
            .await
            .map_err(|_| McpError::Unavailable)?;
        *entry.tools.write().await = compile_catalog(&entry.id, discovered);
        Ok(())
    }

    async fn stop_entry(entry: &Arc<McpEntry>) {
        if let Some(connection) = entry.connection.lock().await.take() {
            Self::stop_connection(&connection).await;
        }
        *entry.state.write().await = McpServerState::Stopped;
    }

    async fn stop_connection(connection: &Arc<McpConnection>) {
        connection.service.cancellation_token().cancel();
        let mut child = connection.child.lock().await;
        let _ = child.start_kill();
        let _ = timeout(
            Duration::from_millis(connection.config.termination_grace_millis),
            child.wait(),
        )
        .await;
    }
}

fn compile_catalog(server_id: &str, tools: Vec<rmcp::model::Tool>) -> Vec<McpToolDefinition> {
    let mut output = Vec::new();
    for tool in tools {
        let public_name = format!("mcp__{server_id}__{}", tool.name);
        if public_name.len() > 64
            || !public_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            continue;
        }
        let schema = Value::Object((*tool.input_schema).clone());
        if jsonschema::validator_for(&schema).is_err() {
            continue;
        }
        output.push(McpToolDefinition {
            name: public_name,
            server_id: server_id.to_owned(),
            server_tool_name: tool.name.into_owned(),
            description: tool.description.map(|value| value.into_owned()),
            input_schema: schema,
        });
    }
    let mut counts = BTreeMap::<String, usize>::new();
    for tool in &output {
        *counts.entry(tool.name.clone()).or_default() += 1;
    }
    output.retain(|tool| counts[&tool.name] == 1);
    output
}

/// Native tools implemented by Piqo itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeTool {
    Read,
    Write,
    Edit,
    Bash,
}

impl NativeTool {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "read" => Some(Self::Read),
            "write" => Some(Self::Write),
            "edit" => Some(Self::Edit),
            "bash" => Some(Self::Bash),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Edit => "edit",
            Self::Bash => "bash",
        }
    }

    /// `edit` and `write` deliberately share Piqo's existing write capability.
    pub fn permission_name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write | Self::Edit => "write",
            Self::Bash => "bash",
        }
    }
}

/// Bounded native-tool execution settings. Values are local server policy,
/// rather than model-controlled arguments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeToolLimits {
    pub max_read_bytes: usize,
    pub max_read_lines: usize,
    pub max_write_bytes: usize,
    pub max_result_bytes: usize,
    pub max_result_lines: usize,
    pub bash_timeout: Duration,
    pub termination_grace: Duration,
}

impl Default for NativeToolLimits {
    fn default() -> Self {
        Self {
            max_read_bytes: 50 * 1024,
            max_read_lines: 2_000,
            max_write_bytes: 1024 * 1024,
            max_result_bytes: 50 * 1024,
            max_result_lines: 2_000,
            bash_timeout: Duration::from_secs(30),
            termination_grace: Duration::from_secs(1),
        }
    }
}

/// A resolved shell executable. The historical tool name remains `bash`, but
/// the executable can be PowerShell or cmd on Windows just like OpenCode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShellProgram {
    pub program: PathBuf,
}

impl ShellProgram {
    pub fn discover(configured: Option<&Path>) -> Result<Self, NativeToolError> {
        if let Some(configured) = configured {
            if configured.is_absolute() && configured.is_file() {
                return Ok(Self {
                    program: configured.to_path_buf(),
                });
            }
            return Err(NativeToolError::Unavailable);
        }
        #[cfg(windows)]
        {
            for candidate in [find_in_path("pwsh"), find_in_path("powershell"), git_bash()] {
                if let Some(program) = candidate {
                    return Ok(Self { program });
                }
            }
            if let Some(value) = std::env::var_os("COMSPEC") {
                return Ok(Self {
                    program: PathBuf::from(value),
                });
            }
            return Err(NativeToolError::Unavailable);
        }
        #[cfg(not(windows))]
        {
            if let Some(shell) = std::env::var_os("SHELL") {
                let shell = PathBuf::from(shell);
                if shell.is_absolute() && shell.is_file() {
                    return Ok(Self { program: shell });
                }
            }
            #[cfg(target_os = "macos")]
            if Path::new("/bin/zsh").is_file() {
                return Ok(Self {
                    program: PathBuf::from("/bin/zsh"),
                });
            }
            if let Some(program) = find_in_path("bash") {
                return Ok(Self { program });
            }
            if Path::new("/bin/sh").is_file() {
                return Ok(Self {
                    program: PathBuf::from("/bin/sh"),
                });
            }
            Err(NativeToolError::Unavailable)
        }
    }

    pub fn display_name(&self) -> String {
        self.program
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("shell")
            .to_ascii_lowercase()
    }

    fn arguments(&self, command: &str) -> Vec<String> {
        match self.display_name().as_str() {
            "cmd" => vec!["/c".into(), command.into()],
            "powershell" | "pwsh" => vec!["-NoProfile".into(), "-Command".into(), command.into()],
            _ => vec!["-c".into(), command.into()],
        }
    }
}

#[cfg(windows)]
fn git_bash() -> Option<PathBuf> {
    let git = find_in_path("git")?;
    let candidate = git.parent()?.parent()?.join("bin").join("bash.exe");
    candidate.is_file().then_some(candidate)
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    let extensions: Vec<OsString> = if cfg!(windows) {
        std::env::var_os("PATHEXT")
            .unwrap_or_else(|| OsString::from(".EXE;.CMD;.BAT"))
            .to_string_lossy()
            .split(';')
            .map(OsString::from)
            .collect()
    } else {
        vec![OsString::new()]
    };
    for directory in std::env::split_paths(&paths) {
        for extension in &extensions {
            let mut candidate = directory.join(name);
            candidate.push(extension);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

#[derive(Debug, Clone, PartialEq)]
enum NativeArguments {
    Read {
        file_path: String,
        offset: usize,
        limit: usize,
    },
    Write {
        file_path: String,
        content: String,
        mode: WriteMode,
    },
    Edit {
        file_path: String,
        old_string: String,
        new_string: String,
        replace_all: bool,
    },
    Bash {
        command: String,
        cwd: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteMode {
    Create,
    Overwrite,
}

impl NativeArguments {
    fn parse(
        tool: NativeTool,
        value: &Value,
        limits: &NativeToolLimits,
    ) -> Result<Self, NativeToolError> {
        let object = value.as_object().ok_or(NativeToolError::InvalidArguments)?;
        let string = |name: &str| {
            object
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or(NativeToolError::InvalidArguments)
        };
        let positive = |name: &str, default: usize| match object.get(name) {
            None => Ok(default),
            Some(value) => value
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or(NativeToolError::InvalidArguments),
        };
        match tool {
            NativeTool::Read => Ok(Self::Read {
                file_path: string("filePath")?,
                offset: positive("offset", 1)?,
                limit: positive("limit", limits.max_read_lines)?.min(limits.max_read_lines),
            }),
            NativeTool::Write => {
                let content = string("content")?;
                if content.len() > limits.max_write_bytes {
                    return Err(NativeToolError::TooLarge);
                }
                let mode = match string("mode")?.as_str() {
                    "create" => WriteMode::Create,
                    "overwrite" => WriteMode::Overwrite,
                    _ => return Err(NativeToolError::InvalidArguments),
                };
                Ok(Self::Write {
                    file_path: string("filePath")?,
                    content,
                    mode,
                })
            }
            NativeTool::Edit => {
                let old_string = string("oldString")?;
                let new_string = string("newString")?;
                if old_string.is_empty()
                    || old_string == new_string
                    || old_string.len() + new_string.len() > limits.max_write_bytes
                {
                    return Err(NativeToolError::InvalidArguments);
                }
                Ok(Self::Edit {
                    file_path: string("filePath")?,
                    old_string,
                    new_string,
                    replace_all: object
                        .get("replaceAll")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                })
            }
            NativeTool::Bash => {
                let command = string("command")?;
                if command.len() > limits.max_write_bytes {
                    return Err(NativeToolError::TooLarge);
                }
                let cwd = match object.get("cwd") {
                    None => None,
                    Some(value) => Some(
                        value
                            .as_str()
                            .map(str::to_owned)
                            .ok_or(NativeToolError::InvalidArguments)?,
                    ),
                };
                Ok(Self::Bash { command, cwd })
            }
        }
    }
}

/// Executes native tools under one selected project workspace.
#[derive(Debug, Clone)]
pub struct NativeExecutor {
    workspace: PathBuf,
    limits: NativeToolLimits,
    shell: ShellProgram,
}

impl NativeExecutor {
    pub fn new(
        workspace: PathBuf,
        limits: NativeToolLimits,
        shell: ShellProgram,
    ) -> Result<Self, NativeToolError> {
        let workspace = fs::canonicalize(workspace).map_err(|_| NativeToolError::NotFound)?;
        if !workspace.is_dir() {
            return Err(NativeToolError::NotDirectory);
        }
        Ok(Self {
            workspace,
            limits,
            shell,
        })
    }

    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    pub fn shell(&self) -> &ShellProgram {
        &self.shell
    }

    pub async fn execute(
        &self,
        tool: NativeTool,
        arguments: &Value,
        cancellation: CancellationToken,
    ) -> Value {
        let parsed = match NativeArguments::parse(tool, arguments, &self.limits) {
            Ok(parsed) => parsed,
            Err(error) => return error.result(),
        };
        let result = match parsed {
            NativeArguments::Read {
                file_path,
                offset,
                limit,
            } => {
                self.blocking(move |executor| executor.read(&file_path, offset, limit))
                    .await
            }
            NativeArguments::Write {
                file_path,
                content,
                mode,
            } => {
                self.blocking(move |executor| executor.write(&file_path, &content, mode))
                    .await
            }
            NativeArguments::Edit {
                file_path,
                old_string,
                new_string,
                replace_all,
            } => {
                self.blocking(move |executor| {
                    executor.edit(&file_path, &old_string, &new_string, replace_all)
                })
                .await
            }
            NativeArguments::Bash { command, cwd } => {
                return self.bash(&command, cwd.as_deref(), cancellation).await
            }
        };
        result.unwrap_or_else(NativeToolError::result)
    }

    async fn blocking<T>(
        &self,
        operation: impl FnOnce(Self) -> Result<T, NativeToolError> + Send + 'static,
    ) -> Result<Value, NativeToolError>
    where
        T: Send + Into<Value> + 'static,
    {
        let executor = self.clone();
        tokio::task::spawn_blocking(move || operation(executor))
            .await
            .map_err(|_| NativeToolError::Io)?
            .map(Into::into)
    }

    fn resolve(&self, input: &str, allow_missing_leaf: bool) -> Result<PathBuf, NativeToolError> {
        let input = Path::new(input);
        let relative = if input.is_absolute() {
            input
                .strip_prefix(&self.workspace)
                .map_err(|_| NativeToolError::OutsideWorkspace)?
        } else {
            input
        };
        let mut resolved = self.workspace.clone();
        let components: Vec<_> = relative.components().collect();
        if components.is_empty() {
            return Err(NativeToolError::InvalidArguments);
        }
        for (index, component) in components.iter().enumerate() {
            match component {
                Component::Normal(part) => resolved.push(part),
                Component::CurDir => continue,
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(NativeToolError::OutsideWorkspace)
                }
            }
            match fs::symlink_metadata(&resolved) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(NativeToolError::SymlinkNotAllowed)
                }
                Ok(metadata) if index + 1 != components.len() && !metadata.is_dir() => {
                    return Err(NativeToolError::NotDirectory)
                }
                Ok(_) => {}
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound
                        && allow_missing_leaf
                        && index + 1 == components.len() => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(NativeToolError::NotFound)
                }
                Err(_) => return Err(NativeToolError::Io),
            }
        }
        Ok(resolved)
    }

    fn read(&self, file_path: &str, offset: usize, limit: usize) -> Result<Value, NativeToolError> {
        let path = self.resolve(file_path, false)?;
        let metadata = fs::metadata(&path).map_err(map_io)?;
        if !metadata.is_file() {
            return Err(NativeToolError::NotRegularFile);
        }
        let mut bytes = Vec::with_capacity(self.limits.max_read_bytes.min(metadata.len() as usize));
        File::open(&path)
            .map_err(map_io)?
            .take((self.limits.max_read_bytes + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| NativeToolError::Io)?;
        let capped_by_bytes = bytes.len() > self.limits.max_read_bytes;
        bytes.truncate(self.limits.max_read_bytes);
        if bytes.contains(&0) {
            return Err(NativeToolError::BinaryContent);
        }
        let text = String::from_utf8(bytes).map_err(|_| NativeToolError::BinaryContent)?;
        let all_lines: Vec<_> = text.lines().collect();
        let start = offset.saturating_sub(1);
        let selected: Vec<_> = all_lines.iter().skip(start).take(limit).copied().collect();
        let more_lines = all_lines.len() > start.saturating_add(selected.len());
        Ok(json!({
            "path": file_path,
            "text": selected.join("\n"),
            "line_start": offset,
            "line_end": offset.saturating_add(selected.len()).saturating_sub(1),
            "total_lines": all_lines.len(),
            "truncated": capped_by_bytes || more_lines,
        }))
    }

    fn write(
        &self,
        file_path: &str,
        content: &str,
        mode: WriteMode,
    ) -> Result<Value, NativeToolError> {
        let path = self.resolve(file_path, true)?;
        let exists = path.exists();
        match (mode, exists) {
            (WriteMode::Create, true) => return Err(NativeToolError::AlreadyExists),
            (WriteMode::Overwrite, false) => return Err(NativeToolError::NotFound),
            _ => {}
        }
        atomic_write(&path, content.as_bytes(), matches!(mode, WriteMode::Create))?;
        Ok(json!({
            "path": file_path,
            "mode": match mode { WriteMode::Create => "create", WriteMode::Overwrite => "overwrite" },
            "bytes_written": content.len()
        }))
    }

    fn edit(
        &self,
        file_path: &str,
        old_string: &str,
        new_string: &str,
        replace_all: bool,
    ) -> Result<Value, NativeToolError> {
        let path = self.resolve(file_path, false)?;
        let bytes = fs::read(&path).map_err(map_io)?;
        if bytes.contains(&0) {
            return Err(NativeToolError::BinaryContent);
        }
        let source = String::from_utf8(bytes).map_err(|_| NativeToolError::BinaryContent)?;
        let matches = source.match_indices(old_string).count();
        if matches == 0 || (!replace_all && matches != 1) {
            return Err(NativeToolError::Conflict);
        }
        let ending = if source.contains("\r\n") {
            "\r\n"
        } else {
            "\n"
        };
        let old = old_string.replace("\r\n", "\n").replace('\n', ending);
        let new = new_string.replace("\r\n", "\n").replace('\n', ending);
        let next = if replace_all {
            source.replace(&old, &new)
        } else {
            source.replacen(&old, &new, 1)
        };
        if next.len() > self.limits.max_write_bytes {
            return Err(NativeToolError::TooLarge);
        }
        atomic_write(&path, next.as_bytes(), false)?;
        Ok(json!({
            "path": file_path,
            "replacements": if replace_all { matches } else { 1 },
            "bytes_written": next.len()
        }))
    }

    async fn bash(
        &self,
        command: &str,
        cwd: Option<&str>,
        cancellation: CancellationToken,
    ) -> Value {
        let cwd = match cwd {
            Some(cwd) => self.resolve(cwd, false),
            None => Ok(self.workspace.clone()),
        };
        let cwd = match cwd {
            Ok(path) if path.is_dir() => path,
            Ok(_) => return NativeToolError::NotDirectory.result(),
            Err(error) => return error.result(),
        };
        let mut process = Command::new(&self.shell.program);
        process
            .args(self.shell.arguments(command))
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        controlled_environment(&mut process);
        #[cfg(unix)]
        unsafe {
            process.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = match process.spawn() {
            Ok(child) => child,
            Err(_) => return NativeToolError::SpawnFailed.result(),
        };
        let pid = child.id();
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");
        let stdout_task = tokio::spawn(read_limited(
            stdout,
            self.limits.max_result_bytes,
            self.limits.max_result_lines,
        ));
        let stderr_task = tokio::spawn(read_limited(
            stderr,
            self.limits.max_result_bytes,
            self.limits.max_result_lines,
        ));
        enum Outcome {
            Exited(std::process::ExitStatus),
            Cancelled,
            TimedOut,
        }
        let outcome = tokio::select! {
            status = child.wait() => status.map(Outcome::Exited).unwrap_or(Outcome::Cancelled),
            _ = cancellation.cancelled() => Outcome::Cancelled,
            _ = tokio::time::sleep(self.limits.bash_timeout) => Outcome::TimedOut,
        };
        if !matches!(outcome, Outcome::Exited(_)) {
            terminate_process(&mut child, pid, self.limits.termination_grace).await;
        }
        let stdout = stdout_task
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_else(LimitedOutput::empty);
        let stderr = stderr_task
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_else(LimitedOutput::empty);
        match outcome {
            Outcome::Exited(status) => json!({
                "status": "exited",
                "exit_code": status.code(),
                "success": status.success(),
                "stdout": stdout.text,
                "stderr": stderr.text,
                "stdout_truncated": stdout.truncated,
                "stderr_truncated": stderr.truncated,
            }),
            Outcome::Cancelled => json!({
                "error": {"code": "cancelled"},
                "stdout": stdout.text,
                "stderr": stderr.text,
                "stdout_truncated": stdout.truncated,
                "stderr_truncated": stderr.truncated,
            }),
            Outcome::TimedOut => json!({
                "error": {"code": "timed_out"},
                "stdout": stdout.text,
                "stderr": stderr.text,
                "stdout_truncated": stdout.truncated,
                "stderr_truncated": stderr.truncated,
            }),
        }
    }
}

fn atomic_write(path: &Path, content: &[u8], create_only: bool) -> Result<(), NativeToolError> {
    let parent = path.parent().ok_or(NativeToolError::NotDirectory)?;
    if !parent.is_dir() {
        return Err(NativeToolError::NotDirectory);
    }
    let temporary = parent.join(format!(".piqo-{}.tmp", Uuid::now_v7()));
    let write_result = (|| -> Result<(), NativeToolError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| NativeToolError::Io)?;
        file.write_all(content).map_err(|_| NativeToolError::Io)?;
        file.sync_all().map_err(|_| NativeToolError::Io)?;
        if create_only {
            fs::hard_link(&temporary, path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    NativeToolError::AlreadyExists
                } else {
                    NativeToolError::Io
                }
            })?;
            fs::remove_file(&temporary).map_err(|_| NativeToolError::Io)?;
        } else {
            fs::rename(&temporary, path).map_err(|_| NativeToolError::Io)?;
        }
        Ok(())
    })();
    if temporary.exists() {
        let _ = fs::remove_file(temporary);
    }
    write_result
}

fn map_io(error: std::io::Error) -> NativeToolError {
    match error.kind() {
        std::io::ErrorKind::NotFound => NativeToolError::NotFound,
        _ => NativeToolError::Io,
    }
}

fn controlled_environment(command: &mut Command) {
    command.env_clear();
    #[cfg(unix)]
    for name in ["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    #[cfg(windows)]
    for name in [
        "PATH",
        "PATHEXT",
        "SYSTEMROOT",
        "COMSPEC",
        "TEMP",
        "TMP",
        "USERPROFILE",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
}

struct LimitedOutput {
    text: String,
    truncated: bool,
}

impl LimitedOutput {
    fn empty() -> Self {
        Self {
            text: String::new(),
            truncated: false,
        }
    }
}

async fn read_limited<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    max_bytes: usize,
    max_lines: usize,
) -> Result<LimitedOutput, std::io::Error> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let remaining = max_bytes.saturating_sub(bytes.len());
        if read > remaining {
            bytes.extend_from_slice(&buffer[..remaining]);
            truncated = true;
        } else {
            bytes.extend_from_slice(&buffer[..read]);
        }
        if bytes.len() >= max_bytes {
            truncated = true;
        }
    }
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    let lines = text.lines().take(max_lines).collect::<Vec<_>>();
    if text.lines().count() > lines.len() {
        truncated = true;
    }
    text = lines.join("\n");
    Ok(LimitedOutput { text, truncated })
}

async fn terminate_process(child: &mut tokio::process::Child, pid: Option<u32>, grace: Duration) {
    #[cfg(unix)]
    if let Some(pid) = pid {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGTERM);
        }
        let _ = tokio::time::timeout(grace, child.wait()).await;
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
        let _ = child.wait().await;
        return;
    }
    #[cfg(windows)]
    if let Some(pid) = pid {
        let _ = Command::new("taskkill")
            .args(["/pid", &pid.to_string(), "/f", "/t"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
        let _ = child.wait().await;
        return;
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[derive(Debug, Clone, Copy, Error)]
pub enum NativeToolError {
    #[error("invalid tool arguments")]
    InvalidArguments,
    #[error("path is outside the workspace")]
    OutsideWorkspace,
    #[error("symbolic links are not allowed")]
    SymlinkNotAllowed,
    #[error("path was not found")]
    NotFound,
    #[error("path is not a regular file")]
    NotRegularFile,
    #[error("path is not a directory")]
    NotDirectory,
    #[error("content is binary or invalid UTF-8")]
    BinaryContent,
    #[error("file already exists")]
    AlreadyExists,
    #[error("edit precondition failed")]
    Conflict,
    #[error("content exceeds the configured limit")]
    TooLarge,
    #[error("native tool is unavailable")]
    Unavailable,
    #[error("process could not be spawned")]
    SpawnFailed,
    #[error("I/O operation failed")]
    Io,
}

impl NativeToolError {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidArguments => "invalid_arguments",
            Self::OutsideWorkspace => "outside_workspace",
            Self::SymlinkNotAllowed => "symlink_not_allowed",
            Self::NotFound => "not_found",
            Self::NotRegularFile => "not_regular_file",
            Self::NotDirectory => "not_directory",
            Self::BinaryContent => "binary_content",
            Self::AlreadyExists => "already_exists",
            Self::Conflict => "conflict",
            Self::TooLarge => "too_large",
            Self::Unavailable => "unavailable",
            Self::SpawnFailed => "spawn_failed",
            Self::Io => "io_failure",
        }
    }

    pub fn result(self) -> Value {
        json!({"error": {"code": self.code()}})
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use serde_json::json;
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::{McpServerConfig, NativeExecutor, NativeTool, NativeToolLimits, ShellProgram};

    #[test]
    fn mcp_configuration_uses_bounded_safe_defaults() {
        let config = McpServerConfig::new("fixture", ["--stdio"]);
        assert!(config.enabled);
        assert_eq!(config.startup_timeout_seconds, 10);
        assert_eq!(config.call_timeout_seconds, 30);
        assert_eq!(config.max_result_bytes, 50 * 1024);
        assert!(config.validate().is_ok());
    }

    fn executor(root: &std::path::Path) -> NativeExecutor {
        NativeExecutor::new(
            root.to_path_buf(),
            NativeToolLimits {
                bash_timeout: Duration::from_secs(2),
                ..NativeToolLimits::default()
            },
            ShellProgram::discover(None).expect("a test shell is available"),
        )
        .expect("workspace is accepted")
    }

    #[tokio::test]
    async fn read_write_and_edit_stay_inside_workspace() {
        let root = tempdir().expect("temp root");
        let executor = executor(root.path());
        let cancellation = CancellationToken::new();

        let created = executor
            .execute(
                NativeTool::Write,
                &json!({"filePath":"note.txt","content":"first\nsecond\n","mode":"create"}),
                cancellation.clone(),
            )
            .await;
        assert_eq!(created["mode"], "create");
        let read = executor
            .execute(
                NativeTool::Read,
                &json!({"filePath":"note.txt","offset":2,"limit":1}),
                cancellation.clone(),
            )
            .await;
        assert_eq!(read["text"], "second");
        let edited = executor
            .execute(
                NativeTool::Edit,
                &json!({"filePath":"note.txt","oldString":"second","newString":"third"}),
                cancellation.clone(),
            )
            .await;
        assert_eq!(edited["replacements"], 1);
        assert_eq!(
            fs::read_to_string(root.path().join("note.txt")).unwrap(),
            "first\nthird\n"
        );

        let outside = executor
            .execute(
                NativeTool::Write,
                &json!({"filePath":"../outside.txt","content":"no","mode":"create"}),
                cancellation,
            )
            .await;
        assert_eq!(outside["error"]["code"], "outside_workspace");
    }

    #[tokio::test]
    async fn edit_requires_an_exact_single_match_and_binary_is_rejected() {
        let root = tempdir().expect("temp root");
        fs::write(root.path().join("same.txt"), "one one").unwrap();
        fs::write(root.path().join("binary.bin"), b"a\0b").unwrap();
        let executor = executor(root.path());
        let cancellation = CancellationToken::new();
        let conflict = executor
            .execute(
                NativeTool::Edit,
                &json!({"filePath":"same.txt","oldString":"one","newString":"two"}),
                cancellation.clone(),
            )
            .await;
        assert_eq!(conflict["error"]["code"], "conflict");
        let binary = executor
            .execute(
                NativeTool::Read,
                &json!({"filePath":"binary.bin"}),
                cancellation,
            )
            .await;
        assert_eq!(binary["error"]["code"], "binary_content");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_and_provider_secrets_are_not_usable() {
        use std::os::unix::fs::symlink;

        let root = tempdir().expect("temp root");
        let outside = tempdir().expect("outside root");
        fs::write(outside.path().join("secret.txt"), "secret").unwrap();
        symlink(outside.path(), root.path().join("escape")).unwrap();
        let executor = executor(root.path());
        let cancellation = CancellationToken::new();
        let escaped = executor
            .execute(
                NativeTool::Read,
                &json!({"filePath":"escape/secret.txt"}),
                cancellation.clone(),
            )
            .await;
        assert_eq!(escaped["error"]["code"], "symlink_not_allowed");

        std::env::set_var("OPENAI_API_KEY", "must-not-reach-native-shell");
        let shell = executor
            .execute(
                NativeTool::Bash,
                &json!({"command":"test -z \"$OPENAI_API_KEY\""}),
                cancellation,
            )
            .await;
        std::env::remove_var("OPENAI_API_KEY");
        assert_eq!(shell["success"], true);
    }

    #[tokio::test]
    async fn bash_timeout_and_cancellation_have_structured_results() {
        let root = tempdir().expect("temp root");
        let executor = executor(root.path());
        let timeout = executor
            .execute(
                NativeTool::Bash,
                &json!({"command":"sleep 5"}),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(timeout["error"]["code"], "timed_out");

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let cancelled = executor
            .execute(
                NativeTool::Bash,
                &json!({"command":"sleep 5"}),
                cancellation,
            )
            .await;
        assert_eq!(cancelled["error"]["code"], "cancelled");
    }
}
