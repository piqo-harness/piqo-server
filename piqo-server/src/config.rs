use std::{
    collections::HashMap,
    env,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use chrono::{SecondsFormat, Utc};
use piqo_provider::{ProviderProtocol, ProviderTransport};
use piqo_tools::NativeToolLimits;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use toml_edit::{value, Array, DocumentMut, Item, Table};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PiqoConfig {
    #[serde(default)]
    pub defaults: BodyLayer,
    #[serde(default)]
    pub providers: HashMap<String, ProviderConfig>,
    #[serde(default)]
    pub models: HashMap<String, BodyLayer>,
    #[serde(default)]
    pub agents: HashMap<String, AgentConfigOverride>,
    #[serde(default)]
    pub variants: HashMap<String, BodyLayer>,
    #[serde(default)]
    pub native_tools: NativeToolsConfig,
    #[serde(skip)]
    markdown_agents: HashMap<String, AgentDefinition>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeToolsConfig {
    pub max_read_bytes: Option<usize>,
    pub max_read_lines: Option<usize>,
    pub max_write_bytes: Option<usize>,
    pub max_result_bytes: Option<usize>,
    pub max_result_lines: Option<usize>,
    pub bash_timeout_seconds: Option<u64>,
    pub termination_grace_millis: Option<u64>,
    pub shell: Option<PathBuf>,
}

impl NativeToolsConfig {
    pub fn limits(&self) -> NativeToolLimits {
        let defaults = NativeToolLimits::default();
        NativeToolLimits {
            max_read_bytes: self.max_read_bytes.unwrap_or(defaults.max_read_bytes),
            max_read_lines: self.max_read_lines.unwrap_or(defaults.max_read_lines),
            max_write_bytes: self.max_write_bytes.unwrap_or(defaults.max_write_bytes),
            max_result_bytes: self.max_result_bytes.unwrap_or(defaults.max_result_bytes),
            max_result_lines: self.max_result_lines.unwrap_or(defaults.max_result_lines),
            bash_timeout: Duration::from_secs(
                self.bash_timeout_seconds
                    .unwrap_or(defaults.bash_timeout.as_secs()),
            ),
            termination_grace: Duration::from_millis(
                self.termination_grace_millis
                    .unwrap_or(defaults.termination_grace.as_millis() as u64),
            ),
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        for (name, value) in [
            ("max_read_bytes", self.max_read_bytes),
            ("max_read_lines", self.max_read_lines),
            ("max_write_bytes", self.max_write_bytes),
            ("max_result_bytes", self.max_result_bytes),
            ("max_result_lines", self.max_result_lines),
        ] {
            if value == Some(0) {
                return Err(ConfigError::InvalidNativeTools(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        if self.bash_timeout_seconds == Some(0) || self.termination_grace_millis == Some(0) {
            return Err(ConfigError::InvalidNativeTools(
                "shell durations must be greater than zero".to_owned(),
            ));
        }
        if self.shell.as_ref().is_some_and(|path| !path.is_absolute()) {
            return Err(ConfigError::InvalidNativeTools(
                "shell must be an absolute path".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BodyLayer {
    #[serde(default)]
    pub body: Value,
    #[serde(default = "default_max_model_turns")]
    pub max_model_turns: u32,
}

impl Default for BodyLayer {
    fn default() -> Self {
        Self {
            body: Value::Null,
            max_model_turns: default_max_model_turns(),
        }
    }
}

fn default_max_model_turns() -> u32 {
    32
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AgentConfigOverride {
    pub description: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub instructions: Option<String>,
    pub permissions: Option<AgentPermissions>,
    #[serde(default)]
    pub body: Value,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentPermissions {
    pub read: Option<PermissionSetting>,
    pub write: Option<PermissionSetting>,
    pub bash: Option<PermissionSetting>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum PermissionSetting {
    Allow,
    Ask,
    Deny,
}

#[derive(Debug, Clone, Default)]
pub struct AgentDefinition {
    pub id: String,
    pub description: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub instructions: Option<String>,
    pub permissions: AgentPermissions,
    markdown_body: Value,
    toml_body: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentFrontMatter {
    description: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    permissions: Option<AgentPermissions>,
    #[serde(default)]
    body: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ProviderConfig {
    pub base_url: String,
    #[serde(default = "default_protocol")]
    pub protocol: String,
    pub api_key: Option<String>,
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_seconds: u64,
    #[serde(default)]
    pub models: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct ResolvedProvider {
    pub name: String,
    pub endpoint: String,
    pub models_endpoint: String,
    pub protocol: ProviderProtocol,
    pub headers: HashMap<String, String>,
    pub connect_timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderCredentialInput {
    None,
    ApiKey {
        #[schema(write_only)]
        value: String,
    },
    Environment {
        variable: String,
    },
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderCredentialSummary {
    None,
    ApiKey,
    Environment { variable: String },
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateProviderRequest {
    pub name: String,
    pub base_url: String,
    pub protocol: Option<String>,
    pub credentials: Option<ProviderCredentialInput>,
    #[schema(write_only)]
    pub headers: Option<HashMap<String, String>>,
    pub connect_timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct UpdateProviderRequest {
    pub base_url: Option<String>,
    pub protocol: Option<String>,
    pub credentials: Option<ProviderCredentialInput>,
    #[schema(write_only)]
    pub headers: Option<HashMap<String, String>>,
    pub connect_timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct ReplaceProviderModelsRequest {
    pub models: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ModelSource {
    Manual,
    Discovery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryStatus {
    Pending,
    Succeeded,
    Failed,
    NotApplicable,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ModelDiscovery {
    pub status: DiscoveryStatus,
    pub last_attempt_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProviderCatalogEntry {
    pub name: String,
    pub base_url: String,
    pub protocol: String,
    pub connect_timeout_seconds: u64,
    pub credentials: ProviderCredentialSummary,
    pub header_names: Vec<String>,
    pub streaming: bool,
    pub non_streaming: bool,
    pub models: Vec<String>,
    pub model_source: ModelSource,
    pub discovery: ModelDiscovery,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProviderModelsResponse {
    pub provider: String,
    pub models: Vec<String>,
    pub source: ModelSource,
    pub discovery: ModelDiscovery,
}

#[derive(Debug, Clone)]
pub struct ConfigSnapshot {
    pub revision: u64,
    pub loaded_at: String,
    pub config: Arc<PiqoConfig>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to write configuration {path}: {source}")]
    Write {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse TOML configuration: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("failed to read agent definition {path}: {source}")]
    AgentRead {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid agent definition {path}: {reason}")]
    InvalidAgentDefinition { path: String, reason: String },
    #[error("agent {0} is not configured")]
    AgentNotFound(String),
    #[error("failed to edit TOML configuration: {0}")]
    Edit(#[from] toml_edit::TomlError),
    #[error("provider {0} is not configured")]
    ProviderNotFound(String),
    #[error("provider {0} already exists")]
    ProviderAlreadyExists(String),
    #[error("provider {0} has a manual model override")]
    ManualModelOverride(String),
    #[error("provider {0} has both api_key and api_key_env")]
    ConflictingCredentials(String),
    #[error("environment variable {variable} for provider {provider} is not set")]
    MissingCredential { provider: String, variable: String },
    #[error("provider {provider} uses an invalid protocol: {source}")]
    InvalidProtocol {
        provider: String,
        source: piqo_provider::ProviderTransportError,
    },
    #[error("invalid provider configuration: {0}")]
    InvalidProvider(String),
    #[error("invalid native tool configuration: {0}")]
    InvalidNativeTools(String),
    #[error("configuration is read-only in this server instance")]
    ReadOnly,
    #[error("configuration state lock was poisoned")]
    LockPoisoned,
    #[error("configuration task failed: {0}")]
    Task(String),
    #[error("configuration revision is exhausted")]
    RevisionExhausted,
}

#[derive(Debug, Clone)]
struct DiscoveryCache {
    models: Vec<String>,
    discovery: ModelDiscovery,
}

struct ConfigManagerInner {
    path: Option<PathBuf>,
    config: RwLock<ConfigSnapshot>,
    mutation: Mutex<()>,
    discovery: RwLock<HashMap<String, DiscoveryCache>>,
    transport: ProviderTransport,
}

#[derive(Clone)]
pub struct ConfigManager {
    inner: Arc<ConfigManagerInner>,
}

impl PiqoConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_ref = path.as_ref();
        let mut config = if path_ref.exists() {
            let text = fs::read_to_string(path_ref).map_err(|source| ConfigError::Read {
                path: path_ref.display().to_string(),
                source,
            })?;
            toml::from_str(&text)?
        } else {
            Self::default()
        };
        config.load_markdown_agents(path_ref)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        for (name, provider) in &self.providers {
            validate_provider_name(name)?;
            provider.validate(name)?;
        }
        self.native_tools.validate()?;
        Ok(())
    }

    fn load_markdown_agents(&mut self, config_path: &Path) -> Result<(), ConfigError> {
        let directory = config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("agents");
        if !directory.exists() {
            return Ok(());
        }
        let entries = fs::read_dir(&directory).map_err(|source| ConfigError::AgentRead {
            path: directory.display().to_string(),
            source,
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| ConfigError::AgentRead {
                path: directory.display().to_string(),
                source,
            })?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("md") {
                continue;
            }
            if !entry
                .file_type()
                .map_err(|source| ConfigError::AgentRead {
                    path: path.display().to_string(),
                    source,
                })?
                .is_file()
            {
                continue;
            }
            let id = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .ok_or_else(|| ConfigError::InvalidAgentDefinition {
                    path: path.display().to_string(),
                    reason: "file name must be valid UTF-8".to_owned(),
                })?;
            if !valid_agent_id(id) {
                return Err(ConfigError::InvalidAgentDefinition {
                    path: path.display().to_string(),
                    reason: "file name must use letters, digits, underscores, or hyphens"
                        .to_owned(),
                });
            }
            let text = fs::read_to_string(&path).map_err(|source| ConfigError::AgentRead {
                path: path.display().to_string(),
                source,
            })?;
            let (front_matter, instructions) = split_front_matter(&text).map_err(|reason| {
                ConfigError::InvalidAgentDefinition {
                    path: path.display().to_string(),
                    reason,
                }
            })?;
            let front_matter: AgentFrontMatter =
                serde_yaml::from_str(front_matter).map_err(|error| {
                    ConfigError::InvalidAgentDefinition {
                        path: path.display().to_string(),
                        reason: error.to_string(),
                    }
                })?;
            let definition = AgentDefinition {
                id: id.to_owned(),
                description: front_matter.description,
                provider: front_matter.provider,
                model: front_matter.model,
                instructions: (!instructions.trim().is_empty()).then(|| instructions.to_owned()),
                permissions: front_matter.permissions.unwrap_or_default(),
                markdown_body: front_matter.body,
                toml_body: Value::Null,
            };
            if self
                .markdown_agents
                .insert(id.to_owned(), definition)
                .is_some()
            {
                return Err(ConfigError::InvalidAgentDefinition {
                    path: path.display().to_string(),
                    reason: format!("duplicate agent {id}"),
                });
            }
        }
        Ok(())
    }

    pub fn agent(&self, name: &str) -> Result<AgentDefinition, ConfigError> {
        let mut agent =
            self.markdown_agents
                .get(name)
                .cloned()
                .unwrap_or_else(|| AgentDefinition {
                    id: name.to_owned(),
                    ..AgentDefinition::default()
                });
        match self.agents.get(name) {
            Some(override_config) => {
                agent.description = override_config.description.clone().or(agent.description);
                agent.provider = override_config.provider.clone().or(agent.provider);
                agent.model = override_config.model.clone().or(agent.model);
                agent.instructions = override_config.instructions.clone().or(agent.instructions);
                merge_permissions(&mut agent.permissions, override_config.permissions.as_ref());
                agent.toml_body = override_config.body.clone();
            }
            None if !self.markdown_agents.contains_key(name) => {
                return Err(ConfigError::AgentNotFound(name.to_owned()));
            }
            None => {}
        }
        Ok(agent)
    }

    pub fn agents(&self) -> Vec<AgentDefinition> {
        let mut names: Vec<_> = self
            .markdown_agents
            .keys()
            .chain(self.agents.keys())
            .collect();
        names.sort();
        names.dedup();
        names
            .into_iter()
            .filter_map(|name| self.agent(name).ok())
            .collect()
    }

    pub fn resolve_provider(&self, name: &str) -> Result<ResolvedProvider, ConfigError> {
        let config = self
            .providers
            .get(name)
            .ok_or_else(|| ConfigError::ProviderNotFound(name.to_owned()))?;
        config.resolve(name)
    }

    pub fn body_layers(
        &self,
        model: &str,
        agent: Option<&str>,
        variant: Option<&str>,
        request: Value,
    ) -> Vec<Value> {
        let mut layers = vec![normalize_body(&self.defaults.body)];
        if let Some(layer) = self.models.get(model) {
            layers.push(normalize_body(&layer.body));
        }
        if let Some(name) = agent {
            if let Ok(agent) = self.agent(name) {
                if self.markdown_agents.contains_key(name) {
                    layers.push(normalize_body(&agent.markdown_body));
                }
                if self.agents.contains_key(name) {
                    layers.push(normalize_body(&agent.toml_body));
                }
            }
        }
        if let Some(name) = variant {
            if let Some(layer) = self.variants.get(name) {
                layers.push(normalize_body(&layer.body));
            }
        }
        layers.push(normalize_body(&request));
        layers
    }
}

fn merge_permissions(
    target: &mut AgentPermissions,
    override_permissions: Option<&AgentPermissions>,
) {
    let Some(override_permissions) = override_permissions else {
        return;
    };
    target.read = override_permissions.read.or(target.read);
    target.write = override_permissions.write.or(target.write);
    target.bash = override_permissions.bash.or(target.bash);
}

fn valid_agent_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn split_front_matter(text: &str) -> Result<(&str, &str), String> {
    let opening_end = text
        .find('\n')
        .ok_or_else(|| "front matter must start with a --- delimiter".to_owned())?;
    if text[..opening_end].trim_end_matches('\r') != "---" {
        return Err("front matter must start with a --- delimiter".to_owned());
    }
    let remaining = &text[opening_end + 1..];
    let mut offset = 0;
    for line in remaining.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == "---" {
            return Ok((&remaining[..offset], &remaining[offset + line.len()..]));
        }
        offset += line.len();
    }
    Err("front matter must end with a --- delimiter".to_owned())
}

impl ProviderConfig {
    fn from_create(request: &CreateProviderRequest) -> Result<Self, ConfigError> {
        let (api_key, api_key_env) = credential_values(request.credentials.as_ref())?;
        let config = Self {
            base_url: request.base_url.trim().to_owned(),
            protocol: request.protocol.clone().unwrap_or_else(default_protocol),
            api_key,
            api_key_env,
            headers: request.headers.clone().unwrap_or_default(),
            connect_timeout_seconds: request
                .connect_timeout_seconds
                .unwrap_or_else(default_connect_timeout),
            models: None,
        };
        config.validate(&request.name)?;
        Ok(config)
    }

    fn validate(&self, name: &str) -> Result<(), ConfigError> {
        if self.api_key.is_some() && self.api_key_env.is_some() {
            return Err(ConfigError::ConflictingCredentials(name.to_owned()));
        }
        let url = reqwest::Url::parse(&self.base_url)
            .map_err(|error| ConfigError::InvalidProvider(error.to_string()))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ConfigError::InvalidProvider(
                "base_url must use http or https".to_owned(),
            ));
        }
        if self.connect_timeout_seconds == 0 {
            return Err(ConfigError::InvalidProvider(
                "connect_timeout_seconds must be greater than zero".to_owned(),
            ));
        }
        if self.connect_timeout_seconds > i64::MAX as u64 {
            return Err(ConfigError::InvalidProvider(
                "connect_timeout_seconds is too large".to_owned(),
            ));
        }
        self.protocol
            .parse::<ProviderProtocol>()
            .map_err(|source| ConfigError::InvalidProtocol {
                provider: name.to_owned(),
                source,
            })?;
        for (header_name, header_value) in &self.headers {
            reqwest::header::HeaderName::from_bytes(header_name.as_bytes())
                .map_err(|error| ConfigError::InvalidProvider(error.to_string()))?;
            reqwest::header::HeaderValue::from_str(header_value)
                .map_err(|error| ConfigError::InvalidProvider(error.to_string()))?;
        }
        if let Some(models) = &self.models {
            normalize_models(models.clone())?;
        }
        Ok(())
    }

    fn resolve(&self, name: &str) -> Result<ResolvedProvider, ConfigError> {
        if self.api_key.is_some() && self.api_key_env.is_some() {
            return Err(ConfigError::ConflictingCredentials(name.to_owned()));
        }
        let protocol = self
            .protocol
            .parse()
            .map_err(|source| ConfigError::InvalidProtocol {
                provider: name.to_owned(),
                source,
            })?;
        let mut headers = self.headers.clone();
        if let Some(key) = self.api_key.clone().or_else(|| {
            self.api_key_env
                .as_ref()
                .and_then(|variable| env::var(variable).ok())
        }) {
            headers
                .entry("authorization".to_owned())
                .or_insert_with(|| format!("Bearer {key}"));
        } else if let Some(variable) = &self.api_key_env {
            return Err(ConfigError::MissingCredential {
                provider: name.to_owned(),
                variable: variable.clone(),
            });
        }
        Ok(ResolvedProvider {
            name: name.to_owned(),
            endpoint: endpoint(&self.base_url, protocol),
            models_endpoint: models_endpoint(&self.base_url),
            protocol,
            headers,
            connect_timeout_seconds: self.connect_timeout_seconds.max(1),
        })
    }
}

impl ConfigManager {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref().to_owned();
        let config = PiqoConfig::load(&path)?;
        Ok(Self::new(Some(path), config))
    }

    pub fn memory(config: PiqoConfig) -> Self {
        Self::new(None, config)
    }

    pub fn file(path: impl Into<PathBuf>, config: PiqoConfig) -> Self {
        Self::new(Some(path.into()), config)
    }

    fn new(path: Option<PathBuf>, config: PiqoConfig) -> Self {
        Self {
            inner: Arc::new(ConfigManagerInner {
                path,
                config: RwLock::new(ConfigSnapshot {
                    revision: 1,
                    loaded_at: now(),
                    config: Arc::new(config),
                }),
                mutation: Mutex::new(()),
                discovery: RwLock::new(HashMap::new()),
                transport: ProviderTransport::new(),
            }),
        }
    }

    pub fn snapshot(&self) -> Result<Arc<PiqoConfig>, ConfigError> {
        self.inner
            .config
            .read()
            .map(|snapshot| snapshot.config.clone())
            .map_err(|_| ConfigError::LockPoisoned)
    }

    pub fn versioned_snapshot(&self) -> Result<ConfigSnapshot, ConfigError> {
        self.inner
            .config
            .read()
            .map(|snapshot| snapshot.clone())
            .map_err(|_| ConfigError::LockPoisoned)
    }

    pub async fn reload(&self) -> Result<ConfigSnapshot, ConfigError> {
        let path = self.inner.path.clone().ok_or(ConfigError::ReadOnly)?;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = inner
                .mutation
                .lock()
                .map_err(|_| ConfigError::LockPoisoned)?;
            let config = PiqoConfig::load(&path)?;
            let mut current = inner
                .config
                .write()
                .map_err(|_| ConfigError::LockPoisoned)?;
            let snapshot = ConfigSnapshot {
                revision: current
                    .revision
                    .checked_add(1)
                    .ok_or(ConfigError::RevisionExhausted)?,
                loaded_at: now(),
                config: Arc::new(config),
            };
            *current = snapshot.clone();
            drop(current);
            inner
                .discovery
                .write()
                .map_err(|_| ConfigError::LockPoisoned)?
                .clear();
            Ok(snapshot)
        })
        .await
        .map_err(|error| ConfigError::Task(error.to_string()))?
    }

    pub fn resolve_provider(&self, name: &str) -> Result<ResolvedProvider, ConfigError> {
        self.snapshot()?.resolve_provider(name)
    }

    pub fn body_layers(
        &self,
        model: &str,
        agent: Option<&str>,
        variant: Option<&str>,
        request: Value,
    ) -> Result<Vec<Value>, ConfigError> {
        Ok(self.snapshot()?.body_layers(model, agent, variant, request))
    }

    pub fn agent(&self, name: &str) -> Result<AgentDefinition, ConfigError> {
        self.snapshot()?.agent(name)
    }

    pub fn agents(&self) -> Result<Vec<AgentDefinition>, ConfigError> {
        Ok(self.snapshot()?.agents())
    }

    pub fn catalog(&self) -> Result<Vec<ProviderCatalogEntry>, ConfigError> {
        let config = self.snapshot()?;
        let mut providers = config
            .providers
            .iter()
            .map(|(name, provider)| self.view_for(name, provider))
            .collect::<Result<Vec<_>, _>>()?;
        providers.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(providers)
    }

    pub fn provider(&self, name: &str) -> Result<ProviderCatalogEntry, ConfigError> {
        let config = self.snapshot()?;
        let provider = config
            .providers
            .get(name)
            .ok_or_else(|| ConfigError::ProviderNotFound(name.to_owned()))?;
        self.view_for(name, provider)
    }

    pub fn models(&self, name: &str) -> Result<ProviderModelsResponse, ConfigError> {
        let provider = self.provider(name)?;
        Ok(ProviderModelsResponse {
            provider: provider.name,
            models: provider.models,
            source: provider.model_source,
            discovery: provider.discovery,
        })
    }

    pub async fn create_provider(
        &self,
        request: CreateProviderRequest,
    ) -> Result<ProviderCatalogEntry, ConfigError> {
        validate_provider_name(&request.name)?;
        let name = request.name.clone();
        let provider = ProviderConfig::from_create(&request)?;
        self.mutate(move |document| {
            let providers = providers_table_mut(document)?;
            if providers.contains_key(&name) {
                return Err(ConfigError::ProviderAlreadyExists(name));
            }
            providers.insert(&name, Item::Table(provider_table(&provider)));
            Ok(())
        })
        .await?;
        self.clear_discovery(&request.name)?;
        self.discover_provider(&request.name).await
    }

    pub async fn update_provider(
        &self,
        name: &str,
        request: UpdateProviderRequest,
    ) -> Result<ProviderCatalogEntry, ConfigError> {
        if request.base_url.is_none()
            && request.protocol.is_none()
            && request.credentials.is_none()
            && request.headers.is_none()
            && request.connect_timeout_seconds.is_none()
        {
            return Err(ConfigError::InvalidProvider(
                "at least one provider field is required".to_owned(),
            ));
        }
        let name_owned = name.to_owned();
        self.mutate(move |document| {
            let table = provider_table_mut(document, &name_owned)?;
            if let Some(base_url) = request.base_url {
                table["base_url"] = value(base_url.trim());
            }
            if let Some(protocol) = request.protocol {
                table["protocol"] = value(protocol);
            }
            if let Some(credentials) = request.credentials {
                set_credentials(table, &credentials)?;
            }
            if let Some(headers) = request.headers {
                table["headers"] = Item::Table(headers_table(headers));
            }
            if let Some(timeout) = request.connect_timeout_seconds {
                table["connect_timeout_seconds"] = value(i64::try_from(timeout).map_err(|_| {
                    ConfigError::InvalidProvider("connect timeout is too large".to_owned())
                })?);
            }
            Ok(())
        })
        .await?;
        self.clear_discovery(name)?;
        if self
            .snapshot()?
            .providers
            .get(name)
            .is_some_and(|provider| provider.models.is_some())
        {
            self.provider(name)
        } else {
            self.discover_provider(name).await
        }
    }

    pub async fn delete_provider(&self, name: &str) -> Result<(), ConfigError> {
        let name_owned = name.to_owned();
        self.mutate(move |document| {
            let providers = providers_table_mut(document)?;
            if providers.remove(&name_owned).is_none() {
                return Err(ConfigError::ProviderNotFound(name_owned));
            }
            Ok(())
        })
        .await?;
        self.clear_discovery(name)
    }

    pub async fn replace_models(
        &self,
        name: &str,
        models: Vec<String>,
    ) -> Result<ProviderModelsResponse, ConfigError> {
        let models = normalize_models(models)?;
        let name_owned = name.to_owned();
        let stored_models = models.clone();
        self.mutate(move |document| {
            let table = provider_table_mut(document, &name_owned)?;
            table["models"] = value(model_array(&stored_models));
            Ok(())
        })
        .await?;
        self.clear_discovery(name)?;
        self.models(name)
    }

    pub async fn clear_models(&self, name: &str) -> Result<ProviderModelsResponse, ConfigError> {
        let name_owned = name.to_owned();
        self.mutate(move |document| {
            let table = provider_table_mut(document, &name_owned)?;
            table.remove("models");
            Ok(())
        })
        .await?;
        self.clear_discovery(name)?;
        self.discover_provider(name).await?;
        self.models(name)
    }

    pub async fn refresh_models(&self, name: &str) -> Result<ProviderModelsResponse, ConfigError> {
        let config = self.snapshot()?;
        let provider = config
            .providers
            .get(name)
            .ok_or_else(|| ConfigError::ProviderNotFound(name.to_owned()))?;
        if provider.models.is_some() {
            return Err(ConfigError::ManualModelOverride(name.to_owned()));
        }
        drop(config);
        self.discover_provider(name).await?;
        self.models(name)
    }

    pub async fn discover_all(&self) {
        let names = match self.snapshot() {
            Ok(config) => config
                .providers
                .iter()
                .filter(|(_, provider)| provider.models.is_none())
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>(),
            Err(error) => {
                tracing::warn!(%error, "unable to read providers for model discovery");
                return;
            }
        };
        let mut tasks = tokio::task::JoinSet::new();
        for name in names {
            let manager = self.clone();
            tasks.spawn(async move {
                if let Err(error) = manager.discover_provider(&name).await {
                    tracing::warn!(provider = %name, %error, "model discovery failed");
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                tracing::warn!(%error, "model discovery task stopped");
            }
        }
    }

    async fn discover_provider(&self, name: &str) -> Result<ProviderCatalogEntry, ConfigError> {
        let original = self
            .snapshot()?
            .providers
            .get(name)
            .cloned()
            .ok_or_else(|| ConfigError::ProviderNotFound(name.to_owned()))?;
        if original.models.is_some() {
            return Err(ConfigError::ManualModelOverride(name.to_owned()));
        }
        let attempted_at = now();
        self.set_discovery(
            name,
            DiscoveryCache {
                models: Vec::new(),
                discovery: ModelDiscovery {
                    status: DiscoveryStatus::Pending,
                    last_attempt_at: Some(attempted_at.clone()),
                    error: None,
                },
            },
        )?;
        let result = match original.resolve(name) {
            Ok(provider) => self
                .inner
                .transport
                .discover_models(
                    &provider.models_endpoint,
                    &provider.headers,
                    Duration::from_secs(provider.connect_timeout_seconds),
                )
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        let current = self
            .snapshot()?
            .providers
            .get(name)
            .cloned()
            .ok_or_else(|| ConfigError::ProviderNotFound(name.to_owned()))?;
        if current != original {
            return self.provider(name);
        }
        let cache = match result {
            Ok(models) => DiscoveryCache {
                models,
                discovery: ModelDiscovery {
                    status: DiscoveryStatus::Succeeded,
                    last_attempt_at: Some(attempted_at),
                    error: None,
                },
            },
            Err(error) => DiscoveryCache {
                models: Vec::new(),
                discovery: ModelDiscovery {
                    status: DiscoveryStatus::Failed,
                    last_attempt_at: Some(attempted_at),
                    error: Some(error),
                },
            },
        };
        self.set_discovery(name, cache)?;
        self.provider(name)
    }

    fn view_for(
        &self,
        name: &str,
        provider: &ProviderConfig,
    ) -> Result<ProviderCatalogEntry, ConfigError> {
        let credentials = if provider.api_key.is_some() {
            ProviderCredentialSummary::ApiKey
        } else if let Some(variable) = &provider.api_key_env {
            ProviderCredentialSummary::Environment {
                variable: variable.clone(),
            }
        } else {
            ProviderCredentialSummary::None
        };
        let mut header_names = provider.headers.keys().cloned().collect::<Vec<_>>();
        header_names.sort();
        let (models, model_source, discovery) = if let Some(models) = &provider.models {
            (
                models.clone(),
                ModelSource::Manual,
                ModelDiscovery {
                    status: DiscoveryStatus::NotApplicable,
                    last_attempt_at: None,
                    error: None,
                },
            )
        } else {
            let cache = self
                .inner
                .discovery
                .read()
                .map_err(|_| ConfigError::LockPoisoned)?
                .get(name)
                .cloned()
                .unwrap_or_else(|| DiscoveryCache {
                    models: Vec::new(),
                    discovery: ModelDiscovery {
                        status: DiscoveryStatus::Pending,
                        last_attempt_at: None,
                        error: None,
                    },
                });
            (cache.models, ModelSource::Discovery, cache.discovery)
        };
        Ok(ProviderCatalogEntry {
            name: name.to_owned(),
            base_url: provider.base_url.clone(),
            protocol: provider.protocol.clone(),
            connect_timeout_seconds: provider.connect_timeout_seconds,
            credentials,
            header_names,
            streaming: true,
            non_streaming: true,
            models,
            model_source,
            discovery,
        })
    }

    fn set_discovery(&self, name: &str, cache: DiscoveryCache) -> Result<(), ConfigError> {
        self.inner
            .discovery
            .write()
            .map_err(|_| ConfigError::LockPoisoned)?
            .insert(name.to_owned(), cache);
        Ok(())
    }

    fn clear_discovery(&self, name: &str) -> Result<(), ConfigError> {
        self.inner
            .discovery
            .write()
            .map_err(|_| ConfigError::LockPoisoned)?
            .remove(name);
        Ok(())
    }

    async fn mutate<F>(&self, operation: F) -> Result<(), ConfigError>
    where
        F: FnOnce(&mut DocumentMut) -> Result<(), ConfigError> + Send + 'static,
    {
        let path = self.inner.path.clone().ok_or(ConfigError::ReadOnly)?;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = inner
                .mutation
                .lock()
                .map_err(|_| ConfigError::LockPoisoned)?;
            let mut document = read_document(&path)?;
            operation(&mut document)?;
            let text = document.to_string();
            let config: PiqoConfig = toml::from_str(&text)?;
            config.validate()?;
            write_atomic(&path, text.as_bytes())?;
            let mut current = inner
                .config
                .write()
                .map_err(|_| ConfigError::LockPoisoned)?;
            *current = ConfigSnapshot {
                revision: current
                    .revision
                    .checked_add(1)
                    .ok_or(ConfigError::RevisionExhausted)?,
                loaded_at: now(),
                config: Arc::new(config),
            };
            Ok(())
        })
        .await
        .map_err(|error| ConfigError::Task(error.to_string()))?
    }
}

fn validate_provider_name(name: &str) -> Result<(), ConfigError> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(ConfigError::InvalidProvider(
            "provider name must be 1-128 URL-safe characters".to_owned(),
        ))
    }
}

fn credential_values(
    credential: Option<&ProviderCredentialInput>,
) -> Result<(Option<String>, Option<String>), ConfigError> {
    match credential.unwrap_or(&ProviderCredentialInput::None) {
        ProviderCredentialInput::None => Ok((None, None)),
        ProviderCredentialInput::ApiKey { value } if !value.is_empty() => {
            Ok((Some(value.clone()), None))
        }
        ProviderCredentialInput::Environment { variable } if !variable.trim().is_empty() => {
            Ok((None, Some(variable.trim().to_owned())))
        }
        _ => Err(ConfigError::InvalidProvider(
            "credential value must not be empty".to_owned(),
        )),
    }
}

fn normalize_models(models: Vec<String>) -> Result<Vec<String>, ConfigError> {
    let mut normalized = Vec::new();
    for model in models {
        let model = model.trim().to_owned();
        if model.is_empty() {
            return Err(ConfigError::InvalidProvider(
                "model names must not be empty".to_owned(),
            ));
        }
        if !normalized.contains(&model) {
            normalized.push(model);
        }
    }
    Ok(normalized)
}

fn normalize_body(value: &Value) -> Value {
    if value.is_object() {
        value.clone()
    } else {
        Value::Object(Map::new())
    }
}

fn endpoint(base_url: &str, protocol: ProviderProtocol) -> String {
    let base = base_url.trim_end_matches('/');
    let suffix = match protocol {
        ProviderProtocol::ChatCompletions => "/v1/chat/completions",
        ProviderProtocol::Responses => "/v1/responses",
    };
    if base.ends_with(suffix) {
        base.to_owned()
    } else if base.ends_with("/v1") {
        format!("{base}{}", suffix.trim_start_matches("/v1"))
    } else {
        format!("{base}{suffix}")
    }
}

fn models_endpoint(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    for suffix in ["/v1/chat/completions", "/v1/responses"] {
        if let Some(root) = base.strip_suffix(suffix) {
            return format!("{root}/v1/models");
        }
    }
    if base.ends_with("/v1") {
        format!("{base}/models")
    } else {
        format!("{base}/v1/models")
    }
}

fn default_protocol() -> String {
    "chat_completions".to_owned()
}

fn default_connect_timeout() -> u64 {
    10
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn read_document(path: &Path) -> Result<DocumentMut, ConfigError> {
    if !path.exists() {
        return Ok(DocumentMut::new());
    }
    let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.display().to_string(),
        source,
    })?;
    DocumentMut::from_str(&text).map_err(ConfigError::Edit)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), ConfigError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| ConfigError::Write {
        path: parent.display().to_string(),
        source,
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("piqo.toml");
    let temporary = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        Uuid::now_v7()
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = path
                .metadata()
                .map(|metadata| metadata.permissions())
                .unwrap_or_else(|_| fs::Permissions::from_mode(0o600));
            file.set_permissions(permissions)?;
        }
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok::<(), std::io::Error>(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(|source| ConfigError::Write {
        path: path.display().to_string(),
        source,
    })
}

fn providers_table_mut(document: &mut DocumentMut) -> Result<&mut Table, ConfigError> {
    if !document.contains_key("providers") {
        document["providers"] = Item::Table(Table::new());
    }
    document["providers"]
        .as_table_mut()
        .ok_or_else(|| ConfigError::InvalidProvider("providers must be a TOML table".to_owned()))
}

fn provider_table_mut<'a>(
    document: &'a mut DocumentMut,
    name: &str,
) -> Result<&'a mut Table, ConfigError> {
    providers_table_mut(document)?
        .get_mut(name)
        .and_then(Item::as_table_mut)
        .ok_or_else(|| ConfigError::ProviderNotFound(name.to_owned()))
}

fn provider_table(provider: &ProviderConfig) -> Table {
    let mut table = Table::new();
    table["base_url"] = value(&provider.base_url);
    table["protocol"] = value(&provider.protocol);
    table["connect_timeout_seconds"] = value(provider.connect_timeout_seconds as i64);
    if let Some(api_key) = &provider.api_key {
        table["api_key"] = value(api_key);
    }
    if let Some(variable) = &provider.api_key_env {
        table["api_key_env"] = value(variable);
    }
    if !provider.headers.is_empty() {
        table["headers"] = Item::Table(headers_table(provider.headers.clone()));
    }
    if let Some(models) = &provider.models {
        table["models"] = value(model_array(models));
    }
    table
}

fn headers_table(headers: HashMap<String, String>) -> Table {
    let mut table = Table::new();
    for (name, header_value) in headers {
        table[&name] = value(header_value);
    }
    table
}

fn model_array(models: &[String]) -> Array {
    let mut array = Array::new();
    for model in models {
        array.push(model.as_str());
    }
    array
}

fn set_credentials(
    table: &mut Table,
    credential: &ProviderCredentialInput,
) -> Result<(), ConfigError> {
    let (api_key, api_key_env) = credential_values(Some(credential))?;
    table.remove("api_key");
    table.remove("api_key_env");
    if let Some(api_key) = api_key {
        table["api_key"] = value(api_key);
    }
    if let Some(variable) = api_key_env {
        table["api_key_env"] = value(variable);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn merges_the_five_body_layers_without_normalizing_unknown_keys() {
        let config: PiqoConfig = toml::from_str(
            r#"
                [defaults.body]
                temperature = 0.2
                [models.qwen.body]
                top_k = 40
                [agents.default.body]
                agent_flag = true
                [variants.fast.body]
                temperature = 0.8
            "#,
        )
        .expect("config parses");
        let layers = config.body_layers(
            "qwen",
            Some("default"),
            Some("fast"),
            serde_json::json!({"vendor": "x"}),
        );
        assert_eq!(layers.len(), 5);
        assert_eq!(layers[0]["temperature"], 0.2);
        assert_eq!(layers[3]["temperature"], 0.8);
    }

    #[test]
    fn loads_markdown_agents_and_applies_toml_overrides() {
        let directory = tempdir().expect("temporary directory");
        let agents = directory.path().join("agents");
        fs::create_dir(&agents).expect("agent directory creates");
        fs::write(
            agents.join("reviewer.md"),
            r#"---
description: Review code without edits
provider: local
model: reviewer-model
permissions:
  read: allow
  write: deny
body:
  temperature: 0.1
---
Focus on correctness.
"#,
        )
        .expect("agent fixture writes");
        let path = directory.path().join("piqo.toml");
        fs::write(
            &path,
            r#"[agents.reviewer]
model = "override-model"
instructions = "Use the repository conventions."
[agents.reviewer.permissions]
bash = "ask"
[agents.reviewer.body]
temperature = 0.2
"#,
        )
        .expect("config fixture writes");

        let config = PiqoConfig::load(&path).expect("config loads");
        let agent = config.agent("reviewer").expect("agent resolves");
        assert_eq!(
            agent.description.as_deref(),
            Some("Review code without edits")
        );
        assert_eq!(agent.provider.as_deref(), Some("local"));
        assert_eq!(agent.model.as_deref(), Some("override-model"));
        assert_eq!(
            agent.instructions.as_deref(),
            Some("Use the repository conventions.")
        );
        assert_eq!(agent.permissions.read, Some(PermissionSetting::Allow));
        assert_eq!(agent.permissions.write, Some(PermissionSetting::Deny));
        assert_eq!(agent.permissions.bash, Some(PermissionSetting::Ask));
        let layers = config.body_layers("override-model", Some("reviewer"), None, Value::Null);
        assert_eq!(layers.len(), 4);
        assert_eq!(layers[1]["temperature"], 0.1);
        assert_eq!(layers[2]["temperature"], 0.2);
    }

    #[test]
    fn rejects_invalid_markdown_agent_front_matter() {
        let directory = tempdir().expect("temporary directory");
        let agents = directory.path().join("agents");
        fs::create_dir(&agents).expect("agent directory creates");
        fs::write(agents.join("broken.md"), "---\nunknown: true\n---\nPrompt")
            .expect("agent fixture writes");
        let path = directory.path().join("piqo.toml");
        fs::write(&path, "").expect("config fixture writes");

        assert!(matches!(
            PiqoConfig::load(&path),
            Err(ConfigError::InvalidAgentDefinition { .. })
        ));
    }

    #[test]
    fn derives_generation_and_model_endpoints() {
        let config: PiqoConfig = toml::from_str(
            r#"[providers.local]
            base_url = "http://localhost:8000/v1"
            "#,
        )
        .expect("config parses");
        let provider = config.resolve_provider("local").expect("provider resolves");
        assert_eq!(
            provider.endpoint,
            "http://localhost:8000/v1/chat/completions"
        );
        assert_eq!(provider.models_endpoint, "http://localhost:8000/v1/models");
    }

    #[tokio::test]
    async fn mutations_preserve_comments_and_unknown_sections() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("piqo.toml");
        fs::write(&path, "# keep this comment\n[unknown]\nflag = true\n").expect("fixture writes");
        let manager = ConfigManager::load(&path).expect("manager loads");
        let request = CreateProviderRequest {
            name: "local".to_owned(),
            base_url: "http://127.0.0.1:9".to_owned(),
            protocol: None,
            credentials: None,
            headers: None,
            connect_timeout_seconds: Some(1),
        };
        let _ = manager.create_provider(request).await;
        let text = fs::read_to_string(path).expect("configuration reads");
        assert!(text.contains("# keep this comment"));
        assert!(text.contains("[unknown]"));
        assert!(text.contains("[providers.local]"));
    }

    #[tokio::test]
    async fn concurrent_mutations_are_serialized_without_losing_updates() {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join("piqo.toml");
        fs::write(
            &path,
            r#"
# keep concurrent edits
[providers.one]
base_url = "http://127.0.0.1:1"
models = []

[providers.two]
base_url = "http://127.0.0.1:2"
models = []
"#,
        )
        .expect("fixture writes");
        let manager = ConfigManager::load(&path).expect("manager loads");
        let first = manager.update_provider(
            "one",
            UpdateProviderRequest {
                connect_timeout_seconds: Some(11),
                ..UpdateProviderRequest::default()
            },
        );
        let second = manager.update_provider(
            "two",
            UpdateProviderRequest {
                connect_timeout_seconds: Some(22),
                ..UpdateProviderRequest::default()
            },
        );
        let (first, second) = tokio::join!(first, second);
        first.expect("first update succeeds");
        second.expect("second update succeeds");
        let snapshot = manager.snapshot().expect("snapshot reads");
        assert_eq!(snapshot.providers["one"].connect_timeout_seconds, 11);
        assert_eq!(snapshot.providers["two"].connect_timeout_seconds, 22);
        let text = fs::read_to_string(path).expect("configuration reads");
        assert!(text.contains("# keep concurrent edits"));
    }

    #[tokio::test]
    async fn reload_replaces_the_snapshot_atomically_and_increments_revision() {
        let directory = tempdir().expect("temporary directory creates");
        let path = directory.path().join("piqo.toml");
        fs::write(
            &path,
            "[providers.first]\nbase_url = \"http://127.0.0.1:8000\"\nmodels = []\n",
        )
        .expect("initial configuration writes");
        let manager = ConfigManager::load(&path).expect("manager loads");
        let before = manager.versioned_snapshot().expect("snapshot reads");

        fs::write(
            &path,
            "[providers.second]\nbase_url = \"https://example.com\"\nmodels = []\n",
        )
        .expect("replacement configuration writes");
        let after = manager.reload().await.expect("configuration reloads");

        assert_eq!(before.revision, 1);
        assert_eq!(after.revision, 2);
        assert!(before.config.providers.contains_key("first"));
        assert!(after.config.providers.contains_key("second"));
        assert_eq!(
            manager
                .versioned_snapshot()
                .expect("snapshot reads")
                .revision,
            2
        );
    }

    #[tokio::test]
    async fn reload_rejects_invalid_configuration_without_replacing_the_snapshot() {
        let directory = tempdir().expect("temporary directory creates");
        let path = directory.path().join("piqo.toml");
        fs::write(&path, "").expect("initial configuration writes");
        let manager = ConfigManager::load(&path).expect("manager loads");
        fs::write(&path, "invalid = [").expect("invalid configuration writes");

        assert!(manager.reload().await.is_err());
        assert_eq!(
            manager
                .versioned_snapshot()
                .expect("snapshot reads")
                .revision,
            1
        );
    }

    #[test]
    fn normalizes_manual_models_without_sorting_them() {
        let models = normalize_models(vec![
            " second ".to_owned(),
            "first".to_owned(),
            "second".to_owned(),
        ])
        .expect("models normalize");
        assert_eq!(models, vec!["second", "first"]);
    }
}
