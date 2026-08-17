use std::{collections::HashMap, env, path::Path};

use piqo_provider::ProviderProtocol;
use serde::Deserialize;
use serde_json::{Map, Value};
use thiserror::Error;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PiqoConfig {
    #[serde(default)]
    pub defaults: BodyLayer,
    #[serde(default)]
    pub providers: HashMap<String, ProviderConfig>,
    #[serde(default)]
    pub models: HashMap<String, BodyLayer>,
    #[serde(default)]
    pub agents: HashMap<String, BodyLayer>,
    #[serde(default)]
    pub variants: HashMap<String, BodyLayer>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BodyLayer {
    #[serde(default)]
    pub body: Value,
}

#[derive(Debug, Clone, Deserialize)]
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
}

#[derive(Debug, Clone)]
pub struct ResolvedProvider {
    pub name: String,
    pub endpoint: String,
    pub protocol: ProviderProtocol,
    pub headers: HashMap<String, String>,
    pub connect_timeout_seconds: u64,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse TOML configuration: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("provider {0} is not configured")]
    ProviderNotFound(String),
    #[error("provider {0} has both api_key and api_key_env")]
    ConflictingCredentials(String),
    #[error("environment variable {variable} for provider {provider} is not set")]
    MissingCredential { provider: String, variable: String },
    #[error("provider {provider} uses an invalid protocol: {source}")]
    InvalidProtocol {
        provider: String,
        source: piqo_provider::ProviderTransportError,
    },
}

impl PiqoConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_ref = path.as_ref();
        if !path_ref.exists() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(path_ref).map_err(|source| ConfigError::Read {
            path: path_ref.display().to_string(),
            source,
        })?;
        Ok(toml::from_str(&text)?)
    }

    pub fn resolve_provider(&self, name: &str) -> Result<ResolvedProvider, ConfigError> {
        let config = self
            .providers
            .get(name)
            .ok_or_else(|| ConfigError::ProviderNotFound(name.to_owned()))?;
        if config.api_key.is_some() && config.api_key_env.is_some() {
            return Err(ConfigError::ConflictingCredentials(name.to_owned()));
        }
        let protocol = config
            .protocol
            .parse()
            .map_err(|source| ConfigError::InvalidProtocol {
                provider: name.to_owned(),
                source,
            })?;
        let mut headers = config.headers.clone();
        if let Some(key) = config.api_key.clone().or_else(|| {
            config
                .api_key_env
                .as_ref()
                .and_then(|variable| env::var(variable).ok())
        }) {
            headers
                .entry("authorization".to_owned())
                .or_insert_with(|| format!("Bearer {key}"));
        } else if let Some(variable) = &config.api_key_env {
            return Err(ConfigError::MissingCredential {
                provider: name.to_owned(),
                variable: variable.clone(),
            });
        }
        Ok(ResolvedProvider {
            name: name.to_owned(),
            endpoint: endpoint(&config.base_url, protocol),
            protocol,
            headers,
            connect_timeout_seconds: config.connect_timeout_seconds.max(1),
        })
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
            if let Some(layer) = self.agents.get(name) {
                layers.push(normalize_body(&layer.body));
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

    pub fn catalog(&self) -> Vec<ProviderCatalogEntry> {
        let mut entries: Vec<_> = self
            .providers
            .iter()
            .filter_map(|(name, provider)| {
                provider
                    .protocol
                    .parse()
                    .ok()
                    .map(|protocol| ProviderCatalogEntry {
                        name: name.clone(),
                        protocol: protocol_name(protocol).to_owned(),
                        streaming: true,
                        non_streaming: true,
                        models: self.models.keys().cloned().collect(),
                    })
            })
            .collect();
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        entries
    }
}

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct ProviderCatalogEntry {
    pub name: String,
    pub protocol: String,
    pub streaming: bool,
    pub non_streaming: bool,
    pub models: Vec<String>,
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

fn protocol_name(protocol: ProviderProtocol) -> &'static str {
    match protocol {
        ProviderProtocol::ChatCompletions => "chat_completions",
        ProviderProtocol::Responses => "responses",
    }
}

fn default_protocol() -> String {
    "chat_completions".to_owned()
}

fn default_connect_timeout() -> u64 {
    10
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn defaults_to_chat_completions() {
        let config: PiqoConfig = toml::from_str(
            r#"[providers.local]
            base_url = "http://localhost:8000"
            "#,
        )
        .expect("config parses");
        let provider = config.resolve_provider("local").expect("provider resolves");
        assert_eq!(
            provider.endpoint,
            "http://localhost:8000/v1/chat/completions"
        );
    }
}
