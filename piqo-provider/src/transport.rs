use std::{collections::HashMap, str::FromStr};

use serde_json::Value;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderProtocol {
    ChatCompletions,
    Responses,
}

impl FromStr for ProviderProtocol {
    type Err = ProviderTransportError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "chat_completions" | "chat-completion" | "chat" => Ok(Self::ChatCompletions),
            "responses" | "response" => Ok(Self::Responses),
            other => Err(ProviderTransportError::UnsupportedProtocol(
                other.to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderDelta {
    Text(String),
    ToolCall {
        call_id: Option<String>,
        name: Option<String>,
        arguments: String,
    },
    ToolCallDelta {
        index: Option<u64>,
        call_id: Option<String>,
        name: Option<String>,
        arguments: String,
    },
    Usage(Value),
    Completed,
    RequiresAction,
}

#[derive(Debug, Error)]
pub enum ProviderTransportError {
    #[error("provider request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("unsupported provider protocol {0}")]
    UnsupportedProtocol(String),
    #[error("provider response is not a JSON object")]
    InvalidResponse,
    #[error("provider response is missing field {0}")]
    MissingField(&'static str),
    #[error("provider response contains invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("provider SSE event is malformed: {0}")]
    MalformedSse(String),
    #[error("provider request contains an invalid header: {0}")]
    InvalidHeader(String),
    #[error("provider returned HTTP status {0}")]
    HttpStatus(reqwest::StatusCode),
    #[error("provider model catalog is malformed")]
    MalformedModelCatalog,
}

#[derive(Clone, Debug)]
pub struct ProviderTransport {
    client: reqwest::Client,
}

impl Default for ProviderTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl ProviderTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub fn build_request(
        &self,
        endpoint: &str,
        body: Value,
    ) -> Result<reqwest::Request, reqwest::Error> {
        self.client.post(endpoint).json(&body).build()
    }

    pub fn build_request_with_headers(
        &self,
        endpoint: &str,
        body: Value,
        headers: &HashMap<String, String>,
    ) -> Result<reqwest::Request, ProviderTransportError> {
        let mut request = self.client.post(endpoint).json(&body).build()?;
        for (name, value) in headers {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| ProviderTransportError::InvalidHeader("invalid name".into()))?;
            let value = reqwest::header::HeaderValue::from_str(value)
                .map_err(|_| ProviderTransportError::InvalidHeader("invalid value".into()))?;
            request.headers_mut().insert(name, value);
        }
        Ok(request)
    }

    pub async fn send(
        &self,
        request: reqwest::Request,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.client.execute(request).await
    }

    /// Execute a request with the provider-specific connection timeout.
    /// The request remains otherwise unbounded so generation can stream for
    /// as long as the provider needs.
    pub async fn send_with_connect_timeout(
        &self,
        request: reqwest::Request,
        timeout: Duration,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let client = reqwest::Client::builder()
            .connect_timeout(timeout)
            .build()?;
        client.execute(request).await
    }

    /// Query an OpenAI-compatible model catalog without retaining response
    /// bodies in errors, since upstream errors may contain sensitive data.
    pub async fn discover_models(
        &self,
        endpoint: &str,
        headers: &HashMap<String, String>,
        connect_timeout: Duration,
    ) -> Result<Vec<String>, ProviderTransportError> {
        let client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(connect_timeout)
            .build()?;
        let mut request = client.get(endpoint).build()?;
        for (name, value) in headers {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| ProviderTransportError::InvalidHeader("invalid name".into()))?;
            let value = reqwest::header::HeaderValue::from_str(value)
                .map_err(|_| ProviderTransportError::InvalidHeader("invalid value".into()))?;
            request.headers_mut().insert(name, value);
        }
        let response = client.execute(request).await?;
        if !response.status().is_success() {
            return Err(ProviderTransportError::HttpStatus(response.status()));
        }
        let value: Value = response.json().await?;
        parse_model_catalog(&value)
    }
}

fn parse_model_catalog(value: &Value) -> Result<Vec<String>, ProviderTransportError> {
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or(ProviderTransportError::MalformedModelCatalog)?;
    let mut models = data
        .iter()
        .map(|model| {
            model
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.trim().is_empty())
                .map(str::to_owned)
                .ok_or(ProviderTransportError::MalformedModelCatalog)
        })
        .collect::<Result<Vec<_>, _>>()?;
    models.sort();
    models.dedup();
    Ok(models)
}

pub fn parse_sse_event(
    protocol: ProviderProtocol,
    event_type: Option<&str>,
    data: &str,
) -> Result<Vec<ProviderDelta>, ProviderTransportError> {
    if data.trim() == "[DONE]" {
        return Ok(vec![ProviderDelta::Completed]);
    }
    let value: Value = serde_json::from_str(data)?;
    match protocol {
        ProviderProtocol::ChatCompletions => parse_chat_event(&value, true),
        ProviderProtocol::Responses => parse_responses_event(event_type, &value),
    }
}

pub fn parse_non_stream_response(
    protocol: ProviderProtocol,
    body: &str,
) -> Result<Vec<ProviderDelta>, ProviderTransportError> {
    let value: Value = serde_json::from_str(body)?;
    let mut deltas = match protocol {
        ProviderProtocol::ChatCompletions => parse_chat_event(&value, false)?,
        ProviderProtocol::Responses => parse_responses_event(None, &value)?,
    };
    deltas.push(ProviderDelta::Completed);
    Ok(deltas)
}

fn parse_chat_event(
    value: &Value,
    streaming: bool,
) -> Result<Vec<ProviderDelta>, ProviderTransportError> {
    let mut output = Vec::new();
    if let Some(usage) = value.get("usage") {
        if !usage.is_null() {
            output.push(ProviderDelta::Usage(usage.clone()));
        }
    }
    if let Some(choices) = value.get("choices").and_then(Value::as_array) {
        for choice in choices {
            let delta = choice.get("delta").or_else(|| choice.get("message"));
            if let Some(text) = delta
                .and_then(|value| value.get("content"))
                .and_then(Value::as_str)
            {
                if !text.is_empty() {
                    output.push(ProviderDelta::Text(text.to_owned()));
                }
            }
            if let Some(tool_calls) = delta
                .and_then(|value| value.get("tool_calls"))
                .and_then(Value::as_array)
            {
                for call in tool_calls {
                    let function = call.get("function");
                    let arguments = function
                        .and_then(|value| value.get("arguments"))
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let call_id = call
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .or_else(|| {
                            call.get("index")
                                .and_then(Value::as_u64)
                                .map(|index| format!("stream-index-{index}"))
                        });
                    let delta = if streaming {
                        ProviderDelta::ToolCallDelta {
                            index: call.get("index").and_then(Value::as_u64),
                            call_id,
                            name: function
                                .and_then(|value| value.get("name"))
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            arguments: arguments.to_owned(),
                        }
                    } else {
                        ProviderDelta::ToolCall {
                            call_id,
                            name: function
                                .and_then(|value| value.get("name"))
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            arguments: arguments.to_owned(),
                        }
                    };
                    output.push(delta);
                }
            }
        }
    }
    Ok(output)
}

fn parse_responses_event(
    event_type: Option<&str>,
    value: &Value,
) -> Result<Vec<ProviderDelta>, ProviderTransportError> {
    let kind = event_type
        .or_else(|| value.get("type").and_then(Value::as_str))
        .unwrap_or_default();
    let mut output = Vec::new();
    match kind {
        "response.output_text.delta" => {
            if let Some(text) = value.get("delta").and_then(Value::as_str) {
                output.push(ProviderDelta::Text(text.to_owned()));
            }
        }
        // Responses sends argument fragments followed by a `done` event that
        // contains the complete JSON string. Persisting each fragment as a
        // tool call would create duplicate calls and pause the run too early.
        "response.function_call_arguments.delta" => {}
        "response.function_call_arguments.done" => {
            output.push(ProviderDelta::ToolCall {
                call_id: value
                    .get("item_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                name: value.get("name").and_then(Value::as_str).map(str::to_owned),
                arguments: value
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            });
        }
        "response.completed" => {
            if let Some(usage) = value.pointer("/response/usage") {
                output.push(ProviderDelta::Usage(usage.clone()));
            }
            output.push(ProviderDelta::Completed);
        }
        "response.failed" | "error" => {
            return Err(ProviderTransportError::MalformedSse(value.to_string()))
        }
        "response.requires_action" => output.push(ProviderDelta::RequiresAction),
        // `output_text.done` summarizes the deltas already emitted. Re-emitting
        // its full text would duplicate the assistant message in the journal.
        "response.output_text.done" => {}
        "response.created" | "response.in_progress" | "response.queued" => {}
        "response" | "" => {
            if let Some(usage) = value.get("usage") {
                if !usage.is_null() {
                    output.push(ProviderDelta::Usage(usage.clone()));
                }
            }
            if let Some(items) = value.get("output").and_then(Value::as_array) {
                for item in items {
                    match item.get("type").and_then(Value::as_str) {
                        Some("message") => {
                            if let Some(contents) = item.get("content").and_then(Value::as_array) {
                                for content in contents {
                                    if let Some(text) = content.get("text").and_then(Value::as_str)
                                    {
                                        output.push(ProviderDelta::Text(text.to_owned()));
                                    }
                                }
                            }
                        }
                        Some("function_call") => output.push(ProviderDelta::ToolCall {
                            call_id: item
                                .get("call_id")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                            name: item.get("name").and_then(Value::as_str).map(str::to_owned),
                            arguments: item
                                .get("arguments")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                        }),
                        _ => {}
                    }
                }
            }
        }
        _ => {}
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_chat_text_and_usage() {
        let deltas = parse_sse_event(
            ProviderProtocol::ChatCompletions,
            None,
            r#"{"choices":[{"delta":{"content":"hello"}}],"usage":{"total_tokens":1}}"#,
        )
        .expect("chat event parses");
        assert!(deltas.contains(&ProviderDelta::Text("hello".into())));
        assert!(deltas
            .iter()
            .any(|delta| matches!(delta, ProviderDelta::Usage(_))));
    }

    #[test]
    fn parses_responses_named_text_event() {
        let deltas = parse_sse_event(
            ProviderProtocol::Responses,
            Some("response.output_text.delta"),
            r#"{"delta":"hello","item_id":"m"}"#,
        )
        .expect("responses event parses");
        assert_eq!(deltas, vec![ProviderDelta::Text("hello".into())]);
    }

    #[test]
    fn does_not_duplicate_responses_done_text() {
        let deltas = parse_sse_event(
            ProviderProtocol::Responses,
            Some("response.output_text.done"),
            r#"{"text":"hello"}"#,
        )
        .expect("done event parses");
        assert!(deltas.is_empty());
    }

    #[test]
    fn recognizes_done_sentinel() {
        assert_eq!(
            parse_sse_event(ProviderProtocol::ChatCompletions, None, "[DONE]")
                .expect("done parses"),
            vec![ProviderDelta::Completed]
        );
    }

    #[test]
    fn parses_non_stream_responses_output() {
        let deltas = parse_non_stream_response(
            ProviderProtocol::Responses,
            r#"{"object":"response","output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}]}"#,
        )
        .expect("response parses");
        assert!(deltas.contains(&ProviderDelta::Text("hello".into())));
        assert_eq!(deltas.last(), Some(&ProviderDelta::Completed));
    }

    #[test]
    fn marks_chat_stream_tool_arguments_as_fragments() {
        let deltas = parse_sse_event(
            ProviderProtocol::ChatCompletions,
            None,
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]}}]}"#,
        )
        .expect("tool fragment parses");
        assert!(matches!(
            deltas.first(),
            Some(ProviderDelta::ToolCallDelta { .. })
        ));
    }

    #[test]
    fn parses_and_deduplicates_model_catalogs() {
        let models = parse_model_catalog(&serde_json::json!({
            "data": [{"id": "zeta"}, {"id": "alpha"}, {"id": "zeta"}]
        }))
        .expect("catalog parses");
        assert_eq!(models, vec!["alpha", "zeta"]);
    }

    #[test]
    fn rejects_malformed_model_catalogs() {
        assert!(matches!(
            parse_model_catalog(&serde_json::json!({"data": [{"name": "missing-id"}]})),
            Err(ProviderTransportError::MalformedModelCatalog)
        ));
    }
}
