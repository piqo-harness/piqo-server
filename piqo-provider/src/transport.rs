use serde_json::Value;

/// Thin outbound HTTP transport. Request bodies are accepted as raw JSON so
/// provider-specific fields survive all the way to the wire.
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

    pub fn build_request(
        &self,
        endpoint: &str,
        body: Value,
    ) -> Result<reqwest::Request, reqwest::Error> {
        self.client.post(endpoint).json(&body).build()
    }
}
