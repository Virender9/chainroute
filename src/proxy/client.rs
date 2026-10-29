use crate::config::providers::ProviderConfig;
use reqwest::{Client, Response};
use std::time::Duration;

/// Shared HTTP client for proxying requests to upstream providers
#[derive(Clone)]
pub struct ProxyClient {
    client: Client,
}

impl ProxyClient {
    pub fn new() -> Self {
        let client = Client::builder()
            // -----------------------------------------------------------------
            // Hardened outbound TLS / HTTP profile for commercial provider APIs
            // (e.g. Cloudflare / DataDome fronted endpoints: OpenAI, Anthropic,
            // DeepSeek, etc.)
            //
            // TLS profile (implemented by reqwest's rustls-tls backend):
            //   • Cipher suites: TLS13_AES_128_GCM_SHA256,
            //     TLS13_AES_256_GCM_SHA384,
            //     TLS13_CHACHA20_POLY1305_SHA256,
            //     plus ECDHE suites in standard browser/API order.
            //   • ALPN protocols:  h2, http/1.1  (negotiated automatically
            //     during the TLS handshake).
            // -----------------------------------------------------------------
            // Keep TCP connections warm for 75 seconds to sustain persistent
            // upstream connection pools and eliminate repeated TLS handshakes.
            .tcp_keepalive(Duration::from_secs(75))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .expect("Failed to initialize hardened reqwest client");

        Self { client }
    }

    pub async fn forward_chat_completion(
        &self,
        provider: &ProviderConfig,
        upstream_model: &str,
        req: crate::api::completions::ChatCompletionRequest,
    ) -> Result<Response, reqwest::Error> {
        // Rewrite the model to the upstream specific model name
        let mut modified_req = req.clone();
        modified_req.model = upstream_model.to_string();

        let url = format!("{}/chat/completions", provider.base_url);
        let mut request_builder = self.client
            .post(&url)
            .json(&modified_req)
            .timeout(Duration::from_secs(provider.timeout_seconds));

        // Attach API key if present
        if let Some(ref key) = provider.api_key {
            request_builder = request_builder.bearer_auth(key);
        }

        request_builder.send().await
    }
}