use crate::api::completions::ChatCompletionRequest;
use crate::config::providers::{ProviderConfig, ProviderType};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::Value;

pub struct UpstreamPayload {
    pub url: String,
    pub headers: HeaderMap,
    pub body: Value,
}

/// Builds the appropriate upstream payload based on the provider's protocol type.
pub fn build_upstream_payload(
    provider: &ProviderConfig,
    upstream_model: &str,
    req: &ChatCompletionRequest,
) -> UpstreamPayload {
    match provider.provider_type {
        ProviderType::Anthropic => {
            let (url, headers, body) =
                crate::providers::anthropic::transform_openai_to_anthropic(provider, upstream_model, req);
            UpstreamPayload { url, headers, body }
        }
        ProviderType::Gemini => {
            let (url, headers, body) =
                crate::providers::gemini::transform_openai_to_gemini(provider, upstream_model, req);
            UpstreamPayload { url, headers, body }
        }
        ProviderType::OpenAi => {
            let mut modified_req = req.clone();
            modified_req.model = upstream_model.to_string();

            let base = provider.base_url.trim_end_matches('/');
            let url = format!("{}/chat/completions", base);

            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

            if let Some(ref key) = provider.api_key {
                if let Ok(val) = HeaderValue::from_str(&format!("Bearer {}", key)) {
                    headers.insert(AUTHORIZATION, val);
                }
            }

            let body = serde_json::to_value(&modified_req).unwrap_or_default();
            UpstreamPayload { url, headers, body }
        }
    }
}
