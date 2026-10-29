use crate::config::providers::GatewayConfig;
use crate::proxy::{ProxyClient, pipe_upstream_sse};
use crate::ratelimit::sliding_window::SlidingWindowLimiter;
use crate::router::adaptive::AdaptiveScoreEngine;
use crate::server::drain::InFlightTracker;
use crate::telemetry::metrics as metrics;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response as AxumResponse};
use axum::body::Body;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;

/// OpenAI-compatible Chat Completion Request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: Option<bool>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub tools: Option<Vec<crate::security::mcp_guard::ToolDefinition>>,

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: Value,
}

/// Combined application state shared across Axum routes
pub struct AppState {
    pub config: std::sync::Arc<arc_swap::ArcSwap<GatewayConfig>>,
    pub config_path: Option<std::path::PathBuf>,
    pub client: ProxyClient,
    pub limiter: SlidingWindowLimiter,
    pub mcp_guard: Arc<crate::security::mcp_guard::McpSchemaGuard>,
    pub keystore: Arc<crate::auth::keys::KeyStore>,
    pub tracker: Arc<InFlightTracker>,
    pub start_time: Instant,
    pub adaptive_engine: Arc<AdaptiveScoreEngine>,
}

/// Handler for chat completions endpoint
pub async fn handle_chat_completions(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<ChatCompletionRequest>,
) -> AxumResponse {
    // Track this request for graceful shutdown / in-flight draining.
    let _stream_guard = state.tracker.track();

    // Extract and authenticate Bearer token
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|val| val.to_str().ok());

    let token = match auth_header {
        Some(h) if h.starts_with("Bearer ") => h["Bearer ".len()..].trim(),
        _ => {
            let error_json = serde_json::json!({
                "error": {
                    "message": "Invalid or missing API key",
                    "type": "invalid_request_error",
                    "code": 401
                }
            });
            return (StatusCode::UNAUTHORIZED, Json(error_json)).into_response();
        }
    };

    let virtual_key = match state.keystore.authenticate(token).await {
        Ok(k) => k,
        Err(_) => {
            let error_json = serde_json::json!({
                "error": {
                    "message": "Invalid or missing API key",
                    "type": "invalid_request_error",
                    "code": 401
                }
            });
            return (StatusCode::UNAUTHORIZED, Json(error_json)).into_response();
        }
    };

    // Verify model access
    let model_allowed = virtual_key.allowed_models.iter().any(|m| m == "*" || m == &request.model);
    if !model_allowed {
        let error_json = serde_json::json!({
            "error": {
                "message": "Model access denied for this virtual key",
                "type": "permission_denied",
                "code": 403
            }
        });
        return (StatusCode::FORBIDDEN, Json(error_json)).into_response();
    }

    // Check cumulative token budget
    if let Some(max_budget) = virtual_key.max_budget_tokens {
        let used = virtual_key.used_tokens.load(std::sync::atomic::Ordering::Relaxed);
        if used >= max_budget {
            let error_json = serde_json::json!({
                "error": {
                    "message": "Virtual key budget exhausted",
                    "type": "budget_exceeded",
                    "code": 403
                }
            });
            return (StatusCode::FORBIDDEN, Json(error_json)).into_response();
        }
    }

    if let Some(ref tools) = request.tools {
        for tool in tools {
            if let Err(e) = state.mcp_guard.register_tool(tool) {
                tracing::warn!(error = %e, tool = %tool.name, "Failed to register tool definition in MCP guard");
            }
        }
    }

    let start = Instant::now();

    let config = state.config.load();
    
    // Check if the model alias exists in our routes
    let route = match config.routes.get(&request.model) {
        Some(r) => r,
        None => {
            metrics::inc_requests_total(&request.model, "", StatusCode::NOT_FOUND.as_u16());
            let error_response = serde_json::json!({
                "error": format!("Model '{}' not found", request.model)
            });
            return (StatusCode::NOT_FOUND, Json(error_response)).into_response();
        }
    };

    // Estimate tokens for rate limiting
    let estimated_tokens = estimate_request_tokens(&request.model, &request.messages, request.max_tokens);

    // Check rate limit
    if let Err(rate_error) = state.limiter.check_and_reserve(estimated_tokens).await {
        metrics::inc_rate_limit_rejections_total(&request.model);
        let error_json = serde_json::json!({
            "error": {
                "message": "Rate limit exceeded. Please wait.",
                "type": "requests",
                "code": 429
            }
        });
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("Retry-After", rate_error.retry_after_secs.to_string())],
            Json(error_json),
        ).into_response();
    }

    // Try each target in order
    for target in &route.targets {
        let provider = match config.providers.get(&target.provider_name) {
            Some(p) => p,
            None => continue,
        };

        match state.client
            .forward_chat_completion(provider, &target.upstream_model, request.clone())
            .await
        {
            Ok(upstream_res) => {
                let status = upstream_res.status().as_u16();
                metrics::inc_requests_total(&request.model, &target.provider_name, status);
                
                if status == 200 {
                    let elapsed = start.elapsed().as_secs_f64();
                    metrics::record_duration_seconds(&request.model, &target.provider_name, elapsed);
                    
                    if request.stream.unwrap_or(false) {
                        return pipe_upstream_sse(upstream_res, _stream_guard);
                    } else {
                        let headers = upstream_res.headers().clone();
                        let body = match upstream_res.bytes().await {
                            Ok(b) => b,
                            Err(_) => {
                                return (
                                    StatusCode::BAD_GATEWAY,
                                    Json(serde_json::json!({"error": "Failed to read upstream response"})),
                                ).into_response();
                            }
                        };

                        if let Ok(mut json_value) = serde_json::from_slice::<Value>(&body) {
                            // MCP Tool Call Validation
                            if let Some(choices) = json_value.get_mut("choices").and_then(|c| c.as_array_mut()) {
                                if let Some(first_choice) = choices.first_mut() {
                                    if let Some(message) = first_choice.get_mut("message") {
                                        if let Some(tool_calls_val) = message.get("tool_calls") {
                                            if let Ok(tool_calls) = serde_json::from_value::<Vec<crate::security::mcp_guard::ToolCall>>(tool_calls_val.clone()) {
                                                let mut validation_failed = false;
                                                let mut violation_details = String::new();

                                                for tc in &tool_calls {
                                                    if let Err(err) = state.mcp_guard.validate_tool_call(tc) {
                                                        validation_failed = true;
                                                        violation_details = err.to_string();
                                                        let violation_type = match &err {
                                                            crate::security::mcp_guard::McpSecurityError::SchemaViolation { .. } => "schema_violation",
                                                            crate::security::mcp_guard::McpSecurityError::UnregisteredTool(_) => "unregistered_tool",
                                                            crate::security::mcp_guard::McpSecurityError::JsonParseError(_) => "json_parse_error",
                                                        };
                                                        metrics::inc_mcp_violations_total(&tc.function.name, violation_type);
                                                        tracing::error!(
                                                            tool = %tc.function.name,
                                                            error = %err,
                                                            "MCP security violation detected: blocked tool call execution attempt."
                                                        );
                                                        break;
                                                    }
                                                }

                                                if validation_failed {
                                                    let error_resp = serde_json::json!({
                                                        "error": {
                                                            "message": format!("Tool execution blocked by gateway schema validation: {}", violation_details),
                                                            "type": "mcp_security_violation",
                                                            "code": 400
                                                        }
                                                    });
                                                    return (StatusCode::BAD_REQUEST, Json(error_resp)).into_response();
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if let Some(usage) = json_value.get("usage") {
                                let input = usage.get("prompt_tokens").and_then(|t| t.as_u64()).unwrap_or(0);
                                let output = usage.get("completion_tokens").and_then(|t| t.as_u64()).unwrap_or(0);
                                let total = usage.get("total_tokens").and_then(|t| t.as_u64()).unwrap_or(input + output);

                                if input > 0 {
                                    metrics::inc_input_tokens_total(&request.model, input);
                                }
                                if output > 0 {
                                    metrics::inc_output_tokens_total(&request.model, output);
                                }
                                virtual_key.used_tokens.fetch_add(total, std::sync::atomic::Ordering::Relaxed);
                            }
                        }

                        let mut response = AxumResponse::new(Body::from(body));
                        *response.status_mut() = StatusCode::OK;
                        *response.headers_mut() = headers;
                        return response;
                    }
                }
            }
            Err(_) => {
                continue;
            }
        }
    }

    // All targets failed
    metrics::inc_requests_total(&request.model, "", StatusCode::BAD_GATEWAY.as_u16());
    let error_json = serde_json::json!({
        "error": {
            "message": "All fallback providers failed",
            "type": "gateway_timeout",
            "code": 502
        }
    });
    (StatusCode::BAD_GATEWAY, Json(error_json)).into_response()
}

/// Administrative handler for reloading gateway configuration from disk
pub async fn handle_admin_reload(
    State(state): State<Arc<AppState>>,
) -> AxumResponse {
    let path = match &state.config_path {
        Some(p) => p.clone(),
        None => std::path::PathBuf::from(
            std::env::var("GATEWAY_CONFIG_PATH").unwrap_or_else(|_| "./config.yaml".to_string()),
        ),
    };

    match crate::config::loader::load_config_from_file(&path) {
        Ok(new_config) => {
            tracing::info!(
                path = %path.display(),
                providers = new_config.providers.len(),
                routes = new_config.routes.len(),
                virtual_keys = new_config.virtual_keys.len(),
                "Admin reload triggered: configuration updated successfully"
            );

            state.keystore.reload_keys(&new_config.virtual_keys).await;
            state.config.store(Arc::new(new_config));

            (
                StatusCode::OK,
                Json(serde_json::json!({ "status": "reloaded" })),
            )
                .into_response()
        }
        Err(err) => {
            tracing::error!(
                path = %path.display(),
                error = %err,
                "Admin reload triggered: failed to load or validate configuration"
            );
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": {
                        "message": format!("Configuration reload failed: {}", err),
                        "type": "config_reload_error",
                        "code": 400
                    }
                })),
            )
                .into_response()
        }
    }
}

/// Estimate token count using normalizer
fn estimate_request_tokens(model: &str, messages: &[ChatMessage], max_tokens_param: Option<u32>) -> u64 {
    crate::proxy::normalizer::estimate_request_tokens(model, messages, max_tokens_param)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test]
    async fn test_admin_reload_endpoint() {
        let temp_dir = std::env::temp_dir().join(format!("gw_reload_test_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let config_path = temp_dir.join("config.yaml");

        let initial_yaml = r#"
providers:
  local:
    base_url: "http://127.0.0.1:11434/v1"
routes:
  default:
    targets:
      - provider_name: local
        upstream_model: "llama3"
"#;
        std::fs::write(&config_path, initial_yaml).unwrap();

        let initial_cfg = crate::config::loader::load_config_from_file(&config_path).unwrap();
        let state = Arc::new(AppState {
            config: Arc::new(arc_swap::ArcSwap::from_pointee(initial_cfg)),
            config_path: Some(config_path.clone()),
            client: crate::proxy::ProxyClient::new(),
            limiter: crate::ratelimit::sliding_window::SlidingWindowLimiter::new(100_000, 60),
            mcp_guard: Arc::new(crate::security::mcp_guard::McpSchemaGuard::new()),
            keystore: Arc::new(crate::auth::keys::KeyStore::default_dev_keystore()),
            tracker: Arc::new(InFlightTracker::new()),
            start_time: Instant::now(),
            adaptive_engine: Arc::new(AdaptiveScoreEngine::new(0.2)),
        });

        // Modify file on disk
        let updated_yaml = r#"
providers:
  local:
    base_url: "http://127.0.0.1:11434/v1"
routes:
  default:
    targets:
      - provider_name: local
        upstream_model: "llama3"
  admin-model:
    targets:
      - provider_name: local
        upstream_model: "mistral"
"#;
        let mut f = std::fs::File::create(&config_path).unwrap();
        f.write_all(updated_yaml.as_bytes()).unwrap();
        drop(f);

        let resp = handle_admin_reload(State(state.clone())).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(state.config.load().routes.contains_key("admin-model"));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}