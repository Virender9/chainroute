use crate::api::completions::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct AdminStatusResponse {
    pub in_flight_requests: usize,
    pub uptime_seconds: u64,
    pub total_requests: u64,
    pub memory_mb: u64,
    pub routes: Vec<AdminRouteInfo>,
    pub providers: Vec<AdminProviderInfo>,
    pub virtual_keys: Vec<AdminKeyInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::providers::GatewayConfig;
    use crate::server::drain::InFlightTracker;
    use crate::router::adaptive::AdaptiveScoreEngine;
    use std::time::Instant;

    #[tokio::test]
    async fn test_admin_status_response() {
        let config = GatewayConfig::default_local();
        let keystore = Arc::new(crate::auth::keys::KeyStore::from_configs(&config.virtual_keys));
        let state = Arc::new(AppState {
            config: Arc::new(arc_swap::ArcSwap::from_pointee(config)),
            config_path: None,
            client: crate::proxy::ProxyClient::new(),
            limiter: crate::ratelimit::sliding_window::SlidingWindowLimiter::new(100_000, 60),
            mcp_guard: Arc::new(crate::security::mcp_guard::McpSchemaGuard::new()),
            keystore,
            tracker: Arc::new(InFlightTracker::new()),
            start_time: Instant::now(),
            adaptive_engine: Arc::new(AdaptiveScoreEngine::new(0.2)),
        });

        let result = handle_admin_status(State(state)).await;
        assert!(result.is_ok());
        let resp = result.unwrap().0;
        assert_eq!(resp.in_flight_requests, 0);
        assert!(!resp.routes.is_empty());
    }
}

#[derive(Debug, Serialize)]
pub struct AdminRouteInfo {
    pub alias: String,
    pub targets: Vec<AdminRouteTarget>,
    pub context_window: Option<u64>,
    pub routing_strategy: String,
}

#[derive(Debug, Serialize)]
pub struct AdminRouteTarget {
    pub provider_name: String,
    pub upstream_model: String,
    pub priority: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct AdminProviderInfo {
    pub name: String,
    pub base_url: String,
    pub ewma_latency_ms: f64,
    pub consecutive_errors: u32,
    pub circuit_state: String,
}

#[derive(Debug, Serialize)]
pub struct AdminKeyInfo {
    pub id: String,
    pub name: String,
    pub allowed_models: Vec<String>,
    pub used_tokens: u64,
    pub max_budget_tokens: Option<u64>,
    pub enabled: bool,
}

pub async fn handle_admin_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<AdminStatusResponse>, StatusCode> {
    let tracker = &state.tracker;
    let in_flight = tracker.len();

    let uptime_seconds = state
        .start_time
        .elapsed()
        .as_secs();

    let config = state.config.load();
    let keystore = &state.keystore;
    let adaptive = &state.adaptive_engine;

    // Build provider info with circuit breaker state from adaptive engine
    let adaptive_state = adaptive.state.read().await;
    let providers: Vec<AdminProviderInfo> = config
        .providers
        .iter()
        .map(|(name, provider)| {
            let target_key = format!("{}:{}", name, "default");
            let stats = adaptive_state.get(&target_key);
            let ewma = stats.map(|s| s.ewma_latency_ms).unwrap_or(0.0);
            let errors = stats.map(|s| s.consecutive_errors).unwrap_or(0);
            let circuit_state = if errors >= 5 {
                "Open".to_string()
            } else if errors >= 3 {
                "HalfOpen".to_string()
            } else {
                "Closed".to_string()
            };
            AdminProviderInfo {
                name: provider.name.clone(),
                base_url: provider.base_url.clone(),
                ewma_latency_ms: ewma,
                consecutive_errors: errors,
                circuit_state,
            }
        })
        .collect();
    drop(adaptive_state);

    // Build route info
    let routes: Vec<AdminRouteInfo> = config
        .routes
        .iter()
        .map(|(alias, route)| {
            AdminRouteInfo {
                alias: alias.clone(),
                targets: route
                    .targets
                    .iter()
                    .map(|t| AdminRouteTarget {
                        provider_name: t.provider_name.clone(),
                        upstream_model: t.upstream_model.clone(),
                        priority: t.priority,
                    })
                    .collect(),
                context_window: route.context_window,
                routing_strategy: format!("{:?}", route.routing_strategy),
            }
        })
        .collect();

    // Build virtual key info
    let keystore_guard = keystore.keys.read().await;
    let virtual_keys: Vec<AdminKeyInfo> = keystore_guard
        .values()
        .map(|vkey| {
            AdminKeyInfo {
                id: vkey.id.clone(),
                name: vkey.name.clone(),
                allowed_models: vkey.allowed_models.clone(),
                used_tokens: vkey.used_tokens.load(std::sync::atomic::Ordering::Relaxed),
                max_budget_tokens: vkey.max_budget_tokens,
                enabled: vkey.enabled,
            }
        })
        .collect();
    drop(keystore_guard);

    // Memory approximation (RSS) - best effort on Windows/Unix
    let memory_mb = get_memory_mb();

    Ok(Json(AdminStatusResponse {
        in_flight_requests: in_flight,
        uptime_seconds,
        total_requests: 0, // TODO: wire up request counter
        memory_mb,
        routes,
        providers,
        virtual_keys,
    }))
}

fn get_memory_mb() -> u64 {
    // Best-effort memory reading; returns 0 if not available
    #[cfg(unix)]
    {
        use std::process::Command;
        if let Ok(output) = Command::new("ps").args(["-o", "rss=", "-p", &std::process::id().to_string()]).output() {
            if let Ok(text) = String::from_utf8(output.stdout) {
                if let Ok(kb) = text.trim().parse::<u64>() {
                    return kb / 1024;
                }
            }
        }
    }
    0
}