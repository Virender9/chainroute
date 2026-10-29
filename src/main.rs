mod api;
mod auth;
mod config;
mod proxy;
mod router;
mod ratelimit;
mod telemetry;
mod security;
mod server;


use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{routing::{get, post}, Json, Router};
use serde_json::{json, Value};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gateway_core=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Initialize metrics recorder
    let metrics_handle = crate::telemetry::metrics::setup_metrics_recorder();
    
    // Check for GATEWAY_CONFIG_PATH environment variable or fallback to ./config.yaml
    let config_path = std::env::var("GATEWAY_CONFIG_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("./config.yaml"));

    // If file exists, load it; otherwise fallback to GatewayConfig::default_local()
    let initial_config = if config_path.exists() {
        match crate::config::loader::load_config_from_file(&config_path) {
            Ok(cfg) => {
                tracing::info!(path = %config_path.display(), "Loaded configuration from file");
                cfg
            }
            Err(err) => {
                tracing::error!(path = %config_path.display(), error = %err, "Failed to load configuration file; falling back to default local config");
                crate::config::providers::GatewayConfig::default_local()
            }
        }
    } else {
        tracing::info!(path = %config_path.display(), "Config file not found; falling back to default local config");
        crate::config::providers::GatewayConfig::default_local()
    };

    let keystore = Arc::new(crate::auth::keys::KeyStore::from_configs(&initial_config.virtual_keys));
    let config_holder = Arc::new(arc_swap::ArcSwap::from_pointee(initial_config));

    // Spawn background file watcher thread/task
    crate::config::watcher::start_config_watcher_with_keystore(
        config_path.clone(),
        config_holder.clone(),
        Some(keystore.clone()),
    );

    // Initialize shared application state
    let start_time = Instant::now();
    let adaptive_engine = Arc::new(crate::router::adaptive::AdaptiveScoreEngine::new(0.2));
    let tracker = Arc::new(crate::server::drain::InFlightTracker::new());
    let app_state = Arc::new(crate::api::AppState {
        config: config_holder,
        config_path: Some(config_path),
        client: crate::proxy::ProxyClient::new(),
        limiter: crate::ratelimit::sliding_window::SlidingWindowLimiter::new(100_000, 60),
        mcp_guard: Arc::new(crate::security::mcp_guard::McpSchemaGuard::new()),
        keystore,
        tracker,
        start_time,
        adaptive_engine,
    });

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get({
            let handle = metrics_handle.clone();
            move || {
                let h = handle.clone();
                async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                        h.render()
                    )
                }
            }
        }))
        .route("/v1/models", get(crate::api::models::handle_list_models))
        .route(
            "/v1/chat/completions",
            post(crate::api::completions::handle_chat_completions),
        )
        .route(
            "/admin/reload",
            post(crate::api::completions::handle_admin_reload),
        )
        .route("/admin/status", get(crate::api::admin::handle_admin_status))
        .route("/", get(crate::server::embedded_ui::serve_embedded_ui))
        .route("/dashboard", get(crate::server::embedded_ui::serve_embedded_ui))
        .with_state(app_state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tracing::info!("gateway-core listening on {}", addr);
    tracing::info!("Admin dashboard available at http://{}/dashboard", addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(crate::server::drain::shutdown_signal())
        .await
        .unwrap();

    tracing::info!("Server listener closed, draining active streaming connections...");
    let drained = app_state.tracker.wait_for_drain(Duration::from_secs(30)).await;
    if drained {
        tracing::info!("All in-flight streams drained successfully. Exiting.");
    } else {
        tracing::warn!(
            remaining = app_state.tracker.len(),
            "Graceful shutdown timed out: {} in-flight streams still active, forcing exit.",
            app_state.tracker.len()
        );
    }
}

async fn health_check() -> Json<Value> {
    Json(json!({"status": "ok"}))
}