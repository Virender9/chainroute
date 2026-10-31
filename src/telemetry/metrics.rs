use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics::{counter, histogram};

/// Initialize the global Prometheus recorder.
pub fn setup_metrics_recorder() -> PrometheusHandle {
    PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder")
}

/// Record the total request count labeled by model, provider, and HTTP status.
pub fn inc_requests_total(model: &str, provider: &str, status: u16) {
    counter!("gen_ai_requests_total", "model" => model.to_string(), "provider" => provider.to_string(), "status" => status.to_string())
        .increment(1);
}

/// Record the wall-clock duration of the request.
pub fn record_duration_seconds(model: &str, provider: &str, elapsed: f64) {
    histogram!("gen_ai_request_duration_seconds", "model" => model.to_string(), "provider" => provider.to_string())
        .record(elapsed);
}

/// Increment the input token counter.
pub fn inc_input_tokens_total(model: &str, tokens: u64) {
    counter!("gen_ai_input_tokens_total", "model" => model.to_string()).increment(tokens);
}

/// Increment the output token counter.
pub fn inc_output_tokens_total(model: &str, tokens: u64) {
    counter!("gen_ai_output_tokens_total", "model" => model.to_string()).increment(tokens);
}

/// Increment the prefix-cache hit counter.
#[allow(dead_code)]
pub fn inc_prefix_cache_hits_total(model: &str) {
    counter!("gen_ai_prefix_cache_hits_total", "model" => model.to_string()).increment(1);
}

/// Increment the rate-limit rejection counter.
pub fn inc_rate_limit_rejections_total(model: &str) {
    counter!("gen_ai_rate_limit_rejections_total", "model" => model.to_string()).increment(1);
}

/// Increment the MCP security violation counter labeled by tool and violation_type.
pub fn inc_mcp_violations_total(tool: &str, violation_type: &str) {
    counter!("gen_ai_mcp_violations_total", "tool" => tool.to_string(), "violation_type" => violation_type.to_string()).increment(1);
}

/// Record EWMA latency for a target (gauge metric).
#[allow(dead_code)]
pub fn set_target_ewma_latency_ms(target: &str, latency_ms: f64) {
    histogram!("gen_ai_target_ewma_latency_ms", "target" => target.to_string()).record(latency_ms);
}

/// Increment the semantic coalescing counter labeled by model.
pub fn inc_coalesced_requests_total(model: &str) {
    counter!("gen_ai_coalesced_requests_total", "model" => model.to_string()).increment(1);
}

/// Increment the plugin execution counter labeled by plugin, hook, and status.
pub fn inc_plugin_executions(plugin: &str, hook: &str, status: &str) {
    counter!("gen_ai_plugin_executions_total", "plugin" => plugin.to_string(), "hook" => hook.to_string(), "status" => status.to_string()).increment(1);
}

/// Record plugin execution duration labeled by plugin and hook.
pub fn record_plugin_duration(plugin: &str, hook: &str, elapsed: f64) {
    histogram!("gen_ai_plugin_duration_seconds", "plugin" => plugin.to_string(), "hook" => hook.to_string()).record(elapsed);
}
