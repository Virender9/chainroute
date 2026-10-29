use anyhow::{Result, anyhow};
use std::sync::Arc;
use serde_json::Value;
use crate::api::completions::ChatMessage;
use crate::plugins::sandbox::WasmPlugin;
use tracing::{warn, info};

/// Run ALL preflight wasm transforms that are enabled for the request.
/// If any plugin fails, the original messages are left untouched and
/// a warning is logged.  The function mutates the slice in‑place.
pub fn run_preflight_transformations(messages: &mut Vec<ChatMessage>, plugins: &mut [WasmPlugin]) {
    for plugin in plugins.iter_mut() {
        // Serialize current messages to JSON
        let payload = serde_json::to_string(messages).unwrap_or_else(|_| "[]".into());
        match plugin.execute_transform(&payload) {
            Ok(out) => {
                // Expect the plugin to return a JSON array of ChatMessage
                match serde_json::from_str::<Vec<ChatMessage>>(&out) {
                    Ok(transformed) => {
                        let orig_len = messages.len();
                        messages.extend(transformed);
                        let new_len = messages.len();
                        info!(?plugin.name, changes = new_len - orig_len, "transformed messages via wasm plugin");
                    }
                    Err(e) => {
                        warn!(?plugin.name, ?e, "plugin returned non‑ChatMessage JSON");
                    }
                }
            }
            Err(e) => {
                warn!(?plugin.name, %e, "plugin transform failed – skipping");
            }
        }
    }
}
