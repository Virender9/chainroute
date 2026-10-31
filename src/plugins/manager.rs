use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::api::completions::ChatCompletionRequest;
use crate::plugins::sandbox::{PluginError, WasmPlugin};
use serde_json::Value;
use tracing::warn;

/// Manager for loading and executing sandboxed WASM plugins.
#[derive(Clone)]
pub struct PluginManager {
    pub plugins: Arc<RwLock<Vec<WasmPlugin>>>,
}

impl PluginManager {
    /// Loads all *.wasm files from `dir_path`.
    /// If the directory does not exist or is empty, initializes with an empty list.
    pub fn load_from_dir(dir_path: &Path) -> Result<Self, PluginError> {
        let mut plugins = Vec::new();
        
        if !dir_path.exists() {
            return Ok(Self { plugins: Arc::new(RwLock::new(plugins)) });
        }
        
        let entries = match std::fs::read_dir(dir_path) {
            Ok(e) => e,
            Err(err) => return Err(PluginError::Init(err.to_string())),
        };

        for entry in entries {
            let entry = entry.map_err(|e| PluginError::Init(e.to_string()))?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "wasm") {
                let name = path
                    .file_stem()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
                let bytes = std::fs::read(&path)
                    .map_err(|e| PluginError::Init(format!("Failed to read {:?}: {}", path, e)))?;
                // Use defaults: 50ms timeout, 32 MiB memory
                let plugin = WasmPlugin::new(name.clone(), &bytes, 50, 32 * 1024 * 1024)?;
                plugins.push(plugin);
            }
        }
        
        Ok(Self { plugins: Arc::new(RwLock::new(plugins)) })
    }
    
    /// Runs pre-flight plugins. If a plugin traps/times out, log warning and continue.
    pub async fn run_preflight(&self, req: &mut ChatCompletionRequest) {
        let plugins = self.plugins.read().await;
        let mut input = serde_json::to_value(&*req).unwrap_or_default();
        
        for plugin in plugins.iter() {
            if !plugin.has_function("on_request").await {
                continue;
            }
            
            let start = std::time::Instant::now();
            match plugin.call_json("on_request", &input).await {
                Ok(output) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    crate::telemetry::metrics::record_plugin_duration(&plugin.name, "preflight", elapsed);
                    crate::telemetry::metrics::inc_plugin_executions(&plugin.name, "preflight", "success");
                    if let Ok(transformed) = serde_json::from_value::<ChatCompletionRequest>(output.clone()) {
                        *req = transformed;
                        input = output;
                    } else {
                        warn!(plugin = %plugin.name, "on_request returned non-request JSON, ignoring");
                    }
                }
                Err(e) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    crate::telemetry::metrics::record_plugin_duration(&plugin.name, "preflight", elapsed);
                    crate::telemetry::metrics::inc_plugin_executions(&plugin.name, "preflight", "error");
                    warn!(plugin = %plugin.name, error = %e, "plugin on_request failed – skipping");
                }
            }
        }
    }
    
    /// Runs post-flight plugins. If a plugin traps/times out, log warning and continue.
    pub async fn run_postflight(&self, res: &mut Value) {
        let plugins = self.plugins.read().await;
        
        for plugin in plugins.iter() {
            if !plugin.has_function("on_response").await {
                continue;
            }
            
            let start = std::time::Instant::now();
            match plugin.call_json("on_response", res).await {
                Ok(output) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    crate::telemetry::metrics::record_plugin_duration(&plugin.name, "postflight", elapsed);
                    crate::telemetry::metrics::inc_plugin_executions(&plugin.name, "postflight", "success");
                    *res = output;
                }
                Err(e) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    crate::telemetry::metrics::record_plugin_duration(&plugin.name, "postflight", elapsed);
                    crate::telemetry::metrics::inc_plugin_executions(&plugin.name, "postflight", "error");
                    warn!(plugin = %plugin.name, error = %e, "plugin on_response failed – skipping");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_from_nonexistent_dir() {
        let mgr = PluginManager::load_from_dir(Path::new("/nonexistent/path")).unwrap();
        assert_eq!(mgr.plugins.blocking_read().len(), 0);
    }

    #[test]
    fn test_load_empty_dir() {
        let tmp = std::env::temp_dir().join("gw_plugins_empty_test");
        std::fs::create_dir_all(&tmp).unwrap();
        let mgr = PluginManager::load_from_dir(&tmp).unwrap();
        assert_eq!(mgr.plugins.blocking_read().len(), 0);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_plugin_manager_bypass() {
        let mgr = PluginManager::load_from_dir(Path::new("/nonexistent/path")).unwrap();
        let mut req = ChatCompletionRequest {
            model: "default".to_string(),
            messages: vec![],
            stream: None,
            temperature: None,
            max_tokens: None,
            tools: None,
        };
        mgr.run_preflight(&mut req).await;
        assert_eq!(req.model, "default");

        let mut res = serde_json::json!({"ok": true});
        mgr.run_postflight(&mut res).await;
        assert_eq!(res["ok"], true);
    }
}

