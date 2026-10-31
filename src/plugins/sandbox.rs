use std::sync::Arc;
use tokio::sync::Mutex;
use thiserror::Error;
use serde_json::Value;

#[derive(Debug, Error)]
pub enum PluginError {
    #[error("Extism initialization error: {0}")]
    Init(String),
    #[error("WASM execution trapped or timed out: {0}")]
    Execution(String),
    #[error("Invalid JSON input/output: {0}")]
    Serialization(String),
}

#[derive(Clone)]
pub struct WasmPlugin {
    pub name: String,
    pub inner: Arc<Mutex<extism::Plugin>>,
}

impl WasmPlugin {
    pub fn new(
        name: String,
        wasm_bytes: &[u8],
        timeout_ms: u64,
        memory_max_bytes: usize,
    ) -> Result<Self, PluginError> {
        let manifest = extism::Manifest::new([extism::Wasm::data(wasm_bytes.to_vec())])
            .with_memory_max(memory_max_bytes as u32);
        let plugin = extism::PluginBuilder::new(manifest)
            .with_wasi(true)
            .with_fuel_limit(timeout_ms)
            .build()
            .map_err(|e| PluginError::Init(e.to_string()))?;

        Ok(Self {
            name,
            inner: Arc::new(Mutex::new(plugin)),
        })
    }

    pub async fn call_json(
        &self,
        func_name: &str,
        input_json: &Value,
    ) -> Result<Value, PluginError> {
        let input_str = serde_json::to_string(input_json)
            .map_err(|e| PluginError::Serialization(e.to_string()))?;

        let mut plugin = self.inner.lock().await;
        
        let output_bytes = plugin.call::<&str, &[u8]>(func_name, &input_str)
            .map_err(|e| PluginError::Execution(e.to_string()))?;

        let output_str = std::str::from_utf8(output_bytes)
            .map_err(|e| PluginError::Serialization(e.to_string()))?;

        let output_val = serde_json::from_str::<Value>(output_str)
            .map_err(|e| PluginError::Serialization(e.to_string()))?;

        Ok(output_val)
    }

    pub async fn has_function(&self, func_name: &str) -> bool {
        let plugin = self.inner.lock().await;
        plugin.function_exists(func_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_wasm_error() {
        let bad_bytes = b"not a wasm module";
        let res = WasmPlugin::new("bad".into(), bad_bytes, 50, 32 * 1024 * 1024);
        assert!(res.is_err());
        match res {
            Err(PluginError::Init(_)) => {}
            _ => panic!("Expected Init error"),
        }
    }
}
