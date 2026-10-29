use std::collections::HashMap;
use std::path::Path;
use serde::Deserialize;
use thiserror::Error;
use crate::config::providers::{GatewayConfig, ModelRoute, ProviderConfig, VirtualKeyConfig};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read configuration file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse YAML configuration: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("Configuration validation error: {0}")]
    Validation(String),
}

/// Scans for patterns like `${ENV_VAR}` or `${ENV_VAR:-default_val}` and
/// substitutes them with system environment variables before deserialization.
pub fn interpolate_env_vars(raw_yaml: &str) -> String {
    let mut result = String::with_capacity(raw_yaml.len());
    let mut chars = raw_yaml.char_indices().peekable();

    while let Some((_, ch)) = chars.next() {
        // Handle escaped \${
        if ch == '\\' {
            if let Some(&(_, next_ch)) = chars.peek() {
                if next_ch == '$' {
                    chars.next();
                    if let Some(&(_, after_next)) = chars.peek() {
                        if after_next == '{' {
                            chars.next();
                            result.push_str("${");
                            continue;
                        }
                    }
                    result.push('$');
                    continue;
                }
            }
            result.push('\\');
            continue;
        }

        if ch == '$' {
            if let Some(&(_, next_ch)) = chars.peek() {
                if next_ch == '{' {
                    chars.next();
                    let mut var_spec = String::new();
                    let mut closed = false;
                    while let Some((_, inner_ch)) = chars.next() {
                        if inner_ch == '}' {
                            closed = true;
                            break;
                        }
                        var_spec.push(inner_ch);
                    }

                    if closed {
                        if let Some((var_name, default_val)) = var_spec.split_once(":-") {
                            let var_name = var_name.trim();
                            match std::env::var(var_name) {
                                Ok(val) if !val.is_empty() => result.push_str(&val),
                                _ => result.push_str(default_val),
                            }
                        } else {
                            let var_name = var_spec.trim();
                            if let Ok(val) = std::env::var(var_name) {
                                result.push_str(&val);
                            }
                        }
                        continue;
                    } else {
                        result.push_str("${");
                        result.push_str(&var_spec);
                        continue;
                    }
                }
            }
        }

        result.push(ch);
    }

    result
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ProvidersFormat {
    Map(HashMap<String, ProviderConfig>),
    List(Vec<ProviderConfig>),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RoutesFormat {
    Map(HashMap<String, ModelRoute>),
    List(Vec<ModelRoute>),
}

#[derive(Debug, Deserialize)]
struct RawGatewayConfig {
    #[serde(default)]
    providers: Option<ProvidersFormat>,
    #[serde(default)]
    routes: Option<RoutesFormat>,
    #[serde(default)]
    virtual_keys: Vec<VirtualKeyConfig>,
}

/// Parses a YAML string into a validated `GatewayConfig`.
pub fn parse_config_str(content: &str) -> Result<GatewayConfig, ConfigError> {
    let raw: RawGatewayConfig = serde_yaml::from_str(content)?;

    let mut providers = HashMap::new();
    if let Some(p_fmt) = raw.providers {
        match p_fmt {
            ProvidersFormat::Map(map) => {
                for (k, mut v) in map {
                    if v.name.is_empty() {
                        v.name = k.clone();
                    }
                    providers.insert(k, v);
                }
            }
            ProvidersFormat::List(list) => {
                for v in list {
                    providers.insert(v.name.clone(), v);
                }
            }
        }
    }

    let mut routes = HashMap::new();
    if let Some(r_fmt) = raw.routes {
        match r_fmt {
            RoutesFormat::Map(map) => {
                for (k, mut v) in map {
                    if v.alias.is_empty() {
                        v.alias = k.clone();
                    }
                    routes.insert(k, v);
                }
            }
            RoutesFormat::List(list) => {
                for v in list {
                    routes.insert(v.alias.clone(), v);
                }
            }
        }
    }

    let config = GatewayConfig {
        providers,
        routes,
        virtual_keys: raw.virtual_keys,
    };

    config.validate()?;
    Ok(config)
}

/// Reads a YAML file from disk, expands environment variables, and parses it into `GatewayConfig`.
pub fn load_config_from_file(path: &Path) -> Result<GatewayConfig, ConfigError> {
    let raw_yaml = std::fs::read_to_string(path)?;
    let interpolated = interpolate_env_vars(&raw_yaml);
    parse_config_str(&interpolated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interpolate_env_vars_basic() {
        std::env::set_var("TEST_GATEWAY_PORT", "9090");
        std::env::set_var("TEST_GATEWAY_KEY", "sk-secret123");

        let input = "port: ${TEST_GATEWAY_PORT}\nkey: ${TEST_GATEWAY_KEY}\nother: ${UNSET_VAR_XYZ:-default_val}";
        let out = interpolate_env_vars(input);

        assert!(out.contains("port: 9090"));
        assert!(out.contains("key: sk-secret123"));
        assert!(out.contains("other: default_val"));
    }

    #[test]
    fn test_interpolate_env_vars_defaults_and_escapes() {
        let input = r#"val: \${ESCAPED_VAR} and ${UNSET_XYZ:-fallback}"#;
        let out = interpolate_env_vars(input);
        assert_eq!(out, "val: ${ESCAPED_VAR} and fallback");
    }

    #[test]
    fn test_parse_config_str_map_format() {
        let yaml = r#"
providers:
  local:
    base_url: "http://127.0.0.1:11434/v1"
    timeout_seconds: 45
  openai:
    base_url: "https://api.openai.com/v1"
    api_key: "test-key"

routes:
  default:
    context_window: 8192
    routing_strategy: priority
    targets:
      - provider_name: local
        upstream_model: "llama3"
      - provider_name: openai
        upstream_model: "gpt-4o"

virtual_keys:
  - id: "vk-1"
    name: "Key 1"
    key: "secret-1"
    allowed_models: ["*"]
    max_budget_tokens: 1000
"#;
        let cfg = parse_config_str(yaml).unwrap();
        assert_eq!(cfg.providers.len(), 2);
        assert_eq!(cfg.providers["local"].name, "local");
        assert_eq!(cfg.providers["local"].timeout_seconds, 45);
        assert_eq!(cfg.routes.len(), 1);
        assert_eq!(cfg.routes["default"].alias, "default");
        assert_eq!(cfg.routes["default"].targets.len(), 2);
        assert_eq!(cfg.virtual_keys.len(), 1);
        assert_eq!(cfg.virtual_keys[0].id, "vk-1");
    }

    #[test]
    fn test_parse_config_validation_failure() {
        // Target points to undefined provider
        let yaml = r#"
providers:
  local:
    base_url: "http://127.0.0.1:11434/v1"
routes:
  default:
    targets:
      - provider_name: nonexistent
        upstream_model: "llama3"
"#;
        let res = parse_config_str(yaml);
        assert!(res.is_err());
        match res {
            Err(ConfigError::Validation(msg)) => {
                assert!(msg.contains("references undefined provider 'nonexistent'"));
            }
            _ => panic!("Expected Validation error"),
        }
    }

    #[test]
    fn test_load_example_config_file() {
        let example_path = Path::new("config.example.yaml");
        assert!(example_path.exists(), "config.example.yaml must exist at root");
        let cfg = load_config_from_file(example_path).expect("Failed to load config.example.yaml");

        assert!(cfg.providers.contains_key("local"));
        assert!(cfg.providers.contains_key("openai"));
        assert!(cfg.routes.contains_key("default"));
        assert!(cfg.routes.contains_key("coding-fast"));
        assert!(cfg.routes.contains_key("deepseek"));
        assert_eq!(cfg.virtual_keys.len(), 3);
        assert_eq!(cfg.routes["deepseek"].context_window, Some(65536));
        assert_eq!(cfg.routes["coding-fast"].routing_strategy, crate::config::providers::RoutingStrategy::LowestLatency);
    }
}