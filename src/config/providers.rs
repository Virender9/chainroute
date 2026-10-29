use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::config::loader::ConfigError;

/// Routing strategy for candidate targets
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutingStrategy {
    #[default]
    Priority,
    LowestCost,
    LowestLatency,
    AdaptiveBalanced,
}

/// Configuration for a single provider (e.g., Ollama, OpenAI)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderConfig {
    #[serde(default)]
    pub name: String,
    pub base_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

fn default_timeout() -> u64 {
    60
}

impl ProviderConfig {
    pub fn new(name: String, base_url: String, api_key: Option<String>, timeout_seconds: u64) -> Self {
        Self {
            name,
            base_url,
            api_key,
            timeout_seconds,
        }
    }
}

/// A candidate target within a route's fallback chain
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouteTarget {
    pub provider_name: String,
    pub upstream_model: String,
    #[serde(default)]
    pub priority: Option<u32>,
    #[serde(default)]
    pub weight: Option<u32>,
    #[serde(default)]
    pub input_cost_per_million: f64,
    #[serde(default)]
    pub output_cost_per_million: f64,
}

impl RouteTarget {
    pub fn new(provider_name: String, upstream_model: String) -> Self {
        Self {
            provider_name,
            upstream_model,
            priority: None,
            weight: None,
            input_cost_per_million: 0.0,
            output_cost_per_million: 0.0,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelRoute {
    #[serde(default)]
    pub alias: String,
    pub targets: Vec<RouteTarget>,
    #[serde(default)]
    pub context_window: Option<u64>,
    #[serde(default)]
    pub routing_strategy: RoutingStrategy,
    #[serde(default)]
    pub input_cost_per_million: Option<f64>,
    #[serde(default)]
    pub output_cost_per_million: Option<f64>,
}

impl ModelRoute {
    pub fn new(alias: String, targets: Vec<RouteTarget>) -> Self {
        Self {
            alias,
            targets,
            context_window: None,
            routing_strategy: RoutingStrategy::Priority,
            input_cost_per_million: None,
            output_cost_per_million: None,
        }
    }
}

/// Virtual key configuration defined in YAML
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VirtualKeyConfig {
    #[serde(default)]
    pub id: String,
    pub name: String,
    pub key: String,
    #[serde(default = "default_allowed_models")]
    pub allowed_models: Vec<String>,
    #[serde(default)]
    pub max_budget_tokens: Option<u64>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_allowed_models() -> Vec<String> {
    vec!["*".to_string()]
}

fn default_true() -> bool {
    true
}

/// The overall gateway configuration containing providers, model routes, and virtual keys
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct GatewayConfig {
    #[serde(default)]
    pub providers: HashMap<String, ProviderConfig>,
    #[serde(default)]
    pub routes: HashMap<String, ModelRoute>,
    #[serde(default)]
    pub virtual_keys: Vec<VirtualKeyConfig>,
}

impl GatewayConfig {
    /// Creates a default configuration with a local Ollama/vLLM provider and an optional OpenAI provider.
    pub fn default_local() -> Self {
        let mut providers = HashMap::new();
        let mut routes = HashMap::new();

        // Local provider (Ollama/vLLM) - no API key required
        let local_provider = ProviderConfig::new(
            "local".to_string(),
            "http://127.0.0.1:11434/v1".to_string(),
            None,
            60,
        );
        providers.insert(local_provider.name.clone(), local_provider);

        let openai_key = std::env::var("OPENAI_API_KEY").ok();
        let openai_provider = ProviderConfig::new(
            "openai".to_string(),
            "https://api.openai.com/v1".to_string(),
            openai_key,
            60,
        );
        providers.insert(openai_provider.name.clone(), openai_provider);

        // Default route: alias "default" points to local provider's "default" model (or whatever the local server uses)
        // with a fallback target pointing to a secondary provider
        let local_target = RouteTarget::new("local".to_string(), "default".to_string());
        // Fallback target placeholder – could be an alternate local port or another provider
        let fallback_target = RouteTarget::new("openai".to_string(), "gpt-4o-mini".to_string());
        let default_route = ModelRoute::new(
            "default".to_string(),
            vec![local_target, fallback_target],
        );
        routes.insert(default_route.alias.clone(), default_route);

        Self {
            providers,
            routes,
            virtual_keys: Vec::new(),
        }
    }

    /// Validates the configuration consistency
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (name, provider) in &self.providers {
            if provider.base_url.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "Provider '{}' has an empty base_url",
                    name
                )));
            }
        }

        for (alias, route) in &self.routes {
            if route.targets.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "Route '{}' has no targets configured",
                    alias
                )));
            }
            for target in &route.targets {
                if !self.providers.contains_key(&target.provider_name) {
                    return Err(ConfigError::Validation(format!(
                        "Route '{}' references undefined provider '{}'",
                        alias, target.provider_name
                    )));
                }
            }
        }

        Ok(())
    }
}
