use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::Instant;
use crate::config::providers::{RouteTarget, RoutingStrategy};
use crate::telemetry::metrics;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TargetHealthStats {
    pub ewma_latency_ms: f64,
    pub total_requests: u64,
    pub consecutive_errors: u32,
    pub last_updated: Instant,
}

impl Default for TargetHealthStats {
    fn default() -> Self {
        Self {
            ewma_latency_ms: 200.0,
            total_requests: 0,
            consecutive_errors: 0,
            last_updated: Instant::now(),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AdaptiveScoreEngine {
    pub state: Arc<RwLock<HashMap<String, TargetHealthStats>>>,
    alpha: f64,
}

impl Default for AdaptiveScoreEngine {
    fn default() -> Self {
        Self::new(0.2)
    }
}

#[allow(dead_code)]
impl AdaptiveScoreEngine {
    #[allow(dead_code)]
    pub fn new(alpha: f64) -> Self {
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
            alpha,
        }
    }

    fn make_target_key(provider_name: &str, upstream_model: &str) -> String {
        format!("{}:{}", provider_name, upstream_model)
    }

    pub async fn record_observation(&self, target_key: &str, latency_ms: f64, is_success: bool) {
        let mut map = self.state.write().await;
        let stats = map.entry(target_key.to_string()).or_default();

        stats.total_requests += 1;
        stats.last_updated = Instant::now();

        if is_success {
            stats.consecutive_errors = 0;
            stats.ewma_latency_ms = self.alpha * latency_ms + (1.0 - self.alpha) * stats.ewma_latency_ms;
        } else {
            stats.consecutive_errors += 1;
            // Optionally increase ewma penalty on failure or keep last
        }

        // Emit metric for ewma latency
        metrics::set_target_ewma_latency_ms(target_key, stats.ewma_latency_ms);
    }

    pub async fn rank_targets<'a>(
        &self,
        targets: &'a [RouteTarget],
        strategy: RoutingStrategy,
        estimated_tokens: u64,
    ) -> Vec<&'a RouteTarget> {
        let map = self.state.read().await;

        match strategy {
            RoutingStrategy::Priority => {
                let mut result = Vec::new();
                for target in targets {
                    let _key = Self::make_target_key(&target.provider_name, &target.upstream_model);
                    result.push(target);
                }
                result
            }
            RoutingStrategy::LowestCost => {
                let mut scored: Vec<(&RouteTarget, f64)> = targets
                    .iter()
                    .map(|target| {
                        let cost = (estimated_tokens as f64 / 1_000_000.0) * target.input_cost_per_million;
                        (target, cost)
                    })
                    .collect();

                scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                scored.into_iter().map(|(t, _)| t).collect()
            }
            RoutingStrategy::LowestLatency => {
                let mut scored: Vec<(&RouteTarget, f64)> = targets
                    .iter()
                    .map(|target| {
                        let key = Self::make_target_key(&target.provider_name, &target.upstream_model);
                        let latency = map.get(&key).map(|s| s.ewma_latency_ms).unwrap_or(200.0);
                        (target, latency)
                    })
                    .collect();

                scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                scored.into_iter().map(|(t, _)| t).collect()
            }
            RoutingStrategy::AdaptiveBalanced => {
                // Compute min/max for normalization or robust fallback
                let mut items = Vec::new();
                for target in targets {
                    let key = Self::make_target_key(&target.provider_name, &target.upstream_model);
                    let stats = map.get(&key);
                    let latency = stats.map(|s| s.ewma_latency_ms).unwrap_or(200.0);
                    let cost = (estimated_tokens as f64 / 1_000_000.0) * target.input_cost_per_million;
                    let errors = stats.map(|s| s.consecutive_errors).unwrap_or(0);
                    items.push((target, latency, cost, errors));
                }

                let min_latency = items.iter().map(|i| i.1).fold(f64::INFINITY, f64::min);
                let max_latency = items.iter().map(|i| i.1).fold(f64::NEG_INFINITY, f64::max);
                let lat_range = if max_latency - min_latency < 1e-6 { 1.0 } else { max_latency - min_latency };

                let min_cost = items.iter().map(|i| i.2).fold(f64::INFINITY, f64::min);
                let max_cost = items.iter().map(|i| i.2).fold(f64::NEG_INFINITY, f64::max);
                let cost_range = if max_cost - min_cost < 1e-6 { 1.0 } else { max_cost - min_cost };

                let mut scored: Vec<(&RouteTarget, f64)> = items
                    .into_iter()
                    .map(|(target, latency, cost, errors)| {
                        let norm_latency = (latency - min_latency) / lat_range;
                        let norm_cost = (cost - min_cost) / cost_range;
                        let score = (0.6 * norm_latency) + (0.4 * norm_cost) + (errors as f64 * 50.0);
                        (target, score)
                    })
                    .collect();

                scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                scored.into_iter().map(|(t, _)| t).collect()
            }
        }
    }
}
