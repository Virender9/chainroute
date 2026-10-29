use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::Instant;
use thiserror::Error;

/// Error when a request exceeds the rate limit.
#[derive(Debug, Error)]
#[error("Rate limit exceeded, retry after {retry_after_secs}s")]
pub struct RateLimitExceeded {
    pub retry_after_secs: u64,
}

/// A token-aware sliding-window limiter.
#[derive(Clone)]
pub struct SlidingWindowLimiter {
    pub max_tokens_per_window: u64,
    pub window_duration_secs: u64,
    state: Arc<RwLock<LimiterState>>,
}

#[derive(Debug)]
struct LimiterState {
    current_window_tokens: u64,
    previous_window_tokens: u64,
    window_start: Instant,
}

impl SlidingWindowLimiter {
    pub fn new(max: u64, dur: u64) -> Self {
        let now = Instant::now();
        Self {
            max_tokens_per_window: max,
            window_duration_secs: dur,
            state: Arc::new(RwLock::new(LimiterState {
                current_window_tokens: 0,
                previous_window_tokens: 0,
                window_start: now,
            })),
        }
    }

    async fn advance_window(&self) {
        let mut guard = self.state.write().await;
        let elapsed = guard.window_start.elapsed().as_secs();
        if elapsed >= self.window_duration_secs {
            guard.previous_window_tokens = guard.current_window_tokens;
            guard.current_window_tokens = 0;
            guard.window_start = Instant::now();
        }
    }

    async fn usage(&self) -> u64 {
        let guard = self.state.read().await;
        let elapsed = guard.window_start.elapsed().as_secs_f64();
        let remaining = self.window_duration_secs as f64 - elapsed;
        let factor = remaining / self.window_duration_secs as f64;
        guard.current_window_tokens + ((guard.previous_window_tokens as f64 * factor).ceil() as u64)
    }

    pub async fn check_and_reserve(&self, estimated_tokens: u64) -> Result<(), RateLimitExceeded> {
        self.advance_window().await;
        let usage = self.usage().await;
        if usage + estimated_tokens > self.max_tokens_per_window {
            let elapsed = self.state.read().await.window_start.elapsed().as_secs();
            let wait = self.window_duration_secs.saturating_sub(elapsed);
            return Err(RateLimitExceeded { retry_after_secs: wait.max(1) });
        }
        let mut guard = self.state.write().await;
        guard.current_window_tokens += estimated_tokens;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn reconcile(&self, reserved_tokens: u64, actual_tokens: u64) {
        if actual_tokens >= reserved_tokens {
            return;
        }
        let diff = reserved_tokens - actual_tokens;
        let mut guard = self.state.write().await;
        guard.current_window_tokens = guard.current_window_tokens.saturating_sub(diff);
    }
}