use std::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,    // Normal operation: all traffic allowed
    Open,      // Tripped: fail-fast, reject all requests instantly
    HalfOpen,  // Probing: allow a limited probe request to test health
}

#[derive(Debug)]
pub struct CircuitBreaker {
    failure_threshold: u32,
    cooldown_duration: Duration,
    target_id: String,
    state: RwLock<CircuitState>,
    failure_count: RwLock<u32>,
    last_failure_time: RwLock<Option<Instant>>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, cooldown_duration: Duration, target_id: String) -> Self {
        Self {
            failure_threshold,
            cooldown_duration,
            target_id,
            state: RwLock::new(CircuitState::Closed),
            failure_count: RwLock::new(0),
            last_failure_time: RwLock::new(None),
        }
    }

    pub fn can_execute(&self) -> bool {
        let current_state = *self.state.read().unwrap();
        match current_state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                let last_time = *self.last_failure_time.read().unwrap();
                if let Some(t) = last_time {
                    if t.elapsed() >= self.cooldown_duration {
                        let mut state_w = self.state.write().unwrap();
                        *state_w = CircuitState::HalfOpen;
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true,
        }
    }

    pub fn record_success(&self) {
        let mut state_w = self.state.write().unwrap();
        let mut count_w = self.failure_count.write().unwrap();
        *count_w = 0;
        *state_w = CircuitState::Closed;
    }

    pub fn record_failure(&self) {
        let mut count_w = self.failure_count.write().unwrap();
        let mut time_w = self.last_failure_time.write().unwrap();
        let mut state_w = self.state.write().unwrap();

        *count_w += 1;
        *time_w = Some(Instant::now());

        if *count_w >= self.failure_threshold && *state_w != CircuitState::Open {
            *state_w = CircuitState::Open;
            warn!(
                target = %self.target_id,
                failure_count = *count_w,
                threshold = self.failure_threshold,
                "Circuit breaker tripped to OPEN state due to repeated failures"
            );
        }
    }
}
