use reqwest::StatusCode;
use rand::Rng;

/// Determine if a request is worth retrying
#[allow(dead_code)]
/// based on HTTP status code or network error
pub fn is_retryable(status: Option<StatusCode>, err: Option<&reqwest::Error>) -> bool {
    // If we got a response status, classify it
    if let Some(code) = status {
        match code {
            // Retryable: 429, 502, 503, 504
            StatusCode::TOO_MANY_REQUESTS
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => return true,
            _ => return false,
        }
    }

    // If we only have an error, check network failures
    if let Some(e) = err {
        // Connection reset, timeout, DNS resolution, etc.
        if e.is_timeout() || e.is_connect() || e.is_body() {
            return true;
        }
        // Generic network error
        if e.is_request() {
            return true;
        }
    }

    false
}

/// Decorrelated jitter backoff calculation
/// `base_ms`: base delay
/// `cap_ms`: maximum delay cap
/// `prev_ms`: previous delay used
#[allow(dead_code)]
pub fn calculate_jitter_delay(base_ms: u64, cap_ms: u64, prev_ms: u64) -> u64 {
    let mut rng = rand::thread_rng();
    let min_delay = base_ms;
    let max_delay = std::cmp::min(cap_ms, prev_ms * 3);
    rng.gen_range(min_delay..=max_delay)
}
