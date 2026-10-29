use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Tracks active concurrent requests and streaming sessions.
///
/// Uses an atomic counter that is incremented when a [`StreamGuard`] is created
/// and decremented when it is dropped, guaranteeing accurate counts even if a
/// stream is aborted or canceled.
#[derive(Debug, Clone, Default)]
pub struct InFlightTracker {
    count: Arc<AtomicUsize>,
}

impl InFlightTracker {
    pub fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns the current number of in-flight requests/streams.
    pub fn len(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    /// Returns `true` if there are no in-flight requests.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Increments the counter and returns a RAII [`StreamGuard`] that decrements
    /// on drop.
    pub fn track(&self) -> StreamGuard {
        self.count.fetch_add(1, Ordering::SeqCst);
        StreamGuard {
            tracker: self.clone(),
        }
    }

    /// Polls every 100 ms until active requests drop to 0 or `timeout` expires.
    ///
    /// Returns `true` if the drain completed within the timeout, `false` if the
    /// timeout was reached with active streams still remaining.
    pub async fn wait_for_drain(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        let interval = Duration::from_millis(100);

        loop {
            if self.len() == 0 {
                return true;
            }
            if start.elapsed() >= timeout {
                return false;
            }
            tokio::time::sleep(interval).await;
        }
    }
}

/// RAII guard that decrements the in-flight counter on drop.
///
/// The guard is `Send` so it can be moved into async tasks spawned for streaming
/// responses, keeping the counter incremented until the final SSE chunk is
/// delivered or the client disconnects.
#[derive(Debug)]
pub struct StreamGuard {
    tracker: InFlightTracker,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.tracker.count.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Listens concurrently for `SIGINT` (via `ctrl_c`) on Windows/Unix and
/// `SIGTERM` on Unix systems.
///
/// Returns once a termination signal has been received.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install SIGINT/ctrl-c handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let sigterm = async {
        // On non-Unix platforms there is no SIGTERM; pend forever.
        std::future::pending::<()>().await
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm => {},
    }

    info!("Received shutdown signal, initiating graceful shutdown...");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_flight_tracker_guard_drop() {
        let tracker = InFlightTracker::new();
        assert_eq!(tracker.len(), 0);

        {
            let _guard = tracker.track();
            assert_eq!(tracker.len(), 1);

            {
                let _guard2 = tracker.track();
                assert_eq!(tracker.len(), 2);
            }
            assert_eq!(tracker.len(), 1);
        }
        assert_eq!(tracker.len(), 0);
    }

    #[tokio::test]
    async fn test_wait_for_drain_completes() {
        let tracker = InFlightTracker::new();

        // Spawn a task that holds a guard briefly, then drops it
        let tracker_clone = tracker.clone();
        tokio::spawn(async move {
            let _g = tracker_clone.track();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        });

        // Should drain quickly
        let drained = tracker
            .wait_for_drain(Duration::from_secs(5))
            .await;
        assert!(drained);
        assert_eq!(tracker.len(), 0);
    }

    #[tokio::test]
    async fn test_wait_for_drain_timeout() {
        let tracker = InFlightTracker::new();

        // Spawn a task that holds a guard for longer than the timeout
        let tracker_clone = tracker.clone();
        tokio::spawn(async move {
            let _g = tracker_clone.track();
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        });

        // Give the spawned task a moment to increment the counter
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Timeout after 100ms should return false
        let drained = tracker
            .wait_for_drain(Duration::from_millis(100))
            .await;
        assert!(!drained);
    }
}