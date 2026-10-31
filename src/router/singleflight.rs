use axum::response::Response as AxumResponse;
use axum::http::StatusCode;
use axum::body::Body;
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;

use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use bytes::Bytes;
use std::collections::HashMap;

#[derive(Clone)]
pub struct InFlightBroadcast {
    pub sender: broadcast::Sender<Result<Bytes, String>>,
    pub receiver_count: Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Debug)]
pub enum CoalesceStatus {
    Leader(broadcast::Sender<Result<Bytes, String>>),
    Follower(broadcast::Receiver<Result<Bytes, String>>),
}

#[derive(Clone)]
pub struct RequestCoalescer {
    flights: Arc<RwLock<HashMap<String, InFlightBroadcast>>>,
}

impl Default for RequestCoalescer {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestCoalescer {
    pub fn new() -> Self {
        Self {
            flights: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn acquire_or_join(&self, cache_key: &str) -> CoalesceStatus {
        let mut map = self.flights.write().await;
        if let Some(flight) = map.get(cache_key) {
            flight.receiver_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            CoalesceStatus::Follower(flight.sender.subscribe())
        } else {
            let (sender, _receiver) = broadcast::channel(128);
            let receiver_count = Arc::new(std::sync::atomic::AtomicUsize::new(1));
            map.insert(
                cache_key.to_string(),
                InFlightBroadcast {
                    sender: sender.clone(),
                    receiver_count,
                },
            );
            CoalesceStatus::Leader(sender)
        }
    }

    pub async fn complete(&self, cache_key: &str) {
        let mut map = self.flights.write().await;
        map.remove(cache_key);
    }
}

pub fn compute_cache_key(request: &crate::api::completions::ChatCompletionRequest) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(request.model.as_bytes());
    if let Ok(json) = serde_json::to_vec(&request.messages) {
        hasher.update(&json);
    }
    if let Some(t) = request.temperature {
        hasher.update(&t.to_bits().to_le_bytes());
    }
    if let Some(m) = request.max_tokens {
        hasher.update(&m.to_le_bytes());
    }
    if let Some(ref tools) = request.tools {
        if let Ok(json) = serde_json::to_vec(tools) {
            hasher.update(&json);
        }
    }
    format!("{:x}", hasher.finalize())
}

pub async fn handle_follower(mut rx: broadcast::Receiver<Result<Bytes, String>>, stream: Option<bool>, model: &str) -> AxumResponse {
    crate::telemetry::metrics::inc_coalesced_requests_total(model);
    let is_streaming = stream.unwrap_or(false);
    if is_streaming {
        let (tx, client_rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(64);
        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                match msg {
                    Ok(bytes) => {
                        if tx.send(Ok(bytes)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(std::io::Error::new(std::io::ErrorKind::Other, e))).await;
                        break;
                    }
                }
            }
        });
        let stream_res = ReceiverStream::new(client_rx);
        let mut response = AxumResponse::new(Body::from_stream(stream_res));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(axum::http::header::CONTENT_TYPE, axum::http::header::HeaderValue::from_static("text/event-stream"));
        response.headers_mut().insert(axum::http::header::CACHE_CONTROL, axum::http::header::HeaderValue::from_static("no-cache"));
        response.headers_mut().insert(axum::http::header::CONNECTION, axum::http::header::HeaderValue::from_static("keep-alive"));
        response
    } else {
        let mut accumulated = Vec::new();
        while let Ok(msg) = rx.recv().await {
            match msg {
                Ok(bytes) => accumulated.extend_from_slice(&bytes),
                Err(_) => break,
            }
        }
        let response = AxumResponse::new(Body::from(accumulated));
        let mut resp = response;
        *resp.status_mut() = StatusCode::OK;
        resp.headers_mut().insert(axum::http::header::CONTENT_TYPE, axum::http::header::HeaderValue::from_static("application/json"));
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_request_coalescer_parallel() {
        let coalescer = RequestCoalescer::new();
        let cache_key = "test-model-prompt-hash";

        // First request is leader
        let status1 = coalescer.acquire_or_join(cache_key).await;
        let sender = match status1 {
            CoalesceStatus::Leader(s) => s,
            CoalesceStatus::Follower(_) => panic!("Expected leader"),
        };

        // Subsequent 3 requests are followers
        let mut receivers = Vec::new();
        for _ in 0..3 {
            let status = coalescer.acquire_or_join(cache_key).await;
            match status {
                CoalesceStatus::Follower(r) => receivers.push(r),
                CoalesceStatus::Leader(_) => panic!("Expected follower"),
            }
        }

        // Broadcast test chunks from leader
        let chunk1 = Bytes::from("data: chunk 1\n\n");
        let chunk2 = Bytes::from("data: chunk 2\n\n");

        sender.send(Ok(chunk1.clone())).unwrap();
        sender.send(Ok(chunk2.clone())).unwrap();

        // Verify followers receive identical broadcast data
        for mut rx in receivers {
            let received1 = rx.recv().await.unwrap().unwrap();
            assert_eq!(received1, chunk1);

            let received2 = rx.recv().await.unwrap().unwrap();
            assert_eq!(received2, chunk2);
        }

        coalescer.complete(cache_key).await;
        // Verify entry removed
        let status_after = coalescer.acquire_or_join(cache_key).await;
        assert!(matches!(status_after, CoalesceStatus::Leader(_)));
    }
}
