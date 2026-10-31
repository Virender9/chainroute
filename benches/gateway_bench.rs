use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{body::Body, extract::State, http::StatusCode, response::{IntoResponse, Response}, routing::post, Router};
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchConfig {
    pub concurrency: usize,
    pub total_requests: usize,
    pub target_url: String,
    pub api_key: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct BenchResult {
    pub total_duration_secs: f64,
    pub requests_per_second: f64,
    pub ttft_p50_ms: f64,
    pub ttft_p95_ms: f64,
    pub ttft_p99_ms: f64,
    pub total_tokens_received: u64,
    pub error_count: usize,
}

#[derive(Clone)]
struct MockState { tokens_per_response: usize, chunk_delay_ms: u64 }

async fn mock_chat_completions(State(state): State<Arc<MockState>>) -> impl IntoResponse {
    let tokens = state.tokens_per_response;
    let delay = state.chunk_delay_ms;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<axum::body::Bytes, std::convert::Infallible>>(64);
    tokio::spawn(async move {
        for i in 0..tokens {
            if delay > 0 { sleep(Duration::from_millis(delay)).await; }
            let chunk = format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"token{}\"}}}}]}}\n\n", i);
            if tx.send(Ok(axum::body::Bytes::from(chunk))).await.is_err() { break; }
        }
        let _ = tx.send(Ok(axum::body::Bytes::from("data: [DONE]\n\n"))).await;
    });
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream);
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(axum::http::header::CONTENT_TYPE, axum::http::header::HeaderValue::from_static("text/event-stream"));
    response.headers_mut().insert(axum::http::header::CACHE_CONTROL, axum::http::header::HeaderValue::from_static("no-cache"));
    response
}

pub async fn start_mock_upstream(port: u16) -> tokio::task::JoinHandle<()> {
    let state = Arc::new(MockState { tokens_per_response: 20, chunk_delay_ms: 1 });
    let app = Router::new().route("/v1/chat/completions", post(mock_chat_completions)).with_state(state);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = TcpListener::bind(addr).await.unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); })
}

pub async fn run_bench(config: BenchConfig) -> BenchResult {
    let client = Client::builder().pool_max_idle_per_host(config.concurrency).build().unwrap();
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let mut handles = Vec::with_capacity(config.total_requests);
    let hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    let error_count = Arc::new(Mutex::new(0usize));
    let total_tokens = Arc::new(Mutex::new(0u64));
    let start_total = Instant::now();
    for _ in 0..config.total_requests {
        let client = client.clone();
        let sem = semaphore.clone();
        let hist = hist.clone();
        let err = error_count.clone();
        let tokens = total_tokens.clone();
        let url = config.target_url.clone();
        let key = config.api_key.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let t0 = Instant::now();
            let body = json!({ "model": "test", "messages": [{"role": "user", "content": "hi"}], "stream": true });
            let res = client.post(&url).header("Authorization", format!("Bearer {}", key)).header("Content-Type", "application/json").json(&body).send().await;
            match res {
                Ok(resp) => {
                    let mut stream = resp.bytes_stream();
                    let mut ttft_recorded = false;
                    let mut token_count: u64 = 0;
                    while let Some(chunk) = stream.next().await {
                        match chunk {
                            Ok(bytes) => {
                                if !ttft_recorded {
                                    let ttft_ms = t0.elapsed().as_millis() as u64;
                                    ttft_recorded = true;
                                    let mut h = hist.lock().await;
                                    let _ = h.record(ttft_ms);
                                }
                                let s = String::from_utf8_lossy(&bytes);
                                if s.contains("content") { token_count += 1; }
                                if s.contains("[DONE]") { break; }
                            },
                            Err(_) => { let mut e = err.lock().await; *e += 1; break; }
                        }
                    }
                    let mut t = tokens.lock().await;
                    *t += token_count;
                },
                Err(_) => { let mut e = err.lock().await; *e += 1; }
            }
        }));
    }
    for h in handles { let _ = h.await; }
    let total_duration = start_total.elapsed().as_secs_f64();
    let hist = hist.lock().await;
    let ttft_p50_ms = hist.value_at_quantile(0.50) as f64;
    let ttft_p95_ms = hist.value_at_quantile(0.95) as f64;
    let ttft_p99_ms = hist.value_at_quantile(0.99) as f64;
    let error_count = *error_count.lock().await;
    let total_tokens_received = *total_tokens.lock().await;
    BenchResult {
        total_duration_secs: total_duration,
        requests_per_second: if total_duration > 0.0 { config.total_requests as f64 / total_duration } else { 0.0 },
        ttft_p50_ms, ttft_p95_ms, ttft_p99_ms,
        total_tokens_received, error_count,
    }
}

#[tokio::test]
async fn bench_quick() {
    let _mock = start_mock_upstream(3999).await;
    sleep(Duration::from_millis(200)).await;
    let cfg = BenchConfig { concurrency: 10, total_requests: 50, target_url: "http://127.0.0.1:3999/v1/chat/completions".to_string(), api_key: "test".to_string() };
    let res = run_bench(cfg).await;
    println!("{}", serde_json::to_string_pretty(&res).unwrap());
}

