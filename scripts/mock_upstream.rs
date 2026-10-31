use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use axum::{
    body::Body,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use tokio::net::TcpListener;
use tokio::time::sleep;

#[derive(Clone)]
struct MockState {
    tokens_per_response: usize,
}

async fn mock_chat_completions(State(state): State<Arc<MockState>>) -> impl IntoResponse {
    let tokens = state.tokens_per_response;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<axum::body::Bytes, std::convert::Infallible>>(64);

    tokio::spawn(async move {
        // Cold start / scheduling jitter (Box-Muller transform: mean 120ms, stddev 30ms)
        let u1: f64 = rand::random();
        let u2: f64 = rand::random();
        let z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
        let cold_start_ms = (120.0 + 30.0 * z0).max(10.0) as u64;
        sleep(Duration::from_millis(cold_start_ms)).await;

        for i in 0..tokens {
            // Inter-token jitter
            let u1: f64 = rand::random();
            let u2: f64 = rand::random();
            let z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            let token_delay_ms = (15.0 + 4.0 * z0).max(2.0) as u64;
            sleep(Duration::from_millis(token_delay_ms)).await;

            let chunk = format!(
                "data: {{\"id\":\"chatcmpl-mock\",\"object\":\"chat.completion.chunk\",\"created\":1710000000,\"model\":\"mock-model\",\"choices\":[{\"index\":0,\"delta\":{{\"content\":\"token{} \"}},\"finish_reason\":null}]}}\n\n",
                i
            );
            if tx.send(Ok(axum::body::Bytes::from(chunk))).await.is_err() {
                break;
            }
        }

        let finish_chunk = "data: {\"id\":\"chatcmpl-mock\",\"object\":\"chat.completion.chunk\",\"created\":1710000000,\"model\":\"mock-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":50,\"completion_tokens\":250,\"total_tokens\":300}}\n\ndata: [DONE]\n\n";
        let _ = tx.send(Ok(axum::body::Bytes::from(finish_chunk))).await;
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream);
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(axum::http::header::CONTENT_TYPE, axum::http::header::HeaderValue::from_static("text/event-stream"));
    response
        .headers_mut()
        .insert(axum::http::header::CACHE_CONTROL, axum::http::header::HeaderValue::from_static("no-cache"));
    response
}

#[tokio::main]
async fn main() {
    let state = Arc::new(MockState { tokens_per_response: 250 });
    let app = Router::new()
        .route("/v1/chat/completions", post(mock_chat_completions))
        .with_state(state);
    let addr = SocketAddr::from(([127, 0, 0, 1], 8001));
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Mock upstream with synthetic jitter listening on {}", addr);
    axum::serve(listener, app).await.unwrap();
}
