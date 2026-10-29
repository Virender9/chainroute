use bytes::Bytes;
use crate::server::drain::StreamGuard;
use tokio::sync::mpsc;
use reqwest::Response;
use axum::response::Response as AxumResponse;
use axum::body::Body;
use tokio_stream::wrappers::ReceiverStream;

/// Pipes upstream SSE response body to the client with cancellation safety.
///
/// The `guard` keeps the in-flight counter incremented for the entire lifetime
/// of the stream, ensuring graceful shutdown can wait for active SSE sessions
/// to complete.
pub fn pipe_upstream_sse(mut upstream_res: Response, guard: StreamGuard) -> AxumResponse {
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(64);

    tokio::spawn(async move {
        // The guard is intentionally moved into this task so the counter stays
        // incremented until the stream completes or the client disconnects.
        let _guard = guard;
        while let Ok(Some(chunk)) = upstream_res.chunk().await {
            if tx.send(Ok(chunk)).await.is_err() {
                break;
            }
        }
    });

    let stream = ReceiverStream::new(rx);
    let mut response = AxumResponse::new(Body::from_stream(stream));

    *response.status_mut() = axum::http::StatusCode::OK;
    response
        .headers_mut()
        .insert(axum::http::header::CONTENT_TYPE, axum::http::header::HeaderValue::from_static("text/event-stream"));
    response
        .headers_mut()
        .insert(axum::http::header::CACHE_CONTROL, axum::http::header::HeaderValue::from_static("no-cache"));
    response
        .headers_mut()
        .insert(axum::http::header::CONNECTION, axum::http::header::HeaderValue::from_static("keep-alive"));

    response
}