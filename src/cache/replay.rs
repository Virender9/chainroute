use axum::body::Body;
use axum::http::{header, StatusCode, HeaderValue};
use axum::response::{IntoResponse, Response as AxumResponse};
use bytes::Bytes;
use serde_json::{json, Value};

pub fn replay_cached_as_sse(cached_json_bytes: Bytes, model: &str) -> AxumResponse {
    let content_str = match serde_json::from_slice::<Value>(&cached_json_bytes) {
        Ok(val) => {
            if let Some(choices) = val.get("choices").and_then(|c| c.as_array()) {
                if let Some(choice) = choices.first() {
                    if let Some(msg) = choice.get("message") {
                        if let Some(content) = msg.get("content").and_then(|c| c.as_str()) {
                            content.to_string()
                        } else {
                            msg.get("content").map(|v| v.to_string()).unwrap_or_default()
                        }
                    } else if let Some(text) = choice.get("text").and_then(|t| t.as_str()) {
                        text.to_string()
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                }
            } else {
                String::new()
            }
        }
        Err(_) => String::from_utf8_lossy(&cached_json_bytes).to_string(),
    };

    let chunk = json!({
        "id": "chatcmpl-cached",
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{
            "index": 0,
            "delta": {
                "content": content_str
            },
            "finish_reason": "stop"
        }]
    });

    let sse_data = format!("data: {}\n\ndata: [DONE]\n\n", chunk.to_string());

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream")),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-cache")),
            (header::HeaderName::from_static("x-cache"), HeaderValue::from_static("HIT")),
        ],
        Body::from(sse_data),
    )
        .into_response()
}

pub fn replay_cached_json(cached_json_bytes: Bytes) -> AxumResponse {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
            (header::HeaderName::from_static("x-cache"), HeaderValue::from_static("HIT")),
        ],
        Body::from(cached_json_bytes),
    )
        .into_response()
}

