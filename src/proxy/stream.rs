use bytes::Bytes;
use crate::server::drain::StreamGuard;
use tokio::sync::mpsc;
use reqwest::Response;
use axum::response::Response as AxumResponse;
use axum::body::Body;
use tokio_stream::wrappers::ReceiverStream;
use std::sync::Arc;
use crate::security::mcp_guard::{McpSchemaGuard, McpSecurityError};
use crate::security::stream_guard::StreamingToolCallAccumulator;
use crate::telemetry::metrics;
use serde_json::json;

/// Pipes upstream SSE response body to the client with cancellation safety and MCP schema validation.
pub fn pipe_upstream_sse(
    mut upstream_res: Response,
    guard: StreamGuard,
    is_anthropic: bool,
    is_gemini: bool,
    mcp_guard: Arc<McpSchemaGuard>,
) -> AxumResponse {
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(64);

    tokio::spawn(async move {
        let _guard = guard;
        let mut accumulator = StreamingToolCallAccumulator::new();
        let mut collecting_tool_calls = false;
        let mut tool_call_buffer: Vec<Bytes> = Vec::new();

        let mut handle_chunk = async move |chunk_bytes: Bytes| -> bool {
            let chunk_str = String::from_utf8_lossy(&chunk_bytes);
            
            if chunk_str.contains("tool_calls") {
                collecting_tool_calls = true;
            }

            if collecting_tool_calls {
                tool_call_buffer.push(chunk_bytes.clone());

                if let Some(tool_calls) = accumulator.feed_chunk(&chunk_str) {
                    match accumulator.validate_all(&tool_calls, &mcp_guard) {
                        Ok(()) => {
                            for b in tool_call_buffer.drain(..) {
                                if tx.send(Ok(b)).await.is_err() {
                                    return false;
                                }
                            }
                        }
                        Err(err) => {
                            tool_call_buffer.clear();
                            let violation_type = match &err {
                                McpSecurityError::SchemaViolation { .. } => "schema_violation",
                                McpSecurityError::UnregisteredTool(_) => "unregistered_tool",
                                McpSecurityError::JsonParseError(_) => "json_parse_error",
                            };
                            for tc in &tool_calls {
                                metrics::inc_mcp_violations_total(&tc.function.name, violation_type);
                            }
                            let tool_name = tool_calls.first().map(|tc| tc.function.name.as_str()).unwrap_or("unknown");
                            tracing::error!(tool = %tool_name, error = %err, "Streaming MCP security violation detected: blocked tool call execution attempt.");

                            let error_json = json!({
                                "choices": [{
                                    "delta": {},
                                    "finish_reason": "content_filter"
                                }],
                                "error": {
                                    "message": format!("Streaming tool call blocked by MCP schema guard: {}", err),
                                    "type": "mcp_security_violation"
                                }
                            });
                            let error_chunk = format!("data: {}\n\n", error_json);
                            let _ = tx.send(Ok(Bytes::from(error_chunk))).await;
                            let _ = tx.send(Ok(Bytes::from("data: [DONE]\n\n"))).await;
                            return false;
                        }
                    }
                }
            } else {
                if tx.send(Ok(chunk_bytes)).await.is_err() {
                    return false;
                }
            }
            true
        };

        if is_anthropic {
            let mut translator = crate::providers::anthropic::AnthropicStreamTranslator::new();
            while let Ok(Some(chunk)) = upstream_res.chunk().await {
                for oc in translator.ingest(&chunk) {
                    if !handle_chunk(Bytes::from(oc)).await { break; }
                }
            }
        } else if is_gemini {
            let mut translator = crate::providers::gemini::GeminiStreamTranslator::new();
            while let Ok(Some(chunk)) = upstream_res.chunk().await {
                for oc in translator.ingest(&chunk) {
                    if !handle_chunk(Bytes::from(oc)).await { break; }
                }
            }
        } else {
            while let Ok(Some(chunk)) = upstream_res.chunk().await {
                if !handle_chunk(chunk).await { break; }
            }
        }
    });

    let stream = ReceiverStream::new(rx);
    let mut response = AxumResponse::new(Body::from_stream(stream));

    *response.status_mut() = axum::http::StatusCode::OK;
    response.headers_mut().insert(axum::http::header::CONTENT_TYPE, axum::http::header::HeaderValue::from_static("text/event-stream"));
    response.headers_mut().insert(axum::http::header::CACHE_CONTROL, axum::http::header::HeaderValue::from_static("no-cache"));
    response.headers_mut().insert(axum::http::header::CONNECTION, axum::http::header::HeaderValue::from_static("keep-alive"));
    response
}
