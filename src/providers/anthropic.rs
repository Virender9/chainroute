use crate::api::completions::ChatCompletionRequest;
use crate::config::providers::ProviderConfig;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde_json::{json, Value};

/// Translates an OpenAI ChatCompletionRequest into an Anthropic Messages API payload.
pub fn transform_openai_to_anthropic(
    provider: &ProviderConfig,
    upstream_model: &str,
    req: &ChatCompletionRequest,
) -> (String, HeaderMap, Value) {
    let base = provider.base_url.trim_end_matches('/');
    let url = format!("{}/messages", base);

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

    if let Some(ref key) = provider.api_key {
        if let Ok(val) = HeaderValue::from_str(key) {
            headers.insert("x-api-key", val);
        }
    }

    let mut system_text = String::new();
    let mut anthropic_messages = Vec::new();

    for msg in &req.messages {
        let role = msg.role.as_str();
        let content_str = match &msg.content {
            Value::String(s) => s.clone(),
            Value::Array(arr) => arr
                .iter()
                .filter_map(|item| item.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join(""),
            _ => msg.content.to_string(),
        };

        if role == "system" {
            if !system_text.is_empty() {
                system_text.push('\n');
            }
            system_text.push_str(&content_str);
        } else {
            let mapped_role = if role == "assistant" { "assistant" } else { "user" };
            anthropic_messages.push(json!({
                "role": mapped_role,
                "content": content_str
            }));
        }
    }

    let max_tokens = req.max_tokens.unwrap_or(4096);

    let mut body = json!({
        "model": upstream_model,
        "messages": anthropic_messages,
        "max_tokens": max_tokens,
        "stream": req.stream.unwrap_or(false)
    });

    if !system_text.is_empty() {
        body["system"] = json!(system_text);
    }

    if let Some(temp) = req.temperature {
        body["temperature"] = json!(temp);
    }

    (url, headers, body)
}

/// Converts a non-streaming Anthropic Messages response into an OpenAI ChatCompletion response.
pub fn transform_anthropic_to_openai_response(anthropic_json: &Value, model_alias: &str) -> Value {
    let mut text_parts = Vec::new();
    if let Some(content_arr) = anthropic_json.get("content").and_then(|c| c.as_array()) {
        for item in content_arr {
            if item.get("type").and_then(|t| t.as_str()) == Some("text") {
                if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                    text_parts.push(text);
                }
            }
        }
    }
    let content = text_parts.join("");

    let prompt_tokens = anthropic_json
        .get("usage")
        .and_then(|u| u.get("input_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let completion_tokens = anthropic_json
        .get("usage")
        .and_then(|u| u.get("output_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let total_tokens = prompt_tokens + completion_tokens;

    let stop_reason = anthropic_json
        .get("stop_reason")
        .and_then(|s| s.as_str())
        .unwrap_or("stop");
    let finish_reason = match stop_reason {
        "end_turn" | "stop_sequence" => "stop",
        "max_tokens" => "length",
        _ => "stop",
    };

    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let id = anthropic_json
        .get("id")
        .and_then(|i| i.as_str())
        .unwrap_or("chatcmpl-anthropic");

    json!({
        "id": id,
        "object": "chat.completion",
        "created": created,
        "model": model_alias,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content
                },
                "finish_reason": finish_reason
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens
        }
    })
}


/// State machine for translating an Anthropic SSE stream into OpenAI SSE chunks.
#[derive(Default)]
pub struct AnthropicStreamTranslator {
    buffer: String,
    current_event: String,
}

impl AnthropicStreamTranslator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ingests a new slice of incoming bytes and returns any completed OpenAI SSE chunks.
    pub fn ingest(&mut self, chunk: &[u8]) -> Vec<String> {
        let chunk_str = String::from_utf8_lossy(chunk);
        self.buffer.push_str(&chunk_str);

        let mut output_chunks = Vec::new();

        while let Some(pos) = self.buffer.find('\n') {
            let line = self.buffer[..pos].trim_end_matches('\r').to_string();
            self.buffer.drain(..=pos);

            if line.starts_with("event:") {
                self.current_event = line["event:".len()..].trim().to_string();
            } else if line.starts_with("data:") {
                let data_str = line["data:".len()..].trim();
                if let Ok(val) = serde_json::from_str::<Value>(data_str) {
                    let ev_type = val.get("type").and_then(|t| t.as_str()).unwrap_or(&self.current_event);

                    if ev_type == "content_block_delta" {
                        if let Some(delta) = val.get("delta") {
                            if delta.get("type").and_then(|t| t.as_str()) == Some("text_delta") {
                                if let Some(text) = delta.get("text").and_then(|t| t.as_str()) {
                                    let open_ai_chunk = json!({
                                        "choices": [
                                            {
                                                "index": 0,
                                                "delta": {
                                                    "content": text
                                                },
                                                "finish_reason": null
                                            }
                                        ]
                                    });
                                    output_chunks.push(format!("data: {}\n\n", open_ai_chunk));
                                }
                            }
                        }
                    } else if ev_type == "message_delta" {
                        if let Some(usage) = val.get("usage") {
                            let output_tokens = usage.get("output_tokens").and_then(|t| t.as_u64());
                            if let Some(tokens) = output_tokens {
                                let usage_chunk = json!({
                                    "choices": [],
                                    "usage": {
                                        "completion_tokens": tokens
                                    }
                                });
                                output_chunks.push(format!("data: {}\n\n", usage_chunk));
                            }
                        }
                    } else if ev_type == "message_stop" {
                        output_chunks.push("data: [DONE]\n\n".to_string());
                    }
                }
                self.current_event.clear();
            } else if line.is_empty() {
                self.current_event.clear();
            }
        }

        output_chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::completions::{ChatMessage, ChatCompletionRequest};

    #[test]
    fn test_transform_request() {
        let provider = ProviderConfig::new(
            "anthropic".to_string(),
            "https://api.anthropic.com/v1".to_string(),
            Some("ant-test-key".to_string()),
            60,
        );

        let req = ChatCompletionRequest {
            model: "claude-test".to_string(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: json!("You are an AI assistant."),
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: json!("Hello"),
                },
            ],
            stream: Some(true),
            temperature: Some(0.7),
            max_tokens: Some(1024),
            tools: None,
        };

        let (url, headers, body) = transform_openai_to_anthropic(&provider, "claude-3-5-sonnet", &req);
        assert_eq!(url, "https://api.anthropic.com/v1/messages");
        assert_eq!(headers.get("x-api-key").unwrap(), "ant-test-key");
        assert_eq!(headers.get("anthropic-version").unwrap(), "2023-06-01");
        assert_eq!(body["system"], "You are an AI assistant.");
        assert_eq!(body["max_tokens"], 1024);
        assert_eq!(body["stream"], true);
        assert!((body["temperature"].as_f64().unwrap() - 0.7).abs() < 1e-4);
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_transform_response() {
        let anthropic_resp = json!({
            "id": "msg_01X",
            "type": "message",
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": "Hello world!"
                }
            ],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 15,
                "output_tokens": 25
            }
        });

        let openai_resp = transform_anthropic_to_openai_response(&anthropic_resp, "claude-test");
        assert_eq!(openai_resp["id"], "msg_01X");
        assert_eq!(openai_resp["choices"][0]["message"]["content"], "Hello world!");
        assert_eq!(openai_resp["choices"][0]["finish_reason"], "stop");
        assert_eq!(openai_resp["usage"]["total_tokens"], 40);
    }

    #[test]
    fn test_stream_translator() {
        let mut translator = AnthropicStreamTranslator::new();
        let chunk1 = b"event: content_block_delta\ndata: {\"type\": \"content_block_delta\", \"index\": 0, \"delta\": {\"type\": \"text_delta\", \"text\": \"Hi!\"}}\n\n";
        let outputs = translator.ingest(chunk1);
        assert_eq!(outputs.len(), 1);
        assert!(outputs[0].contains("\"content\":\"Hi!\""));

        let chunk2 = b"event: message_stop\ndata: {\"type\": \"message_stop\"}\n\n";
        let outputs2 = translator.ingest(chunk2);
        assert_eq!(outputs2.len(), 1);
        assert_eq!(outputs2[0], "data: [DONE]\n\n");
    }
}


