use crate::api::completions::ChatCompletionRequest;
use crate::config::providers::ProviderConfig;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde_json::{json, Value};

/// Translates an OpenAI ChatCompletionRequest into a Gemini generateContent / streamGenerateContent payload.
pub fn transform_openai_to_gemini(
    provider: &ProviderConfig,
    upstream_model: &str,
    req: &ChatCompletionRequest,
) -> (String, HeaderMap, Value) {
    let is_stream = req.stream.unwrap_or(false);
    let base = provider.base_url.trim_end_matches('/');
    let url = if is_stream {
        format!("{}/models/{}:streamGenerateContent?alt=sse", base, upstream_model)
    } else {
        format!("{}/models/{}:generateContent", base, upstream_model)
    };

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    if let Some(ref key) = provider.api_key {
        if let Ok(val) = HeaderValue::from_str(key) {
            headers.insert("x-goog-api-key", val);
        }
    }

    let mut system_parts = String::new();
    let mut contents = Vec::new();

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
            if !system_parts.is_empty() {
                system_parts.push('\n');
            }
            system_parts.push_str(&content_str);
        } else {
            let gemini_role = if role == "assistant" { "model" } else { "user" };
            contents.push(json!({
                "role": gemini_role,
                "parts": [{"text": content_str}]
            }));
        }
    }

    let mut body = json!({
        "contents": contents
    });

    if !system_parts.is_empty() {
        body["system_instruction"] = json!({
            "parts": [{"text": system_parts}]
        });
    }

    let mut generation_config = serde_json::Map::new();
    if let Some(temp) = req.temperature {
        generation_config.insert("temperature".to_string(), json!(temp));
    }
    if let Some(max_t) = req.max_tokens {
        generation_config.insert("maxOutputTokens".to_string(), json!(max_t));
    }
    if !generation_config.is_empty() {
        body["generationConfig"] = json!(generation_config);
    }

    (url, headers, body)
}

/// Converts a non-streaming Gemini response into an OpenAI ChatCompletion response.
pub fn transform_gemini_to_openai_response(gemini_json: &Value, model_alias: &str) -> Value {
    let mut text_parts = Vec::new();
    if let Some(candidates) = gemini_json.get("candidates").and_then(|c| c.as_array()) {
        if let Some(first_candidate) = candidates.first() {
            if let Some(content) = first_candidate.get("content") {
                if let Some(parts) = content.get("parts").and_then(|p| p.as_array()) {
                    for part in parts {
                        if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                            text_parts.push(text);
                        }
                    }
                }
            }
        }
    }
    let content = text_parts.join("");

    let usage = gemini_json.get("usageMetadata");
    let prompt_tokens = usage.and_then(|u| u.get("promptTokenCount")).and_then(|v| v.as_u64()).unwrap_or(0);
    let completion_tokens = usage.and_then(|u| u.get("candidatesTokenCount")).and_then(|v| v.as_u64()).unwrap_or(0);
    let total_tokens = usage.and_then(|u| u.get("totalTokenCount")).and_then(|v| v.as_u64()).unwrap_or(prompt_tokens + completion_tokens);

    let finish_reason = if let Some(candidates) = gemini_json.get("candidates").and_then(|c| c.as_array()) {
        if let Some(first_candidate) = candidates.first() {
            match first_candidate.get("finishReason").and_then(|f| f.as_str()) {
                Some("STOP") => "stop",
                Some("MAX_TOKENS") => "length",
                _ => "stop",
            }
        } else {
            "stop"
        }
    } else {
        "stop"
    };

    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let id = "chatcmpl-gemini";

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

/// State machine for translating a Gemini SSE stream into OpenAI SSE chunks.
#[derive(Default)]
pub struct GeminiStreamTranslator {
    buffer: String,
}

impl GeminiStreamTranslator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ingest(&mut self, chunk: &[u8]) -> Vec<String> {
        let chunk_str = String::from_utf8_lossy(chunk);
        self.buffer.push_str(&chunk_str);

        let mut output_chunks = Vec::new();

        while let Some(pos) = self.buffer.find('\n') {
            let line = self.buffer[..pos].trim_end_matches('\r').to_string();
            self.buffer.drain(..=pos);

            if line.starts_with("data:") {
                let data_str = line["data:".len()..].trim();
                if data_str == "[DONE]" {
                    output_chunks.push("data: [DONE]\n\n".to_string());
                    continue;
                }

                if let Ok(val) = serde_json::from_str::<Value>(data_str) {
                    if let Some(candidates) = val.get("candidates").and_then(|c| c.as_array()) {
                        if let Some(first_cand) = candidates.first() {
                            if let Some(content) = first_cand.get("content") {
                                if let Some(parts) = content.get("parts").and_then(|p| p.as_array()) {
                                    for part in parts {
                                        if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                                            if !text.is_empty() {
                                                let open_ai_chunk = json!({
                                                    "id": "chatcmpl-gemini",
                                                    "object": "chat.completion.chunk",
                                                    "created": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
                                                    "model": "gemini",
                                                    "choices": [
                                                        {
                                                            "index": 0,
                                                            "delta": {
                                                                "content": text
                                                            },
                                                            "finish_reason": first_cand.get("finishReason").and_then(|f| f.as_str()).map(|r| match r {
                                                                "STOP" => "stop",
                                                                "MAX_TOKENS" => "length",
                                                                _ => "stop",
                                                            })
                                                        }
                                                    ]
                                                });
                                                output_chunks.push(format!("data: {}\n\n", open_ai_chunk));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if let Some(usage) = val.get("usageMetadata") {
                        let prompt_tokens = usage.get("promptTokenCount").and_then(|v| v.as_u64()).unwrap_or(0);
                        let completion_tokens = usage.get("candidatesTokenCount").and_then(|v| v.as_u64()).unwrap_or(0);
                        let total_tokens = usage.get("totalTokenCount").and_then(|v| v.as_u64()).unwrap_or(prompt_tokens + completion_tokens);

                        let usage_chunk = json!({
                            "id": "chatcmpl-gemini",
                            "object": "chat.completion.chunk",
                            "created": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
                            "model": "gemini",
                            "choices": [],
                            "usage": {
                                "prompt_tokens": prompt_tokens,
                                "completion_tokens": completion_tokens,
                                "total_tokens": total_tokens
                            }
                        });
                        output_chunks.push(format!("data: {}\n\n", usage_chunk));
                    }
                }
            }
        }

        output_chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::completions::ChatMessage;

    #[test]
    fn test_transform_request() {
        let provider = ProviderConfig::new(
            "gemini".to_string(),
            "https://generativelanguage.googleapis.com/v1beta".to_string(),
            Some("gemini-test-key".to_string()),
            60,
        );

        let req = ChatCompletionRequest {
            model: "gemini-2.5-flash".to_string(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: json!("You are a helpful assistant."),
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: json!("Hi Gemini"),
                },
            ],
            stream: Some(false),
            temperature: Some(0.5),
            max_tokens: Some(256),
            tools: None,
        };

        let (url, headers, body) = transform_openai_to_gemini(&provider, "gemini-2.5-flash", &req);
        assert_eq!(url, "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent");
        assert_eq!(headers.get("x-goog-api-key").unwrap(), "gemini-test-key");
        assert_eq!(body["system_instruction"]["parts"][0]["text"], "You are a helpful assistant.");
        assert_eq!(body["generationConfig"]["temperature"], 0.5);
        assert_eq!(body["generationConfig"]["maxOutputTokens"], 256);
        assert_eq!(body["contents"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_transform_response() {
        let gemini_resp = json!({
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": "Hello from Gemini!"
                            }
                        ]
                    },
                    "finishReason": "STOP"
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 10,
                "candidatesTokenCount": 20,
                "totalTokenCount": 30
            }
        });

        let openai_resp = transform_gemini_to_openai_response(&gemini_resp, "gemini-test");
        assert_eq!(openai_resp["choices"][0]["message"]["content"], "Hello from Gemini!");
        assert_eq!(openai_resp["choices"][0]["finish_reason"], "stop");
        assert_eq!(openai_resp["usage"]["total_tokens"], 30);
    }

    #[test]
    fn test_stream_translator() {
        let mut translator = GeminiStreamTranslator::new();
        let chunk1 = b"data: {\"candidates\": [{\"content\": {\"parts\": [{\"text\": \"Hello \"}]}}], \"usageMetadata\": {\"promptTokenCount\": 5, \"candidatesTokenCount\": 1, \"totalTokenCount\": 6}}\n\n";
        let outputs = translator.ingest(chunk1);
        assert!(!outputs.is_empty());
        assert!(outputs[0].contains("Hello "));
        assert!(outputs.iter().any(|o| o.contains("prompt_tokens")));
    }
}
