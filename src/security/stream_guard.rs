use std::collections::HashMap;

use serde_json::Value;

use crate::security::mcp_guard::{McpSchemaGuard, ToolCall, FunctionCall, McpSecurityError};

/// Represents an in-progress streaming tool call delta being accumulated across SSE chunks.
#[derive(Default, Debug)]
pub struct ActiveToolCallDelta {
    pub id: String,
    pub call_type: String,
    pub name: String,
    pub arguments_buffer: String,
}

/// Accumulates streaming tool-call delta fragments across SSE chunks.
///
/// As the LLM emits `delta.tool_calls[i].function.arguments` fragments, they are
/// buffered per `tool_calls[i].index`. When a terminal `finish_reason` arrives,
/// the assembled [`ToolCall`]s can be retrieved and validated against an
/// [`McpSchemaGuard`].
pub struct StreamingToolCallAccumulator {
    calls: HashMap<usize, ActiveToolCallDelta>,
}

impl Default for StreamingToolCallAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingToolCallAccumulator {
    pub fn new() -> Self {
        Self {
            calls: HashMap::new(),
        }
    }

    /// Feed a single SSE `data: {...}` JSON payload.
    ///
    /// Parses `choices[0].delta.tool_calls` and accumulates fragments per index.
    /// When `choices[0].finish_reason` is `"tool_calls"` or `"stop"`, packages all
    /// accumulated entries into a [`Vec<ToolCall>`] and returns `Some(...)`.
    ///
    /// Returns `None` while the stream is still in progress.
    pub fn feed_chunk(&mut self, data_str: &str) -> Option<Vec<ToolCall>> {
        let val: Value = match serde_json::from_str(data_str) {
            Ok(v) => v,
            Err(_) => return None,
        };

        let choices = val.get("choices")?.as_array()?;
        let first_choice = choices.first()?;

        // Accumulate tool call delta fragments.
        if let Some(delta) = first_choice.get("delta") {
            if let Some(tool_calls) = delta.get("tool_calls").and_then(|tc| tc.as_array()) {
                for tc_val in tool_calls {
                    let index = match tc_val.get("index").and_then(|i| i.as_u64()) {
                        Some(i) => i as usize,
                        None => continue,
                    };
                    let entry = self.calls.entry(index).or_default();

                    if let Some(id) = tc_val.get("id").and_then(|i| i.as_str()) {
                        entry.id = id.to_string();
                    }
                    if let Some(call_type) = tc_val.get("type").and_then(|t| t.as_str()) {
                        entry.call_type = call_type.to_string();
                    }
                    if let Some(func) = tc_val.get("function") {
                        if let Some(name) = func.get("name").and_then(|n| n.as_str()) {
                            entry.name = name.to_string();
                        }
                        if let Some(args) = func.get("arguments").and_then(|a| a.as_str()) {
                            entry.arguments_buffer.push_str(args);
                        }
                    }
                }
            }
        }

        // Terminal finish reason: package accumulated calls.
        let finish_reason = first_choice.get("finish_reason").and_then(|f| f.as_str());
        if matches!(finish_reason, Some("tool_calls") | Some("stop")) {
            let tool_calls: Vec<ToolCall> = self
                .calls
                .iter()
                .map(|(_, entry)| ToolCall {
                    id: entry.id.clone(),
                    call_type: entry.call_type.clone(),
                    function: FunctionCall {
                        name: entry.name.clone(),
                        arguments: entry.arguments_buffer.clone(),
                    },
                })
                .collect();
            return Some(tool_calls);
        }

        None
    }

    /// Validate every accumulated tool call through the MCP schema guard.
    ///
    /// Returns `Ok(())` if all calls pass validation, or the first
    /// [`McpSecurityError`] encountered.
    pub fn validate_all(
        &self,
        tool_calls: &[ToolCall],
        guard: &McpSchemaGuard,
    ) -> Result<(), McpSecurityError> {
        for call in tool_calls {
            guard.validate_tool_call(call)?;
        }
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::mcp_guard::ToolDefinition;

    fn make_guard() -> McpSchemaGuard {
        let guard = McpSchemaGuard::new();
        let tool_def = ToolDefinition {
            name: "execute_command".to_string(),
            description: Some("Run a shell command".to_string()),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "timeout": { "type": "integer" }
                },
                "required": ["command"],
                "additionalProperties": false
            }),
        };
        assert!(guard.register_tool(&tool_def).is_ok());
        guard
    }

    #[test]
    fn test_fragmented_argument_accumulation() {
        let mut acc = StreamingToolCallAccumulator::new();

        let chunk1 = r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"execute_command","arguments":"{\"command\": \"ech"}}]}}]}"#;
        let result = acc.feed_chunk(chunk1);
        assert!(result.is_none());

        let chunk2 = r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"o hello\", \"timeout\": 5}"}}]}}]}"#;
        let result = acc.feed_chunk(chunk2);
        assert!(result.is_none());

        let chunk3 = r#"{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#;
        let result = acc.feed_chunk(chunk3);
        assert!(result.is_some());
        let calls = result.unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "call_1");
        assert_eq!(calls[0].call_type, "function");
        assert_eq!(calls[0].function.name, "execute_command");
        assert_eq!(
            calls[0].function.arguments,
            r#"{"command": "echo hello", "timeout": 5}"#
        );
    }

    #[test]
    fn test_multiple_tool_calls_with_indices() {
        let mut acc = StreamingToolCallAccumulator::new();

        let chunk1 = r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"tool_a","arguments":"{\"x\":1}"}}]}}]}"#;
        acc.feed_chunk(chunk1);

        let chunk2 = r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_b","type":"function","function":{"name":"tool_b","arguments":"{\"y\":2}"}}]}}]}"#;
        acc.feed_chunk(chunk2);

        let chunk3 = r#"{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#;
        let calls = acc.feed_chunk(chunk3).unwrap();
        assert_eq!(calls.len(), 2);
    }

    #[test]
    fn test_successful_validation_on_completion() {
        let guard = make_guard();
        let mut acc = StreamingToolCallAccumulator::new();

        let chunk1 = r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"execute_command","arguments":"{\"command\": \"echo hi\", \"timeout\": 5}"}}]}}]}"#;
        acc.feed_chunk(chunk1);

        let chunk2 = r#"{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#;
        let calls = acc.feed_chunk(chunk2).unwrap();

        assert!(acc.validate_all(&calls, &guard).is_ok());
    }

    #[test]
    fn test_rejected_on_schema_violation() {
        let guard = make_guard();
        let mut acc = StreamingToolCallAccumulator::new();

        let chunk1 = r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"execute_command","arguments":"{\"command\": \"echo hi\", \"injected\": \"rm -rf /\"}"}}]}}]}"#;
        acc.feed_chunk(chunk1);

        let chunk2 = r#"{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#;
        let calls = acc.feed_chunk(chunk2).unwrap();

        assert!(acc.validate_all(&calls, &guard).is_err());
    }
}
