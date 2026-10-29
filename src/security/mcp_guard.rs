use std::sync::Arc;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub parameters: serde_json::Value, // JSON Schema object
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub call_type: String, // typically "function"
    pub function: FunctionCall,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String, // Stringified JSON arguments
}

#[derive(Debug, thiserror::Error)]
pub enum McpSecurityError {
    #[error("Tool schema validation failed for tool '{tool}': {details}")]
    SchemaViolation {
        tool: String,
        details: String,
    },
    #[error("Tool '{0}' is unregistered")]
    UnregisteredTool(String),
    #[error("Failed to parse tool arguments as JSON: {0}")]
    JsonParseError(#[from] serde_json::Error),
}

pub struct McpSchemaGuard {
    validators: Arc<tokio::sync::RwLock<std::collections::HashMap<String, jsonschema::Validator>>>,
}

impl McpSchemaGuard {
    pub fn new() -> Self {
        Self {
            validators: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    pub fn register_tool(&self, tool: &ToolDefinition) -> Result<(), String> {
        let compiled = jsonschema::validator_for(&tool.parameters)
            .map_err(|e| format!("Failed to compile JSON schema for tool '{}': {}", tool.name, e))?;
        let mut guard = self.validators.blocking_write();
        guard.insert(tool.name.clone(), compiled);
        Ok(())
    }

    pub fn validate_tool_call(&self, tool_call: &ToolCall) -> Result<(), McpSecurityError> {
        let args_val: serde_json::Value = serde_json::from_str(&tool_call.function.arguments)?;
        let guard = self.validators.blocking_read();
        let validator = guard
            .get(&tool_call.function.name)
            .ok_or_else(|| McpSecurityError::UnregisteredTool(tool_call.function.name.clone()))?;

        if let Err(errors) = validator.validate(&args_val) {
            let error_msg = errors.to_string();
            return Err(McpSecurityError::SchemaViolation {
                tool: tool_call.function.name.clone(),
                details: error_msg,
            });
        }

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_guard_validation() {
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

        // Valid call
        let valid_call = ToolCall {
            id: "call_1".to_string(),
            call_type: "function".to_string(),
            function: FunctionCall {
                name: "execute_command".to_string(),
                arguments: r#"{"command": "echo hello", "timeout": 5}"#.to_string(),
            },
        };
        assert!(guard.validate_tool_call(&valid_call).is_ok());

        // Invalid call: extra property disallowed
        let invalid_call_extra = ToolCall {
            id: "call_2".to_string(),
            call_type: "function".to_string(),
            function: FunctionCall {
                name: "execute_command".to_string(),
                arguments: r#"{"command": "echo hello", "injected": "malicious"}"#.to_string(),
            },
        };
        assert!(guard.validate_tool_call(&invalid_call_extra).is_err());

        // Unregistered tool call
        let unregistered_call = ToolCall {
            id: "call_3".to_string(),
            call_type: "function".to_string(),
            function: FunctionCall {
                name: "unknown_tool".to_string(),
                arguments: r#"{}"#.to_string(),
            },
        };
        assert!(guard.validate_tool_call(&unregistered_call).is_err());
    }
}

