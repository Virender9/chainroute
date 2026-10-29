use crate::api::completions::ChatMessage;
use tiktoken_rs::get_bpe_from_model;
use tiktoken_rs::cl100k_base;

/// Estimate the token count for a request.
pub fn estimate_request_tokens(model: &str, messages: &[ChatMessage], max_tokens_param: Option<u32>) -> u64 {
    let bpe = get_bpe_from_model(model).unwrap_or_else(|_| cl100k_base().expect("failed to load cl100k_base"));

    let prompt_tokens: usize = messages
        .iter()
        .map(|m| {
            let text = match &m.content {
                serde_json::Value::String(s) => s.as_str(),
                other => &other.to_string(),
            };
            bpe.encode_with_special_tokens(text).len()
        })
        .sum();

    let completion_allowance = std::cmp::min(max_tokens_param.unwrap_or(256) as usize, 2048);
    (prompt_tokens + completion_allowance) as u64
}