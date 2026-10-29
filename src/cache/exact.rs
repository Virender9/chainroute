use crate::api::completions::ChatMessage;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use twox_hash::XxHash64;
use std::hash::Hasher;

#[derive(Clone, Debug)]
pub struct CachedResponse {
    pub body: Bytes,
    pub created_at: Instant,
    pub ttl: Duration,
}

#[derive(Clone)]
pub struct ExactResponseCache {
    inner: Arc<RwLock<HashMap<String, CachedResponse>>>,
}

impl Default for ExactResponseCache {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl ExactResponseCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn compute_cache_key(model: &str, messages: &[ChatMessage], temperature: Option<f32>) -> String {
        let mut hasher = XxHash64::default();
        hasher.write(model.as_bytes());
        for msg in messages {
            hasher.write(msg.role.as_bytes());
            if let Ok(content_bytes) = serde_json::to_vec(&msg.content) {
                hasher.write(&content_bytes);
            }
        }
        if let Some(temp) = temperature {
            hasher.write(&temp.to_le_bytes());
        }
        let hash_val = hasher.finish();
        format!("{:016x}", hash_val)
    }

    pub async fn get(&self, key: &str) -> Option<Bytes> {
        let mut cache = self.inner.write().await;
        if let Some(entry) = cache.get(key) {
            if entry.created_at.elapsed() < entry.ttl {
                return Some(entry.body.clone());
            } else {
                cache.remove(key);
            }
        }
        None
    }

    pub async fn set(&self, key: String, body: Bytes, ttl: Duration) {
        let mut cache = self.inner.write().await;
        cache.insert(
            key,
            CachedResponse {
                body,
                created_at: Instant::now(),
                ttl,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_exact_response_cache_flow() {
        let cache = ExactResponseCache::new();
        let msg = ChatMessage {
            role: "user".to_string(),
            content: serde_json::json!("hello"),
        };
        let key = ExactResponseCache::compute_cache_key("gpt-4", &[msg], Some(0.7));
        assert!(!key.is_empty());

        assert!(cache.get(&key).await.is_none());

        let payload = Bytes::from("{\"choices\":[{\"message\":{\"content\":\"world\"}}]}");
        cache.set(key.clone(), payload.clone(), Duration::from_secs(300)).await;

        let cached = cache.get(&key).await.expect("should be cached");
        assert_eq!(cached, payload);
    }
}

