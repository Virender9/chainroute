use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::RwLock;
use sha2::{Sha256, Digest};
use subtle::ConstantTimeEq;
use thiserror::Error;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct VirtualKey {
    pub id: String,
    pub name: String,
    pub key_hash: [u8; 32],                // SHA-256 digest of plaintext key
    pub allowed_models: Vec<String>,       // e.g., ["*"] or ["default", "gpt-4o-mini"]
    pub max_budget_tokens: Option<u64>,    // Cumulative hard cap
    pub used_tokens: Arc<AtomicU64>,
    pub enabled: bool,
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("API key not found")]
    KeyNotFound,
    #[error("API key is disabled")]
    KeyDisabled,
}

#[derive(Debug, Clone)]
pub struct KeyStore {
    pub keys: Arc<RwLock<HashMap<[u8; 32], VirtualKey>>>,
}

impl KeyStore {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn hash_key(plaintext_key: &str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(plaintext_key.as_bytes());
        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }

    #[allow(dead_code)]
    pub async fn add_key(
        &self,
        plaintext_key: &str,
        name: String,
        allowed_models: Vec<String>,
        max_budget_tokens: Option<u64>,
    ) -> String {
        let key_hash = Self::hash_key(plaintext_key);
        let id = format!("vk-{}", uuid_simple());
        let vkey = VirtualKey {
            id: id.clone(),
            name,
            key_hash,
            allowed_models,
            max_budget_tokens,
            used_tokens: Arc::new(AtomicU64::new(0)),
            enabled: true,
        };
        let mut guard = self.keys.write().await;
        guard.insert(key_hash, vkey);
        id
    }

    pub async fn authenticate(&self, plaintext_key: &str) -> Result<VirtualKey, AuthError> {
        let input_hash = Self::hash_key(plaintext_key);
        let guard = self.keys.read().await;

        for (stored_hash, vkey) in guard.iter() {
            if input_hash.ct_eq(stored_hash).into() {
                if !vkey.enabled {
                    return Err(AuthError::KeyDisabled);
                }
                return Ok(vkey.clone());
            }
        }

        Err(AuthError::KeyNotFound)
    }

    pub fn default_dev_keystore() -> Self {
        let dev_key = "gw-dev-master-key";
        let key_hash = Self::hash_key(dev_key);
        let vkey = VirtualKey {
            id: "vk-dev-master".to_string(),
            name: "Development Master Key".to_string(),
            key_hash,
            allowed_models: vec!["*".to_string()],
            max_budget_tokens: None,
            used_tokens: Arc::new(AtomicU64::new(0)),
            enabled: true,
        };
        let mut keys = HashMap::new();
        keys.insert(key_hash, vkey);
        Self {
            keys: Arc::new(RwLock::new(keys)),
        }
    }

    pub fn from_configs(configs: &[crate::config::providers::VirtualKeyConfig]) -> Self {
        if configs.is_empty() {
            return Self::default_dev_keystore();
        }

        let mut keys = HashMap::new();
        for c in configs {
            let key_hash = Self::hash_key(&c.key);
            let id = if c.id.is_empty() {
                format!("vk-{}", uuid_simple())
            } else {
                c.id.clone()
            };
            keys.insert(
                key_hash,
                VirtualKey {
                    id,
                    name: c.name.clone(),
                    key_hash,
                    allowed_models: c.allowed_models.clone(),
                    max_budget_tokens: c.max_budget_tokens,
                    used_tokens: Arc::new(AtomicU64::new(0)),
                    enabled: c.enabled,
                },
            );
        }

        Self {
            keys: Arc::new(RwLock::new(keys)),
        }
    }

    pub async fn reload_keys(&self, configs: &[crate::config::providers::VirtualKeyConfig]) {
        if configs.is_empty() {
            return;
        }

        let mut new_keys = HashMap::new();
        let guard = self.keys.read().await;

        for c in configs {
            let key_hash = Self::hash_key(&c.key);
            // Retain cumulative token count if the key existed previously
            let used_tokens = if let Some(existing) = guard.get(&key_hash) {
                existing.used_tokens.clone()
            } else {
                Arc::new(AtomicU64::new(0))
            };

            let id = if c.id.is_empty() {
                format!("vk-{}", uuid_simple())
            } else {
                c.id.clone()
            };

            new_keys.insert(
                key_hash,
                VirtualKey {
                    id,
                    name: c.name.clone(),
                    key_hash,
                    allowed_models: c.allowed_models.clone(),
                    max_budget_tokens: c.max_budget_tokens,
                    used_tokens,
                    enabled: c.enabled,
                },
            );
        }
        drop(guard);

        let mut write_guard = self.keys.write().await;
        *write_guard = new_keys;
    }
}

#[allow(dead_code)]
fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let start = SystemTime::now();
    let since_epoch = start.duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{:x}", since_epoch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_keystore_authentication() {
        let keystore = KeyStore::default_dev_keystore();

        // Authenticate with valid master key
        let auth_res = keystore.authenticate("gw-dev-master-key").await;
        assert!(auth_res.is_ok());
        let vkey = auth_res.unwrap();
        assert_eq!(vkey.id, "vk-dev-master");
        assert_eq!(vkey.allowed_models, vec!["*"]);

        // Authenticate with invalid key
        let invalid_res = keystore.authenticate("invalid-key").await;
        assert!(matches!(invalid_res, Err(AuthError::KeyNotFound)));

        // Add custom key
        let key_id = keystore
            .add_key("custom-secret", "Custom Key".to_string(), vec!["gpt-4o-mini".to_string()], Some(1000))
            .await;
        assert!(key_id.starts_with("vk-"));

        let custom_res = keystore.authenticate("custom-secret").await.unwrap();
        assert_eq!(custom_res.name, "Custom Key");
        assert_eq!(custom_res.max_budget_tokens, Some(1000));

        // Test disabled key
        {
            let mut guard = keystore.keys.write().await;
            for vkey in guard.values_mut() {
                if vkey.name == "Custom Key" {
                    vkey.enabled = false;
                }
            }
        }
        let disabled_res = keystore.authenticate("custom-secret").await;
        assert!(matches!(disabled_res, Err(AuthError::KeyDisabled)));
    }
}