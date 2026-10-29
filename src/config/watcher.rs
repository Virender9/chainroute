use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use arc_swap::ArcSwap;
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use crate::auth::keys::KeyStore;
use crate::config::loader::load_config_from_file;
use crate::config::providers::GatewayConfig;

/// Starts a zero-downtime background filesystem watcher for `config.yaml`.
///
/// - Watches the target file for modifications.
/// - Debounces multiple filesystem events within 200ms.
/// - Validates new configuration before applying.
/// - Preserves the active configuration if validation or parsing fails.
/// - Atomically swaps pointer via ArcSwap if valid without locking or downtime.
#[allow(dead_code)]
pub fn start_config_watcher(
    path: PathBuf,
    config_holder: Arc<ArcSwap<GatewayConfig>>,
) {
    start_config_watcher_with_keystore(path, config_holder, None);
}

/// Extended version that also updates the keystore if virtual keys are modified.
pub fn start_config_watcher_with_keystore(
    path: PathBuf,
    config_holder: Arc<ArcSwap<GatewayConfig>>,
    keystore: Option<Arc<KeyStore>>,
) {
    tokio::spawn(async move {
        run_watcher_loop(path, config_holder, keystore).await;
    });
}

async fn run_watcher_loop(
    path: PathBuf,
    config_holder: Arc<ArcSwap<GatewayConfig>>,
    keystore: Option<Arc<KeyStore>>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(100);

    // Resolve directory to watch (watching parent directory captures atomic file replacement/renames)
    let full_path = if path.is_relative() {
        std::env::current_dir().map(|cwd| cwd.join(&path)).unwrap_or_else(|_| path.clone())
    } else {
        path.clone()
    };

    let watch_dir = match full_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };

    let target_filename = full_path
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| "config.yaml".to_string());

    let tx_clone = tx.clone();
    let watcher_res = RecommendedWatcher::new(
        move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let affects_target = event.paths.iter().any(|p| {
                    p.file_name()
                        .map(|name| name.to_string_lossy() == target_filename)
                        .unwrap_or(false)
                });

                if affects_target {
                    let _ = tx_clone.blocking_send(());
                }
            }
        },
        Config::default(),
    );

    let mut watcher = match watcher_res {
        Ok(w) => w,
        Err(err) => {
            tracing::error!(
                error = %err,
                path = %full_path.display(),
                "Failed to initialize filesystem watcher for configuration"
            );
            return;
        }
    };

    if let Err(err) = watcher.watch(&watch_dir, RecursiveMode::NonRecursive) {
        tracing::error!(
            error = %err,
            watch_dir = %watch_dir.display(),
            "Failed to start watching directory for configuration changes"
        );
        return;
    }

    tracing::info!(
        path = %full_path.display(),
        watch_dir = %watch_dir.display(),
        "Zero-downtime configuration file watcher active"
    );

    while rx.recv().await.is_some() {
        // Debounce filesystem change events
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Drain any bursts of duplicate events
        while rx.try_recv().is_ok() {}

        tracing::debug!(path = %full_path.display(), "Detected configuration file change, reloading...");

        match load_config_from_file(&full_path) {
            Ok(new_config) => {
                tracing::info!(
                    path = %full_path.display(),
                    providers = new_config.providers.len(),
                    routes = new_config.routes.len(),
                    virtual_keys = new_config.virtual_keys.len(),
                    "Live reload: Successfully swapped to new configuration snapshot"
                );

                if let Some(ks) = &keystore {
                    ks.reload_keys(&new_config.virtual_keys).await;
                }

                // Atomic lock-free pointer swap
                config_holder.store(Arc::new(new_config));
            }
            Err(err) => {
                tracing::error!(
                    path = %full_path.display(),
                    error = %err,
                    "Live reload validation failure: preserving existing running configuration without downtime"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test]
    async fn test_watcher_hot_reload_and_safety_gate() {
        let temp_dir = std::env::temp_dir().join(format!("gw_test_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let config_path = temp_dir.join("config.yaml");

        let initial_yaml = r#"
providers:
  local:
    base_url: "http://127.0.0.1:11434/v1"
routes:
  default:
    targets:
      - provider_name: local
        upstream_model: "llama3"
"#;
        std::fs::write(&config_path, initial_yaml).unwrap();

        let initial_cfg = load_config_from_file(&config_path).unwrap();
        let holder = Arc::new(ArcSwap::from_pointee(initial_cfg));

        start_config_watcher(config_path.clone(), holder.clone());

        // Allow watcher thread to initialize
        tokio::time::sleep(Duration::from_millis(150)).await;

        // 1. Valid update: add a new route
        let updated_yaml = r#"
providers:
  local:
    base_url: "http://127.0.0.1:11434/v1"
routes:
  default:
    targets:
      - provider_name: local
        upstream_model: "llama3"
  coding-fast:
    targets:
      - provider_name: local
        upstream_model: "qwen2.5-coder"
"#;
        let mut f = std::fs::File::create(&config_path).unwrap();
        f.write_all(updated_yaml.as_bytes()).unwrap();
        f.sync_all().unwrap();
        drop(f);

        // Wait for debounce and processing
        tokio::time::sleep(Duration::from_millis(500)).await;

        let snapshot = holder.load();
        assert!(snapshot.routes.contains_key("coding-fast"), "Expected coding-fast route to be present after live reload");

        // 2. Invalid update: malformed YAML / validation error
        let invalid_yaml = r#"
providers:
  local:
    base_url: "http://127.0.0.1:11434/v1"
routes:
  default:
    targets:
      - provider_name: non_existent_provider
        upstream_model: "dummy"
"#;
        let mut f = std::fs::File::create(&config_path).unwrap();
        f.write_all(invalid_yaml.as_bytes()).unwrap();
        f.sync_all().unwrap();
        drop(f);

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify safety gate preserved previous valid configuration
        let snapshot_after_bad = holder.load();
        assert!(snapshot_after_bad.routes.contains_key("coding-fast"), "Safety gate failed: configuration was dropped on invalid YAML");

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}