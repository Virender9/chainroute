| Gateway Target | Concurrency | RPS | TPS | TTFT p50 (ms) | TTFT p95 (ms) | TTFT p99 (ms) | Memory RSS (MB) | Error Rate |
|---|---|---|---|---|---|---|---|---|
| gateway-core (Rust) | 50 | 23.6 | 0.0 | 0.0 | 0.0 | 0.0 | 18.2 MB | 1000 |
| gateway-core (Rust) | 100 | 48.52 | 0.0 | 0.0 | 0.0 | 0.0 | 21.5 MB | 1000 |
| gateway-core (Rust) | 200 | 95.3 | 0.0 | 0.0 | 0.0 | 0.0 | 26.4 MB | 1000 |
| OmniRoute / LiteLLM (Node/Python) | 50 | 24.36 | 0.0 | 0.0 | 0.0 | 0.0 | 185.0 MB | 1000 |
| OmniRoute / LiteLLM (Node/Python) | 100 | 48.32 | 0.0 | 0.0 | 0.0 | 0.0 | 340.0 MB | 1000 |
| OmniRoute / LiteLLM (Node/Python) | 200 | 95.56 | 0.0 | 0.0 | 0.0 | 0.0 | 680.0 MB | 1000 |
