# Stage 1: Builder
FROM rust:1-bookworm AS builder

WORKDIR /usr/src/app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy Cargo manifests for dependency caching
COPY Cargo.toml Cargo.lock ./

# Create dummy main.rs to build dependencies
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src target/release/deps/gateway_core*

# Copy actual source code and UI assets
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY ui ./ui

# Build optimized release binary
RUN cargo build --release
RUN strip target/release/gateway-core

# Stage 2: Runtime
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /usr/src/app/target/release/gateway-core /app/gateway-core

# Create non-root runtime user
RUN useradd -u 65532 -M -s /bin/false gateway && \
    chown -R gateway:gateway /app

# Expose port
EXPOSE 3000

# Set environment variables
ENV GATEWAY_CONFIG_PATH=/app/config.yaml
ENV RUST_LOG=gateway_core=info,tower_http=info

USER 65532:65532

ENTRYPOINT ["/app/gateway-core"]
