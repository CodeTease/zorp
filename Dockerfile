# Builder Stage
FROM rust:1.81-bookworm as builder

WORKDIR /app

# Install build dependencies
# - pkg-config, libssl-dev: Required for crates like `reqwest` and `sqlx` (if checking)
# - protobuf-compiler: sometimes needed for otel/other deps (check if needed, adding just in case for robustness)
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Pre-cache dependencies
# 1. Copy Cargo files
COPY Cargo.toml Cargo.lock ./

# 2. Create dummy main to build deps
RUN mkdir src && echo "fn main() {}" > src/main.rs

# 3. Build dependencies (release mode)
# Note: We enable 'postgres' feature to build for production
RUN cargo build --release --no-default-features --features postgres

# 4. Clean up dummy build
RUN rm -rf src

# Copy source code
COPY . .

# Build the actual application
# Set SQLX_OFFLINE to true to bypass compile-time DB checks (requires sqlx-data.json or no macros)
# Note: If sqlx-data.json is missing and macros are used, this might fail, but it's the standard Docker approach.
ENV SQLX_OFFLINE=true
RUN cargo build --release --no-default-features --features postgres

# Runtime Stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
# - ca-certificates: Needed for HTTPS (S3, Webhooks)
# - libssl3: Runtime lib for OpenSSL
# - curl: Useful for healthchecks
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/zorp /app/zorp

# Create directory for migrations if needed (embedded in binary usually, but good practice)
COPY --from=builder /app/migrations /app/migrations

# Expose the API port
EXPOSE 3000

# Set Entrypoint
CMD ["./zorp"]
