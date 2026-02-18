# ── Build stage ───────────────────────────────────────
FROM rust:1.77-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
# Pre-build dependencies (cache layer)
RUN mkdir src && echo "fn main(){}" > src/main.rs && cargo build --release && rm -rf src
COPY . .
RUN cargo build --release

# ── Runtime stage ─────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Run as non-root user for security
RUN groupadd -r transcoder && useradd -r -g transcoder -m -s /bin/false transcoder

WORKDIR /app
COPY --from=builder /app/target/release/transcoder /app/transcoder
COPY migrations ./migrations

# Create temp directory owned by the app user
RUN mkdir -p /tmp/transcoder && chown transcoder:transcoder /tmp/transcoder

USER transcoder

ENV TEMP_DIR=/tmp/transcoder
EXPOSE 3000

# Health check for orchestrators (Docker Compose, Kubernetes, etc.)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

CMD ["/app/transcoder"]
