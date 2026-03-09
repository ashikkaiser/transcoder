#!/usr/bin/env bash
# Local deploy for Debian – builds on this machine, never removes/overwrites
# host files (.env, database, etc.)
# Run as root: su -c './deploy-local.sh'  (sudo not required)
set -euo pipefail

[ "$(id -u)" -eq 0 ] || { echo "Run as root: su -c './deploy-local.sh'"; exit 1; }

# ── Configuration ────────────────────────────────────────────────────────
INSTALL_DIR="${INSTALL_DIR:-$(pwd)}"
SERVICE_NAME="transcoder"
BINARY_NAME="transcoder"

# ── Colors ───────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
step() { echo -e "\n${CYAN}▸ $1${NC}"; }
ok()   { echo -e "${GREEN}  ✓ $1${NC}"; }
warn() { echo -e "${YELLOW}  ⚠ $1${NC}"; }
fail() { echo -e "${RED}  ✗ $1${NC}"; exit 1; }

# ── Run from project root ────────────────────────────────────────────────
cd "$(dirname "$0")"
PROJECT_DIR="$(pwd)"

step "Installing system dependencies (Debian/Ubuntu)..."

export DEBIAN_FRONTEND=noninteractive
PACKAGES=""
command -v gcc    &>/dev/null || PACKAGES="$PACKAGES build-essential"
command -v ffmpeg &>/dev/null || PACKAGES="$PACKAGES ffmpeg"
command -v curl   &>/dev/null || PACKAGES="$PACKAGES curl"
command -v pkg-config &>/dev/null || PACKAGES="$PACKAGES pkg-config"
dpkg -s libssl-dev &>/dev/null 2>&1 || PACKAGES="$PACKAGES libssl-dev"

if [ -n "$PACKAGES" ]; then
    echo "  Installing:$PACKAGES"
    apt-get update -qq
    apt-get install -y -qq $PACKAGES
fi
ok "System dependencies ready"

# ── Rust ────────────────────────────────────────────────────────────────
step "Checking Rust..."
if ! command -v cargo &>/dev/null; then
    echo "  Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
    ok "Rust installed"
else
    source "$HOME/.cargo/env" 2>/dev/null || true
    ok "Rust already installed ($(rustc --version))"
fi

# ── Build ────────────────────────────────────────────────────────────────
step "Building release binary..."
cargo build --release 2>&1

BINARY="target/release/${BINARY_NAME}"
if [ ! -f "$BINARY" ]; then
    fail "Build failed — binary not found"
fi
SIZE=$(du -sh "$BINARY" | cut -f1)
ok "Built ($SIZE)"

# ── .env: never overwrite existing ───────────────────────────────────────
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        warn "Created .env from .env.example — edit it with your secrets!"
    else
        cat > .env <<'EOF'
PORT=3005
TEMP_DIR=/tmp/transcoder
DATABASE_URL=sqlite:./transcoder.db
ADMIN_USERNAME=admin
ADMIN_PASSWORD=changeme
R2_ACCOUNT_ID=
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET_NAME=
R2_PUBLIC_URL=
EOF
        warn "Created minimal .env — fill in R2 credentials!"
    fi
else
    ok ".env exists (unchanged)"
fi

mkdir -p /tmp/transcoder

# ── Systemd service ──────────────────────────────────────────────────────
step "Configuring systemd service..."
grep -v '^#' .env | grep -v '^$' > .env.systemd 2>/dev/null || true
ok "Generated .env.systemd"

# Use project dir as working directory (no /opt/transcoder)
WORK_DIR="$(pwd)"
tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null <<EOF
[Unit]
Description=Video Transcoder Service (Local)
After=network.target
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=simple
WorkingDirectory=${WORK_DIR}
EnvironmentFile=${WORK_DIR}/.env.systemd
ExecStart=${WORK_DIR}/target/release/${BINARY_NAME}
Restart=on-failure
RestartSec=5

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${WORK_DIR} /tmp/transcoder
PrivateTmp=false

StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}" 2>/dev/null || true

# ── Start / restart ──────────────────────────────────────────────────────
step "Starting service..."
if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
    systemctl restart "${SERVICE_NAME}"
    ok "Restarted ${SERVICE_NAME}"
else
    systemctl start "${SERVICE_NAME}"
    ok "Started ${SERVICE_NAME}"
fi

sleep 3
if systemctl is-active --quiet "${SERVICE_NAME}"; then
    sleep 2
    PORT="${PORT:-3005}"
    if curl -sf "http://127.0.0.1:${PORT}/health" 2>/dev/null; then
        echo ""
        ok "Health check passed"
    else
        warn "Health check not responding yet"
    fi
else
    fail "${SERVICE_NAME} failed to start"
    journalctl -u "${SERVICE_NAME}" --no-pager -n 20
fi

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Local deploy complete${NC}"
echo -e "${GREEN}  UI:     http://127.0.0.1:3005/auth/login${NC}"
echo -e "${GREEN}  Health: http://127.0.0.1:3005/health${NC}"
echo -e "${GREEN}  Logs:   journalctl -u ${SERVICE_NAME} -f${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
