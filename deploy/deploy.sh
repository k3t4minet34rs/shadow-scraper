#!/usr/bin/env bash
# deploy.sh — push latest code to EC2 and (re)start shadow services
#
# Usage (from your Mac):
#   ./deploy/deploy.sh
#
# On first run it also:
#   - installs poetry (if missing)
#   - configures in-project venv
#   - creates /home/ubuntu/shadow-bot/data/
#   - generates .env.service from .env
#   - installs & enables the two systemd units
#
# Subsequent runs just: rsync code → poetry install → restart services

set -euo pipefail

# ── config ─────────────────────────────────────────────────────────────────────
EC2_HOST="${EC2_HOST:-ubuntu@YOUR_EC2_IP}"   # override via env or edit here
REMOTE_DIR="/home/ubuntu/shadow-bot"
DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$DEPLOY_DIR")"

# ── colours ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[deploy]${NC} $*"; }
warn()    { echo -e "${YELLOW}[deploy]${NC} $*"; }
die()     { echo -e "${RED}[deploy] ERROR:${NC} $*" >&2; exit 1; }

# ── preflight ──────────────────────────────────────────────────────────────────
[[ "$EC2_HOST" == *"YOUR_EC2_IP"* ]] && die "Set EC2_HOST or edit deploy.sh (line 14)"
[[ -f "$PROJECT_DIR/.env" ]] || die ".env not found at $PROJECT_DIR/.env"
command -v rsync >/dev/null || die "rsync not found on local machine"

info "Target: $EC2_HOST:$REMOTE_DIR"

# ── 1. rsync code (exclude venv, data, pycache) ───────────────────────────────
info "Syncing code …"
rsync -az --delete \
  --exclude='.venv/' \
  --exclude='data/' \
  --exclude='__pycache__/' \
  --exclude='*.pyc' \
  --exclude='.git/' \
  --exclude='.env' \
  --exclude='debug/' \
  "$PROJECT_DIR/" "$EC2_HOST:$REMOTE_DIR/"

# ── 2. sync .env separately (not --delete'd above) ────────────────────────────
info "Syncing .env …"
rsync -az "$PROJECT_DIR/.env" "$EC2_HOST:$REMOTE_DIR/.env"

# ── 3. remote setup ───────────────────────────────────────────────────────────
info "Running remote setup …"
ssh "$EC2_HOST" bash <<'REMOTE'
set -euo pipefail
cd /home/ubuntu/shadow-bot

# ── poetry ──────────────────────────────────────────────────────────────────
if ! command -v poetry &>/dev/null; then
  echo "[remote] installing poetry …"
  curl -sSL https://install.python-poetry.org | python3 -
  export PATH="$HOME/.local/bin:$PATH"
  echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
fi
export PATH="$HOME/.local/bin:$PATH"

# keep venv inside the project dir — simpler for systemd ExecStart paths
poetry config virtualenvs.in-project true

# ── install deps ────────────────────────────────────────────────────────────
echo "[remote] poetry install …"
poetry install --without dev --no-interaction

# ── data dir ────────────────────────────────────────────────────────────────
mkdir -p data

# ── .env.service (strip "export " prefix for systemd EnvironmentFile) ───────
echo "[remote] generating .env.service …"
sed 's/^export //' .env > .env.service
chmod 600 .env.service

# ── systemd units ───────────────────────────────────────────────────────────
echo "[remote] installing systemd units …"
sudo cp deploy/shadow-scraper.service  /etc/systemd/system/
sudo cp deploy/shadow-executor.service /etc/systemd/system/
sudo systemctl daemon-reload

sudo systemctl enable shadow-scraper  shadow-executor
sudo systemctl restart shadow-scraper shadow-executor

# ── status ──────────────────────────────────────────────────────────────────
sleep 2
echo ""
echo "=== shadow-scraper ==="
sudo systemctl status shadow-scraper  --no-pager -l | head -20
echo ""
echo "=== shadow-executor ==="
sudo systemctl status shadow-executor --no-pager -l | head -20
REMOTE

info "Deploy complete ✓"
info ""
info "Useful commands on EC2:"
info "  journalctl -u shadow-scraper  -f        # scraper live logs"
info "  journalctl -u shadow-executor -f        # executor live logs"
info "  journalctl -u shadow-executor -f --grep 'SIGNAL\\|Order\\|Resolved'"
info "  sudo systemctl stop  shadow-executor    # pause trading"
info "  sudo systemctl start shadow-executor    # resume trading"
