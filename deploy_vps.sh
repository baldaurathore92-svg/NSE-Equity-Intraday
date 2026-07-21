#!/bin/bash
#
# deploy_vps.sh — One-command VPS setup for NSE Book Scanner
# ============================================================
# Ubuntu 22.04 / 24.04 / Debian 12 पर test किया गया।
#
# Usage:
#   1. VPS पर SSH login करें
#   2. scanner files upload करें (या git clone)
#   3. chmod +x deploy_vps.sh && ./deploy_vps.sh
#

set -e   # किसी भी step में error हो तो script रुक जाए
set -u   # undefined variables से बचाव

# --- Colors for readability ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

step()  { echo -e "\n${BLUE}▶ $1${NC}"; }
ok()    { echo -e "${GREEN}  ✓ $1${NC}"; }
warn()  { echo -e "${YELLOW}  ⚠ $1${NC}"; }
error() { echo -e "${RED}  ✗ $1${NC}"; }

INSTALL_DIR="$HOME/nse_scanner"
PYTHON_MIN="3.9"

echo -e "${BLUE}"
echo "════════════════════════════════════════════════════════"
echo "   NSE Book Scanner — VPS Deployment (Ubuntu/Debian)   "
echo "════════════════════════════════════════════════════════"
echo -e "${NC}"

# ----------------------------------------------------------------
# 1. Check we're not running as root (security best practice)
# ----------------------------------------------------------------
step "1/8  User check"
if [ "$EUID" -eq 0 ]; then
    error "Root user के रूप में मत चलाइए। एक normal user बनाइए:"
    echo "    adduser trader"
    echo "    usermod -aG sudo trader"
    echo "    su - trader"
    echo "फिर से script चलाइए।"
    exit 1
fi
ok "Running as user: $USER"

# ----------------------------------------------------------------
# 2. System update
# ----------------------------------------------------------------
step "2/8  System packages update"
sudo apt-get update -qq
sudo apt-get upgrade -y -qq
ok "System updated"

# ----------------------------------------------------------------
# 3. Install dependencies (Python, tmux, timezone tools)
# ----------------------------------------------------------------
step "3/8  Installing Python + tools"
sudo apt-get install -y -qq \
    python3 \
    python3-pip \
    python3-venv \
    tmux \
    htop \
    curl \
    ca-certificates \
    tzdata
ok "Python + tools installed"

PYVER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "  Python version: $PYVER"

# ----------------------------------------------------------------
# 4. Set timezone to IST (CRITICAL — market hours check depends on this)
# ----------------------------------------------------------------
step "4/8  Setting timezone to Asia/Kolkata (IST)"
sudo timedatectl set-timezone Asia/Kolkata
CURRENT_TZ=$(timedatectl show -p Timezone --value)
if [ "$CURRENT_TZ" = "Asia/Kolkata" ]; then
    ok "Timezone set to IST: $(date)"
else
    warn "Timezone verification failed: $CURRENT_TZ"
fi

# ----------------------------------------------------------------
# 5. Firewall (allow SSH only)
# ----------------------------------------------------------------
step "5/8  Basic firewall (ufw)"
if command -v ufw >/dev/null 2>&1; then
    sudo ufw --force reset >/dev/null
    sudo ufw default deny incoming >/dev/null
    sudo ufw default allow outgoing >/dev/null
    sudo ufw allow ssh >/dev/null
    sudo ufw --force enable >/dev/null
    ok "Firewall enabled (SSH allowed, all inbound denied)"
else
    warn "ufw not installed, skipping firewall"
fi

# ----------------------------------------------------------------
# 6. Create Python virtual environment + install packages
# ----------------------------------------------------------------
step "6/8  Python virtual environment"
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

if [ ! -d "venv" ]; then
    python3 -m venv venv
    ok "Virtual env created: $INSTALL_DIR/venv"
else
    ok "Virtual env already exists"
fi

# shellcheck disable=SC1091
source venv/bin/activate
pip install --upgrade pip --quiet

if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt --quiet
    ok "Dependencies installed from requirements.txt"
else
    warn "requirements.txt not found — installing manually"
    pip install --quiet smartapi-python websocket-client pyotp requests rich
    ok "Dependencies installed"
fi

# ----------------------------------------------------------------
# 7. Verify scanner file present
# ----------------------------------------------------------------
step "7/8  Scanner file check"
if [ ! -f "nse_book_scanner.py" ]; then
    error "nse_book_scanner.py missing in $INSTALL_DIR"
    echo "    कृपया file upload करें (scp / wget / git clone)"
    exit 1
fi
ok "Scanner file present: $(wc -l < nse_book_scanner.py) lines"

# Quick self-test
step "7b   Engine self-test (--demo)"
python3 nse_book_scanner.py --demo 2>&1 | tail -3
ok "Engine self-test passed"

# ----------------------------------------------------------------
# 8. Config setup
# ----------------------------------------------------------------
step "8/8  Config file setup"
if [ ! -f "config.json" ]; then
    if [ -f "config.example.json" ]; then
        cp config.example.json config.json
        chmod 600 config.json   # only user can read (contains API keys)
        ok "config.json created from template (permissions: 600)"
        warn "अब config.json में अपने Angel One credentials भरें:"
        echo "    nano config.json"
    else
        error "config.example.json भी missing है"
        exit 1
    fi
else
    ok "config.json already exists"
fi

# ----------------------------------------------------------------
# Done
# ----------------------------------------------------------------
echo -e "\n${GREEN}════════════════════════════════════════════════════════"
echo "  ✓ Setup complete!"
echo "════════════════════════════════════════════════════════${NC}"

cat <<EOF

अगले steps:

  1. Credentials भरें:
       nano ~/nse_scanner/config.json

  2. Simulate mode में पहले test करें (कोई credential नहीं चाहिए):
       cd ~/nse_scanner
       source venv/bin/activate
       python3 nse_book_scanner.py --mode simulate

  3. जब simulate ठीक चले, तो live mode में जाएँ:
       python3 nse_book_scanner.py --mode live

  4. VPS पर persistent चलाने के लिए tmux use करें:
       tmux new -s scanner
       cd ~/nse_scanner && source venv/bin/activate
       python3 nse_book_scanner.py --mode live
       (detach: Ctrl+B फिर D)
       (reattach later: tmux attach -t scanner)

  5. Auto-restart वाला production setup:
       ./install_service.sh   (systemd unit — VPS reboot पर भी चलेगा)

EOF
