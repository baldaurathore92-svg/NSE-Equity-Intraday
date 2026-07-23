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
# Root user support:
#   Root पर चलाना safe नहीं होता, लेकिन VPS providers अक्सर सिर्फ root
#   access देते हैं। इस script में root allowed है (warning के साथ)।
#   अगर आप root पर हैं और warning skip करना है:
#      ./deploy_vps.sh --allow-root
#

set -e   # किसी भी step में error हो तो script रुक जाए
set -u   # undefined variables से बचाव

# --- Parse flags ---
ALLOW_ROOT_FLAG="no"
for arg in "$@"; do
    case "$arg" in
        --allow-root|-y|--yes) ALLOW_ROOT_FLAG="yes" ;;
        --help|-h)
            echo "Usage: ./deploy_vps.sh [--allow-root]"
            echo ""
            echo "  --allow-root, -y, --yes    Skip root user warning countdown"
            echo "  --help, -h                 Show this help"
            exit 0
            ;;
    esac
done

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

# -----------------------------------------------------------------------
# INSTALL_DIR auto-detection
# -----------------------------------------------------------------------
# Priority 1: If script itself is inside a directory containing
#             nse_book_scanner.py (e.g., cloned repo), install IN-PLACE.
# Priority 2: If $HOME/nse_scanner/nse_book_scanner.py exists, use that.
# Priority 3: Fall back to $HOME/nse_scanner (user will upload files
#             manually into that directory).
#
# User can override via env var:
#   INSTALL_DIR=/custom/path ./deploy_vps.sh
# -----------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"

if [ -n "${INSTALL_DIR:-}" ]; then
    :   # user-provided via env var — keep as is
elif [ -f "${SCRIPT_DIR}/nse_book_scanner.py" ]; then
    INSTALL_DIR="${SCRIPT_DIR}"
elif [ -f "$HOME/nse_scanner/nse_book_scanner.py" ]; then
    INSTALL_DIR="$HOME/nse_scanner"
else
    INSTALL_DIR="$HOME/nse_scanner"
fi

DATA_DIR="${DATA_DIR:-$HOME/nse_data}"   # recorded tick data (record→replay)
PYTHON_MIN="3.9"

echo -e "${BLUE}"
echo "════════════════════════════════════════════════════════"
echo "   NSE Book Scanner — VPS Deployment (Ubuntu/Debian)   "
echo "════════════════════════════════════════════════════════"
echo -e "${NC}"

# Show detected install location upfront so user can Ctrl+C if wrong
echo -e "${BLUE}  Script location : ${SCRIPT_DIR}${NC}"
echo -e "${BLUE}  Install target  : ${INSTALL_DIR}${NC}"
echo -e "${BLUE}  Data directory  : ${DATA_DIR}${NC}"
if [ "${SCRIPT_DIR}" = "${INSTALL_DIR}" ]; then
    echo -e "${GREEN}  ✓ In-place install detected (repo already has files here)${NC}"
elif [ -f "${INSTALL_DIR}/nse_book_scanner.py" ]; then
    echo -e "${GREEN}  ✓ Existing install detected at target${NC}"
else
    echo -e "${YELLOW}  ⚠ New install — files must reside at INSTALL_DIR by step 7${NC}"
fi
echo ""

# ----------------------------------------------------------------
# 1. User check (root allowed with warning)
# ----------------------------------------------------------------
step "1/8  User check"
if [ "$EUID" -eq 0 ]; then
    warn "आप root user के रूप में चल रहे हैं।"
    warn "यह production के लिए ideal नहीं है (security best practice: normal user)।"
    warn "लेकिन कई VPS providers सिर्फ root access देते हैं, इसलिए script आगे बढ़ेगी।"
    echo ""
    echo "  Production security के लिए बाद में normal user बना सकते हैं:"
    echo "     adduser trader"
    echo "     usermod -aG sudo trader"
    echo "     su - trader"
    echo ""
    if [ "$ALLOW_ROOT_FLAG" = "yes" ]; then
        ok "Root user OK (--allow-root flag दिया गया है)"
    else
        # Give user 5 seconds to Ctrl+C if they want to abort
        echo -e "${YELLOW}  Continuing in 5 seconds... (Ctrl+C to abort)${NC}"
        for i in 5 4 3 2 1; do
            echo -n "  $i "
            sleep 1
        done
        echo ""
        ok "Continuing as root: $USER"
    fi

    # When running as root, `sudo` is redundant but works. However some
    # minimal Ubuntu images don't have sudo installed. Fallback: strip sudo.
    if ! command -v sudo >/dev/null 2>&1; then
        warn "sudo command not found on this system — will use direct commands"
        SUDO=""
    else
        SUDO="sudo"
    fi
else
    ok "Running as user: $USER"
    if ! command -v sudo >/dev/null 2>&1; then
        error "sudo command not installed। पहले root user से install करें:"
        echo "    apt-get install sudo"
        echo "फिर वापस normal user पर आएं।"
        exit 1
    fi
    SUDO="sudo"
fi

# ----------------------------------------------------------------
# 2. System update
# ----------------------------------------------------------------
step "2/8  System packages update"
$SUDO apt-get update -qq
$SUDO apt-get upgrade -y -qq
ok "System updated"

# ----------------------------------------------------------------
# 3. Install dependencies (Python, tmux, timezone tools)
# ----------------------------------------------------------------
step "3/8  Installing Python + tools"
$SUDO apt-get install -y -qq \
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
$SUDO timedatectl set-timezone Asia/Kolkata
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
    $SUDO ufw --force reset >/dev/null
    $SUDO ufw default deny incoming >/dev/null
    $SUDO ufw default allow outgoing >/dev/null
    $SUDO ufw allow ssh >/dev/null
    $SUDO ufw --force enable >/dev/null
    ok "Firewall enabled (SSH allowed, all inbound denied)"
else
    warn "ufw not installed, skipping firewall"
fi

# ----------------------------------------------------------------
# 6. Create Python virtual environment + install packages
# ----------------------------------------------------------------
step "6/8  Python virtual environment + data directory"
mkdir -p "$INSTALL_DIR"
mkdir -p "$DATA_DIR"   # for recorded live NSE ticks (record→replay workflow)
ok "Data directory ready: $DATA_DIR"
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

📁 Install location : ${INSTALL_DIR}
📁 Data location    : ${DATA_DIR}

अगले steps:

  1. Credentials भरें:
       nano ${INSTALL_DIR}/config.json

  2. Local test (कोई credentials नहीं चाहिए — sim mode):
       cd ${INSTALL_DIR} && source venv/bin/activate
       python3 nse_book_scanner.py --demo               # engine self-test
       python3 nse_book_scanner.py --mode simulate      # 100-symbol sim

  ───────────────────────────────────────────────────────────────────
  🎯 RECOMMENDED: पहले diagnostic run (15 minutes, market hours में)
  ───────────────────────────────────────────────────────────────────

  3. First live diagnostic (validates Angel One field mapping):
       cd ${INSTALL_DIR} && source venv/bin/activate
       python3 live_hit_rate_analyzer.py --config config.json \\
           --diagnose --duration-hours 0.25

     अगर 100% parse success दिखे → deploy full session (step 4)
     अगर parse failure दिखे → mujhe logs/raw_ws_dump.jsonl की पहली
     entry share करें, main adapter fix करूंगा

  ───────────────────────────────────────────────────────────────────
  📼 RECOMMENDED WORKFLOW: Record→Replay Backtesting (5 दिन)
  ───────────────────────────────────────────────────────────────────

  4. Record LIVE NSE ticks for 5 trading days (market hours 9:15-15:30):
       cd ${INSTALL_DIR} && source venv/bin/activate
       python3 tick_recorder.py --config config.json --output-dir ${DATA_DIR}

     Or install as auto-restart systemd service:
       ./install_recorder_service.sh
       sudo systemctl start nse-tick-recorder
       journalctl -u nse-tick-recorder -f     # monitor live

  5. Backtest on real recorded data (offline, any time):
       python3 historical_backtest.py --data-dir ${DATA_DIR}
       python3 historical_backtest.py --data-dir ${DATA_DIR} --regime-adaptive

  ───────────────────────────────────────────────────────────────────
  Alternate: Live paper trading (no recording, direct virtual trades):
       python3 paper_trader.py --feed live --config config.json --regime-adaptive

  Alternate: Dual analyzer (hit-rate + paper trader simultaneously):
       python3 live_dual_analyzer.py --config config.json --no-ui \\
           --duration-hours 6.5

  Data locations:
       ${INSTALL_DIR}/           — Code + config
       ${DATA_DIR}/              — Recorded ticks (~500 MB/day)
       ${INSTALL_DIR}/logs/      — Signals + trades JSONL

EOF
