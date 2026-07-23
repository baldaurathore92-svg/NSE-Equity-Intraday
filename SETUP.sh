#!/bin/bash
#
# SETUP.sh — Single-file NSE Hit Rate Analyzer setup + runner
# ============================================================
# यह एक ही file सब कुछ करती है:
#
#   1. System packages install (git, python3, venv)
#   2. GitHub से repo clone / update
#   3. Python virtual environment create
#   4. सारे Python packages install (Python 3.12 compatible)
#   5. Config file check + nano से credentials भरवाना
#   6. Timezone IST set
#   7. live_hit_rate_analyzer.py launch
#
# बस एक command:
#
#   bash SETUP.sh                    # Setup + 15-min diagnostic (default)
#   bash SETUP.sh --full             # Setup + 6.5-hour full session
#   bash SETUP.sh --setup-only       # सिर्फ setup, analyzer मत चलाओ
#

set -e

# ============================================================
# Colors
# ============================================================
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

stage() { echo -e "\n${BLUE}▶ $1${NC}"; }
ok()    { echo -e "${GREEN}  ✓ $1${NC}"; }
warn()  { echo -e "${YELLOW}  ⚠ $1${NC}"; }
err()   { echo -e "${RED}  ✗ $1${NC}"; }
info()  { echo -e "${CYAN}  → $1${NC}"; }

# ============================================================
# Parse args
# ============================================================
MODE="diagnostic"
DURATION="0.25"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --full)         MODE="full"; DURATION="6.5"; shift ;;
        --setup-only)   MODE="setup-only"; shift ;;
        --duration)     DURATION="$2"; MODE="custom"; shift 2 ;;
        --help|-h)
            cat <<HELP
SETUP.sh - One file, one command, everything works.

Usage:
  bash SETUP.sh                    Setup + 15-minute diagnostic (default)
  bash SETUP.sh --full             Setup + 6.5-hour full trading day
  bash SETUP.sh --duration N       Setup + N-hour custom run
  bash SETUP.sh --setup-only       Setup only (don't launch analyzer)
  bash SETUP.sh -- <args>          Pass extra args to analyzer
  bash SETUP.sh --help             Show this

Examples with pass-through:
  bash SETUP.sh --full -- --strong-threshold 3.5
  bash SETUP.sh --full -- --min-rvol 1.5 --session-filter
  bash SETUP.sh -- --strong-threshold 5.0 --ema-alpha 0.5
HELP
            exit 0 ;;
        --)
            # Everything after '--' passes through to python analyzer
            shift
            EXTRA_ARGS=("$@")
            break ;;
        *) err "Unknown flag: $1 (use --help)"; exit 2 ;;
    esac
done

# ============================================================
# Banner
# ============================================================
clear 2>/dev/null || true
echo -e "${BLUE}"
cat <<'BANNER'
╔══════════════════════════════════════════════════════════╗
║                                                          ║
║     NSE Hit Rate Analyzer — Complete Setup + Run        ║
║                                                          ║
║          One file. One command. Everything works.        ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝
BANNER
echo -e "${NC}"

# ============================================================
# Determine repo location (root vs user)
# ============================================================
REPO_URL="https://github.com/baldaurathore92-svg/NSE-Equity-Intraday.git"
if [ "$EUID" -eq 0 ]; then
    REPO_DIR="/root/NSE-Equity-Intraday"
    SUDO=""
else
    REPO_DIR="$HOME/NSE-Equity-Intraday"
    if command -v sudo >/dev/null 2>&1; then
        SUDO="sudo"
    else
        err "Not root and sudo not installed. Run as root or install sudo first."
        exit 3
    fi
fi

info "Repository target: $REPO_DIR"

# ============================================================
# STAGE 1: System packages
# ============================================================
stage "STAGE 1/6 — System packages"

if ! command -v apt-get >/dev/null 2>&1; then
    err "This script needs apt-get (Ubuntu/Debian). Cannot continue."
    exit 3
fi

info "Updating package lists..."
$SUDO apt-get update -qq >/dev/null 2>&1 || warn "apt-get update had warnings"

info "Installing core packages (git, python3, venv, tmux)..."
$SUDO apt-get install -y -qq \
    git python3 python3-pip python3-venv \
    tmux curl ca-certificates >/dev/null 2>&1

# python3-full is optional (Ubuntu 24.04+ has it, older releases don't)
$SUDO apt-get install -y -qq python3-full >/dev/null 2>&1 || true

# Verify essentials
for cmd in git python3; do
    if ! command -v $cmd >/dev/null 2>&1; then
        err "$cmd installation failed"
        exit 3
    fi
done

if ! python3 -c "import venv" 2>/dev/null; then
    err "python3-venv module not working after install"
    exit 3
fi

PYVER=$(python3 --version 2>&1)
ok "System packages OK — $PYVER"

# ============================================================
# STAGE 2: Repository (clone or pull)
# ============================================================
stage "STAGE 2/6 — Repository"

if [ -d "$REPO_DIR/.git" ]; then
    info "Repository exists, pulling latest from GitHub..."
    cd "$REPO_DIR"
    git pull origin main --quiet 2>&1 | tail -5 || warn "git pull had issues (continuing)"
    ok "Updated: $REPO_DIR"
else
    info "Cloning fresh from GitHub..."
    [ -d "$REPO_DIR" ] && rm -rf "$REPO_DIR"
    git clone --quiet "$REPO_URL" "$REPO_DIR"
    ok "Cloned: $REPO_DIR"
fi
cd "$REPO_DIR"

# Verify files
for f in live_hit_rate_analyzer.py nse_book_scanner.py paper_trader.py config.example.json; do
    if [ ! -f "$REPO_DIR/$f" ]; then
        err "$f missing after clone/pull"
        exit 4
    fi
done

# ============================================================
# STAGE 3: Python virtual environment
# ============================================================
stage "STAGE 3/6 — Python virtual environment"

VENV_DIR="$REPO_DIR/venv"

# Check if existing venv is healthy
if [ -d "$VENV_DIR" ]; then
    if [ -x "$VENV_DIR/bin/python3" ] && "$VENV_DIR/bin/python3" -c "print('ok')" >/dev/null 2>&1; then
        info "Existing venv healthy, keeping it"
    else
        warn "Existing venv is broken, removing..."
        rm -rf "$VENV_DIR"
    fi
fi

if [ ! -d "$VENV_DIR" ]; then
    info "Creating fresh venv..."
    # --upgrade-deps upgrades pip/setuptools inside venv (Python 3.9+)
    python3 -m venv --upgrade-deps "$VENV_DIR" 2>/dev/null || python3 -m venv "$VENV_DIR"
fi

if [ ! -f "$VENV_DIR/bin/activate" ]; then
    err "venv creation failed — bin/activate missing"
    exit 5
fi

# Activate
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
ok "venv ready: $VENV_DIR"

# ============================================================
# STAGE 4: Python packages (with Python 3.12 compat)
# ============================================================
stage "STAGE 4/6 — Python packages"

# CRITICAL for Python 3.12: upgrade pip + setuptools + wheel FIRST
# (Python 3.12 removed distutils; older packages need latest setuptools)
info "Upgrading pip + setuptools + wheel (Python 3.12 compatibility)..."
pip install --upgrade pip setuptools wheel --quiet 2>&1 | tail -1

# Function to test the exact imports the analyzer needs
verify_imports() {
    python3 - <<'PYCHECK' 2>&1
try:
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    import pyotp
    import rich
    import websocket
    print("ALL_IMPORTS_OK")
except ImportError as e:
    print(f"IMPORT_ERROR: {e}")
except Exception as e:
    print(f"OTHER_ERROR: {type(e).__name__}: {e}")
PYCHECK
}

# Attempt 1: standard install from requirements.txt
info "Installing packages (may take 2-3 min first time)..."
if [ -f "$REPO_DIR/requirements.txt" ]; then
    pip install --quiet -r "$REPO_DIR/requirements.txt" 2>&1 | tail -5
else
    pip install --quiet smartapi-python websocket-client pyotp requests rich 2>&1 | tail -5
fi

# Verify
RESULT=$(verify_imports)
if [[ "$RESULT" != *"ALL_IMPORTS_OK"* ]]; then
    warn "Initial install did not produce working imports:"
    echo "$RESULT" | sed 's|^|      |'

    # Attempt 2: force reinstall
    info "Attempt 2: force-reinstalling smartapi-python + deps..."
    pip install --force-reinstall --no-cache-dir --upgrade \
        smartapi-python pyotp websocket-client 2>&1 | tail -3

    RESULT=$(verify_imports)
    if [[ "$RESULT" != *"ALL_IMPORTS_OK"* ]]; then
        warn "Attempt 2 failed:"
        echo "$RESULT" | sed 's|^|      |'

        # Attempt 3: install common transitive deps for Python 3.12
        info "Attempt 3: installing transitive deps (logzero, pandas, numpy)..."
        pip install --upgrade --no-cache-dir \
            logzero 'pandas' 'numpy' \
            'pyotp>=2.9' 'websocket-client>=1.6' 2>&1 | tail -3

        RESULT=$(verify_imports)
    fi
fi

# Final verification
if [[ "$RESULT" != *"ALL_IMPORTS_OK"* ]]; then
    err "Python packages could not be installed after 3 attempts"
    echo ""
    echo "  Final error:"
    echo "$RESULT" | sed 's|^|      |'
    echo ""
    echo "  Diagnostic info (share with developer):"
    python3 --version
    pip list | grep -iE "smart|pyotp|websocket|rich|logzero|pandas|numpy|setuptools" | sed 's|^|    |'
    exit 6
fi
ok "All Python packages verified working"

# ============================================================
# STAGE 5: Configuration
# ============================================================
stage "STAGE 5/6 — Configuration"

CONFIG="$REPO_DIR/config.json"

# Look for existing config in known locations
if [ ! -f "$CONFIG" ]; then
    for other in \
        "/tmp/config_backup.json" \
        "/root/nse_scanner/config.json" \
        "$HOME/nse_scanner/config.json"; do
        if [ -f "$other" ] && ! grep -q "YOUR_API_KEY_HERE" "$other" 2>/dev/null; then
            info "Found filled config at $other — copying..."
            cp "$other" "$CONFIG"
            chmod 600 "$CONFIG"
            break
        fi
    done
fi

# Still missing? Create from template.
if [ ! -f "$CONFIG" ]; then
    cp "$REPO_DIR/config.example.json" "$CONFIG"
    chmod 600 "$CONFIG"
    info "Created config.json from template"
fi

# Check if credentials still have placeholders
if grep -q "YOUR_API_KEY_HERE\|YOUR_CLIENT_CODE_HERE\|YOUR_4_DIGIT_MPIN\|YOUR_BASE32_TOTP_SECRET" "$CONFIG"; then
    warn "Config में credentials भरने बाकी हैं"
    echo ""
    echo "  ══════════════════════════════════════════════════════"
    echo "  आपको 4 fields भरने हैं (nano editor खुलेगा):"
    echo "  ══════════════════════════════════════════════════════"
    echo ""
    echo "    api_key       → smartapi.angelbroking.com → My Apps"
    echo "    client_code   → Angel One login ID (जैसे A1234567)"
    echo "    pin           → 4-digit trading MPIN"
    echo "    totp_secret   → Google Authenticator base32 secret"
    echo ""
    echo "  Editor में:"
    echo "    1. Values type/paste करो (\"YOUR_...\" को अपने real value से replace)"
    echo "    2. Ctrl+O  →  Enter  (save)"
    echo "    3. Ctrl+X  (exit)"
    echo ""
    echo "  Press ENTER when ready to open nano..."
    read -r

    ${EDITOR:-nano} "$CONFIG"

    # Re-check after edit
    if grep -q "YOUR_API_KEY_HERE\|YOUR_CLIENT_CODE_HERE\|YOUR_4_DIGIT_MPIN\|YOUR_BASE32_TOTP_SECRET" "$CONFIG"; then
        err "Config में अभी भी placeholders हैं!"
        echo ""
        echo "  Fix करके फिर से चलाओ:"
        echo "    bash $0"
        exit 7
    fi
fi

chmod 600 "$CONFIG"
ok "Config ready — credentials filled"

# ============================================================
# STAGE 6: Timezone
# ============================================================
stage "STAGE 6/6 — Timezone"

if command -v timedatectl >/dev/null 2>&1; then
    CURRENT_TZ=$(timedatectl show -p Timezone --value 2>/dev/null || echo "unknown")
    if [ "$CURRENT_TZ" != "Asia/Kolkata" ]; then
        info "Setting timezone to Asia/Kolkata (IST)..."
        $SUDO timedatectl set-timezone Asia/Kolkata 2>/dev/null || \
            warn "Could not set (no problem — analyzer uses fixed IST internally)"
    fi
    ok "Timezone: Asia/Kolkata"
else
    warn "timedatectl not available (no problem — analyzer uses fixed IST internally)"
fi

IST_TIME=$(python3 -c "
from datetime import datetime, timezone, timedelta
now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
print(now.strftime('%a %Y-%m-%d %H:%M:%S IST'))
")
info "Current IST time: $IST_TIME"

# ============================================================
# LAUNCH
# ============================================================
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
if [ "$MODE" = "setup-only" ]; then
    echo -e "${GREEN}  ✓ SETUP COMPLETE${NC}"
    echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  अगली बार analyzer चलाने के लिए:"
    echo "    bash $0                    # 15-min diagnostic"
    echo "    bash $0 --full             # 6.5-hour full session"
    exit 0
fi
echo -e "${GREEN}  ✓ SETUP COMPLETE — Launching analyzer...${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""
echo "  Mode          : $MODE"
echo "  Duration      : $DURATION hours"
echo "  Config file   : $CONFIG"
echo "  Working dir   : $REPO_DIR"
echo ""

# Build launch command
CMD=(python3 live_hit_rate_analyzer.py --config "$CONFIG" --duration-hours "$DURATION")
if [ "$MODE" = "diagnostic" ]; then
    CMD+=(--diagnose)
fi
# Append any pass-through args (after '--' in CLI)
if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
    CMD+=("${EXTRA_ARGS[@]}")
fi

info "Running: ${CMD[*]}"
echo ""

# Replace shell with python (clean signal handling on Ctrl+C)
exec "${CMD[@]}"
