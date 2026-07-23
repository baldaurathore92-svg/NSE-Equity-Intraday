#!/bin/bash
#
# run_hitrate.sh — Foolproof one-command runner for live_hit_rate_analyzer.py
# ==========================================================================
# This script AUTO-FIXES common deployment problems and just runs the
# hit rate analyzer. No matter where you are, no matter what state the
# VPS is in, this will figure it out.
#
# It handles:
#   1. Finding the code directory (searches known locations)
#   2. Creating Python venv if missing
#   3. Installing dependencies if missing
#   4. Checking config.json exists + credentials filled
#   5. Running live_hit_rate_analyzer.py with sensible defaults
#
# USAGE
#   ./run_hitrate.sh                        # 15-min diagnostic run (default)
#   ./run_hitrate.sh --full                 # 6.5-hour full trading session
#   ./run_hitrate.sh --duration 1.5         # custom duration in hours
#   ./run_hitrate.sh --no-diagnose          # skip diagnostic mode
#   ./run_hitrate.sh -- <extra args>        # everything after -- goes to analyzer
#
# EXAMPLES
#   ./run_hitrate.sh                        # first-time diagnostic
#   ./run_hitrate.sh --full                 # full day
#   ./run_hitrate.sh --full -- --min-rvol 1.5 --session-filter
#

set -e

# ------------------------------------------------------------------
# Colors
# ------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

step()  { echo -e "\n${BLUE}▶ $1${NC}"; }
ok()    { echo -e "${GREEN}  ✓ $1${NC}"; }
warn()  { echo -e "${YELLOW}  ⚠ $1${NC}"; }
error() { echo -e "${RED}  ✗ $1${NC}"; }
info()  { echo -e "${CYAN}  → $1${NC}"; }

# ------------------------------------------------------------------
# Parse CLI args
# ------------------------------------------------------------------
DURATION_HOURS="0.25"     # 15 minutes default (diagnostic)
USE_DIAGNOSE="yes"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --full)
            DURATION_HOURS="6.5"
            USE_DIAGNOSE="no"
            shift
            ;;
        --duration)
            DURATION_HOURS="$2"
            shift 2
            ;;
        --duration=*)
            DURATION_HOURS="${1#*=}"
            shift
            ;;
        --no-diagnose)
            USE_DIAGNOSE="no"
            shift
            ;;
        --diagnose)
            USE_DIAGNOSE="yes"
            shift
            ;;
        --help|-h)
            cat <<'HELPEOF'
run_hitrate.sh — Foolproof one-command runner for live_hit_rate_analyzer.py

This script AUTO-FIXES common deployment problems and just runs the
hit rate analyzer. No matter where you are, no matter what state the
VPS is in, this will figure it out.

It handles:
  1. Finding the code directory (searches known locations)
  2. Creating Python venv if missing
  3. Installing dependencies if missing
  4. Checking config.json exists + credentials filled
  5. Running live_hit_rate_analyzer.py with sensible defaults

USAGE
  ./run_hitrate.sh                     15-min diagnostic run (default, safe)
  ./run_hitrate.sh --full              6.5-hour full trading session
  ./run_hitrate.sh --duration 1.5      Custom duration in hours
  ./run_hitrate.sh --no-diagnose       Skip diagnostic mode
  ./run_hitrate.sh -- <extra args>     Everything after -- goes to analyzer

EXAMPLES
  ./run_hitrate.sh                                       # first diagnostic
  ./run_hitrate.sh --full                                # full day
  ./run_hitrate.sh --full -- --min-rvol 1.5 --session-filter
HELPEOF
            exit 0
            ;;
        --)
            shift
            EXTRA_ARGS=("$@")
            break
            ;;
        *)
            error "Unknown flag: $1"
            echo "Run './run_hitrate.sh --help' for usage"
            exit 2
            ;;
    esac
done

# ------------------------------------------------------------------
# Banner
# ------------------------------------------------------------------
echo -e "${BLUE}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   NSE Live Hit Rate Analyzer — Auto-Setup Runner        ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ------------------------------------------------------------------
# 1. Find code directory (where live_hit_rate_analyzer.py lives)
# ------------------------------------------------------------------
step "1/6  Finding code directory"

CANDIDATES=(
    "$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"   # script's own dir
    "$(pwd)"                                             # current dir
    "/root/NSE-Equity-Intraday"
    "$HOME/NSE-Equity-Intraday"
    "/root/nse_scanner"
    "$HOME/nse_scanner"
    "/opt/nse-scanner"
)

CODE_DIR=""
for candidate in "${CANDIDATES[@]}"; do
    if [ -f "$candidate/live_hit_rate_analyzer.py" ] && \
       [ -f "$candidate/nse_book_scanner.py" ] && \
       [ -f "$candidate/paper_trader.py" ]; then
        CODE_DIR="$candidate"
        ok "Found code at: $CODE_DIR"
        break
    fi
done

if [ -z "$CODE_DIR" ]; then
    error "Cannot find live_hit_rate_analyzer.py + nse_book_scanner.py"
    echo ""
    echo "Searched these locations:"
    for c in "${CANDIDATES[@]}"; do
        echo "    $c"
    done
    echo ""
    echo "Fix: git clone the repo first:"
    echo "    git clone https://github.com/baldaurathore92-svg/NSE-Equity-Intraday.git ~/NSE-Equity-Intraday"
    echo "    cd ~/NSE-Equity-Intraday"
    echo "    ./run_hitrate.sh"
    exit 3
fi

# From now on, everything happens in CODE_DIR
cd "$CODE_DIR"

# ------------------------------------------------------------------
# 2. System deps (python3-venv is needed to create venv)
# ------------------------------------------------------------------
step "2/6  System Python check"

if ! command -v python3 >/dev/null 2>&1; then
    error "python3 not installed"
    if [ "$EUID" -eq 0 ]; then
        info "Installing python3 + python3-venv..."
        apt-get update -qq && apt-get install -y -qq python3 python3-pip python3-venv
    else
        echo "  Run: sudo apt install python3 python3-pip python3-venv"
        exit 4
    fi
fi
PYVER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
ok "Python $PYVER installed"

# Check if venv module works (python3-venv package needed on Debian/Ubuntu)
if ! python3 -c "import venv" 2>/dev/null; then
    warn "python3-venv module missing"
    if [ "$EUID" -eq 0 ]; then
        info "Installing python3-venv + python3-full..."
        apt-get install -y -qq python3-venv python3-full 2>/dev/null || \
            apt-get install -y -qq python3-venv
    else
        echo "  Run: sudo apt install python3-venv"
        exit 4
    fi
fi

# ------------------------------------------------------------------
# 3. Virtual environment
# ------------------------------------------------------------------
step "3/6  Virtual environment"

VENV_DIR="$CODE_DIR/venv"

# If venv exists but python3 is broken (e.g., moved system upgrade), rebuild
NEED_VENV="no"
if [ ! -d "$VENV_DIR" ]; then
    NEED_VENV="yes"
    info "venv doesn't exist — creating fresh"
elif [ ! -f "$VENV_DIR/bin/python3" ] && [ ! -f "$VENV_DIR/bin/python" ]; then
    NEED_VENV="yes"
    warn "venv exists but python binary missing — rebuilding"
    rm -rf "$VENV_DIR"
elif ! "$VENV_DIR/bin/python3" -c "print('ok')" >/dev/null 2>&1; then
    NEED_VENV="yes"
    warn "venv python is broken — rebuilding"
    rm -rf "$VENV_DIR"
fi

if [ "$NEED_VENV" = "yes" ]; then
    python3 -m venv "$VENV_DIR"
    ok "Created venv: $VENV_DIR"
else
    ok "Using existing venv: $VENV_DIR"
fi

# Activate for rest of script
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

# ------------------------------------------------------------------
# 4. Dependencies
# ------------------------------------------------------------------
step "4/6  Python dependencies"

# The critical import checks (matches what nse_book_scanner.py does at line 1557)
check_imports() {
    python3 -c "from SmartApi import SmartConnect" 2>&1
}
check_pyotp() {
    python3 -c "import pyotp" 2>&1
}
check_rich() {
    python3 -c "import rich" 2>&1
}

# Full install path if anything is missing
install_all_deps() {
    info "Installing/upgrading dependencies (may take 2-3 minutes on first run)..."
    pip install --upgrade pip --quiet 2>&1 | tail -1
    if [ -f "$CODE_DIR/requirements.txt" ]; then
        pip install -r "$CODE_DIR/requirements.txt" 2>&1 | tail -5
    else
        pip install smartapi-python websocket-client pyotp requests rich 2>&1 | tail -5
    fi
}

NEEDS_INSTALL="no"
for check in "check_imports" "check_pyotp" "check_rich"; do
    if ! $check >/dev/null 2>&1; then
        NEEDS_INSTALL="yes"
        break
    fi
done

if [ "$NEEDS_INSTALL" = "yes" ]; then
    install_all_deps
    ok "Dependencies installed"
else
    ok "All dependencies already installed"
fi

# -- FINAL VERIFICATION (with VERBOSE error output on failure) --
FINAL_ERR="$(check_imports)"
if [ "$(check_imports >/dev/null 2>&1; echo $?)" != "0" ]; then
    warn "SmartApi import still failing. Exact error:"
    echo "$FINAL_ERR" | sed 's|^|      |'
    echo ""
    info "Attempt 1: force-reinstall smartapi-python + pyotp..."
    pip install --force-reinstall --no-cache-dir smartapi-python pyotp 2>&1 | tail -5

    FINAL_ERR="$(check_imports)"
    if [ "$(check_imports >/dev/null 2>&1; echo $?)" != "0" ]; then
        warn "Still failing. Exact error:"
        echo "$FINAL_ERR" | sed 's|^|      |'
        echo ""

        # Common cause on Python 3.12+: smartapi-python needs latest version
        # OR some transitive dep needs pinning
        info "Attempt 2: install known-good deps (may fix Python 3.12+ issues)..."
        pip install --upgrade --no-cache-dir \
            'smartapi-python>=1.5.0' \
            'websocket-client>=1.6.0' \
            'pyotp>=2.9.0' \
            'requests>=2.31.0' \
            'logzero' \
            'pandas' 2>&1 | tail -5

        FINAL_ERR="$(check_imports)"
        if [ "$(check_imports >/dev/null 2>&1; echo $?)" != "0" ]; then
            error "SmartApi import STILL failing after all attempts:"
            echo "$FINAL_ERR" | sed 's|^|      |'
            echo ""
            echo "  DIAGNOSTIC — please share this output with the developer:"
            echo "  ─────────────────────────────────────────────────"
            python3 --version
            echo ""
            echo "  Installed packages:"
            pip list 2>/dev/null | grep -iE "smart|pyotp|websocket|logzero|pandas" | sed 's|^|    |'
            echo ""
            echo "  Python module search path:"
            python3 -c "import sys; [print(f'    {p}') for p in sys.path]"
            echo "  ─────────────────────────────────────────────────"
            exit 6
        fi
    fi
fi
ok "SmartApi import verified working"

# ------------------------------------------------------------------
# 5. Config file check
# ------------------------------------------------------------------
step "5/6  Config file check"

CONFIG_PATH="$CODE_DIR/config.json"

if [ ! -f "$CONFIG_PATH" ]; then
    # Look for it in other common locations
    OTHER_CONFIGS=(
        "/root/nse_scanner/config.json"
        "$HOME/nse_scanner/config.json"
        "/root/NSE-Equity-Intraday/config.json"
        "$HOME/NSE-Equity-Intraday/config.json"
    )
    for c in "${OTHER_CONFIGS[@]}"; do
        if [ "$c" != "$CONFIG_PATH" ] && [ -f "$c" ]; then
            info "Found config at $c — copying to $CONFIG_PATH"
            cp "$c" "$CONFIG_PATH"
            chmod 600 "$CONFIG_PATH"
            break
        fi
    done
fi

if [ ! -f "$CONFIG_PATH" ]; then
    if [ -f "$CODE_DIR/config.example.json" ]; then
        cp "$CODE_DIR/config.example.json" "$CONFIG_PATH"
        chmod 600 "$CONFIG_PATH"
        warn "config.json created from template — YOU MUST FILL CREDENTIALS"
        echo ""
        echo "  Edit करें और Angel One credentials भरें:"
        echo "    nano $CONFIG_PATH"
        echo ""
        echo "  Fill in these 4 fields:"
        echo "    api_key       (smartapi.angelbroking.com → My Apps)"
        echo "    client_code   (Angel One login ID)"
        echo "    pin           (4-digit trading MPIN)"
        echo "    totp_secret   (Google Authenticator base32 secret)"
        echo ""
        echo "  Then re-run: $0"
        exit 5
    else
        error "config.json AND config.example.json both missing"
        echo "  Run 'git pull origin main' to restore missing files"
        exit 5
    fi
fi

# Sanity-check config: are placeholders still there?
if grep -q "YOUR_API_KEY_HERE\|YOUR_CLIENT_CODE_HERE\|YOUR_4_DIGIT_MPIN\|YOUR_BASE32_TOTP_SECRET" "$CONFIG_PATH"; then
    error "config.json में placeholders अभी भी हैं!"
    echo ""
    echo "  Edit करें और सही credentials भरें:"
    echo "    nano $CONFIG_PATH"
    echo ""
    exit 5
fi

chmod 600 "$CONFIG_PATH" 2>/dev/null || true
ok "config.json found and credentials appear filled"

# ------------------------------------------------------------------
# 6. Timezone check (IST recommended for correct market-hours detection)
# ------------------------------------------------------------------
step "6/6  Timezone check"

TZ_NOW=$(date +%Z 2>/dev/null || echo "unknown")
if [ "$TZ_NOW" != "IST" ]; then
    if command -v timedatectl >/dev/null 2>&1; then
        CURRENT_TZ=$(timedatectl show -p Timezone --value 2>/dev/null || echo "unknown")
        if [ "$CURRENT_TZ" != "Asia/Kolkata" ]; then
            warn "System timezone is $CURRENT_TZ (not Asia/Kolkata)"
            if [ "$EUID" -eq 0 ]; then
                info "Setting timezone to Asia/Kolkata..."
                timedatectl set-timezone Asia/Kolkata 2>/dev/null && \
                    ok "Timezone set to IST" || \
                    warn "Could not set timezone (analyzer uses fixed IST internally, this is fine)"
            else
                warn "Run 'sudo timedatectl set-timezone Asia/Kolkata' for consistency"
                info "(Analyzer uses fixed IST internally, so this is OK for now)"
            fi
        else
            ok "Timezone: Asia/Kolkata (IST)"
        fi
    fi
else
    ok "Timezone: IST"
fi

# Show current IST time
IST_TIME=$(python3 -c "
from datetime import datetime, timezone, timedelta
now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
print(now.strftime('%a %Y-%m-%d %H:%M:%S IST'))
")
info "Current IST time: $IST_TIME"

# ------------------------------------------------------------------
# LAUNCH!
# ------------------------------------------------------------------
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✓ Setup complete — Launching live_hit_rate_analyzer.py${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Code directory   : ${CODE_DIR}"
echo -e "  Virtual env      : ${VENV_DIR}"
echo -e "  Config file      : ${CONFIG_PATH}"
echo -e "  Duration         : ${DURATION_HOURS} hours"
echo -e "  Diagnostic mode  : ${USE_DIAGNOSE}"
if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
    echo -e "  Extra args       : ${EXTRA_ARGS[*]}"
fi
echo ""

# Build command
CMD=(python3 live_hit_rate_analyzer.py --config "$CONFIG_PATH" --duration-hours "$DURATION_HOURS")
if [ "$USE_DIAGNOSE" = "yes" ]; then
    CMD+=(--diagnose)
fi
CMD+=("${EXTRA_ARGS[@]}")

info "Running: ${CMD[*]}"
echo ""

# Execute (replace this shell with the python process for clean signals)
exec "${CMD[@]}"
