#!/bin/bash
#
# SETUP.sh — One file, one command, everything works.
# ============================================================================
# NSE Live Hit Rate Analyzer — unified deploy + run + service manager.
#
# सारे काम एक ही file में — इसे chmod +x करके बस चलाइए:
#
#   Everyday runs:
#     bash SETUP.sh                    Install (as needed) + 15-min diagnostic
#     bash SETUP.sh --full             Install + 6.5-hour full trading session
#     bash SETUP.sh --duration N       Install + N-hour custom run
#     bash SETUP.sh --run              Skip install, just launch analyzer
#     bash SETUP.sh --setup-only       Install only, don't launch analyzer
#     bash SETUP.sh --engine-demo      8-scenario engine self-test (no config)
#
#   Auto-start on VPS (systemd, market hours daily):
#     bash SETUP.sh --install-service  Register systemd unit + enable auto-start
#     bash SETUP.sh --service-status   Show current status
#     bash SETUP.sh --service-logs     Tail journalctl logs
#     bash SETUP.sh --service-start    systemctl start
#     bash SETUP.sh --service-stop     systemctl stop
#     bash SETUP.sh --uninstall-service Remove systemd unit
#
#   Pass-through to analyzer (baseline hardening):
#     bash SETUP.sh --full -- --strong-only --entry-confirmation-sec 15
#     bash SETUP.sh --full -- --min-rvol 1.5 --session-filter
#
#   Pass-through — recent fixes (regime gate, EMA warmup, weight overrides):
#     bash SETUP.sh --full -- --regime-gate --skip-weak
#     bash SETUP.sh --full -- --regime-invert                 # contrarian mode
#     bash SETUP.sh --full -- --ema-warmup-ticks 50           # 9:15 AM cold-start fix
#     bash SETUP.sh --full -- --w-iceberg 1.0                 # enable hidden-liquidity signal
#     bash SETUP.sh --full -- --spoof-max-delta-qty 10000     # protect large-cap trades
#     bash SETUP.sh --full -- --aggressor-window-sec 2.0      # slow-market fix
#
#   Post-hoc audit (no config / network needed):
#     bash SETUP.sh --run -- --verify-horizons logs/hit_rate_predictions.jsonl
#

set -e

# ============================================================================
# Colors + logging helpers
# ============================================================================
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

stage() { echo -e "\n${BLUE}▶ $1${NC}"; }
ok()    { echo -e "${GREEN}  ✓ $1${NC}"; }
warn()  { echo -e "${YELLOW}  ⚠ $1${NC}"; }
err()   { echo -e "${RED}  ✗ $1${NC}"; }
info()  { echo -e "${CYAN}  → $1${NC}"; }

# ============================================================================
# Parse args
# ============================================================================
# Command mode (exactly one of these is active at a time)
CMD="install-and-run"     # install-and-run | run-only | setup-only | engine-demo
                          # | install-service | uninstall-service
                          # | service-status | service-logs | service-start | service-stop

# Launch options (only used when CMD ∈ {install-and-run, run-only})
LAUNCH_MODE="diagnostic"  # diagnostic | full | custom
DURATION="0.25"

EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --full)               LAUNCH_MODE="full"; DURATION="6.5"; shift ;;
        --duration)           LAUNCH_MODE="custom"; DURATION="$2"; shift 2 ;;
        --setup-only)         CMD="setup-only"; shift ;;
        --run)                CMD="run-only"; shift ;;
        --engine-demo)        CMD="engine-demo"; shift ;;
        --install-service)    CMD="install-service"; shift ;;
        --uninstall-service)  CMD="uninstall-service"; shift ;;
        --service-status)     CMD="service-status"; shift ;;
        --service-logs)       CMD="service-logs"; shift ;;
        --service-start)      CMD="service-start"; shift ;;
        --service-stop)       CMD="service-stop"; shift ;;
        --help|-h)
            cat <<HELP
SETUP.sh — Unified deploy + run + service manager for NSE Hit Rate Analyzer

USAGE
  bash SETUP.sh [mode] [duration] [-- analyzer-args...]

MODES (default: install + 15-min diagnostic run)
  --full                Install + 6.5-hour full trading session
  --duration N          Install + N-hour custom run
  --setup-only          Install only, don't launch analyzer
  --run                 Skip install (assume env ready), just launch analyzer
  --engine-demo         Run 8-scenario engine self-test (no config needed)

  --install-service     Install systemd auto-start for VPS (runs 6.5h daily)
  --uninstall-service   Remove the systemd unit
  --service-status      systemctl status
  --service-logs        journalctl -u ... -f
  --service-start       systemctl start
  --service-stop        systemctl stop

  --help                Show this

EXAMPLES — basic
  bash SETUP.sh                               First-time install + diagnostic
  bash SETUP.sh --full                        Full trading day run
  bash SETUP.sh --run                         Launch analyzer, skip install
  bash SETUP.sh --engine-demo                 Verify engine works (no broker)
  bash SETUP.sh --install-service             Enable systemd auto-start

EXAMPLES — signal quality
  bash SETUP.sh --full -- --strong-only                    # STRONG signals only
  bash SETUP.sh --full -- --skip-weak                      # skip WEAK (noise)
  bash SETUP.sh --full -- --entry-confirmation-sec 15      # 15s sniper policy
  bash SETUP.sh --full -- --min-rvol 1.5 --session-filter  # volume + session gates

EXAMPLES — regime + contrarian (opt-in, address 'LONG loses / SHORT loses')
  bash SETUP.sh --full -- --regime-gate                    # skip RANDOM regime
  bash SETUP.sh --full -- --regime-invert                  # flip LONG/SHORT in MR

EXAMPLES — engine tuning (address user-reported concerns)
  bash SETUP.sh --full -- --ema-warmup-ticks 50            # 9:15 AM cold start fix
  bash SETUP.sh --full -- --w-iceberg 1.0                  # enable hidden-liquidity
  bash SETUP.sh --full -- --spoof-max-delta-qty 10000      # protect block trades
  bash SETUP.sh --full -- --aggressor-window-sec 2.0       # slow-market volume
  bash SETUP.sh --full -- --kill-switch-spread-mult 2.0    # tighter fast-market

EXAMPLES — combined production preset (all recommended hardening at once)
  bash SETUP.sh --full -- \\
      --strong-only \\
      --entry-confirmation-sec 15 \\
      --regime-gate \\
      --ema-warmup-ticks 50 \\
      --w-iceberg 1.0 \\
      --kill-switch-spread-mult 2.0 \\
      --min-rvol 1.2 \\
      --session-filter

EXAMPLES — post-hoc audit
  bash SETUP.sh --run -- --verify-horizons logs/hit_rate_predictions.jsonl
HELP
            exit 0 ;;
        --)
            shift; EXTRA_ARGS=("$@"); break ;;
        *)
            err "Unknown flag: $1 (use --help)"; exit 2 ;;
    esac
done

# ============================================================================
# Banner (skip for service subcommands to keep output clean)
# ============================================================================
case "$CMD" in
    service-status|service-logs|service-start|service-stop) : ;;
    *)
        clear 2>/dev/null || true
        echo -e "${BLUE}"
        cat <<'BANNER'
╔══════════════════════════════════════════════════════════╗
║                                                          ║
║     NSE Hit Rate Analyzer — Complete Setup + Run         ║
║                                                          ║
║          One file. One command. Everything works.        ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝
BANNER
        echo -e "${NC}"
        ;;
esac

# ============================================================================
# Detect repo location + root vs user privileges
# ============================================================================
REPO_URL="https://github.com/baldaurathore92-svg/NSE-Equity-Intraday.git"
if [ "$EUID" -eq 0 ]; then
    REPO_DIR="/root/NSE-Equity-Intraday"
    SUDO=""
else
    REPO_DIR="$HOME/NSE-Equity-Intraday"
    if command -v sudo >/dev/null 2>&1; then
        SUDO="sudo"
    else
        SUDO=""   # non-root without sudo — only --run / --engine-demo will work
    fi
fi

# If script is being run from inside an already-cloned repo, prefer that dir.
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
if [ -f "$SCRIPT_DIR/live_hit_rate_analyzer.py" ]; then
    REPO_DIR="$SCRIPT_DIR"
fi

# ============================================================================
# Install helper functions (used by install-and-run, setup-only, install-service)
# ============================================================================

install_system_packages() {
    stage "STAGE 1/6 — System packages"

    if ! command -v apt-get >/dev/null 2>&1; then
        err "This script needs apt-get (Ubuntu/Debian). Cannot continue."
        exit 3
    fi

    if [ -z "$SUDO" ] && [ "$EUID" -ne 0 ]; then
        err "System packages need sudo or root. Run as root or install sudo."
        exit 3
    fi

    info "Updating package lists..."
    $SUDO apt-get update -qq >/dev/null 2>&1 || warn "apt-get update had warnings"

    info "Installing core packages (git, python3, venv, tmux)..."
    $SUDO apt-get install -y -qq \
        git python3 python3-pip python3-venv \
        tmux curl ca-certificates >/dev/null 2>&1

    # python3-full is optional (Ubuntu 24.04+); older releases don't need it
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
}

fetch_or_update_repo() {
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

    for f in live_hit_rate_analyzer.py config.example.json; do
        if [ ! -f "$REPO_DIR/$f" ]; then
            err "$f missing after clone/pull"
            exit 4
        fi
    done
}

ensure_venv() {
    stage "STAGE 3/6 — Python virtual environment"

    VENV_DIR="$REPO_DIR/venv"

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

    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
    ok "venv ready: $VENV_DIR"
}

verify_python_imports() {
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

install_python_packages() {
    stage "STAGE 4/6 — Python packages"

    info "Upgrading pip + setuptools + wheel (Python 3.12 compatibility)..."
    pip install --upgrade pip setuptools wheel --quiet 2>&1 | tail -1

    info "Installing packages (may take 2-3 min first time)..."
    if [ -f "$REPO_DIR/requirements.txt" ]; then
        pip install --quiet -r "$REPO_DIR/requirements.txt" 2>&1 | tail -5
    else
        pip install --quiet smartapi-python websocket-client pyotp requests rich 2>&1 | tail -5
    fi

    RESULT=$(verify_python_imports)
    if [[ "$RESULT" != *"ALL_IMPORTS_OK"* ]]; then
        warn "Initial install did not produce working imports:"
        echo "$RESULT" | sed 's|^|      |'
        info "Attempt 2: force-reinstalling smartapi-python + deps..."
        pip install --force-reinstall --no-cache-dir --upgrade \
            smartapi-python pyotp websocket-client 2>&1 | tail -3

        RESULT=$(verify_python_imports)
        if [[ "$RESULT" != *"ALL_IMPORTS_OK"* ]]; then
            warn "Attempt 2 failed:"
            echo "$RESULT" | sed 's|^|      |'
            info "Attempt 3: installing transitive deps (logzero, pandas, numpy)..."
            pip install --upgrade --no-cache-dir \
                logzero 'pandas' 'numpy' \
                'pyotp>=2.9' 'websocket-client>=1.6' 2>&1 | tail -3
            RESULT=$(verify_python_imports)
        fi
    fi

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
}

ensure_config() {
    stage "STAGE 5/6 — Configuration"

    CONFIG="$REPO_DIR/config.json"

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

    if [ ! -f "$CONFIG" ]; then
        cp "$REPO_DIR/config.example.json" "$CONFIG"
        chmod 600 "$CONFIG"
        info "Created config.json from template"
    fi

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
}

ensure_timezone() {
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
}

# --- Lightweight variant of the above stages used by --run mode:
#     detects an existing setup, verifies the env, tops up missing pieces,
#     does NOT touch apt-get or git.
lightweight_env_check() {
    if [ ! -f "$REPO_DIR/live_hit_rate_analyzer.py" ]; then
        err "live_hit_rate_analyzer.py not found in $REPO_DIR"
        echo "  Run 'bash SETUP.sh' first for a full install."
        exit 4
    fi
    cd "$REPO_DIR"

    stage "Verifying Python environment"
    VENV_DIR="$REPO_DIR/venv"
    if [ ! -d "$VENV_DIR" ] || ! "$VENV_DIR/bin/python3" -c "print('ok')" >/dev/null 2>&1; then
        warn "venv missing or broken — running full install first"
        install_system_packages
        fetch_or_update_repo
        ensure_venv
        install_python_packages
    else
        # shellcheck disable=SC1091
        source "$VENV_DIR/bin/activate"
        RESULT=$(verify_python_imports)
        if [[ "$RESULT" != *"ALL_IMPORTS_OK"* ]]; then
            warn "Python packages incomplete — reinstalling"
            install_python_packages
        else
            ok "venv + Python packages verified"
        fi
    fi

    CONFIG="$REPO_DIR/config.json"
    if [ ! -f "$CONFIG" ] || grep -q "YOUR_API_KEY_HERE\|YOUR_CLIENT_CODE_HERE\|YOUR_4_DIGIT_MPIN\|YOUR_BASE32_TOTP_SECRET" "$CONFIG" 2>/dev/null; then
        err "config.json missing or has placeholders"
        echo "  Run 'bash SETUP.sh --setup-only' to configure credentials."
        exit 5
    fi
    ok "config.json verified"
}

# ============================================================================
# Launch analyzer (used by install-and-run, run-only)
# ============================================================================
launch_analyzer() {
    echo ""
    echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  ✓ Environment ready — launching analyzer...${NC}"
    echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  Mode          : $LAUNCH_MODE"
    echo "  Duration      : $DURATION hours"
    echo "  Config file   : $CONFIG"
    echo "  Working dir   : $REPO_DIR"
    if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
        echo "  Extra args    : ${EXTRA_ARGS[*]}"
    fi
    echo ""

    CMD=(python3 live_hit_rate_analyzer.py --config "$CONFIG" --duration-hours "$DURATION")
    if [ "$LAUNCH_MODE" = "diagnostic" ]; then
        CMD+=(--diagnose)
    fi
    if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
        CMD+=("${EXTRA_ARGS[@]}")
    fi

    info "Running: ${CMD[*]}"
    echo ""

    # Replace shell with python (clean signal handling on Ctrl+C)
    exec "${CMD[@]}"
}

# ============================================================================
# Systemd service management (previously install_hitrate_service.sh)
# ============================================================================
SERVICE_NAME="nse-hitrate-analyzer"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

install_systemd_service() {
    stage "Installing systemd service: ${SERVICE_NAME}"

    if [ ! -f "$REPO_DIR/live_hit_rate_analyzer.py" ]; then
        err "live_hit_rate_analyzer.py not found in $REPO_DIR"
        echo "  पहले 'bash SETUP.sh --setup-only' चलाइए, फिर यह command।"
        exit 1
    fi

    if [ ! -f "$REPO_DIR/config.json" ]; then
        err "config.json not found — credentials missing"
        echo "  पहले 'bash SETUP.sh --setup-only' से credentials भरिए।"
        exit 1
    fi

    LOG_DIR="$REPO_DIR/logs"
    mkdir -p "$LOG_DIR"
    ok "Log directory ready: $LOG_DIR"

    # The user that will own the service. When run as root, prefer root (no login user).
    SVC_USER="${SUDO_USER:-$USER}"
    if [ "$EUID" -eq 0 ] && [ -z "$SUDO_USER" ]; then
        SVC_USER="root"
    fi

    info "Creating systemd unit at $SERVICE_FILE (User=${SVC_USER})"

    $SUDO tee "${SERVICE_FILE}" > /dev/null <<EOF
[Unit]
Description=NSE Live Hit Rate Analyzer — virtual trades on real Angel One data
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SVC_USER}
Group=${SVC_USER}
WorkingDirectory=${REPO_DIR}
Environment="PATH=${REPO_DIR}/venv/bin:/usr/local/bin:/usr/bin:/bin"

# Full trading day headless (analyzer auto-stops at 15:30 IST daily).
# Baseline production flags below combine:
#   * 15-second sniper policy (entry confirmation + survival exit)
#   * EMA warmup (fixes 9:15 AM Cold-Start Trap)
#   * Stale-feed guard for systemd restart-on-hang
#
# NOT enabled by default (uncomment / add if your data supports it):
#   --regime-gate                 skip signals in RANDOM regime
#   --regime-invert               contrarian in MEAN_REVERTING
#   --w-iceberg 1.0               include hidden-liquidity signal
#   --spoof-max-delta-qty 10000   protect large-cap block trades
#   --kill-switch-spread-mult 2.0 tighter fast-market protection
#   --min-rvol 1.2 --session-filter  volume + session hygiene
ExecStart=${REPO_DIR}/venv/bin/python3 ${REPO_DIR}/live_hit_rate_analyzer.py \\
    --config ${REPO_DIR}/config.json \\
    --duration-hours 6.5 \\
    --no-ui \\
    --strong-only \\
    --entry-confirmation-sec 15 \\
    --entry-score 4.0 \\
    --entry-evidence 30 \\
    --survival-check-sec 15 \\
    --survival-min-favor-pct 0.0001 \\
    --ema-warmup-ticks 50 \\
    --stale-feed-sec 90 \\
    --log-path ${LOG_DIR}/hit_rate_predictions.jsonl \\
    --report-path ${LOG_DIR}/hit_rate_report.txt

Restart=always
RestartSec=60
StartLimitBurst=10
StartLimitIntervalSec=3600

LimitNOFILE=65536
MemoryMax=1G

StandardOutput=journal
StandardError=journal
SyslogIdentifier=nse-hitrate

[Install]
WantedBy=multi-user.target
EOF

    info "Reloading systemd..."
    $SUDO systemctl daemon-reload
    $SUDO systemctl enable "${SERVICE_NAME}"

    echo ""
    ok "Service installed: ${SERVICE_NAME}"
    echo ""
    echo "Commands (via this same script):"
    echo "  bash SETUP.sh --service-start   # शुरू करें"
    echo "  bash SETUP.sh --service-stop    # रोकें"
    echo "  bash SETUP.sh --service-status  # status"
    echo "  bash SETUP.sh --service-logs    # live journalctl"
    echo ""
    echo "Or use systemctl directly:"
    echo "  $SUDO systemctl start ${SERVICE_NAME}"
    echo "  $SUDO journalctl -u ${SERVICE_NAME} -f"
    echo ""
    echo "EOD output files:"
    echo "  ${LOG_DIR}/hit_rate_predictions.jsonl  — audit trail"
    echo "  ${LOG_DIR}/hit_rate_report.txt          — comprehensive EOD report"
}

uninstall_systemd_service() {
    stage "Removing systemd service: ${SERVICE_NAME}"

    if [ ! -f "$SERVICE_FILE" ]; then
        warn "Service is not installed (nothing to remove)."
        exit 0
    fi

    $SUDO systemctl stop "${SERVICE_NAME}" 2>/dev/null || true
    $SUDO systemctl disable "${SERVICE_NAME}" 2>/dev/null || true
    $SUDO rm -f "${SERVICE_FILE}"
    $SUDO systemctl daemon-reload
    ok "Service ${SERVICE_NAME} removed."
}

service_action() {
    local action="$1"
    if [ ! -f "$SERVICE_FILE" ]; then
        err "Service is not installed. Run: bash SETUP.sh --install-service"
        exit 1
    fi
    case "$action" in
        status) $SUDO systemctl status "${SERVICE_NAME}" --no-pager ;;
        logs)   $SUDO journalctl -u "${SERVICE_NAME}" -f ;;
        start)  $SUDO systemctl start "${SERVICE_NAME}" && ok "Started ${SERVICE_NAME}" ;;
        stop)   $SUDO systemctl stop  "${SERVICE_NAME}" && ok "Stopped ${SERVICE_NAME}" ;;
    esac
}

# ============================================================================
# Engine self-test (no config, no network needed)
# ============================================================================
run_engine_demo() {
    if [ ! -f "$REPO_DIR/live_hit_rate_analyzer.py" ]; then
        # Analyzer not present — do a minimal fetch + venv so demo can run.
        install_system_packages
        fetch_or_update_repo
        ensure_venv
        install_python_packages
    else
        cd "$REPO_DIR"
        VENV_DIR="$REPO_DIR/venv"
        if [ -d "$VENV_DIR" ] && [ -x "$VENV_DIR/bin/python3" ]; then
            # shellcheck disable=SC1091
            source "$VENV_DIR/bin/activate"
        fi
    fi
    exec python3 live_hit_rate_analyzer.py --engine-demo
}

# ============================================================================
# Dispatch
# ============================================================================
info "Repository target: $REPO_DIR"

case "$CMD" in
    engine-demo)
        run_engine_demo
        ;;

    install-service)
        # Install-service assumes the environment is ready. If it isn't, we do
        # a full install first so the systemd unit will actually work.
        if [ ! -f "$REPO_DIR/live_hit_rate_analyzer.py" ] || \
           [ ! -d "$REPO_DIR/venv" ] || \
           [ ! -f "$REPO_DIR/config.json" ]; then
            info "Environment not fully set up — running installer first."
            install_system_packages
            fetch_or_update_repo
            ensure_venv
            install_python_packages
            ensure_config
            ensure_timezone
        fi
        install_systemd_service
        ;;

    uninstall-service)
        uninstall_systemd_service
        ;;

    service-status) service_action status ;;
    service-logs)   service_action logs   ;;
    service-start)  service_action start  ;;
    service-stop)   service_action stop   ;;

    run-only)
        lightweight_env_check
        CONFIG="$REPO_DIR/config.json"
        launch_analyzer
        ;;

    setup-only)
        install_system_packages
        fetch_or_update_repo
        ensure_venv
        install_python_packages
        ensure_config
        ensure_timezone

        echo ""
        echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
        echo -e "${GREEN}  ✓ SETUP COMPLETE${NC}"
        echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
        echo ""
        echo "  अगली बार:"
        echo "    bash $0                    # install + 15-min diagnostic"
        echo "    bash $0 --full             # install + 6.5-hour full session"
        echo "    bash $0 --run              # skip install, just run"
        echo "    bash $0 --install-service  # enable systemd auto-start"
        exit 0
        ;;

    install-and-run|*)
        install_system_packages
        fetch_or_update_repo
        ensure_venv
        install_python_packages
        ensure_config
        ensure_timezone
        CONFIG="$REPO_DIR/config.json"
        launch_analyzer
        ;;
esac
