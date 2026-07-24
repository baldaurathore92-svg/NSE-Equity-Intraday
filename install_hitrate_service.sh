#!/bin/bash
#
# install_hitrate_service.sh — systemd service for live_hit_rate_analyzer.py
# ============================================================
# Auto-start hit rate analyzer during market hours, auto-restart on crash.
#
# Usage: ./install_hitrate_service.sh
#

set -e

# --- sudo fallback (works when running as root without sudo installed) ---
if [ "$EUID" -eq 0 ] && ! command -v sudo >/dev/null 2>&1; then
    SUDO=""
else
    SUDO="sudo"
fi

# --- Detect install directory ---
# Prefer $HOME/nse_scanner (legacy default), fall back to script's
# own directory (works when running from cloned repo like NSE-Equity-Intraday/)
if [ -f "$HOME/nse_scanner/live_hit_rate_analyzer.py" ]; then
    INSTALL_DIR="$HOME/nse_scanner"
elif [ -f "$(dirname "$(readlink -f "$0")")/live_hit_rate_analyzer.py" ]; then
    INSTALL_DIR="$(dirname "$(readlink -f "$0")")"
else
    INSTALL_DIR="$HOME/nse_scanner"   # will fail check below with proper message
fi

LOG_DIR="$INSTALL_DIR/logs"
SERVICE_NAME="nse-hitrate-analyzer"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

if [ ! -f "${INSTALL_DIR}/live_hit_rate_analyzer.py" ]; then
    echo "❌ live_hit_rate_analyzer.py not found in $INSTALL_DIR"
    echo "   पहले bash SETUP.sh --setup-only चलाइए, फिर यह script।"
    echo "   Or manually: cd <repo>; ./install_hitrate_service.sh"
    exit 1
fi

if [ ! -f "${INSTALL_DIR}/config.json" ]; then
    echo "❌ config.json not found. पहले credentials भरिए:"
    echo "   nano ${INSTALL_DIR}/config.json"
    exit 1
fi

mkdir -p "$LOG_DIR"
echo "▶ Log directory ready: $LOG_DIR"

echo "▶ Creating systemd service: ${SERVICE_NAME}"

$SUDO tee "${SERVICE_FILE}" > /dev/null <<EOF
[Unit]
Description=NSE Live Hit Rate Analyzer — virtual trades on real Angel One data
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER}
Group=${USER}
WorkingDirectory=${INSTALL_DIR}
Environment="PATH=${INSTALL_DIR}/venv/bin:/usr/local/bin:/usr/bin:/bin"

# Full trading day headless (auto-stops at 15:30 IST daily)
ExecStart=${INSTALL_DIR}/venv/bin/python3 ${INSTALL_DIR}/live_hit_rate_analyzer.py \\
    --config ${INSTALL_DIR}/config.json \\
    --duration-hours 6.5 \\
    --no-ui \\
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

echo "▶ Reloading systemd…"
$SUDO systemctl daemon-reload
$SUDO systemctl enable "${SERVICE_NAME}"

echo ""
echo "✓ Service installed: ${SERVICE_NAME}"
echo ""
echo "Commands:"
echo "  sudo systemctl start ${SERVICE_NAME}       # शुरू करें"
echo "  sudo systemctl stop ${SERVICE_NAME}        # रोकें"
echo "  sudo systemctl status ${SERVICE_NAME}      # status"
echo "  journalctl -u ${SERVICE_NAME} -f           # live logs"
echo ""
echo "Output files (हर trading day के बाद):"
echo "  ${LOG_DIR}/hit_rate_predictions.jsonl     — audit trail (every prediction)"
echo "  ${LOG_DIR}/hit_rate_report.txt             — comprehensive EOD report"
echo ""
echo "अभी start करने के लिए:"
echo "  sudo systemctl start ${SERVICE_NAME}"
echo "  journalctl -u ${SERVICE_NAME} -f"
