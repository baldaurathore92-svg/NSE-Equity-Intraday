#!/bin/bash
#
# install_recorder_service.sh — Systemd service for tick_recorder.py
# ============================================================
# Scanner के record→replay workflow के लिए tick_recorder को persistent
# background service बनाता है:
#   - VPS reboot होने पर auto-start
#   - Crash हो जाए तो auto-restart (30 sec बाद)
#   - Logs को systemd journal में manage
#   - Recorder खुद ही market close (15:30 IST) पर graceful exit करता है,
#     फिर अगले दिन restart हो जाता है (Restart=always)
#
# Usage: ./install_recorder_service.sh
#

set -e

# --- sudo fallback (works when running as root without sudo installed) ---
if [ "$EUID" -eq 0 ] && ! command -v sudo >/dev/null 2>&1; then
    SUDO=""
else
    SUDO="sudo"
fi

# --- Detect install directory (prefer $HOME/nse_scanner, fall back to script dir) ---
if [ -f "$HOME/nse_scanner/tick_recorder.py" ]; then
    INSTALL_DIR="$HOME/nse_scanner"
elif [ -f "$(dirname "$(readlink -f "$0")")/tick_recorder.py" ]; then
    INSTALL_DIR="$(dirname "$(readlink -f "$0")")"
else
    INSTALL_DIR="$HOME/nse_scanner"
fi

DATA_DIR="${DATA_DIR:-$HOME/nse_data}"
SERVICE_NAME="nse-tick-recorder"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

if [ ! -f "${INSTALL_DIR}/tick_recorder.py" ]; then
    echo "❌ tick_recorder.py not found in $INSTALL_DIR"
    echo "   पहले deploy_vps.sh चलाइए, या git pull करके latest code खींचिए।"
    exit 1
fi

if [ ! -f "${INSTALL_DIR}/config.json" ]; then
    echo "❌ config.json not found. पहले Angel One credentials भरिए:"
    echo "   nano ${INSTALL_DIR}/config.json"
    exit 1
fi

mkdir -p "$DATA_DIR"
echo "▶ Data directory ready: $DATA_DIR"

echo "▶ Creating systemd service: ${SERVICE_NAME}"

$SUDO tee "${SERVICE_FILE}" > /dev/null <<EOF
[Unit]
Description=NSE Tick Recorder — captures live Angel One SnapQuote to gzip JSONL
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER}
Group=${USER}
WorkingDirectory=${INSTALL_DIR}
Environment="PATH=${INSTALL_DIR}/venv/bin:/usr/local/bin:/usr/bin:/bin"

# Main command — recorder auto-stops at market close (15:30 IST)
ExecStart=${INSTALL_DIR}/venv/bin/python3 ${INSTALL_DIR}/tick_recorder.py \\
    --config ${INSTALL_DIR}/config.json \\
    --output-dir ${DATA_DIR}

# Restart policy:
#   - Recorder auto-exits cleanly at market close → systemd re-launches
#   - It will start, notice off-hours, and gracefully stop → re-launched again
#   - Effectively: idles when market closed, records when open
#   - Use `on-failure` NOT `always` — this respects clean exits with code 0
#     during weekends/off-hours and doesn't spam the log
Restart=always
RestartSec=60
StartLimitBurst=10
StartLimitIntervalSec=3600

# Resource limits
LimitNOFILE=65536
MemoryMax=1G

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=nse-tick-recorder

[Install]
WantedBy=multi-user.target
EOF

echo "▶ Reloading systemd + enabling service…"
$SUDO systemctl daemon-reload
$SUDO systemctl enable "${SERVICE_NAME}"

echo ""
echo "✓ Service installed: ${SERVICE_NAME}"
echo ""
echo "Commands to remember:"
echo "  sudo systemctl start ${SERVICE_NAME}       # शुरू करें"
echo "  sudo systemctl stop ${SERVICE_NAME}        # रोकें"
echo "  sudo systemctl restart ${SERVICE_NAME}     # restart"
echo "  sudo systemctl status ${SERVICE_NAME}      # status"
echo "  journalctl -u ${SERVICE_NAME} -f           # live logs"
echo "  journalctl -u ${SERVICE_NAME} --since \"1 hour ago\""
echo ""
echo "Recorded data will accumulate in: ${DATA_DIR}"
echo "  ls ${DATA_DIR}                             # daily directories"
echo "  du -sh ${DATA_DIR}                         # total disk usage"
echo ""
echo "अभी start करने के लिए:"
echo "  sudo systemctl start ${SERVICE_NAME}"
echo "  journalctl -u ${SERVICE_NAME} -f"
echo ""
echo "5 days के बाद, offline backtest करें:"
echo "  cd ${INSTALL_DIR} && source venv/bin/activate"
echo "  python3 historical_backtest.py --data-dir ${DATA_DIR} --regime-adaptive"
