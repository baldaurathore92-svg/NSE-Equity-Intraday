#!/bin/bash
#
# install_service.sh — Systemd service setup for auto-restart
# ============================================================
# Scanner को persistent background service बनाता है जो:
#   - VPS reboot होने पर auto-start
#   - Crash हो जाए तो auto-restart
#   - Logs को systemd journal में manage करता है
#
# Usage: ./install_service.sh
#

set -e

INSTALL_DIR="$HOME/nse_scanner"
SERVICE_NAME="nse-scanner"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

if [ ! -f "${INSTALL_DIR}/nse_book_scanner.py" ]; then
    echo "❌ Scanner not found in $INSTALL_DIR"
    echo "   पहले deploy_vps.sh चलाइए।"
    exit 1
fi

if [ ! -f "${INSTALL_DIR}/config.json" ]; then
    echo "❌ config.json not found. पहले credentials भरिए:"
    echo "   nano ${INSTALL_DIR}/config.json"
    exit 1
fi

echo "▶ Creating systemd service file..."

sudo tee "${SERVICE_FILE}" > /dev/null <<EOF
[Unit]
Description=NSE Book Dynamics Real-Time Scanner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER}
Group=${USER}
WorkingDirectory=${INSTALL_DIR}
Environment="PATH=${INSTALL_DIR}/venv/bin:/usr/local/bin:/usr/bin:/bin"

# Main command — headless (no rich UI, since systemd has no TTY)
ExecStart=${INSTALL_DIR}/venv/bin/python3 ${INSTALL_DIR}/nse_book_scanner.py --mode live --no-ui

# Restart policy
Restart=on-failure
RestartSec=10
StartLimitBurst=5
StartLimitIntervalSec=300

# Resource limits (safety)
LimitNOFILE=65536
MemoryMax=2G

# Standard output & error → journald
StandardOutput=journal
StandardError=journal
SyslogIdentifier=nse-scanner

[Install]
WantedBy=multi-user.target
EOF

echo "▶ Reloading systemd..."
sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}"

echo ""
echo "✓ Service installed: ${SERVICE_NAME}"
echo ""
echo "Commands (याद रखिए):"
echo "  sudo systemctl start ${SERVICE_NAME}       # शुरू करें"
echo "  sudo systemctl stop ${SERVICE_NAME}        # रोकें"
echo "  sudo systemctl restart ${SERVICE_NAME}     # restart"
echo "  sudo systemctl status ${SERVICE_NAME}      # status देखें"
echo "  journalctl -u ${SERVICE_NAME} -f           # live logs"
echo "  journalctl -u ${SERVICE_NAME} --since \"1 hour ago\"   # पिछले 1 घंटे के logs"
echo ""
echo "अभी start करने के लिए:"
echo "  sudo systemctl start ${SERVICE_NAME}"
echo "  journalctl -u ${SERVICE_NAME} -f"
