#!/usr/bin/env bash
set -euo pipefail

SERVICES=(
  "polybot-bot.service"
  "polybot-dashboard.service"
)

echo "Stopping PolyBot services..."
systemctl --user stop "${SERVICES[@]}"

echo ""
echo "Current service states:"
for svc in "${SERVICES[@]}"; do
  state="$(systemctl --user is-active "${svc}" || true)"
  echo "  ${svc}: ${state}"
done

echo ""
echo "Stopped. To start again:"
echo "  bash scripts/start-systemd-user.sh"
