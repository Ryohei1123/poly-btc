#!/usr/bin/env bash
set -euo pipefail

SERVICES=(
  "polybot-bot.service"
  "polybot-dashboard.service"
)

echo "Starting PolyBot services..."
systemctl --user start "${SERVICES[@]}"

echo ""
echo "Current service states:"
for svc in "${SERVICES[@]}"; do
  state="$(systemctl --user is-active "${svc}" || true)"
  echo "  ${svc}: ${state}"
done

echo ""
echo "Tail logs:"
echo "  journalctl --user -u polybot-bot.service -f"
echo "  journalctl --user -u polybot-dashboard.service -f"
