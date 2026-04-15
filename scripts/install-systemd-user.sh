#!/usr/bin/env bash
set -euo pipefail

WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-}"

if [[ -z "${PYTHON_BIN}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  else
    echo "No python interpreter found (python3/python)." >&2
    exit 1
  fi
fi

if [[ ! -f "${WORKDIR}/.env" ]]; then
  echo "Missing ${WORKDIR}/.env" >&2
  echo "Create it first, e.g.: cp envs/live-template.env .env" >&2
  exit 1
fi

mkdir -p "${WORKDIR}/logs"
mkdir -p "${HOME}/.config/systemd/user"

install_unit() {
  local template="$1"
  local target="$2"
  sed \
    -e "s|__WORKDIR__|${WORKDIR}|g" \
    -e "s|__PYTHON__|${PYTHON_BIN}|g" \
    "${template}" > "${target}"
}

install_unit \
  "${WORKDIR}/deploy/systemd/polybot-bot.service.template" \
  "${HOME}/.config/systemd/user/polybot-bot.service"

install_unit \
  "${WORKDIR}/deploy/systemd/polybot-dashboard.service.template" \
  "${HOME}/.config/systemd/user/polybot-dashboard.service"

systemctl --user daemon-reload
systemctl --user enable --now polybot-bot.service
systemctl --user enable --now polybot-dashboard.service

echo ""
echo "Services installed and started."
echo "Note: units do not use systemd EnvironmentFile; bot and dashboard read __WORKDIR__/.env in Python."
echo "Check status:"
echo "  systemctl --user status polybot-bot.service"
echo "  systemctl --user status polybot-dashboard.service"
echo ""
echo "Follow logs:"
echo "  journalctl --user -u polybot-bot.service -f"
echo "  journalctl --user -u polybot-dashboard.service -f"
echo ""
echo "For true 24/7 without login session:"
echo "  sudo loginctl enable-linger ${USER}"
