#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="$ROOT_DIR/.run"
LOG_DIR="$ROOT_DIR/logs"

mkdir -p "$RUN_DIR" "$LOG_DIR"
cd "$ROOT_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON="$ROOT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="$(command -v python3)"
else
  echo "ERROR: python3 not found."
  exit 1
fi

start_proc() {
  local name="$1"
  local script="$2"
  local pid_file="$RUN_DIR/${name}.pid"
  local log_file="$LOG_DIR/${name}.log"

  if [[ -f "$pid_file" ]]; then
    local old_pid
    old_pid="$(<"$pid_file")"
    if kill -0 "$old_pid" 2>/dev/null; then
      echo "$name already running (PID $old_pid)."
      return 0
    fi
    rm -f "$pid_file"
  fi

  echo "Starting $name..."
  nohup "$PYTHON" "$script" >>"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" >"$pid_file"
  echo "  - started PID $pid (log: $log_file)"
}

start_proc "polybot-bot" "market_maker.py"
start_proc "polybot-dashboard" "server.py"

echo
echo "Dashboard: http://38.60.209.128:5050/"
echo "Stop with: ./stop-bot.sh"
