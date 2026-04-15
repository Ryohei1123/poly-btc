#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="$ROOT_DIR/.run"
cd "$ROOT_DIR"

stop_proc() {
  local name="$1"
  local script="$2"
  local pid_file="$RUN_DIR/${name}.pid"

  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(<"$pid_file")"
    if kill -0 "$pid" 2>/dev/null; then
      echo "Stopping $name (PID $pid)..."
      kill "$pid" 2>/dev/null || true
      sleep 1
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
      fi
      echo "  - stopped"
    else
      echo "$name not running (stale pid file)."
    fi
    rm -f "$pid_file"
    return 0
  fi

  if pgrep -f "$script" >/dev/null 2>&1; then
    echo "Stopping $name by script match..."
    pkill -f "$script" || true
    echo "  - stopped"
  else
    echo "$name already stopped."
  fi
}

stop_proc "polybot-bot" "market_maker.py"
stop_proc "polybot-dashboard" "server.py"
