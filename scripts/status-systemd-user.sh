#!/usr/bin/env bash
set -euo pipefail

systemctl --user --no-pager --full status \
  polybot-bot.service \
  polybot-dashboard.service
