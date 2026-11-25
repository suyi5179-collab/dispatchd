#!/usr/bin/env bash
# Start the stack and the Vite dev server if needed, run the Playwright
# suite, and stop Vite again if this script started it (by PID).
set -euo pipefail
cd "$(dirname "$0")/.."
DC=${DC:-$(command -v docker-compose || echo "docker compose")}
$DC -p dispatchd up -d --build --wait >/dev/null 2>&1
started=0
if ! { [ -f .runtime/vite.pid ] && kill -0 "$(cat .runtime/vite.pid)" 2>/dev/null; }; then
  ./scripts/ui.sh start >/dev/null
  started=1
fi
trap '[ $started = 1 ] && ./scripts/ui.sh stop >/dev/null' EXIT
[ -d node_modules ] || npm ci --no-audit --no-fund >/dev/null
npx playwright test -c e2e/playwright.config.ts "$@"
