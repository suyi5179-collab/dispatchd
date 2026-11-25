#!/usr/bin/env bash
# Start (or stop) the Vite dev server on :5185 in the background. The PID
# goes to .runtime/vite.pid and "stop" kills exactly that PID, never by name.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime
pidf=.runtime/vite.pid
case "${1:-start}" in
  start)
    if [ -f $pidf ] && kill -0 "$(cat $pidf)" 2>/dev/null; then echo "vite already running (pid $(cat $pidf))"; exit 0; fi
    [ -d frontend/node_modules ] || (cd frontend && npm ci --no-audit --no-fund)
    node scripts/learn_anchors.mjs
    # @learn:ui-pid
    cd frontend
    nohup node node_modules/vite/bin/vite.js > ../.runtime/vite.log 2>&1 &
    echo $! > ../$pidf # $! is vite itself, started from this shell
    cd ..
    # @learn:end ui-pid
    for _ in $(seq 1 60); do curl -sf localhost:5185 >/dev/null && break; sleep 0.5; done
    echo "vite on http://localhost:5185 (pid $(cat $pidf))"
    ;;
  stop)
    if [ -f $pidf ]; then kill "$(cat $pidf)" 2>/dev/null || true; rm -f $pidf; echo "vite stopped"; fi
    ;;
esac
