#!/usr/bin/env bash
# The three "can it fail?" controls. Each test runs twice: with the
# protection on (must pass) and with it switched off through
# DISPATCHD_SABOTAGE (must fail). Outputs go to docs/results/.
# Needs the compose MySQL and Kafka (scripts/test.sh starts them).
set -uo pipefail
cd "$(dirname "$0")/.."
DC=${DC:-$(command -v docker-compose || echo "docker compose")}
$DC -p dispatchd up -d --wait mysql kafka >/dev/null 2>&1
out=docs/results
mkdir -p "$out"
export DISPATCHD_RESULTS_DIR=$PWD/$out
status=0
run() { # name sabotage package test-regex
  local name=$1 sab=$2 pkg=$3 re=$4
  go test -count=1 -v -run "$re" "$pkg" 2>&1 | grep -E '^\s+\w+_test\.go:|^(--- |ok|FAIL)' > "$out/contrast-$name-on.txt"
  local on=${PIPESTATUS[0]}
  DISPATCHD_SABOTAGE=$sab go test -count=1 -v -run "$re" "$pkg" 2>&1 | grep -E '^\s+\w+_test\.go:|^(--- |ok|FAIL)' > "$out/contrast-$name-off.txt"
  local off=${PIPESTATUS[0]}
  if [ "$on" -eq 0 ] && [ "$off" -ne 0 ]; then
    echo "✓ $name: passes with the protection, fails without it"
  else
    echo "✗ $name: on=$on off=$off (want 0 and non-zero)"; status=1
  fi
}
run idempotency idempotency ./internal/api 'RetryAfterTimeout|SameKey|Concurrent'
run jitter jitter ./internal/harness 'Spike'
run ratelimit ratelimit ./internal/harness 'TokenBucket'
exit $status
