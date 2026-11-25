#!/usr/bin/env bash
# Run the Go tests against the compose MySQL (and Kafka, from chapter 03),
# starting them if needed. Only touches the "dispatchd" compose project.
set -euo pipefail
cd "$(dirname "$0")/.."
DC=${DC:-$(command -v docker-compose || echo "docker compose")}
$DC -p dispatchd up -d --wait mysql kafka >/dev/null
go test -race -count=1 "$@" ./...
