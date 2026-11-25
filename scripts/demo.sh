#!/usr/bin/env bash
# One command: build and start everything (API + relay, worker, fake
# providers, MySQL, Kafka, Prometheus, Grafana), start the web UI on
# :5185 in the background, and push one notification through to prove
# the pipeline works. Safe on an empty database: the API creates the
# schema, the Kafka topics and the demo API key on start.
#   ./scripts/demo.sh          start
#   ./scripts/demo.sh stop     stop the UI and the containers (data kept)
set -euo pipefail
cd "$(dirname "$0")/.."
DC=${DC:-$(command -v docker-compose || echo "docker compose")}
if [ "${1:-}" = "stop" ]; then
  ./scripts/ui.sh stop
  $DC -p dispatchd down
  exit 0
fi
echo "starting the stack (the first build takes a minute or two)…"
$DC -p dispatchd up -d --build --wait
./scripts/ui.sh start

KEY=${DISPATCHD_DEMO_KEY:-dk_demo_local_only}
echo "smoke: one notification through the whole pipeline…"
resp=$(curl -sf -XPOST localhost:8850/v1/notifications -H "Authorization: Bearer $KEY" \
  -H "Idempotency-Key: demo-$(date +%s)-$RANDOM" \
  -d '{"channel":"email","recipient":"ada@example.com","template":"welcome","params":{"name":"Ada"}}')
id=$(printf '%s' "$resp" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
status=""
for _ in $(seq 1 60); do
  status=$(curl -sf "localhost:8850/v1/notifications/$id" -H "Authorization: Bearer $KEY" | sed -n 's/.*"status":"\([A-Z_]*\)".*/\1/p' | head -1)
  [ "$status" = "DELIVERED" ] && break
  sleep 0.5
done
echo "  $id → $status"
[ "$status" = "DELIVERED" ] || { echo "the smoke notification was not delivered"; exit 1; }

cat <<MSG

  web UI      http://localhost:5185        (教学模式: /learn)
  API         http://localhost:8850        (Authorization: Bearer $KEY)
  providers   http://localhost:8851 (email)  http://localhost:8852 (push)
  worker      http://localhost:8853/metrics
  Prometheus  http://localhost:9192
  Grafana     http://localhost:3092        (dashboard "dispatchd")

  stop:  ./scripts/demo.sh stop
MSG
