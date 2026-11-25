# Shortcuts for docs/runbook.md:  source scripts/runbook.sh
# Functions, not variables: DC="docker-compose -p dispatchd" followed by
# $DC breaks in zsh, which does not split an unquoted variable on spaces.
# @learn:runbook-shortcuts
dc() { docker-compose -p dispatchd "$@"; }
SQL() { dc exec -T mysql mysql -udispatchd -pdispatchd dispatchd -e "$1" 2>/dev/null; }
KAFKA_GROUPS() { dc exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 "$@"; }
# @learn:end runbook-shortcuts
