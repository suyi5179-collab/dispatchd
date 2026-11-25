# dispatchd runbook

Every command here runs against this repository's compose stack (`-p dispatchd`). Load the shortcuts first ([`scripts/runbook.sh`](../scripts/runbook.sh#anchor=runbook-shortcuts): shell functions, so they work in bash and zsh alike):

```bash
source scripts/runbook.sh   # dc …, SQL "…", KAFKA_GROUPS …
```

Dashboards: Grafana http://localhost:3092 (dashboard "dispatchd"), Prometheus http://localhost:9192/alerts.
Alert rules: `deploy/prometheus/alerts.yml`; each alert's `runbook` annotation links a section below.

## First look (any alert)

```bash
SQL "SELECT status, COUNT(*) FROM notifications GROUP BY status"
curl -s localhost:8853/metrics | grep -E '^dispatchd_(kafka_consumer_lag|delay_queue_size|attempts_total|ratelimit_tokens)'
dc logs --since 10m worker | grep -E '"level":"(WARN|ERROR)"' | tail -20
```

Where the work is waiting tells you which section to read:

| waiting in | metric | means |
|---|---|---|
| outbox | `dispatchd_outbox_unpublished` | the relay cannot reach Kafka |
| Kafka | `dispatchd_kafka_consumer_lag` | workers are not keeping up, or are down |
| delay queue | `dispatchd_delay_queue_size`, `RETRY_SCHEDULED` rows | a provider is failing and we are backing off |
| `SENDING` rows older than the lease | SQL below | a worker died mid-attempt |

## A provider is down

Symptoms: `DispatchdProviderErrorRateHigh`; `dispatchd_attempts_total{outcome="TEMPORARY"}` climbing; `RETRY_SCHEDULED` growing.

1. Confirm it is the provider, not us: look at the error codes.
   ```bash
   curl -s localhost:8853/metrics | grep dispatchd_provider_errors_total
   SQL "SELECT provider_code, http_status, COUNT(*) FROM delivery_attempts WHERE started_at > NOW() - INTERVAL 10 MINUTE AND outcome <> 'DELIVERED' GROUP BY 1,2"
   ```
   5xx/timeouts = provider side. Mostly `network_error` from every provider at once = our network or DNS.
2. Do nothing drastic. Retries back off with full jitter (1 s, 2 s, 4 s … capped at 2 min, 8 attempts), so a recovered provider is not hit by one synchronized wave. Do **not** restart workers to "speed things up": their delay queues reload from MySQL and nothing is gained.
3. If the outage outlasts the retry budget (about four minutes of ceilings), notifications end in `FAILED_EXHAUSTED`. After the provider is back, redrive them (each gets one more attempt; `MaxAttempts` counts all attempts):
   ```bash
   SQL "UPDATE notifications SET status='RETRY_SCHEDULED', next_attempt_at=NOW(6) WHERE status='FAILED_EXHAUSTED' AND updated_at > NOW() - INTERVAL 2 HOUR"
   dc restart worker      # Reload puts them into the delay queue
   ```
4. Demo: `curl -XPOST localhost:8851/admin/mode -d '{"mode":"fail_5xx"}'`, then `-d '{"mode":"ok"}'` to recover.

## The provider says 429

Symptoms: `DispatchdRateLimited`. The token bucket is set above the provider's real quota (or several workers share one quota: each worker has its own bucket).

```bash
curl -s localhost:8853/metrics | grep -E 'dispatchd_ratelimit_(tokens|wait_seconds_count)'
```

Lower the bucket below quota ÷ number of workers, e.g. `DISPATCHD_RATE_EMAIL=15`, then `dc up -d worker`. 429s are retried with the provider's `Retry-After` as the floor, so nothing is lost meanwhile.

## Kafka lag is growing

Symptoms: `DispatchdKafkaLagHigh` (or `DispatchdOutboxBacklog`).

```bash
KAFKA_GROUPS --describe --group dispatchd-workers.email   # and dispatchd-workers.push
SQL "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"
```

- **Outbox rows pile up, lag is flat:** the API's relay cannot publish. Check `dc logs api | grep relay` and `dc ps kafka`. Notifications are safe in MySQL; the relay sweeps every second and catches up once Kafka is back.
- One consumer group per channel (`dispatchd-workers.email`, `dispatchd-workers.push`), each with its own poll loop, so a throttled email provider does not hold up push.
- **Lag grows, CONSUMER-ID is empty:** no worker is in the group → next section.
- **Lag grows, consumers present:** workers are slower than intake. Check token wait (`dispatchd_ratelimit_wait_seconds`): if workers wait on the bucket, the provider quota is the bottleneck and more workers will not help. Otherwise add a worker (two host ports are reserved):
  ```bash
  dc up -d --scale worker=2
  ```
  Each topic has 3 partitions, so more than 3 workers per topic sit idle.

## Worker crashed or restarting

Symptoms: `DispatchdWorkerDown`; `SENDING` rows not moving.

```bash
dc ps worker
dc logs --since 10m worker | tail -50
SQL "SELECT id, attempts, lease_until FROM notifications WHERE status='SENDING' AND lease_until < NOW(6)"
```

Recovery is automatic once a worker runs: it reloads `RETRY_SCHEDULED` rows into its delay queue and schedules a lease check for each `SENDING` row. When a lease has run out, the attempt is marked `UNKNOWN` (the provider may or may not have received it) and retried with the same provider `Idempotency-Key`. Uncommitted Kafka messages are read again and skipped by the claim (`dispatchd_duplicate_messages_skipped_total`). Measured in `TestWorkerKilledMidBatchLosesNothingAndSendsNothingTwice`.

```bash
dc up -d worker        # or: dc restart worker
```

A worker that exits on purpose (a MySQL write failed five times for one record) does so without committing; compose restarts it (`restart: unless-stopped`).

## MySQL is slow

Symptoms: `DispatchdDeliveryLatencyHigh` while provider latency (`dispatchd_provider_request_seconds`) is normal; API `POST` latency up.

```bash
SQL "SHOW PROCESSLIST"
SQL "SELECT COUNT(*) FROM notification_events"; SQL "SELECT COUNT(*) FROM idempotency_keys"
dc exec -T mysql mysqladmin -udispatchd -pdispatchd status
```

- Every attempt writes several rows (claim, attempt, events). The demo MySQL has a 64 MB buffer pool; under load it is the first thing to saturate (see README, "what this is not").
- `idempotency_keys` older than 24 h are deleted every 10 minutes by the API; if that job fell behind, run `SQL "DELETE FROM idempotency_keys WHERE created_at < NOW() - INTERVAL 1 DAY LIMIT 10000"` until it returns 0 rows.
- Lock waits on `notifications`: two workers fighting over one row only cost one failed claim each; long waits point at a big manual `UPDATE` (like the redrive above). Run those in batches with `LIMIT`.

## Permanent failures

Symptoms: `DispatchdPermanentFailures`. These are never retried. Each one says what to do:

```bash
SQL "SELECT id, channel, recipient, JSON_UNQUOTE(last_error->'$.code') code, JSON_UNQUOTE(last_error->'$.action') action FROM notifications WHERE status='FAILED_PERMANENT' ORDER BY updated_at DESC LIMIT 20"
```

| code | action |
|---|---|
| `invalid_recipient` | correct the address with the user; do not retry |
| `device_token_unregistered` | remove the device token from the user's profile |
| `rejected` | check the message content against the provider's policy |
| 401/403 | rotate the provider API key |
