# 06 观测：队列深度、投递延迟、错误率，还有 runbook

> 讲解视频：[p6-ops 观测与故障演练](../videos/p6-ops.mp4) · tag：`step-06`

## 这一章解决什么问题？

前五章的保护都在默默干活：重复被挡掉、重试被错开、令牌桶在排队。
可值班的人看不见。半夜收到「通知没到」的投诉，他要能在五分钟内回答：
**活卡在哪？** 是 provider 挂了，还是我们自己慢了？

## 三个问题，对应三组指标

1. **队列在不在涨？** 积压有三个地方，各有一个指标：
   - `dispatchd_outbox_unpublished`：还没进 Kafka（relay 出问题）；
   - `dispatchd_kafka_consumer_lag`：进了 Kafka 没被处理（worker 跟不上或者挂了）。这就是队列深度，worker 每五秒用 kadm 算一次；
   - `dispatchd_delay_queue_size`：在延迟堆里等重试（provider 在出错）。
2. **慢不慢？** `dispatchd_delivery_latency_seconds`：从 API 收下到 provider 确认，包括所有重试。另有 `dispatchd_provider_request_seconds` 单看 provider 那一下。
   两个一对比就知道慢在哪：provider 快、端到端慢，说明时间花在排队上。
3. **谁在出错？** `dispatchd_attempts_total{provider,outcome}` 和 `dispatchd_provider_errors_total{provider,kind,code}`。

还有几个是给前几章的保护「记账」的：
`dispatchd_idempotent_replays_total`（幂等回放）、`dispatchd_duplicate_messages_skipped_total`（Kafka 重复被 claim 挡掉）、
`dispatchd_lease_expired_total`（worker 死了留下的尝试）、`dispatchd_ratelimit_wait_seconds`（令牌桶等了多久）、
`dispatchd_delay_queue_wakeups_total{reason}`（堆醒了几次，按定时器和「来了更早的」分开）。

## 假设：错误率用 5xx 的计数就够了？

不够。一秒十个错误，在一秒一万个请求的时候不算事，在一秒二十个的时候就是全挂。
告警用比例：`非 DELIVERED 的尝试 ÷ 全部尝试 > 20%`，持续五分钟（`deploy/prometheus/alerts.yml`）。

## 告警和 runbook

七条告警，每条的 `runbook` 注解指向 `docs/runbook.md` 里的一节：
provider 挂了、provider 回 429、Kafka lag 在涨、worker 崩溃重启、MySQL 慢、永久失败。
runbook 里每一步都是这个仓库能直接跑的命令，我在 zsh 和 bash 里都试过一遍。

一个真踩到的坑：runbook 第一版写的是 `DC="docker-compose -p dispatchd"` 然后 `$DC logs`。
zsh 不会把变量按空格拆开，报 `no such file or directory: docker-compose -p dispatchd`。改成了 shell 函数。

## 真跑一次

让 email provider 三成请求回 503，发六十封邮件和一个失效 token 的推送：

```text
dispatchd_attempts_total{outcome="DELIVERED",provider="email"} 60
dispatchd_attempts_total{outcome="TEMPORARY",provider="email"} 25
dispatchd_attempts_total{outcome="PERMANENT",provider="push"} 1
dispatchd_delay_queue_wakeups_total{reason="earlier_item"} 6
dispatchd_delay_queue_wakeups_total{reason="timer"} 25
dispatchd_kafka_consumer_lag{topic="notifications.email"} 0
dispatchd_notifications_final_total{channel="push",status="FAILED_PERMANENT"} 1
```

二十五次临时失败，堆的定时器正好醒了二十五次，一次重试醒一次。

扩容也能直接看出来：`docker-compose -p dispatchd up -d --scale worker=2` 之后，Prometheus 用 DNS 发现两个 worker：

```text
up{job="worker"}: 172.20.0.9:8853=1, 172.20.0.10:8853=1
```

Grafana（http://localhost:3092）自动加载 `deploy/grafana/dashboards/dispatchd.json`，十个面板，按上面三个问题排。

## 测试

`TestMetricsTheRunbookReadsAreExported`：真跑一次重试和一次幂等回放，再抓 `/metrics`，检查 runbook 和告警用到的序列都在。
改了指标名，这个测试就红，告警规则不会悄悄失效。
`promtool check rules deploy/prometheus/alerts.yml` 在 CI 里跑。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-06
docker-compose -p dispatchd up -d --build --wait
open http://localhost:3092    # Grafana
open http://localhost:9192/alerts
```
