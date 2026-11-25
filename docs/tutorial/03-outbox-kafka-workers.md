# 03 Outbox、Kafka 和 worker：崩溃了也不丢、不重

> tag：`step-03`

## 这一章解决什么问题？

第一章的投递靠进程内的一个 channel。API 进程一崩，channel 里没发出去的通知就没了，库里永远停在 `QUEUED`。
而且 API 和投递挤在一个进程里，provider 一慢，API 也跟着慢。

这一章把投递拆出去：API 只负责「记下来」，Kafka 负责「排队」，worker 负责「发」。

## 假设：API 写完库直接发 Kafka，行不行？

```text
INSERT notification   ✓ 提交
produce to Kafka      ✗ 进程在这里崩了
```

库里有通知，Kafka 里没有消息，永远没人发。反过来先发 Kafka 再写库，worker 可能拿到一个库里还不存在的通知。
两个系统没法放进一个事务。

## 那怎么办？Outbox

在**同一个 MySQL 事务**里写两行：notification 和 outbox（`internal/api/api.go` 的 `afterInsert`）。
要么都在，要么都不在。

然后由 relay 把 outbox 搬到 Kafka（`internal/store/outbox.go` 的 `PublishBatch`）：
1. `SELECT … WHERE published_at IS NULL … FOR UPDATE SKIP LOCKED`：两个 relay 会分着干，不会抢同一行；
2. 发到 Kafka，等 broker 确认（acks=all）；
3. 把 partition、offset 写回 outbox 行，标记 `published_at`，提交。

API 每次提交后会叫醒 relay，所以正常路径没有轮询延迟。另外每秒扫一次，兜住崩溃和 Kafka 宕机时留下的行。

第二步和第三步之间崩了怎么办？这一行会被再发一次。relay 是 **at-least-once**，重复要靠 worker 去掉。

## topic 怎么分？

一个 channel 一个 topic：`notifications.email`、`notifications.push`，三个分区，key 是通知 id。
email provider 出事时只堵 email 的 topic，push 照常发。

## worker 怎么做到「不重」？

worker 消费时**先投递、写库，再提交 offset**（`internal/worker/consumer.go`）。
所以崩溃后，没提交的那批消息会被重新读到。这些消息里有三种：
- 已经发完的：库里是 `DELIVERED`，跳过；
- 正在发的：库里是 `SENDING`，跳过；
- 还没开始的：正常发。

判断「轮到我发吗」靠 `Claim`（`internal/store/delivery.go`）：
`SELECT … WHERE id=? AND attempts=? FOR UPDATE`，状态还是 `QUEUED` 才改成 `SENDING`、`attempts+1`。
两个 worker 拿着同一条消息去抢，只有一个能改成功。去重的 key 就是（通知 id，第几次尝试）。

## 正在发的那一半怎么办？租约

worker 改成 `SENDING` 的同时写一个 `lease_until`（默认 15 秒，比 provider 超时长）。
worker 被 kill，这些通知就卡在 `SENDING`。
新 worker 启动时把它们记下来，等租约到期，把那次尝试标成 `UNKNOWN`，状态退回 `QUEUED`，再发一次（`ExpireLease`）。

为什么是 UNKNOWN？因为 provider 可能已经收到了，只是我们没来得及写库。
所以再发的时候，请求头带 `Idempotency-Key: <通知 id>`。支持幂等键的 provider 会把它合并掉。

## 真跑一次

```text
POST /v1/notifications  Idempotency-Key: step03-demo  {"channel":"push",...}
GET  /v1/notifications/{id}  status DELIVERED
  accepted  ->QUEUED
  published                 topic notifications.push  partition 0  offset 0
  consumed                  topic notifications.push  partition 0  offset 0  worker-1
  claimed   QUEUED->SENDING attempt 1
  attempt   SENDING->DELIVERED  http 200
```

测试里还见过一个顺序：`consumed` 比 `published` 先写进事件表。
因为 relay 是先等 Kafka 确认、后提交事务的，worker 可能在这之间就读到了消息。不影响正确性：通知本身早就提交了。

## 测试：中途 kill -9 一个 worker

`TestWorkerKilledMidBatchLosesNothingAndSendsNothingTwice`（`internal/harness/restart_test.go`）：
三百个通知，provider 每次慢四十毫秒，worker 子进程发到四分之一时 `SIGKILL`，再起一个新的。

```text
killed worker-a after 75 provider requests with 8 attempts in flight
final statuses map[DELIVERED:300]; provider requests 303, delivered 300 distinct, collapsed by provider key 3
Kafka redeliveries skipped by the claim: 80; leases expired and retried: 8
```

读法：
- 八十条 Kafka 消息被重新读到，claim 把它们都挡住了；
- 八个在途的尝试等租约到期后重发；其中三个其实 provider 已经收到了，靠 provider 的幂等键合并；
- 三百个全部送达，没有一个人收到两次。

所以「不重」有两层：worker 的 claim 挡住 Kafka 的重复投递；provider 幂等键挡住「发出去了但没记下来」的那一小段窗口。
如果 provider 不支持幂等键，这一小段窗口里的重复是去不掉的。README 的局限一节写了这一点。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-03
docker-compose -p dispatchd up -d --build --wait
./scripts/test.sh
```
