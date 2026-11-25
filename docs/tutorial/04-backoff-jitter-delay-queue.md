# 04 退避、jitter 和最小堆延迟队列

> 讲解视频：[p4-retry 退避和延迟堆](../videos/p4-retry.mp4) · tag：`step-04`

## 这一章解决什么问题？

到上一章为止，临时失败（503、超时）直接记成失败，不再重试。
可 provider 抖一下是常态。这一章要回答三个问题：
1. 等多久再试？
2. 一大批通知一起失败时，怎么让它们别一起回来？
3. 谁来在「到点」的那一刻叫醒 worker，而且不靠轮询？

## 等多久？指数退避

第 n 次失败之后，等待的上限是 `base × 2^(n-1)`，再封顶到 `cap`（`internal/backoff`）。
默认 base 一秒、cap 两分钟、最多八次。到了第八次还失败，就是 `FAILED_EXHAUSTED`。
429 带的 `Retry-After` 是下限：provider 让我们晚点来，就不能早来。

## 假设：退避就够了，不用随机？

一次故障里，几百个通知在同一毫秒失败。它们算出来的等待时间完全一样，于是在同一毫秒回来。
provider 刚恢复，迎面就是整个积压。它扛不住，又开始拒绝，大家再一起等、再一起回来。这就是重试风暴。

## 加随机：full jitter

等待时间从 `[0, 上限]` 里均匀抽一个（`Policy.Next`）。
为什么选 full jitter 而不是 equal jitter 或 decorrelated jitter？
- full jitter 把重试摊得最开，provider 端的峰值最低；
- 代价是单个通知的平均等待只有上限的一半，最坏情况可能很快就重试，这对通知服务没坏处；
- decorrelated jitter 要记住上一次的等待时间，状态多一个字段，收益在我们的测试里看不出来。

每次决定都写进事件：上限、抽到的随机数、最终等待、`Retry-After`。learn 页面直接显示这一行。

## 谁来叫醒？最小堆 + 一个定时器

假设：每秒 `SELECT … WHERE next_attempt_at <= NOW()`。
一万个通知在等，每秒扫一遍是浪费；想要准时，就得扫得更勤。

`internal/delayq` 是一个按到期时间排的最小堆，配一个 `time.Timer`：
- 定时器永远设在堆顶的到期时间。到点醒一次，把所有到期的弹出来；
- `Push` 只有在新元素比堆顶还早的时候，才去踢一下（kick）那个 goroutine，让它重新算等待时间；
- 堆空着的时候，goroutine 一直睡，一次都不醒。

同一个通知在堆里只有一项（`byID`），再 push 就是挪位置。

## 内存里的堆，重启了怎么办？

到期时间**先写 MySQL**（`next_attempt_at`，状态 `RETRY_SCHEDULED`），再进堆。
worker 启动时、以及每次 Kafka 给它分到分区时，都调 `Reload`：
- `RETRY_SCHEDULED` 的按 `next_attempt_at` 放回堆；
- 还在 `SENDING` 的按 `lease_until` 放回堆，到期时由 `ExpireLease` 判断要不要重发。

分区重新分配通常意味着有同伴死了，它堆里的重试只活在它的内存里，所以要捞一遍。
两个 worker 的堆里可能有同一个通知，没关系，`Claim` 只会让一个人发。

## 真跑一次：一个通知被拒两次

让 email provider 对 `retry-demo@example.com` 的前两次请求回 503（`fail_first`），然后发一个：

```text
18:09:40.908 claimed  QUEUED->SENDING            attempt 1
18:09:40.913 attempt  SENDING->RETRY_SCHEDULED   attempt 1  http 503   上限 1000 ms，随机数 0.88，等 879.7 ms
18:09:40.915 delayed                             heap_len 1
18:09:41.792 woken                               late_ms 0.85
18:09:41.796 claimed  RETRY_SCHEDULED->SENDING   attempt 2
18:09:41.799 attempt  SENDING->RETRY_SCHEDULED   attempt 2  http 503   上限 2000 ms，随机数 0.30，等 594.4 ms
18:09:42.394 woken                               late_ms 1.41
18:09:42.402 attempt  SENDING->DELIVERED         attempt 3  http 200
```

看 `woken` 的 `late_ms`：定时器在到期后一毫秒左右就醒了。没有轮询间隔，也就没有「最多晚一个间隔」。
第二次的上限翻倍成两秒，可抽到的随机数小，实际只等了六百毫秒。这就是 full jitter。

## 测试的输出

堆的单元测试（`internal/delayq/delayq_test.go`）：
- 两百个元素乱序插入，按到期时间依次弹出，没有一个提前；
- 先插一个一秒后的，再插一个五十毫秒后的：五十毫秒那个准时弹出，定时器不会傻等一秒；
- 空堆睡一百毫秒，醒来零次；插一个、等它到期，正好一次定时器 + 一次 kick；再插一个更晚的，不会多醒。

重试风暴（`TestRetriesAfterAnOutageDoNotArriveAsOneSpike`）：
两百个通知积压，worker 在 provider 宕机时开始消费，所有第一次尝试几乎同时失败。
一秒半后 provider 恢复，但像真的刚恢复的服务一样，每秒只扛两百个请求，多了回 429。

```text
开 jitter：
  after recovery: busiest 100 ms 30 requests, 1 rejected with 429
  requests per 100 ms after recovery: 16 16 23 30 22 18 9 7 11 9 4 3 3 3 2 3 5 1 1 4 4 2 2 0 …
  provider requests in total: 874; all delivered after 5.88s
关 jitter（DISPATCHD_SABOTAGE=jitter）：
  after recovery: busiest 100 ms 66 requests, 129 rejected with 429
  requests per 100 ms after recovery: 17 2 42 47 66 26 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 30 38 30 0 …
  provider requests in total: 929; all delivered after 10.01s
  --- FAIL: retry storm: the recovering provider had to reject 129 requests (limit 40)
```

关掉 jitter，请求成波地来：一波打满、一秒多全静、再一波。被拒的一百多个请求又在同一时刻一起重试。
开着 jitter，曲线是平的，几乎不被拒，全部送达反而更快。

注意一个反直觉的地方：第一版测试里我没有限制 provider 的容量，结果关不关 jitter 都是绿的。
见 `docs/problems.md` 的 q03。

## 其他测试

- `TestProviderOutageThenRecoveryDeliversEverything`：五十个通知遇到全量 503，恢复后全部送达，每个都重试过。
- `TestTemporaryFailuresStopAtMaxAttempts`：一直 503，三次之后 `FAILED_EXHAUSTED`，`last_error` 里是最后一次的 503。
- `TestNewWorkerReloadsRetriesFromMySQL`：二十个重试在堆里等着时把 worker 停掉，新 worker 从 MySQL 捞回来，全部送达。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-04
docker-compose -p dispatchd up -d --build --wait
./scripts/test.sh
DISPATCHD_SABOTAGE=jitter go test -run Spike -v ./internal/harness   # 看它变红
```
