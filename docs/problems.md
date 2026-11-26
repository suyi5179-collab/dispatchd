# 日常问题与解决

面试常问：「你遇到过什么问题？怎么定位的？怎么解决的？」
这里有十一个。q01–q07 和 q11 是做这个项目时**真实踩过的坑**（q11 是推上 GitHub 后第一次 CI 抓到的）（还有一个记 PID 的 shell 坑写在第七章教程里），报错和输出都是当时原样贴的；q08–q10 是通知服务在生产上的**典型事故**，就是三个失败对照演示的那些。

每个问题都有一段逐行高亮的讲解视频（`docs/videos/problems/<id>.mp4`），讲解用的代码由 `scripts/build_problem_videos.mjs` 按 `@learn` 锚点从源码里切出来，不是手抄的。

## 真实踩过的坑

<a id="q01"></a>

### q01 · 测试只说「十五秒还是 QUEUED」，真正的错误被吞了

**类别**：真实踩过的坑（第三章，接上 Kafka 之后第一次跑测试）

**现象**

```text
=== RUN   TestSubmitAndDeliver
    api_test.go:53: ntf_01M89XJFV6NNY09J0B2V4EMM5S: status QUEUED after 15s, want DELIVERED
--- FAIL: TestSubmitAndDeliver (15.33s)
```

打开 `DISPATCHD_TEST_LOG=1` 再跑，一行日志都没有。

**在哪里**：[`internal/store/delivery.go` 的 `Claim`](../internal/store/delivery.go#anchor=claim)；修复前的样子在 [`docs/problems/snippets/claim_v1.go`](problems/snippets/claim_v1.go#anchor=claim-v1)。

**怎么定位**

- 假设一：relay 没发出去。写一个临时测试，四秒后把 outbox 和事件表打出来。outbox 有 `published_at`，有 partition 和 offset。排除。
- 假设二：worker 没收到。事件表里有**五条** `consumed`，offset 都是零。worker 收到了，而且同一条消息处理了五次。
- 五次正好是 `handleAll` 的重试次数，说明 `handle` 一直返回错误，重试完 `Run` 就退出了。可 harness 里写的是 `_ = c.Run(ctx)`，错误直接扔掉。
- 把错误打出来：`worker w1 stopped: record …/0@0: sql: expected 5 arguments, got 4`。

**怎么解决**

修复前：加 `lease_until` 时多了一个占位符，参数没跟上。

```go
`UPDATE notifications SET status=?, attempts=?, lease_until=?, updated_at=? WHERE id=?`,
    StatusSending, prev+1, now.Add(lease), id)        // 五个 ?，四个参数
```

修复后：补上 `now`；harness 里 worker 退出时用 `t.Logf` 把错误打出来，以后不会再静默。

```go
`UPDATE notifications SET status=?, attempts=?, lease_until=?, updated_at=? WHERE id=?`,
    StatusSending, prev+1, now.Add(lease), now, id)
```

**怎么证明修好了**：`TestSubmitAndDeliver` 修复前红（上面那行），修复后绿；`internal/api` 全部测试通过。

**面试怎么讲**：接 Kafka 后第一个端到端测试超时，日志是空的（S）。我要找出卡在哪一段（T）。我把 outbox 和事件表打出来，发现同一条消息被消费了五次，顺着重试次数找到被吞掉的错误，是 SQL 占位符比参数多一个（A）。修了 SQL，并让测试框架在 worker 退出时打印原因，这类问题以后第一眼就能看到（R）。

<a id="q02"></a>

### q02 · 测试跑完不退出：卡在 kgo 的 Close

**类别**：真实踩过的坑（第三章）

**现象**

```text
$ go test -race -count=1 ./internal/api
Command was moved to the background after running for 180s.
```

第一个测试其实已经跑完了，进程就是不结束。

**在哪里**：[`internal/worker/consumer.go` 的 `Run`](../internal/worker/consumer.go#anchor=consume)；修复前在 [`docs/problems/snippets/consume_v1.go`](problems/snippets/consume_v1.go#anchor=consume-v1)。

**怎么定位**

- 假设：测试在等 Kafka 超时。可 franz-go 的默认超时都是秒级，三分钟太长了，不像超时，像死锁。
- 卡住的是 cleanup：harness 停 worker 的顺序是取消 ctx → 等 `Run` 返回 → `kc.Close()`。
- 读 `kgo.BlockRebalanceOnPoll` 的文档：开了这个选项，rebalance 要等调用方用 `AllowRebalance` 释放上一次 poll；文档原话是「必须总是调用」。离开消费组（`Close`）也要走 rebalance。
- 我的 `Run` 在 ctx 取消时直接 `return nil`，那次 poll 没释放，`Close` 就永远等着。

**怎么解决**

修复前：

```go
if fetches.IsClientClosed() || ctx.Err() != nil {
    return nil            // 还占着这次 poll
}
```

修复后：在 `Run` 开头 `defer c.Client.AllowRebalance()`，所有退出路径都会释放。

**怎么证明修好了**：同一组测试从三分钟不结束变成 4.9 秒跑完；`TestWorkerKilledMidBatchLosesNothingAndSendsNothingTwice` 里优雅停止的 worker-b 也能正常退出。

**面试怎么讲**：消费者测试挂住不退出（S）。我要搞清楚是超时还是死锁（T）。我看出是 cleanup 卡住，再去读 BlockRebalanceOnPoll 的约定，发现退出路径没有释放 poll，Close 在等一次永远不会来的 AllowRebalance（A）。用 defer 兜住所有返回路径，之后再没卡过（R）。

<a id="q03"></a>

### q03 · 关掉 jitter，重试风暴测试还是绿的

**类别**：真实踩过的坑（第四章）

**现象**：第一版风暴测试，开不开 jitter 都通过，测的东西是死的。

```text
关 jitter（DISPATCHD_SABOTAGE=jitter）：
  after recovery the provider's busiest 100 ms had 26 requests
  requests per 100 ms after recovery: 2 9 3 5 5 4 3 5 2 7 4 6 9 13 7 11 7 6 4 5 4 3 6 26 8 …
  --- PASS
```

**在哪里**：现在的测试是 [`internal/harness/retry_test.go` 的 `stormPeak`](../internal/harness/retry_test.go#anchor=storm-test)；第一版在 [`docs/problems/snippets/storm_v1_test.go`](problems/snippets/storm_v1_test.go#anchor=storm-v1)。

**怎么定位**

- 假设：jitter 的开关没生效。看事件里的 `backoff.jitter`，关掉时确实是 `none`，等待正好等于上限。开关是好的。
- 那为什么没有尖峰？风暴的前提是「大家同时失败」。可测试是一个一个地提交两百个通知，worker 早就在消费，第一次尝试本身就摊在一秒半里。没 jitter 也只是把这个分布原样平移。
- 改成先积压、再启动 worker，所有第一次尝试几毫秒内一起失败。再跑：关 jitter 的峰值四十七，开 jitter 三十二，还是分不开。
- 原因是 provider 恢复后容量无限，同步回来的一波照单全收，没有可以数的「伤害」。真实的服务刚恢复时扛不住满负荷。

**怎么解决**：测试改成积压 + 同时失败 + provider 恢复后限容量（超过就 429），判据从「峰值」改成「恢复中的 provider 被迫拒绝了多少请求」。

**怎么证明修好了**：同一个测试，开 jitter 通过、关 jitter 失败，输出在 `docs/results/contrast-jitter-*.txt`。

```text
开 jitter：busiest 100 ms 20 requests, 29 rejected with 429 … all delivered after 9.27s … PASS
关 jitter：busiest 100 ms 45 requests, 237 rejected with 429 … all delivered after 16.28s … FAIL
```

**面试怎么讲**：我给 jitter 写了对照测试，结果关掉保护它也是绿的（S）。我得让判据真的依赖这层保护（T）。我把风暴的两个前提补上：失败必须同时发生，恢复的一方必须有容量上限，然后数被拒的请求（A）。现在关掉 jitter 被拒二百三十七个、开着二十九个，测试是活的（R）。

<a id="q04"></a>

### q04 · 风暴测试的余量不够：并行跑全量时偶尔红

**类别**：真实踩过的坑（第四章，q03 修好之后）

**现象**：单独跑稳定通过；`go test -race ./...` 全量跑时红了一次。

```text
--- FAIL: TestRetriesAfterAnOutageDoNotArriveAsOneSpike (7.05s)
    retry_test.go:138: … after recovery: busiest 100 ms 44 requests, 32 rejected with 429
    retry_test.go:142: retry storm: the recovering provider had to reject 32 requests (limit 20)
```

后来在后台跑对照脚本时，又出现过开 jitter 三十个、关 jitter 才四十七个，两边几乎碰到一起。

**在哪里**：[`internal/harness/retry_test.go` 的 `stormPeak`](../internal/harness/retry_test.go#anchor=storm-test) 里的退避参数和 provider 容量。

**怎么定位**

- 假设：jitter 实现有问题。可单独连跑三次都是零到九个被拒，不像实现错。
- 差别在负载：全量测试时几个包同时打 MySQL，管道变慢。开 jitter 时，恢复后的重试是「二百个摊在零到一秒多」，速率接近 provider 的容量，稍有抖动就超。
- 也就是说，判据离开 jitter 那一侧太近了，测试测的是机器忙不忙。

**怎么解决**：让开 jitter 的一侧远离容量线：base 从三百毫秒调到五百毫秒（摊得更开），provider 恢复后容量从两百每秒降到一百每秒（关 jitter 的那一波伤害更大），阈值放到总数的百分之三十。

**怎么证明修好了**：开 jitter 连跑都在十八到二十九个之间，关 jitter 在二百一十到四百六十六之间，阈值六十落在中间、两边都有好几倍的余量。

**面试怎么讲**：一个对照测试偶尔在全量跑时变红（S）。我要判断是代码问题还是测试本身不稳（T）。单独跑和并行跑一对比，就看出判据太靠近正常一侧，被机器负载左右（A）。我调了参数让两侧分开好几倍，再把阈值放到中间，之后没再抖过（R）。

<a id="q05"></a>

### q05 · 同一个幂等键，第二次返回的字节不一样

**类别**：真实踩过的坑（第二章）

**现象**：内容一样，可空格和字段顺序变了。

```text
first:  {"id":"ntf_01M89ZJCNY8XZYJNSQFBYW1Z9A","client_id":"t","channel":"email",…}
second: {"id": "ntf_01M89ZJCNY8XZYJNSQFBYW1Z9A", "params": {"name": "Ada"}, "status": "QUEUED", …}
```

**在哪里**：[`internal/store/migrations/002_idempotency.sql` 的 response 列](../internal/store/migrations/002_idempotency.sql#anchor=idem-json-column)；修复在 [`004_idem_response_bytes.sql`](../internal/store/migrations/004_idem_response_bytes.sql#anchor=idem-bytes)。

**怎么定位**

- 假设：Go 的 `json.Marshal` 不稳定。它对 struct 是按字段顺序输出的，第一次的顺序正是 struct 的顺序。排除。
- 第二次返回的是从 MySQL 读回来的。键按长度排、冒号后面加空格，是 MySQL JSON 类型的规范化存储格式。
- 调用方如果对响应做签名校验、或者按字节比较缓存，会认为两次不是同一个结果。

**怎么解决**：响应列改成 `MEDIUMBLOB`，原样存、原样还。

```sql
response        JSON         NOT NULL,      -- 修复前
ALTER TABLE idempotency_keys MODIFY response MEDIUMBLOB NOT NULL;   -- 修复后
```

**怎么证明修好了**：新加的 [`TestReplayReturnsTheFirstResponseByteForByte`](../internal/api/idempotency_test.go#anchor=replay-bytes-test)，去掉这条 migration 时它是红的（上面的输出就是它打的），加上后绿。

**面试怎么讲**：幂等重放的响应和第一次内容一样、字节不同（S）。我要让重放严格等于第一次（T）。我先排除了 Go 的序列化，定位到 MySQL JSON 列会规范化存储（A）。改成二进制列原样存，再加一个按字节比较的测试，之前红、之后绿（R）。

<a id="q06"></a>

### q06 · runbook 的命令在 zsh 里跑不起来

**类别**：真实踩过的坑（第六章）

**现象**

```text
(eval):1: no such file or directory: /opt/homebrew/bin/docker-compose -p dispatchd
```

**在哪里**：现在的写法在 [`scripts/runbook.sh`](../scripts/runbook.sh#anchor=runbook-shortcuts)；第一版在 [`docs/problems/snippets/shell_v1.sh`](problems/snippets/shell_v1.sh#anchor=runbook-v1)。

**怎么定位**

- 假设：docker-compose 路径不对。报错里的路径是对的，可它把整串「命令加参数」当成了一个文件名。
- bash 会把没加引号的 `$DC` 按空格拆开，zsh 默认不拆。runbook 是写给值班的人在自己终端里贴的，他用什么 shell 我控制不了。

**怎么解决**：变量改成函数，放进 `scripts/runbook.sh`，runbook 第一步 `source` 它。函数在 bash 和 zsh 里行为一样。

**怎么证明修好了**：用 zsh 把 runbook 里的命令逐条跑了一遍：`SQL` 查永久失败、`KAFKA_GROUPS --describe`、`dc up -d --scale worker=2`，全部正常。

**面试怎么讲**：写 runbook 时我在自己终端里实际跑了一遍，发现命令在 zsh 下直接报错（S）。runbook 必须在值班人的任何 shell 里都能用（T）。定位到 zsh 不对变量做分词，我把快捷方式改成函数并单独放一个文件（A）。之后 bash 和 zsh 都验证过（R）。

<a id="q07"></a>

### q07 · 一个 worker 读两个 topic：email 被限速，push 也跟着慢

**类别**：真实踩过的坑（第七章，跑仓库的压测时发现）

**现象**：压测每秒一百二十个通知，一半 email、一半 push。email 的令牌桶是每秒四十个，push 是每秒八十个。按设计 push 应该不受影响，可实测：

```text
email {'p50': 4729.971, 'p95': 10406.683, 'p99': 11970.999}   （毫秒，提交到送达）
push  {'p50': 4975.233, 'p95': 11713.705, 'p99': 13035.157}
```

push 和 email 一样慢。README 里「一个 channel 一个 topic，email 出事只堵 email」那句话，被自己的压测推翻了。

**在哪里**：现在是 [`internal/worker/consumers.go` 的 `NewConsumers`](../internal/worker/consumers.go#anchor=per-channel-consumers)，每个 channel 一个消费者；修复前在 [`docs/problems/snippets/worker_main_v1.go`](problems/snippets/worker_main_v1.go#anchor=one-consumer-v1)。

**怎么定位**

- 假设一：push 的令牌桶配错了。看 claimed 事件里的 `bucket_rate`，push 是八十每秒，token 等待几乎为零。排除。
- 假设二：push 的消息在 Kafka 里排队。push topic 的分区确实有 lag，说明 worker 根本没去读。
- 读消费循环：一次 poll 返回两个 topic 的记录，`handleAll` 要等整批都处理完才提交、才下一次 poll。这一批里的 email 在等 email 的令牌，push 就跟着等；而且并发槽也被等令牌的 email 占着。
- 写一个专门的测试复现：email 限五每秒、积压四十个，同时来二十个 push。修复前 push 全部送达要 1.95 秒。

**怎么解决**

修复前：一个客户端、一个消费组、两个 topic。

```go
kc, err := worker.ConsumerClient(brokers, "dispatchd-workers", topics.All(), session, onAssigned)
```

修复后：每个 channel 一个客户端、一个消费组（`dispatchd-workers.email`、`dispatchd-workers.push`）、一个 poll 循环。它们共用同一个 Deliverer、同一个延迟堆。

**怎么证明修好了**：[`TestEmailBacklogDoesNotDelayPush`](../internal/harness/isolation_test.go#anchor=isolation-test)：

```text
修复前：all 20 push delivered 1.95s after the worker started … --- FAIL: push waited 1.94526275s behind the email backlog
修复后：all 20 push delivered 200ms after the worker started; 39 of 40 emails still waiting for tokens … PASS
```

同一个压测（每秒一百二十个，`docs/results/loadtest-120rps.json`）修复后再跑：email 仍然排队（每秒六十个进来、四十个出去），push 的 p50 9.5 毫秒、p99 44.6 毫秒。

**面试怎么讲**：压测时我发现 push 和 email 一样慢，可 push 的限速明明宽得多（S）。我要让一个 channel 的问题只影响它自己（T）。我从 Kafka lag 看出 push 根本没被读，再读消费循环，找到整批等待造成的队头阻塞，写了一个能复现的测试（A）。拆成每个 channel 一个消费循环后，同样场景 push 从将近两秒降到两百毫秒（R）。

<a id="q11"></a>

### q11 · 第一次在 GitHub 上跑 CI，重试风暴测试红了

**类别**：真实踩过的坑（推送之后，CI 第一次跑就抓到的）

**现象**：仓库推到 GitHub，第一次 CI 的 go job 失败，只有一个测试红：

```text
--- FAIL: TestRetriesAfterAnOutageDoNotArriveAsOneSpike (7.31s)
    retry_test.go:143: 200 notifications failed together; after recovery: busiest 100 ms 33 requests, 109 rejected with 429
    retry_test.go:145: provider requests in total: 755; all delivered after 5.98s
    retry_test.go:147: retry storm: the recovering provider had to reject 109 requests (limit 60)
```

同一个测试在本机开着 jitter 一直是绿的（那次是 29 个被拒）。

**在哪里**：[`internal/harness/retry_test.go` 的 `TestRetriesAfterAnOutageDoNotArriveAsOneSpike`](../internal/harness/retry_test.go#anchor=storm-test)；修复前的写法冻结在 [`docs/problems/snippets/storm_v2_test.go`](problems/snippets/storm_v2_test.go#anchor=storm-v2)。

**怎么定位**

- 假设一：jitter 在 CI 上没生效。日志里每个通知都送到了，按 100 ms 统计的请求分布是一条往下走的曲线，不是关掉 jitter 时那种「一波、停、再一波」。排除。
- 假设二：判据本身有问题。上限 60 是在本机上调出来的（[q04](#q04) 已经因为本机负载放宽过一次）。被拒多少，取决于同一个 100 ms 窗口里挤进来多少请求，这跟机器的速度和负载有关。换一台机器，同样的随机退避，数字就不一样。
- 结论：要证明的不是「被拒少于某个数」，而是「jitter 比不加 jitter 好得多」，而且要在同一台机器上比。

**怎么解决**

修复前：一个写死的上限。

```go
if rejected > n*3/10 {
    t.Fatalf("retry storm: the recovering provider had to reject %d requests (limit %d)", rejected, n*3/10)
}
```

修复后：同一个测试里，把 jitter 关掉再跑一遍同样的风暴当基准，开着 jitter 的被拒数必须不到基准的一半。

```go
was := config.Sabotaged(config.NoJitter)
config.SetSabotage(config.NoJitter, true)
_, base, _, _, _ := stormPeak(t, n)
config.SetSabotage(config.NoJitter, was)
if rejected*2 > base { t.Fatalf(...) }
```

失败对照还活着：在 `DISPATCHD_SABOTAGE=jitter` 下两遍都没有 jitter，数字差不多，测试照样红。

**怎么证明修好了**（`scripts/contrasts.sh`，输出在 `docs/results/contrast-jitter-*.txt`）：

```text
开 jitter：39 rejected with 429；same storm with jitter off: 363 rejected → PASS
关 jitter：414 rejected；same storm with jitter off: 422 rejected → FAIL（not under half）
```

修复后推上去，GitHub 上的 CI 是绿的。

**面试怎么讲**：项目推上 GitHub 后，CI 第一次跑就有一个重试风暴测试红了，本机却一直是绿的（S）。我要判断是代码坏了还是测试不稳（T）。我看 CI 日志里的请求分布，确认 jitter 在起作用，问题在于用一个本机调出来的绝对阈值去衡量性能；于是改成在同一台机器上跟关掉 jitter 的基准比（A）。本机三十九对三百六十三通过，失败对照照样变红，CI 也绿了（R）。

## 生产上的典型事故

<a id="q08"></a>

### q08 · 调用方超时重试，用户收到两封一样的邮件

**类别**：生产典型事故（失败对照一：`DISPATCHD_SABOTAGE=idempotency`）

**现象**

```text
--- FAIL: TestRetryAfterTimeoutSendsOnce
    idempotency_test.go:138: notifications stored: 2, provider deliveries: 2
    idempotency_test.go:140: the provider delivered 2 emails for one signup (notifications stored: 2)
```

**在哪里**：[`internal/api/create.go` 的 `createOnce`](../internal/api/create.go#anchor=idempotency)；关掉保护的那条路径是 [`idem-sabotage`](../internal/api/create.go#anchor=idem-sabotage)。测试是 [`TestRetryAfterTimeoutSendsOnce`](../internal/api/idempotency_test.go#anchor=timeout-retry-test)。

**怎么定位**

- 用户投诉收到两封。先看是一个通知发了两次，还是有两个通知。查库：同一个收件人、同一个模板，两个 id，创建时间差一秒。是两个通知。
- 两个通知说明 API 收了两次请求。调用方的日志里，第一次是客户端超时，第二次是重试。服务端其实两次都处理成功了。
- 所以问题不在投递，在入口：服务端没办法认出「这是同一件事的第二次请求」。

**怎么解决**：入口强制要 `Idempotency-Key`；事务里先插 `idempotency_keys`，靠主键冲突挡住第二次，返回第一次的响应。不先查后插，因为重试经常和第一次并发。

**怎么证明修好了**：同一个测试，打开保护 `notifications stored: 1, provider deliveries: 1`；另有并发测试，二十个同 key 请求只落一行。

**面试怎么讲**：调用方超时后重试，用户收到重复通知（S）。要保证同一个业务动作只发一次（T）。我用幂等键加唯一约束在入口去重，冲突时返回第一次的响应，体不一样返回 422（A）。用模拟超时的测试证明：关掉保护送达两次，打开后一次（R）。

<a id="q09"></a>

### q09 · provider 恢复的那一刻，被积压的重试一起打挂

**类别**：生产典型事故（失败对照二：`DISPATCHD_SABOTAGE=jitter`）

**现象**

```text
关 jitter：
  after recovery: busiest 100 ms 45 requests, 237 rejected with 429
  requests per 100 ms after recovery: 20 38 28 45 28 41 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 13 26 31 27 34 5 0 …
  retry storm: the recovering provider had to reject 237 requests (limit 60)
  --- FAIL
```

**在哪里**：[`internal/backoff/backoff.go` 的 `Next`](../internal/backoff/backoff.go#anchor=full-jitter)；关掉 jitter 时走的是 [`no-jitter`](../internal/backoff/backoff.go#anchor=no-jitter)。

**怎么定位**

- provider 刚恢复，监控上请求量是一根一根的柱子，中间是空的，柱子顶上全是 429。
- 柱子之间的间隔正好是一秒、两秒、四秒：是我们的退避间隔。说明所有通知在同一时刻失败、算出同样的等待、又在同一时刻回来。
- 指数退避只解决了「多久回来」，没解决「大家别一起回来」。

**怎么解决**：full jitter，等待从零到上限之间均匀随机抽。

```go
d = ceil                                   // 修复前：所有人一样
d = time.Duration(rand.Float64() * float64(ceil))   // 修复后
```

**怎么证明修好了**：同一个测试打开 jitter：`busiest 100 ms 20 requests, 29 rejected with 429`，全部送达用了 9.3 秒（关掉时 16.3 秒）。

**面试怎么讲**：一次外部服务故障恢复时，积压的重试成波打过去，又把它打挂（S）。要让重试在时间上错开（T）。我在指数退避上加了 full jitter，并做了积压加限容量的对照测试（A）。关掉 jitter 被拒二百三十七次，打开二十九次（R）。

<a id="q10"></a>

### q10 · 积压一来，provider 回了四百多个 429

**类别**：生产典型事故（失败对照三：`DISPATCHD_SABOTAGE=ratelimit`）

**现象**

```text
100 notifications: map[DELIVERED:100] in 5.72s; provider got 528 requests, 428 of them 429; busiest second 327
the provider answered 429 428 times (limit 5)
--- FAIL
```

**在哪里**：[`internal/ratelimit/bucket.go`](../internal/ratelimit/bucket.go#anchor=token-bucket)；关掉保护时走的是 [`ratelimit-sabotage`](../internal/ratelimit/bucket.go#anchor=ratelimit-sabotage)。测试是 [`TestTokenBucketKeepsUnderTheProviderQuota`](../internal/harness/ratelimit_test.go#anchor=ratelimit-test)。

**怎么定位**

- 告警是 provider 的 429 比例高。先看是不是 provider 降了配额：provider 的配额没变，是每秒二十个。
- 再看我们发了多快：最忙的一秒发了三百二十七个。worker 有三十二个并发，积压一来就全打出去了。
- 429 会被重试，所以最后都送到了，但 provider 白挨了四百多次请求。真实的 provider 会因此限流整个账号。

**怎么解决**：每个 provider 一个令牌桶，设在配额下面一点（十八每秒，突发五个）。在 claim 之前拿令牌，拿不到就等，不让等待吃掉租约。

**怎么证明修好了**：打开令牌桶：`provider got 100 requests, 0 of them 429; busiest second 22`（突发五个加每秒十八个）。

**面试怎么讲**：积压来的时候 worker 并发打满，provider 大量回 429（S）。我们要自己守住对方的配额（T）。我给每个 provider 加了令牌桶，在 claim 之前取令牌（A）。一百条积压，关掉令牌桶有四百二十八个 429，打开后是零（R）。
