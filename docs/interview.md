# 面试题

围绕这个仓库的 28 道题。每题先给一句话答案，再展开，最后是追问和答法。
答案里的数字都是这个仓库自己跑出来的（来源写在旁边）；简历上的数字是在公司里的结果，两者分开讲，见最后一节。

## 项目概述

### 讲讲这个项目（1 分钟版）

**一句话答案**：dispatchd 是一个 Go 写的通知服务：调用方通过 HTTP 提交邮件或推送，状态存在 MySQL，经 Kafka 异步交给 worker 投递到 provider，重点解决重复、重试和限流三件事。

**展开**：
- 入口要求 `Idempotency-Key`，靠 `idempotency_keys` 表的主键挡住超时重试带来的重复（[`create.go`](../internal/api/create.go#anchor=idempotency)）。
- 通知和 outbox 在同一个事务里写，relay 再把 outbox 搬进 Kafka，所以不会出现「库里有、队列里没有」。
- worker 消费后先 claim 再投递，再写库，最后提交 offset：at-least-once 加上按（通知 id，第几次尝试）去重。
- 临时失败按指数退避加 full jitter 重试，等待放在一个最小堆延迟队列里，只在堆顶到期时醒一次；永久失败直接终止，并写下该怎么处理。
- 每个 provider 一个令牌桶，拿不到令牌就等，而不是打过去吃 429。
- 三层保护各有一个能失败的对照：关掉之后对应的测试会变红（README 里贴了两次的输出）。

**追问**：为什么是这几个点，不是熔断器？
答：通知的典型事故是「发重了」「重试把对方打挂」「超了对方配额」。熔断解决的是「对方挂了我别等」，在异步投递里，退避和延迟堆已经让 worker 不会阻塞在一个挂掉的 provider 上。

### 一条通知从提交到送达，经过哪些步骤？

**一句话答案**：API 收下并幂等检查 → 同一个事务写 notification 和 outbox → relay 发 Kafka → worker 拿令牌、claim、投递 → 成功就 DELIVERED，临时失败进延迟堆等重试，永久失败直接终止。

**展开**：
- learn 页面把这十步做成了时间线，每一步亮起，是因为后端真的写下了那一行（[`learn.go`](../internal/api/learn.go#anchor=learn-stages)）。
- 状态机：`QUEUED → SENDING → DELIVERED | RETRY_SCHEDULED → SENDING … | FAILED_PERMANENT | FAILED_EXHAUSTED`。
- 每次 from→to 都写进 `notification_events`，GET 接口直接返回这份日志。

**追问**：learn 页面那个「第一次 503」是假的吗？
答：是注入的，但链路是真的。样例会让 email provider 对这个收件人的第一次请求返回 503（`fail_first`），之后的重试、延迟堆、唤醒都是真实代码在跑。

### 这个项目里你最满意的设计是什么？

**一句话答案**：把「谁来做这一次尝试」交给 MySQL 的一次条件更新（claim），让 Kafka 的重复投递、worker 重启、两个 worker 抢同一条消息都变成同一个问题。

**展开**：
- `Claim` 先 `SELECT … WHERE id=? AND attempts=? FOR UPDATE`，状态是可认领的才改成 SENDING、`attempts+1`（[`delivery.go`](../internal/store/delivery.go#anchor=claim)）。
- 第二个拿着同一条消息的 worker 会发现 attempts 已经变了，直接跳过。
- 崩溃留下的 SENDING 由租约兜底（[`ExpireLease`](../internal/store/delivery.go#anchor=expire-lease)）。

**追问**：为什么不用 Kafka 的 exactly-once？
答：Kafka 的事务只管 Kafka 内部的读写。我们的副作用是调用外部 provider 的 HTTP 接口，事务管不到。去重只能落在我们自己的状态上。

## 设计取舍

### 为什么幂等靠唯一约束，不靠先查后插？

**一句话答案**：重试和第一次请求经常是并发的，先查后插两个请求都会看到「没有」，只有数据库的唯一约束能决定谁是第一个。

**展开**：
- 事务里第一条语句就是 `INSERT idempotency_keys`，主键是（client_id, idem_key）。
- InnoDB 会让第二个插入等第一个事务提交，然后报 1062，所以不会读到半成品。
- 拿到 1062 就读出那一行：指纹一样返回第一次的响应，不一样返回 422。
- `TestConcurrentSameKeyStoresOneRow`：二十个并发同 key 请求，最后只有一行，全部拿到同一个 id。

**追问**：第一个事务回滚了呢？
答：第二个的插入会成功，它就成了第一个，这是对的。多个等待者在第一个回滚时可能互相死锁，所以 `create` 对 1213 重试三次。

### 为什么用 outbox，不在 API 里直接发 Kafka？

**一句话答案**：MySQL 和 Kafka 没法放进一个事务，outbox 让「通知存在」和「消息会被发出」变成同一个事务里的两行。

**展开**：
- 直接发：写完库、发 Kafka 前崩了，通知永远没人处理；先发后写，worker 可能拿到库里不存在的通知。
- relay 用 `FOR UPDATE SKIP LOCKED` 取未发布的行，等 Kafka 确认后标记（[`PublishBatch`](../internal/store/outbox.go#anchor=relay-batch)）。
- 代价是多一次写和一点延迟；API 提交后会叫醒 relay，正常路径没有轮询间隔。

**追问**：relay 会重复发吗？
答：会。Kafka 确认之后、事务提交之前崩了，这一行会再发一次。这是 at-least-once，重复由 worker 的 claim 去掉。

### 为什么选 full jitter，不选 equal jitter 或 decorrelated jitter？

**一句话答案**：full jitter 把重试摊得最开，恢复中的 provider 看到的峰值最低；它的代价（个别重试很早）对通知服务无害。

**展开**：
- 等待 = 随机(0, min(cap, base × 2^(n-1)))（[`backoff.go`](../internal/backoff/backoff.go#anchor=full-jitter)）。
- equal jitter 保底等一半，峰值更高；decorrelated 要记住上一次的等待，多一个状态。
- 429 的 `Retry-After` 加在上面当下限。

**追问**：你怎么知道 jitter 真的有用？
答：`TestRetriesAfterAnOutageDoNotArriveAsOneSpike`：两百个通知积压后同时失败，provider 恢复后每秒只扛一百个。开 jitter 被拒二十九个，关掉二百三十七个（`docs/results/contrast-jitter-*.txt`）。

### 为什么用最小堆延迟队列，不定时扫表？

**一句话答案**：扫表要么浪费（没到期的每次都扫），要么不准（最多晚一个扫描间隔）；堆加一个定时器，只在堆顶到期时醒一次。

**展开**：
- 定时器永远设在堆顶的到期时间；`Push` 只有在新元素比堆顶早时才踢醒 goroutine（[`heap-push`](../internal/delayq/delayq.go#anchor=heap-push)、[`heap-run`](../internal/delayq/delayq.go#anchor=heap-run)）。
- 单元测试证明：乱序插入有序弹出；更早的元素会提前唤醒；空堆零次唤醒（[`delayq_test.go`](../internal/delayq/delayq_test.go#anchor=heap-tests)）。
- learn 页面里能看到 `late_ms`：到期后一毫秒左右就醒了。

**追问**：堆在内存里，进程死了呢？
答：到期时间先写进 MySQL（`next_attempt_at`），再进堆。worker 启动和每次分到分区时都从 MySQL 重新装堆（[`Reload`](../internal/worker/deliver.go#anchor=reload)），[`TestNewWorkerReloadsRetriesFromMySQL`](../internal/harness/retry_test.go#anchor=reload-test) 证明了这一点。

### 为什么重试不再发回 Kafka？

**一句话答案**：Kafka 没有「到点再投递」，把重试塞回去要么立刻被消费，要么得再造一套延迟 topic；MySQL 加内存堆已经能按时间唤醒，还能被重启恢复。

**展开**：
- 一条 Kafka 消息只对应「第一次尝试」；它的 offset 在第一次结果写进 MySQL 后就提交。
- 重试的归属在 MySQL：`RETRY_SCHEDULED` 加 `next_attempt_at`。
- 多个 worker 的堆里可能有同一个通知，claim 保证只有一个能发。

**追问**：重试量很大时，内存堆会不会太大？
答：每项只有 id、时间、类型、次数，几十个字节，一百万项也就几十 MB。真正的上限是 MySQL 的写入量，见扩展类问题。

### 令牌桶为什么在 claim 之前拿？

**一句话答案**：等令牌可能要几秒，如果先 claim，等待会吃掉租约，租约一过别的 worker 会以为它死了而重发。

**展开**：
- 顺序是：读状态 → 等令牌 → claim → 投递（[`Deliver`](../internal/worker/deliver.go#anchor=deliver)）。
- claim 输了就把令牌还回去（`Refund`）。
- 令牌数允许变成负数，负几就是前面排着几个人，等待自动按 1/rate 错开（[`bucket.go`](../internal/ratelimit/bucket.go#anchor=token-bucket)）。

**追问**：三个 worker 就是三倍速率，怎么办？
答：现在是每个 worker 一个桶，部署时按「配额 ÷ worker 数」配。要严格守住全局配额，需要把桶放到共享存储（Redis 的令牌桶脚本），代价是每次投递多一次网络往返。

### 为什么 topic 按 channel 分？

**一句话答案**：email provider 出事时只堵 email 的 topic，push 不受影响。

**展开**：
- key 是通知 id，同一个通知总在同一个分区，三个分区让最多三个 worker 并行。
- 通知之间不需要顺序，所以 key 的作用主要是均匀分布。
- 光分 topic 不够：一开始一个 worker 用一个 poll 循环读两个 topic，压测时 email 被限速，push 也跟着慢到五秒。现在每个 channel 一个消费组、一个循环（[`NewConsumers`](../internal/worker/consumers.go#anchor=per-channel-consumers)），见 [q07](problems.md#q07)。

**追问**：一个大客户刷屏会不会饿死别人？
答：会，现在没有按租户的隔离。可以按租户加权的令牌桶，或者给大客户单独的 topic。

### 鉴权为什么用 API key，不用 JWT？

**一句话答案**：调用方是后端服务，不是用户；API key 简单、可吊销，库里只存 SHA-256。

**展开**：
- `Authorization: Bearer <key>` 或 `X-API-Key`，按 hash 查 `api_clients`。
- 默认 key 是公开的 demo 占位值，真实环境用 `DISPATCHD_API_KEYS` 覆盖。
- 幂等键按 client 隔离，两个调用方用同一个 key 不会互相影响。

**追问**：管理接口怎么防？
答：`role=admin` 才能切 provider 故障模式。上生产要换成单独的内部网络或 mTLS。

## 正确性和故障

### worker 在投递中途被 kill -9，会怎样？

**一句话答案**：不丢也不重：没提交的消息会被新 worker 再读到并被 claim 挡掉；在途的尝试等租约到期后重发，provider 用幂等键合并掉已经收到的那几个。

**展开**：
- `TestWorkerKilledMidBatchLosesNothingAndSendsNothingTwice`：三百个通知，四分之一时 SIGKILL（[`restart_test.go`](../internal/harness/restart_test.go#anchor=restart-test)）。
- 一次实测：八十条 Kafka 重复被 claim 跳过，八个租约到期重发，其中三个 provider 已经收到，靠幂等键合并，三百个全部送达、没有人收到两次。
- 也就是说「不重」有两层：claim 挡住 Kafka 的重复，provider 幂等键挡住「发出去了但没记下来」的窗口。

**追问**：provider 不支持幂等键呢？
答：那一小段窗口里的重复去不掉。这是外部副作用的本质：确认和记录之间总有空隙。能做的是把窗口缩短，并且用指标（`dispatchd_lease_expired_total`）知道它发生了几次。

### provider 全挂五分钟，会怎样？

**一句话答案**：通知在延迟堆里按退避等待，provider 恢复后陆续送达；超过重试预算的变成 `FAILED_EXHAUSTED`，可以按 runbook 重新驱动。

**展开**：
- 默认 1 秒起、每次翻倍、封顶两分钟、最多八次，上限加起来大约四分钟。
- `TestProviderOutageThenRecoveryDeliversEverything`：五十个通知遇到全量 503，恢复后全部送达。
- runbook 的「A provider is down」一节写了重新驱动的 SQL（[`runbook.sh`](../scripts/runbook.sh#anchor=runbook-shortcuts)）。

**追问**：为什么不无限重试？
答：无限重试会让「两天前的验证码」在 provider 恢复时发出去。过期的通知比不发更糟，所以要有预算，超了交给人决定。

### 永久失败和临时失败怎么分？

**一句话答案**：2xx 成功；5xx、408、超时、连接错误是临时；429 是被限流；其他 4xx 是永久，再发一次也不会成功。

**展开**：
- 分类在 [`Classify`](../internal/provider/provider.go#anchor=classify)。
- 永久失败记下可操作的信息：错误码、原文、建议动作，比如 `device_token_unregistered` 对应「把这个 token 从用户资料里删掉」。
- `TestPermanentFailureIsRecordedWithAnAction` 检查 410、错误码和建议动作都在。

**追问**：有的 provider 用 400 表示临时问题怎么办？
答：分类表要按 provider 定制，按错误码而不是只看状态码。现在的实现按 code 给建议动作，扩展成按 code 分类是同一个位置。

### 同一个幂等键、不同的请求体，返回什么？

**一句话答案**：422，并告诉调用方这个 key 已经对应了哪个通知。

**展开**：
- 指纹是请求体的 SHA-256；`json.Marshal` 对 map 排序，参数顺序不影响指纹。
- 这通常是调用方的 bug（复用了 key），静默接受会让它以为第二个请求成功了。

**追问**：重放时返回的字节和第一次一样吗？
答：一样，但第一版不是。响应存在 MySQL 的 JSON 列里，被规范化了。改成二进制列后，`TestReplayReturnsTheFirstResponseByteForByte` 才变绿，见 [q05](problems.md#q05)。

### Kafka 挂了会怎样？

**一句话答案**：API 照常收下通知（写 MySQL 不受影响），outbox 积压；Kafka 恢复后 relay 每秒一次的扫描把它们补发出去。

**展开**：
- `dispatchd_outbox_unpublished` 会上涨，`DispatchdOutboxBacklog` 告警。
- 这正是 outbox 的价值：对调用方来说，Kafka 宕机不等于服务不可用。

**追问**：MySQL 挂了呢？
答：那就真的不可用了，API 返回 500。MySQL 是唯一的事实来源，这是设计上的选择。

### 你遇到过最难的问题是什么？

**一句话答案**：一个关掉保护还是绿的对照测试：它看起来在测 jitter，其实什么也没测。

**展开**：
- 第一版风暴测试没让失败同时发生，provider 恢复后也不限容量，于是开关 jitter 结果一样。
- 补上「积压后同时失败」和「恢复后限容量」两个前提，判据改成被拒的请求数，测试才活过来。
- 之后又发现余量不够，全量并行跑时偶尔红，又调了一次参数。
- 详细过程见 [q03](problems.md#q03) 和 [q04](problems.md#q04)。

**追问**：还有别的吗？
答：错误被吞掉的那次也很典型：测试只说超时，真正的 SQL 错误被 `_ = c.Run(ctx)` 扔了，见 [q01](problems.md#q01)；还有 Close 卡死，见 [q02](problems.md#q02)。

## 扩展

### 流量放大 10 倍，瓶颈在哪？

**一句话答案**：先是 provider 配额（令牌桶会让我们主动排队），然后是 MySQL 的写入：每次尝试要写好几行。

**展开**：
- 一次成功投递大约写：accepted 事件、outbox、published 事件、consumed 事件、claim（更新加两次插入）、finish（两次更新加一次插入）。
- 风暴测试里能看到这个上限：两百个尝试摊在零点几秒里，是被数据库写入速度摊开的，不是 Kafka。
- 第一步是去掉非必要的事件写入（consumed、woken 这些是给教学页面的），第二步是批量提交。

**追问**：Kafka 这边要改什么？
答：分区数。现在每个 topic 三个分区，最多三个 worker 并行；十倍流量要更多分区，worker 跟着加。

### 放大 100 倍呢？

**一句话答案**：单个 MySQL 扛不住，要按通知 id 分片，事件日志移出主库。

**展开**：
- notifications 和 idempotency_keys 按 client 或 id 哈希分库，outbox 跟着通知走。
- 事件日志改成写 Kafka 或列存，主库只存当前状态。
- 延迟堆的总量跟着变大，可以改成分层：近的在内存，远的在 MySQL 里按时间分桶。

**追问**：幂等键分片后还能用唯一约束吗？
答：能，只要按 client 分片，同一个 client 的 key 落在同一个分片里。

### 延迟堆里有一百万个重试，worker 重启要多久？

**一句话答案**：重启时要从 MySQL 读一百万行，秒级到十几秒，期间这些重试不会丢，只是会晚一点。

**展开**：
- `Reload` 现在一次性读全量；可以只读最近一段时间内到期的，远的分批装。
- 堆操作是 log n，一百万项的单次插入是微秒级。

**追问**：多个 worker 同时 Reload 会重复吗？
答：会都装进各自的堆，到期时都去 claim，只有一个成功。浪费一次查询，换来不需要协调。

## 测试

### 你怎么证明它是对的？

**一句话答案**：每个保护都有一个故障场景测试，并且每个测试都做过「关掉保护它会不会红」的对照。

**展开**：
- 重复请求：模拟超时重试、二十并发同 key、字节一致的重放。
- provider 故障：全量 503 后恢复、一直 503 直到用完预算、永久失败记录动作。
- worker 重启：子进程 SIGKILL，再起一个，数 provider 收到的请求。
- 延迟堆：乱序、提前唤醒、零忙等，都有单元测试。
- 测试用真的 MySQL 和 Kafka（每个测试一个临时库和一对临时 topic），连不上就失败，不会悄悄跳过。

**追问**：为什么集成测试连不上数据库要失败，不跳过？
答：跳过和通过在 CI 里长得一样。要跳过必须显式设 `DISPATCHD_SKIP_INTEGRATION=1`。

### 那个「能失败的对照」是怎么设计的？

**一句话答案**：三个开关 `DISPATCHD_SABOTAGE=idempotency|jitter|ratelimit`，各自关掉一层保护；同一个测试开着要绿、关掉要红，`scripts/contrasts.sh` 和 CI 都会检查两边。

**展开**：
- 幂等：关掉后 `TestRetryAfterTimeoutSendsOnce` 报 `provider deliveries: 2`。
- jitter：关掉后恢复中的 provider 被拒二百三十七次，开着二十九次。
- 令牌桶：关掉后一百条积压换来四百多个 429，开着是零。
- 第一版 jitter 对照关掉也是绿的，这件事本身证明了对照测试有多必要，见 [q03](problems.md#q03)。

**追问**：开关放在生产代码里不危险吗？
答：它只读环境变量，默认全关；README 和 `.env.example` 都写明只用于测试和演示。更严格的做法是用 build tag 编进测试二进制。

### 前端怎么测？锚点链接怎么保证不过期？

**一句话答案**：Playwright 从界面点一遍；源码链接的行号由脚本从 `@learn` 注释生成，CI 检查生成文件不过期、每个引用都存在。

**展开**：
- e2e 断言十个阶段依次亮起、每一步的真实数据出现、链接格式正确、指向的行确实存在。
- [`source.ts`](../frontend/src/source.ts#anchor=anchor-lookup) 用 commit sha 拼链接，不写死 main。
- [`check_anchors.mjs`](../scripts/check_anchors.mjs#anchor=anchor-refs) 收集前端、文档和视频脚本里引用的所有锚点。

**追问**：为什么锚点注释放在范围外面？
答：这样 GitHub 高亮的是代码本身，不是注释。脚本算出的是两行标记之间的第一行和最后一行。

## 系统设计延伸

### 上生产还缺什么？

**一句话答案**：真实 provider 的适配、全局限流、多租户隔离、事件日志的归档、密钥管理和部署。

**展开**：
- provider：SES/SendGrid/FCM/APNs 的错误码映射、回调（退信、投诉）接收。
- 全局令牌桶，按租户的配额。
- `notification_events` 会无限增长，要分区表或归档。
- API key 放进密钥管理服务，管理接口走内部网络。
- 部署：API 和 worker 分开扩缩，worker 数不超过分区数。

**追问**：用户退订怎么做？
答：发之前查一张 suppression 表（退订、硬退信的地址），命中就直接 `FAILED_PERMANENT`，动作写「用户已退订」。

### 换成 AWS 托管服务怎么改？

**一句话答案**：Kafka 换 MSK 或 SQS，MySQL 换 Aurora，延迟堆可以换成 SQS 的延迟消息或 EventBridge Scheduler，令牌桶放 ElastiCache。

**展开**：
- SQS 的可见性超时就是租约，`DelaySeconds` 最长十五分钟，更长的还是要自己的调度。
- SQS 标准队列是 at-least-once，claim 那一层照样需要。
- SES 自己有发送速率配额，令牌桶按它配。

**追问**：用 SQS 的话 outbox 还要吗？
答：要。MySQL 和 SQS 一样不在一个事务里。

## 简历对应

### 简历上写「幂等键让超时重试不再重复发送」，会怎么被追问？

**一句话答案**：会问怎么防并发、key 存多久、请求体不一样怎么办；这个仓库里三个问题都有对应的代码和测试。

**展开**：
- 并发：唯一约束，不先查后插；`TestConcurrentSameKeyStoresOneRow`。
- 存多久：24 小时，API 每十分钟清一次。
- 体不一样：422。
- 简历上的数字是在公司里的结果；这个仓库里能讲的是：模拟一次超时重试，关掉保护送达两次，打开后一次。

**追问**：你们线上重复率降了多少？
答：那是公司里的结果，按简历讲；这个仓库不复现那个数字，它证明的是机制：同样的场景，有保护一次，没保护两次。

### 「退避加 jitter，永久失败记录可操作的错误」怎么讲？

**一句话答案**：先讲为什么光有退避不够（同时失败就同时回来），再用仓库里的风暴对照给出数字，最后讲永久失败的分类和动作。

**展开**：
- 仓库实测（`docs/results/contrast-jitter-*.txt`）：关 jitter 恢复中的 provider 被拒二百三十七次、全部送达用了 16.3 秒；开 jitter 被拒二十九次、9.3 秒。
- 永久失败的 `last_error` 里有 provider、状态码、错误码、原文和动作。
- runbook 里有一节专门处理永久失败，按错误码给出动作。

**追问**：jitter 会不会让整体变慢？
答：在我们的场景里反而更快（9.3 秒对 16.3 秒），因为没有被拒之后的二次等待。没有容量限制时，jitter 确实会让部分重试晚一些，第一版测试里就看到过。

### 「令牌桶和最小堆延迟队列，不轮询」怎么证明？

**一句话答案**：令牌桶用对照测试证明（关掉后 429 从零涨到四百多），不轮询用延迟堆的唤醒计数证明。

**展开**：
- `TestNoBusyWaiting`：空堆一百毫秒零次唤醒；插一个元素到期，正好一次定时器加一次 kick。
- 线上可以看 `dispatchd_delay_queue_wakeups_total{reason}`：一次重试一次唤醒，第六章那次实测是二十五次临时失败、二十五次定时器唤醒。

**追问**：如果每秒有几千个重试到期呢？
答：到期的会在一次唤醒里一起弹出，唤醒次数不会超过重试个数，通常远少于。

### 「监控队列深度、投递延迟、错误率，写了恢复步骤」怎么讲？

**一句话答案**：三个问题对应三组指标——活卡在哪（outbox、Kafka lag、延迟堆）、慢不慢（端到端和 provider 两个延迟）、谁在出错（按 provider 和错误码），每条告警都指向 runbook 的一节。

**展开**：
- Kafka lag 由 worker 每五秒用 kadm 算一次，就是队列深度。
- 告警用比例不用计数：非成功尝试占比超过百分之二十持续五分钟。
- runbook 的命令我都在 bash 和 zsh 里实际跑过，还因此修了一个 zsh 的坑（[q06](problems.md#q06)）。

**追问**：runbook 里最常用的是哪一节？
答：「A provider is down」：先看错误码判断是不是对方的问题，再决定要不要重新驱动用完预算的通知。
