# 01 最小能跑的通知服务

> tag：`step-01`

## 这一章解决什么问题？

调用方要发一封邮件、一条推送。它不想关心 provider 是谁、挂没挂、要重试几次。
它只想说一句「帮我发」，然后能查到「发出去没有」。

所以第一版只做三件事：收请求、存 MySQL、投递并记下结果。

## 为什么先要假的 provider？

真实的邮件和推送 provider 不会按你的需要出故障。
可后面每一章要测的，恰恰是故障：5xx、429、地址无效、设备 token 失效、慢响应。

`internal/fakeprovider` 是一个可以切换故障模式的 HTTP 服务，同一个二进制跑两份：email 在 8851，push 在 8852。
它把收到的每个请求都记下来（时间、通知 id、第几次尝试、返回码），后面用它数重复、看重试风暴。

## 一个通知在库里长什么样？

`notifications` 一行就是一个通知：channel、收件人、模板、参数、状态、尝试次数、最后一次错误。
`delivery_attempts` 每次尝试一行。`notification_events` 是只追加的事件日志，记每一次状态转移 from→to。

状态机这一章只有四个：`QUEUED → SENDING → DELIVERED | FAILED_PERMANENT`。
临时失败先记成 `FAILED_TEMPORARY`，第四章再加重试。

## 怎么区分「再试一次可能成功」和「再试也没用」？

假设：所有失败都重试。
那地址写错的邮件会被重试到天荒地老，还会把 provider 的配额吃光。

所以 `internal/provider` 的 `Classify` 按返回码分类：
- 2xx：成功；
- 5xx、408、超时、连接错误：临时失败；
- 429：被限流，带上 `Retry-After`；
- 其他 4xx：永久失败。同一个请求再发一遍也不会成功。

永久失败要写清楚「人该做什么」，比如 `device_token_unregistered` 对应「把这个 token 从用户资料里删掉」。

## 真跑一次

```bash
docker-compose -p dispatchd up -d --build --wait
curl -s -XPOST localhost:8850/v1/notifications -H 'Authorization: Bearer dk_demo_local_only' \
  -d '{"channel":"email","recipient":"ada@example.com","template":"welcome","params":{"name":"Ada"}}'
```

```text
{"id":"ntf_01M89WZ7CZ9XG5QWVPMJNF15YX","status":"QUEUED","attempts":0,...}
GET /v1/notifications/ntf_01M89WZ7CZ9XG5QWVPMJNF15YX
  attempts: [{"attempt":1,"outcome":"DELIVERED","http_status":200,"latency_ms":3.87}]
  events:   accepted →QUEUED · claimed QUEUED→SENDING · attempt SENDING→DELIVERED
email provider /admin/requests:
  [{"ms":13450,"notification_id":"ntf_01M89WZ7CZ9XG5QWVPMJNF15YX","attempt":1,"status":200}]
```

## 这一版的问题在哪？

投递靠进程内的一个 channel。进程一崩，channel 里没发的全丢了。
调用方超时重试，会发出第二封一模一样的邮件。
这两个问题分别是第三章和第二章要解决的。

## 测试

- `TestSubmitAndDeliver`：提交 → DELIVERED，provider 正好收到一次。
- `TestPermanentFailureIsRecordedWithAnAction`：失效的推送 token → `FAILED_PERMANENT`，`last_error` 里有 410、错误码和建议动作。
- `internal/provider`：返回码分类、超时算临时失败。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-01
docker-compose -p dispatchd up -d --build --wait
./scripts/test.sh
```
