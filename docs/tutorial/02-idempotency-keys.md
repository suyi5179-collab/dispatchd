# 02 幂等键：调用方重试不再重复发

> tag：`step-02`

## 这一章解决什么问题？

调用方发请求，等了一秒没回音，超时了。它不知道服务端到底做没做。
最常见的处理是：用同样的参数再发一次。
如果服务端第一次其实已经做了，用户就会收到两封一模一样的邮件。

这一章让「再发一次」变得安全：带同一个 `Idempotency-Key` 的请求，只会产生一个通知。

## 假设：先查后插行不行？

```text
SELECT * FROM idempotency_keys WHERE key=?   -- 没有
INSERT INTO notifications ...                 -- 插一个
INSERT INTO idempotency_keys ...
```

两个请求同时到，两个 SELECT 都看到「没有」，于是都插了一个通知。
重试恰恰最容易并发：客户端超时重试的时候，第一个请求往往还在处理。

## 那靠什么？

靠数据库的唯一约束。`idempotency_keys` 的主键是 `(client_id, idem_key)`。
建通知的事务里**第一条语句就是插这一行**，不先查：

1. 插成功了，说明我是第一个，接着插 notification，一起提交；
2. 插的时候报 1062（duplicate key），说明别人已经拿到了这个 key。
   InnoDB 会让第二个事务等第一个提交，再报错，所以不会读到半成品。
   这时读出那一行：请求指纹一样，就把第一次的响应原样返回，加上 `Idempotent-Replayed: true`；
   指纹不一样，说明调用方拿同一个 key 发了不同的内容，返回 422。

请求指纹是请求体的 SHA-256。`json.Marshal` 对 map 的 key 排序，所以参数顺序不影响指纹。

代码在 `internal/api/create.go` 的 `createOnce`。

## 真跑一次

```text
POST /v1/notifications  Idempotency-Key: signup-ada-1
  HTTP/1.1 202 Accepted
  {"id":"ntf_01M89X39AFZ93C0WGTXH4B776T", ...}
POST /v1/notifications  Idempotency-Key: signup-ada-1   （同样的请求体）
  HTTP/1.1 202 Accepted
  Idempotent-Replayed: true
  {"id": "ntf_01M89X39AFZ93C0WGTXH4B776T", ...}
POST /v1/notifications  Idempotency-Key: signup-ada-1   （收件人换成 bob）
  422 {"error":"Idempotency-Key was already used with a different request body", ...}
POST /v1/notifications  （没带 key）
  400 {"error":"Idempotency-Key header is required (at most 255 characters)"}
```

注意第二次的响应体：空格和字段顺序都变了。MySQL 的 JSON 列会把存进去的 JSON 规范化。
内容一样，字节不一样。见 `docs/problems.md` 的 q01。

## key 要存多久？

调用方的重试一般在几分钟之内。这里存 24 小时，API 每十分钟删一次过期的 key。

## 怎么证明它真的有用？

`TestRetryAfterTimeoutSendsOnce` 模拟一次真实的超时：
调用方 100 毫秒就放弃，服务端把活干完了，但响应 300 毫秒后才回来。调用方拿同一个 key 再发一次。
断言：provider 只投递了一次。

能失败的对照：`DISPATCHD_SABOTAGE=idempotency` 让服务忽略 key。

```text
$ DISPATCHD_SABOTAGE=idempotency go test -run 'RetryAfterTimeout|SameKey|Concurrent' ./internal/api
--- FAIL: TestSameKeySameBodyReturnsTheSameNotification
    same key gave two notifications: ntf_01M89X2JC0SH5P4ZYQB6YHM6CQ and ntf_01M89X2JC9N0QCSWMTBT6GQ873
--- FAIL: TestConcurrentSameKeyStoresOneRow
    request 1: 202 "ntf_01M89X2JMHWEP2ZPPQCFFEP43D" (first was "ntf_01M89X2JMHXR4VFWD4BG4A23A7")
--- FAIL: TestRetryAfterTimeoutSendsOnce
    notifications stored: 2, provider deliveries: 2
```

打开保护：同一个测试 `notifications stored: 1, provider deliveries: 1`，PASS。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-02
docker-compose -p dispatchd up -d --build --wait
./scripts/test.sh
```
