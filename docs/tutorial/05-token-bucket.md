# 05 每个 provider 一个令牌桶

> 讲解视频：[p3-token 令牌桶](../videos/p3-token.mp4) · tag：`step-05`

## 这一章解决什么问题？

provider 都有配额。比如每秒二十个请求，超了就回 429。
worker 有三十二个并发，Kafka 里一下子来一百条积压，它会在一瞬间把一百个请求全打过去。
结果五个成功、九十五个 429。这九十五个进延迟堆，再一起回来，再被拒。

429 不是「provider 坏了」，是「你太快了」。该做的是自己先慢下来。

## 假设：用一个固定间隔的 ticker 发？

每五十毫秒放一个请求出去，速率是对了。
可 provider 一般允许短时间的突发（burst）：空闲了一会儿之后，一下来五个是可以的。
ticker 浪费了这个余量，空闲之后第一个请求也要排队。

## 令牌桶怎么工作？

`internal/ratelimit/bucket.go`：
- 桶里最多存 Burst 个令牌，每秒补 Rate 个；
- 每个请求先拿一个令牌。有就直接发；
- 没有的时候，令牌数**可以变成负数**：负几就是前面排着几个人。
  这个调用者要等 `-tokens / Rate` 秒。于是排队的人自动按 `1/Rate` 均匀错开，不用另一个 goroutine 去调度。

```text
Rate=10, Burst=3，连着来六个：
  等待 0, 0, 0, 100ms, 200ms, 300ms
```

## 令牌在哪一步拿？

在 **claim 之前**（`Deliverer.Deliver`）。
假设先 claim 再等令牌：积压很多的时候，等令牌可能要好几秒，租约（十五秒）一直在走。
等太久，租约过期，别的 worker 以为它死了，会再发一次。
所以先等令牌，拿到了再 claim。如果 claim 输了（别人已经在发），把令牌还回去（`Refund`）。

拿令牌的数据写进 claimed 事件：等了多久、桶里还剩几个、速率和容量。learn 页面那一步就显示这个。

## 数字怎么配？

比 provider 的配额低一点。测试里 provider 是每秒二十个、突发五个，我们的桶是十八、五。
compose 里默认 email 四十每秒、push 八十每秒，可以用 `DISPATCHD_RATE_EMAIL` 这类环境变量改。

注意这是**每个 worker 一个桶**。三个 worker 就是三倍的速率。要严格守住全局配额，桶得放到共享的地方（比如 Redis）。README 的局限里写了。

## 真跑一次：能失败的对照

`TestTokenBucketKeepsUnderTheProviderQuota`（`internal/harness/ratelimit_test.go`）：一百条积压一次到达。

```text
开着令牌桶：
  100 notifications: map[DELIVERED:100] in 5.41s; provider got 100 requests, 0 of them 429; busiest second 22; longest token wait 1762 ms
  --- PASS
关掉（DISPATCHD_SABOTAGE=ratelimit）：
  100 notifications: map[DELIVERED:100] in 5.72s; provider got 528 requests, 428 of them 429; busiest second 327; longest token wait 0 ms
  --- FAIL: the provider answered 429 428 times (limit 5)
```

关掉令牌桶以后，最后也全送到了（429 会按退避重试），总时间也差不多。
可 provider 多挨了四百二十八次请求，最忙的一秒是三百二十七个，是它配额的十六倍。真实的 provider 会因此封你的 key。
开着令牌桶：最忙的一秒二十二个（桶里攒着的五个加上每秒补的十八个），一次 429 都没有。

## 和上一章的 jitter 测试是什么关系？

jitter 的测试里没有开令牌桶，为的是单独看 jitter 的作用。
开了令牌桶，同步回来的重试会在桶里排队，provider 看不到尖峰，代价是排队的时间。两层保护各管一件事：jitter 让大家别同时想发，令牌桶保证就算同时想发也发不出去。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-05
docker-compose -p dispatchd up -d --build --wait
./scripts/test.sh
DISPATCHD_SABOTAGE=ratelimit go test -run TokenBucket -v ./internal/harness   # 看它变红
```
