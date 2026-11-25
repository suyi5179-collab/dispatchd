# 教程：从零做出 dispatchd

七章，按「先做出最小能跑的版本，再一步步加功能」的顺序排。每章对应一个 git tag，可以 checkout 到那一步亲手跑。

| 章 | tag | 解决什么问题 | 讲解视频 |
|---|---|---|---|
| [01 最小能跑的通知服务](01-minimal-service.md) | `step-01` | 收请求、存 MySQL、投递并记下结果 | [p1-intake](../videos/p1-intake.mp4) |
| [02 幂等键](02-idempotency-keys.md) | `step-02` | 调用方超时重试不再重复发 | [p1-intake](../videos/p1-intake.mp4) · [p5-delivered](../videos/p5-delivered.mp4) |
| [03 Outbox、Kafka 和 worker](03-outbox-kafka-workers.md) | `step-03` | 崩溃了也不丢、不重 | [p2-outbox](../videos/p2-outbox.mp4) |
| [04 退避、jitter 和最小堆延迟队列](04-backoff-jitter-delay-queue.md) | `step-04` | 临时失败什么时候重试、怎么错开、谁来叫醒 | [p4-retry](../videos/p4-retry.mp4) |
| [05 令牌桶](05-token-bucket.md) | `step-05` | 守住 provider 的配额 | [p3-token](../videos/p3-token.mp4) |
| [06 观测和 runbook](06-observability-runbook.md) | `step-06` | 活卡在哪、慢不慢、谁在出错 | [p6-ops](../videos/p6-ops.mp4) |
| [07 前端和教学模式](07-teaching-mode.md) | `step-07` | 把一个通知的一生摊开给人看 | [p5-delivered](../videos/p5-delivered.mp4) |

```bash
git checkout step-04      # 任意一章；回到最新版用 git checkout main
```
