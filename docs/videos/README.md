# 讲解视频

每个教学阶段一段短片，`/learn` 页面里每个阶段旁边的「看讲解视频」播放的就是这里的文件。清单见 [`video/parts.json`](../../video/parts.json)：

| 文件 | 内容 | 对应教程 |
|---|---|---|
| [`p1-intake.mp4`](p1-intake.mp4) | 收下请求：幂等键和唯一约束 | — |
| [`p2-outbox.mp4`](p2-outbox.mp4) | 进队列：同一个事务写 outbox，relay 发到 Kafka | — |
| [`p3-token.mp4`](p3-token.mp4) | worker 拿令牌，再认领 | — |
| [`p4-retry.mp4`](p4-retry.mp4) | 临时失败：退避、full jitter、最小堆延迟队列 | — |
| [`p5-delivered.mp4`](p5-delivered.mp4) | 送达，以及同一个幂等键再提交一次 | — |
| [`p6-ops.mp4`](p6-ops.mp4) | 观测与故障演练：重试风暴的对照 | — |

画面由 `video/record.mjs` 在真实页面上录制（会话里的数据都是这次真实请求产生的），红框标出每一步该看的地方；旁白来自 `video/narration.json`。旁白是用作者本人声音在本地克隆合成的（CosyVoice 2，不调任何云端 API），成片用本地 whisper 逐段回验过念的是稿子上的话。

## 日常问题（`problems/`）

每个问题一段代码讲解：先看修复前的写法和现象，再看修复后的写法，讲到哪几行就用红框框出哪几行。内容来自 [`video/problems.walk.json`](../../video/problems.walk.json)（由 `scripts/build_problem_videos.mjs` 按锚点从源码生成），问题的来龙去脉见 [`docs/problems.md`](../problems.md)。

- [`q01-swallowed-error.mp4`](problems/q01-swallowed-error.mp4) — 测试只说十五秒还是排队中，真正的错误被吞了
- [`q02-close-hangs.mp4`](problems/q02-close-hangs.mp4) — 测试跑完不退出，卡在关闭卡夫卡客户端
- [`q03-jitter-test-always-green.mp4`](problems/q03-jitter-test-always-green.mp4) — 关掉 jitter，重试风暴测试还是绿的
- [`q04-storm-margin.mp4`](problems/q04-storm-margin.mp4) — 风暴测试余量不够，并行跑全量时偶尔变红
- [`q05-replay-bytes.mp4`](problems/q05-replay-bytes.mp4) — 同一个幂等键，第二次返回的字节不一样
- [`q06-zsh-word-split.mp4`](problems/q06-zsh-word-split.mp4) — runbook 的命令在 zsh 里跑不起来
- [`q07-head-of-line.mp4`](problems/q07-head-of-line.mp4) — 一个 worker 读两个 topic，email 被限速，push 也跟着慢
- [`q08-duplicate-on-retry.mp4`](problems/q08-duplicate-on-retry.mp4) — 调用方超时重试，用户收到两封一样的邮件
- [`q09-retry-storm.mp4`](problems/q09-retry-storm.mp4) — provider 恢复的那一刻，被积压的重试一起打挂
- [`q10-429-flood.mp4`](problems/q10-429-flood.mp4) — 积压一来，provider 回了四百多个四百二十九
