# Teaching videos

Generated outside this repo from `video/narration.json` + `video/record.mjs` (stage clips) and `video/problems.walk.json` (problem walkthroughs). The learn page plays `/videos/<part>.mp4` and `/videos/problems/<id>.mp4` and shows「视频待生成」until a file exists.

| file | what |
|---|---|
| `p1-intake.mp4` | 收下请求：幂等键和唯一约束 |
| `p2-outbox.mp4` | 进队列：同一个事务写 outbox，relay 发到 Kafka |
| `p3-token.mp4` | worker 拿令牌，再认领 |
| `p4-retry.mp4` | 临时失败：退避、full jitter、最小堆延迟队列 |
| `p5-delivered.mp4` | 送达，以及同一个幂等键再提交一次 |
| `p6-ops.mp4` | 观测与故障演练：重试风暴的对照 |
| `problems/q01-swallowed-error.mp4` | 测试只说十五秒还是排队中，真正的错误被吞了 |
| `problems/q02-close-hangs.mp4` | 测试跑完不退出，卡在关闭卡夫卡客户端 |
| `problems/q03-jitter-test-always-green.mp4` | 关掉 jitter，重试风暴测试还是绿的 |
| `problems/q04-storm-margin.mp4` | 风暴测试余量不够，并行跑全量时偶尔变红 |
| `problems/q05-replay-bytes.mp4` | 同一个幂等键，第二次返回的字节不一样 |
| `problems/q06-zsh-word-split.mp4` | runbook 的命令在 zsh 里跑不起来 |
| `problems/q07-head-of-line.mp4` | 一个 worker 读两个 topic，email 被限速，push 也跟着慢 |
| `problems/q08-duplicate-on-retry.mp4` | 调用方超时重试，用户收到两封一样的邮件 |
| `problems/q09-retry-storm.mp4` | provider 恢复的那一刻，被积压的重试一起打挂 |
| `problems/q10-429-flood.mp4` | 积压一来，provider 回了四百多个四百二十九 |
