# 07 前端和教学模式：把一个通知的一生摊开给人看

> 讲解视频：[p1-intake](../videos/p1-intake.mp4) · [p5-delivered](../videos/p5-delivered.mp4) · tag：`step-07`

## 这一章解决什么问题？

前六章的东西都在后端：幂等键、outbox、claim、令牌桶、延迟堆。
讲给别人听的时候，最难的是「证明它真的这么跑」。
这一章做一个 React 前端，其中 `/learn` 页面跟着一个真实的通知走完全程，每一步旁边摆着后端写下的那一行。

## 假设：前端用定时器一步一步亮起来？

那就是动画，不是证据。后端出了 bug，动画照样播完。

所以每个阶段的状态都由后端算：`GET /v1/learn/runs/{id}` 读 MySQL 和 provider 的记录（[`learn.go` 的 `learnRun`](../../internal/api/learn.go#anchor=learn-stages)）：
- 有 `idempotency_keys` 那一行，「幂等检查」才算 done；
- 有 outbox 行，「写 notification + outbox」才算 done；
- 有 `published` 或 `consumed` 事件，「relay 发到 Kafka」才算 done；
- 有 `claimed` 事件（里面带着令牌桶剩余数），「拿令牌」才算 done；
- 以此类推，一直到 `replayed` 事件。

页面每三百毫秒轮询一次。第一个还没 done 的阶段显示为 running。

## 样例怎么保证一定会重试？

`POST /v1/learn/sample`（[`learnSample`](../../internal/api/learn.go#anchor=learn-sample)）做两件事：
1. 让 email provider 对一个新的随机收件人的**第一次**请求返回 503（`fail_first`），不影响别的流量；
2. 通过**真实的** `create` handler 提交通知，带一个新的幂等键。

之后的重试、延迟堆、唤醒，全是生产代码在跑。「用同一个幂等键再提交一次」也是走同一个 handler。

## 一次真跑的时间线

```text
1. 提交样例            +0 ms
2. 幂等检查            +0 ms     idempotency_keys 那一行和通知同一个时刻
3. notification+outbox +3 ms
4. relay 发到 Kafka    +15 ms
5. 拿令牌              +23 ms    桶里还剩 9 个（每秒 40，容量 10）
6. 第一次投递 503      +32 ms
7. 进延迟堆            +34 ms    上限 1000 ms，随机数 0.433，等 433 ms
8. 到期唤醒，重试成功  +468 ms
9. DELIVERED           +467 ms
10. 同一个 key 再提交  +848 ms   同一个 id，provider 实际送达 1 次
```

第一版里第 9 步比第 8 步早一毫秒：「重试成功」用的是第二次 attempt 事件的时间，而它在 `delivered_at` 写下之后才插入。现在第 8 步用延迟堆唤醒的时间，顺序就对了。

## 源码链接怎么做到不过期？

在源码里用两行注释圈住一段：`@learn:<id>` 和 `@learn:end <id>`。
`scripts/learn_anchors.mjs` 扫出 `{id: {path, start, end}}`，写进 `frontend/src/generated/anchors.json`；行号从来不手写。
链接是 `${VITE_REPO_URL}/blob/<commit sha>/<path>#L<start>-L<end>`，sha 在构建时取 `git rev-parse HEAD`。
`scripts/check_anchors.mjs` 在 CI 里检查：生成文件没过期；前端、文档、视频脚本引用的每个 id 都存在；每个锚点都有人引用；文档里的相对路径和锚点所在文件一致。

文档里的写法是 `[文字](../internal/x.go#anchor=<id>)`：在 GitHub 上它是一个普通的相对链接，在页面里会被换成精确到行的链接。

## 其他页面

- 发送：可以故意用同一个 key 发两次，看 `Idempotent-Replayed`；也可以同时发一百个。
- 通知详情：每次尝试按时间轴排开，间隔就是退避的等待；永久失败显示建议动作。
- Provider 故障：一键切换五种故障模式，看最近二十秒每一百毫秒到达 provider 的请求数；下面是仓库里提交的风暴对照实测。
- 面试题、日常问题：渲染 `docs/interview.md` 和 `docs/problems.md`。

## 一个 shell 小坑：PID 记错了

前端开发服务器用 `scripts/ui.sh` 后台启动，PID 写进 `.runtime/vite.pid`，停的时候只按这个 PID 杀，从不按进程名杀。
第一版写成 `(cd frontend && nohup node … & echo $! > ../$pidf)`（[`shell_v1.sh`](../problems/snippets/shell_v1.sh#anchor=ui-v1)），报 `../.runtime/vite.pid: No such file or directory`。
原因是 `&` 比 `&&` 结合得松：`cd` 和启动是同一个后台任务，`echo` 在没 `cd` 过的父 shell 里跑。拆成四行就好了（[`ui.sh`](../../scripts/ui.sh#anchor=ui-pid)）。

## 讲解视频怎么录？

`video/record.mjs` 用 Playwright 打开这个页面，按 `video/narration.json` 一段一段地做：执行动作（点按钮、点开某个阶段、展开 JSON）→ 用红框圈住这一段讲的那个值 → 停留这一段旁白的时长（[`record-pacing`](../../video/record.mjs#anchor=record-pacing)）。
时长来自 `video/durations.json`（配音生成后才有），没有就每段四秒。输出一个 webm 和 `video/timeline.json`（每段真实开始的毫秒数、所属 part、红框和目标的坐标），`video/check_frames.mjs` 检查每个红框都套住了目标、没有框偏或框空。

## 测试

`e2e/dispatchd.spec.ts`（Playwright，headless）：
- 十个阶段依次亮起，前面的都已经 done；每一步的关键数据都在（503、退避上限、offset……）；
- 每个「看源码」链接的格式正确，指向的文件和行真的存在；
- 重复提交返回同一个 id、provider 只送达一次；
- 面试题和日常问题两个标签页能展开。

后端还有一个 Go 测试 `TestLearnSampleWalksEveryStage`，不经过浏览器检查同一件事。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-07
./scripts/demo.sh          # 栈 + 前端，打开 http://localhost:5185/learn
./scripts/e2e.sh
```
