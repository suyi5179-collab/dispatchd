// What the learn page says about each backend stage. Stage ids, order and
// parts come from internal/api/learn.go; src lists @learn anchors.
export type StepInfo = { id: string; part: string; title: string; what: string; src: string[] }

export const STEPS: StepInfo[] = [
  { id: 'submit', part: 'p1-intake', title: '提交样例（带幂等键）', src: ['learn-sample', 'idempotency'],
    what: '页面让后端发一个真实的 POST /v1/notifications，带一个新的 Idempotency-Key。为了演示重试，email provider 会让这个收件人的第一次请求返回 503。' },
  { id: 'idempotency', part: 'p1-intake', title: '幂等检查', src: ['idempotency', 'timeout-retry-test'],
    what: '事务里第一条语句就是插 idempotency_keys：主键是（client_id, idem_key）。插成功说明是第一次；报重复键说明以前来过，直接返回第一次的响应。不先查后插。' },
  { id: 'persist', part: 'p2-outbox', title: '同一个事务写 notification + outbox', src: ['outbox-write', 'idempotency'],
    what: 'notification 行和 outbox 行在同一个 MySQL 事务里提交：要么都在，要么都不在。不会出现库里有通知、Kafka 里没消息的情况。' },
  { id: 'relay', part: 'p2-outbox', title: 'relay 发到 Kafka', src: ['relay-batch', 'consume'],
    what: 'relay 用 FOR UPDATE SKIP LOCKED 锁住未发布的行，发到 Kafka 等 broker 确认，再把 partition 和 offset 写回 outbox。topic 按 channel 分，key 是通知 id。' },
  { id: 'token', part: 'p3-token', title: 'worker 取到消息，拿令牌', src: ['token-bucket', 'deliver', 'claim'],
    what: 'worker 先从这个 provider 的令牌桶拿一个令牌（拿不到就等，而不是打过去吃 429），再 claim：状态 QUEUED 才能改成 SENDING，两个 worker 只有一个能改成功。' },
  { id: 'attempt1', part: 'p4-retry', title: '第一次投递：临时故障', src: ['classify', 'fake-provider'],
    what: 'provider 返回 503。分类器把 5xx、超时、429 算临时失败（会重试），其他 4xx 算永久失败（不重试，记下该怎么处理）。' },
  { id: 'delay', part: 'p4-retry', title: '进延迟堆：退避 + jitter', src: ['full-jitter', 'schedule-retry', 'heap-push'],
    what: '等待上限是 base × 2 的（n−1）次方，实际等待从 0 到上限之间均匀随机抽（full jitter）。到期时间先写进 MySQL，再放进 worker 内存里的最小堆。' },
  { id: 'retry', part: 'p4-retry', title: '到期唤醒，重试成功', src: ['heap-run', 'reload'],
    what: '堆只有一个定时器，永远设在堆顶的到期时间。到点醒一次，不轮询。唤醒后重新 claim、再拿令牌、再发一次。' },
  { id: 'delivered', part: 'p5-delivered', title: '状态 DELIVERED', src: ['claim', 'observe'],
    what: '整条状态转移：QUEUED → SENDING → RETRY_SCHEDULED → SENDING → DELIVERED。每一次 from→to 都在 notification_events 里。' },
  { id: 'replay', part: 'p5-delivered', title: '同一个幂等键再提交一次', src: ['idempotency', 'learn-stages'],
    what: '用完全相同的请求体和同一个 Idempotency-Key 再发一次：返回同一个通知 id，响应头 Idempotent-Replayed: true，provider 那边仍然只送达了一次。' },
]

export const PARTS: Record<string, string> = {
  'p1-intake': '收下请求与幂等',
  'p2-outbox': 'Outbox 与 Kafka',
  'p3-token': '令牌桶与 claim',
  'p4-retry': '退避、jitter、延迟堆',
  'p5-delivered': '送达与重复提交',
  'p6-ops': '观测与故障演练',
}
