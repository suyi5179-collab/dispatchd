import { useEffect, useRef, useState } from 'react'
import { api, post } from '../api'
import { Json, PartVideo, SourceLinks } from '../components'
import { PARTS, STEPS } from '../learnSteps'

type Stage = { id: string; part: string; title: string; status: 'pending' | 'running' | 'done'; at?: string; t_ms?: number; data?: any }
type Run = { notification: any; stages: Stage[]; provider_requests: any[] }

const short = (s?: string, n = 12) => (s && s.length > n ? s.slice(0, n) + '…' : s)
const t = (s?: string) => (s ? s.slice(11, 23) : '')

function KV({ rows, testid }: { rows: [string, React.ReactNode, string?][]; testid?: string }) {
  return (
    <table className="kv" data-testid={testid}>
      <tbody>{rows.map(([k, v, id]) => <tr key={k}><td>{k}</td><td><b data-testid={id}>{v}</b></td></tr>)}</tbody>
    </table>
  )
}

function Transition({ from, to, testid }: { from?: string; to?: string; testid?: string }) {
  return <span className="transition" data-testid={testid}><span className="status">{from || '∅'}</span> → <span className="status">{to}</span></span>
}

function StageData({ s, replay }: { s: Stage; replay: any }) {
  const d = s.data
  if (!d) return <p className="muted">{s.status === 'running' ? '正在进行：后端还没有写下这一步的记录，写下就会出现…' : '还没走到这一步。'}</p>
  switch (s.id) {
    case 'submit':
      return (
        <>
          <KV rows={[
            ['Idempotency-Key', <code>{d.idempotency_key}</code>, 'idem-key-value'],
            ['响应码', d.status_code, 'submit-code'],
            ['通知 id', d.response?.id, 'sample-id'],
            ['收件人', d.request?.recipient],
          ]} />
          <Json value={{ request: d.request, response: d.response }} label="请求体和响应体" />
        </>
      )
    case 'idempotency': {
      const r = d.row
      return (
        <>
          <p className="muted">MySQL 里 idempotency_keys 表的这一行（主键 client_id + idem_key）：</p>
          <KV testid="idem-row" rows={[
            ['client_id', r.client_id], ['idem_key', <code>{r.idem_key}</code>, 'idem-row-key'],
            ['request_hash', <code>{short(r.request_hash, 16)}</code>, 'idem-hash'], ['notification_id', r.notification_id, 'idem-nid'],
            ['status_code', r.status_code], ['created_at', t(r.created_at)],
          ]} />
          <Json value={r} label="这一行的完整 JSON" />
        </>
      )
    }
    case 'persist':
      return (
        <div className="grid2">
          <div>
            <p className="muted">notifications 行（提交时的样子）</p>
            <KV rows={[['id', short(d.notification_row.id, 16)], ['status', d.notification_row.status, 'persist-status'], ['created_at', t(d.notification_row.created_at), 'notif-created']]} />
          </div>
          <div>
            <p className="muted">outbox 行（同一个事务）</p>
            <KV rows={[['outbox id', d.outbox_row.id], ['topic', d.outbox_row.topic, 'outbox-topic'], ['created_at', t(d.outbox_row.created_at), 'outbox-created']]} />
          </div>
          <Json value={d} label="两行的完整 JSON" />
        </div>
      )
    case 'relay':
      return (
        <>
          <KV rows={[
            ['topic', d.topic, 'kafka-topic'], ['partition', d.partition, 'kafka-partition'], ['offset', d.offset, 'kafka-offset'],
            ['key', <code>{short(d.key, 18)}</code>], ['消费它的 worker', d.consumed?.worker, 'consumed-worker'],
          ]} />
          <p className="muted">Kafka 消息体：</p>
          <pre className="json" data-testid="kafka-message">{JSON.stringify(d.message, null, 2)}</pre>
        </>
      )
    case 'token': {
      const c = d.claim
      return (
        <>
          <KV rows={[
            ['令牌桶', `${c.bucket_rate}/s，容量 ${c.bucket_burst}`, 'bucket-rate'],
            ['拿令牌后桶里还剩', c.tokens_left?.toFixed(2), 'tokens-left'],
            ['等令牌', `${c.token_wait_ms?.toFixed(1)} ms`, 'token-wait'],
            ['claim', <Transition from={d.transition.from} to={d.transition.to} />, 'claim-transition'],
            ['租约到', t(c.lease_until)],
          ]} />
          <Json value={c} label="claimed 事件" />
        </>
      )
    }
    case 'attempt1':
      return (
        <>
          <KV rows={[
            ['provider 返回', d.http_status, 'attempt1-status'], ['分类', d.outcome, 'attempt1-kind'],
            ['状态', <Transition from={d.transition.from} to={d.transition.to} />, 'attempt1-transition'],
          ]} />
          <p className="muted">provider 的原始响应：</p>
          <pre className="json" data-testid="attempt1-body">{d.provider_response?.body}</pre>
        </>
      )
    case 'delay': {
      const b = d.backoff
      return (
        <>
          <p className="formula" data-testid="backoff-formula">
            上限 = min(cap, base × 2^(n−1)) = min({b.cap_ms}, {b.base_ms} × 2^{b.attempt - 1}) = <b data-testid="backoff-ceiling">{b.ceiling_ms} ms</b>；
            jitter = {b.jitter}，随机数 <b data-testid="backoff-rand">{b.rand?.toFixed(3)}</b> → 等 <b data-testid="backoff-delay">{b.delay_ms.toFixed(0)} ms</b>
          </p>
          <KV rows={[
            ['next_attempt_at（先写 MySQL）', t(d.next_attempt_at), 'next-attempt'],
            ['放进堆后堆里有', d.delay_queue?.heap_len, 'heap-len'],
            ['堆顶', short(d.delay_queue?.heap_top, 16)],
          ]} />
          <Json value={d} label="退避计算和堆的状态" />
        </>
      )
    }
    case 'retry':
      return (
        <>
          <KV rows={[
            ['到期时间', t(d.woken.due)], ['实际醒来晚了', `${d.woken.late_ms?.toFixed(2)} ms`, 'woken-late'],
            ['这个 worker 的定时器一共醒过', d.woken.timer_fires, 'timer-fires'],
            ['第二次投递', d.http_status, 'attempt2-status'], ['分类', d.outcome],
          ]} />
          <Json value={d} label="woken 事件和第二次尝试" />
        </>
      )
    case 'delivered':
      return (
        <>
          <p>最终状态 <b className="status st-DELIVERED" data-testid="final-status">{d.status}</b>，共 {d.attempts} 次尝试。</p>
          <table className="logs" data-testid="transitions">
            <thead><tr><th>时间</th><th>阶段</th><th>from → to</th></tr></thead>
            <tbody>{d.transitions.map((x: any, i: number) => <tr key={i} data-testid={`transition-${i}`}><td>{t(x.at)}</td><td>{x.stage}</td><td><Transition from={x.from} to={x.to} /></td></tr>)}</tbody>
          </table>
        </>
      )
    case 'replay':
      return (
        <>
          <KV rows={[
            ['返回的通知 id', replay?.response?.id ?? d.notification_id, 'replay-id'],
            ['响应头 Idempotent-Replayed', replay ? String(replay.replayed) : '（刷新前的那次）', 'replay-header'],
            ['这个 key 在库里有几行', d.rows_with_this_key, 'replay-rows'],
            ['provider 实际送达次数', d.provider_deliveries, 'replay-deliveries'],
          ]} />
          <p className="muted">provider 收到的这个通知的全部请求：</p>
          <table className="logs" data-testid="provider-requests">
            <thead><tr><th>时间</th><th>attempt</th><th>返回</th></tr></thead>
            <tbody>{(d.provider_requests || []).map((r: any, i: number) => <tr key={i}><td>{t(r.at)}</td><td>{r.attempt}</td><td>{r.status} {r.code}</td></tr>)}</tbody>
          </table>
        </>
      )
  }
  return <Json value={d} open />
}

export default function Learn() {
  const [id, setId] = useState<string | null>(null)
  const [run, setRun] = useState<Run | null>(null)
  const [selected, setSelected] = useState('submit')
  const [follow, setFollow] = useState(true)
  const [video, setVideo] = useState<string | null>(null)
  const [replay, setReplay] = useState<any>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState('')
  const timer = useRef<number | null>(null)

  useEffect(() => () => { if (timer.current) clearInterval(timer.current) }, [])
  useEffect(() => {
    if (!id) return
    if (timer.current) clearInterval(timer.current)
    const tick = () => api<Run>(`/v1/learn/runs/${id}`).then(setRun).catch((e) => setErr(e.message))
    tick()
    timer.current = window.setInterval(tick, 300)
  }, [id])
  useEffect(() => {
    if (!run || !follow) return
    const last = [...run.stages].reverse().find((s) => s.status !== 'pending')
    if (last) setSelected(last.id)
  }, [run, follow])

  async function start() {
    setErr(''); setBusy(true); setReplay(null); setRun(null); setFollow(true)
    try {
      const r = await post<any>('/v1/learn/sample')
      setId(r.response.id)
    } catch (e: any) { setErr(e.message) } finally { setBusy(false) }
  }
  async function doReplay() {
    if (!id) return
    setFollow(true)
    try { setReplay(await post<any>(`/v1/learn/runs/${id}/replay`)) } catch (e: any) { setErr(e.message) }
  }
  const stages = run?.stages
  const delivered = stages?.[8].status === 'done'
  const step = stages?.find((s) => s.id === selected)
  const info = STEPS.find((s) => s.id === selected)!

  return (
    <div>
      <div className="card learn-head">
        <h2>教学模式：一个通知从收下到送达</h2>
        <p>
          点「提交样例」，后端会发一个真实的通知：API 收下 → 幂等检查 → 写库和 outbox → relay 进 Kafka → worker 拿令牌 → 第一次投递被 provider 拒绝（503）→ 进延迟堆 → 到期重试成功。
          左边每一步亮起，是因为后端真的写下了那一行或那个事件；右边显示的就是那一行。
          另见：<a href="/learn/problems">日常问题与解决</a> · <a href="/learn/interview">面试题</a>
        </p>
        <div className="row">
          <button onClick={start} disabled={busy} data-testid="learn-start">{id ? '重新提交一个样例' : '提交样例'}</button>
          <button onClick={doReplay} disabled={!delivered} className="secondary" data-testid="learn-replay">用同一个幂等键再提交一次</button>
          {id && <span className="muted">通知 <code data-testid="learn-id">{id}</code></span>}
          {err && <span className="error">{err}</span>}
        </div>
      </div>
      <div className="learn">
        <ol className="timeline" data-testid="timeline">
          {STEPS.map((s, i) => {
            const st = stages?.[i]
            const status = st?.status ?? 'pending'
            return (
              <li key={s.id} data-testid={`step-${s.id}`} data-status={status}
                className={`stage ${status}${selected === s.id ? ' selected' : ''}`}
                onClick={() => { setSelected(s.id); setFollow(false) }}>
                <div className="stage-title"><span className="dot" />{i + 1}. {s.title}</div>
                <div className="stage-meta">
                  <span className="part">{PARTS[s.part]}</span>
                  {st?.t_ms !== undefined && <span className="note" data-testid={`t-${s.id}`}>+{st.t_ms.toFixed(0)} ms</span>}
                </div>
                <button className="link" data-testid={`video-${s.id}`} onClick={(e) => { e.stopPropagation(); setVideo(s.part); setSelected(s.id); setFollow(false) }}>看讲解视频</button>
              </li>
            )
          })}
        </ol>
        <div className="card detail" data-testid="step-detail">
          <h3>{info.title} <span className={`pill ${step?.status ?? 'pending'}`} data-testid="detail-status">{step?.status ?? 'pending'}</span></h3>
          <p>{info.what}</p>
          <SourceLinks ids={info.src} />
          {video === info.part && <PartVideo part={info.part} />}
          {step ? <StageData s={step} replay={replay} /> : <p className="muted">先点「提交样例」。</p>}
        </div>
      </div>
    </div>
  )
}
