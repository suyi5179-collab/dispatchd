import { useEffect, useState } from 'react'
import { api, type Attempt, type Event, type Notification } from '../api'
import { Json, Status } from '../components'
import { navigate } from '../router'

const FINAL = ['DELIVERED', 'FAILED_PERMANENT', 'FAILED_EXHAUSTED']

export function NotificationList() {
  const [status, setStatus] = useState('')
  const [rows, setRows] = useState<Notification[]>([])
  useEffect(() => {
    const load = () => api<{ notifications: Notification[] }>(`/v1/notifications?limit=100${status ? `&status=${status}` : ''}`).then((r) => setRows(r.notifications)).catch(() => {})
    load()
    const t = setInterval(load, 2000)
    return () => clearInterval(t)
  }, [status])
  return (
    <div className="card">
      <h2>通知</h2>
      <div className="row">
        状态 <select value={status} onChange={(e) => setStatus(e.target.value)} data-testid="status-filter">
          <option value="">全部</option>
          {['QUEUED', 'SENDING', 'RETRY_SCHEDULED', ...FINAL].map((s) => <option key={s}>{s}</option>)}
        </select>
      </div>
      <table data-testid="notification-list">
        <thead><tr><th>id</th><th>channel</th><th>收件人</th><th>状态</th><th>尝试</th><th>创建</th></tr></thead>
        <tbody>
          {rows.map((n) => (
            <tr key={n.id}>
              <td><a href={`/notifications/${n.id}`} onClick={(e) => { e.preventDefault(); navigate(`/notifications/${n.id}`) }}>{n.id}</a></td>
              <td>{n.channel}</td><td>{n.recipient}</td><td><Status s={n.status} /></td><td>{n.attempts}</td><td>{n.created_at.slice(11, 23)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

type Detail = { notification: Notification; attempts: Attempt[]; events: Event[] }

// Attempts on a time axis from the moment the API accepted the request:
// the gaps are the backoff waits.
function AttemptTimeline({ n, attempts }: { n: Notification; attempts: Attempt[] }) {
  const t0 = Date.parse(n.created_at)
  const end = Math.max(t0 + 1, ...attempts.map((a) => Date.parse(a.finished_at || a.started_at)))
  const span = end - t0
  return (
    <div className="attempt-timeline" data-testid="attempt-timeline">
      {attempts.map((a) => {
        const s = Date.parse(a.started_at) - t0
        return (
          <div key={a.attempt} className="att-row" data-testid={`attempt-${a.attempt}`}>
            <span className="att-label">第 {a.attempt} 次</span>
            <div className="att-track"><div className={`att-dot out-${a.outcome}`} style={{ left: `${(s / span) * 100}%` }} /></div>
            <span className="att-meta">+{s.toFixed(0)} ms · {a.http_status || '-'} {a.outcome} {a.provider_code && `· ${a.provider_code}`}</span>
          </div>
        )
      })}
    </div>
  )
}

export function NotificationDetail({ id }: { id: string }) {
  const [d, setD] = useState<Detail | null>(null)
  const [err, setErr] = useState('')
  useEffect(() => {
    let t: number | undefined
    const load = () => api<Detail>(`/v1/notifications/${id}`).then((x) => {
      setD(x)
      if (!FINAL.includes(x.notification.status)) t = window.setTimeout(load, 700)
    }).catch((e) => setErr(e.message))
    load()
    return () => clearTimeout(t)
  }, [id])
  if (err) return <div className="card error">{err}</div>
  if (!d) return <div className="card">…</div>
  const n = d.notification
  return (
    <div className="card" data-testid="notification-detail">
      <h2>{n.id} <Status s={n.status} /></h2>
      <p>{n.channel} → <b>{n.recipient}</b> · 模板 {n.template} · 尝试 {n.attempts} 次{n.next_attempt_at && n.status === 'RETRY_SCHEDULED' && <> · 下次 {n.next_attempt_at.slice(11, 23)}</>}</p>
      {n.last_error && n.status !== 'DELIVERED' && (
        <div className="last-error" data-testid="last-error">
          最后一次错误：<b>{n.last_error.http_status} {n.last_error.code}</b> {n.last_error.message}
          {n.last_error.action && <div>建议动作：<b data-testid="error-action">{n.last_error.action}</b></div>}
        </div>
      )}
      <h3>每次投递尝试</h3>
      <AttemptTimeline n={n} attempts={d.attempts} />
      <h3>事件日志（状态转移）</h3>
      <table className="logs" data-testid="events">
        <thead><tr><th>时间</th><th>阶段</th><th>from → to</th><th>数据</th></tr></thead>
        <tbody>
          {d.events.map((e) => (
            <tr key={e.id}>
              <td>{e.at.slice(11, 23)}</td><td>{e.stage}</td><td>{e.from || e.to ? `${e.from || '∅'} → ${e.to || ''}` : ''}</td>
              <td><code className="small">{summarize(e)}</code></td>
            </tr>
          ))}
        </tbody>
      </table>
      <Json value={d} />
    </div>
  )
}

function summarize(e: Event): string {
  const x = e.data || {}
  const keys = ['attempt', 'http_status', 'topic', 'partition', 'offset', 'tokens_left', 'late_ms', 'heap_len', 'worker']
  const parts = keys.filter((k) => x[k] !== undefined).map((k) => `${k}=${typeof x[k] === 'number' ? Math.round(x[k] * 100) / 100 : x[k]}`)
  if (x.backoff) parts.push(`wait=${x.backoff.delay_ms.toFixed(0)}ms of ${x.backoff.ceiling_ms}`)
  return parts.join(' ')
}
