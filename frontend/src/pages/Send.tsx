import { useEffect, useState } from 'react'
import { api, newKey, raw, type Notification } from '../api'
import { Json, Status } from '../components'
import { navigate } from '../router'

const TEMPLATES: Record<string, Record<string, string>> = {
  welcome: { name: 'Ada' },
  otp: { code: '482913' },
  order_shipped: { order_id: 'A-1042' },
}

type Result = { status: number; replayed: boolean; body: any; key: string }

export default function Send() {
  const [channel, setChannel] = useState('email')
  const [recipient, setRecipient] = useState('ada@example.com')
  const [template, setTemplate] = useState('welcome')
  const [params, setParams] = useState(JSON.stringify(TEMPLATES.welcome))
  const [key, setKey] = useState(newKey())
  const [results, setResults] = useState<Result[]>([])
  const [recent, setRecent] = useState<Notification[]>([])
  const [batch, setBatch] = useState('')
  const [err, setErr] = useState('')

  const load = () => api<{ notifications: Notification[] }>('/v1/notifications?limit=10').then((r) => setRecent(r.notifications)).catch(() => {})
  useEffect(() => {
    load()
    const t = setInterval(load, 1500)
    return () => clearInterval(t)
  }, [])

  async function send(k = key) {
    setErr('')
    let p: any
    try {
      p = JSON.parse(params || '{}')
    } catch {
      setErr('params 不是合法的 JSON')
      return
    }
    const r = await raw('/v1/notifications', { method: 'POST', headers: { 'Idempotency-Key': k }, body: JSON.stringify({ channel, recipient, template, params: p }) })
    setResults((xs) => [{ status: r.status, replayed: r.headers.get('Idempotent-Replayed') === 'true', body: r.body, key: k }, ...xs].slice(0, 6))
    load()
  }

  async function sendMany(n: number) {
    setBatch(`正在同时发 ${n} 个…`)
    const t0 = performance.now()
    const rs = await Promise.all(Array.from({ length: n }, (_, i) =>
      raw('/v1/notifications', { method: 'POST', headers: { 'Idempotency-Key': newKey('batch') }, body: JSON.stringify({ channel, recipient: channel === 'email' ? `batch${i}@example.com` : `device-${i}`, template: 'welcome', params: { name: `u${i}` } }) })))
    const ok = rs.filter((r) => r.status === 202).length
    setBatch(`发了 ${n} 个，${ok} 个 202，用时 ${(performance.now() - t0).toFixed(0)} ms。去「Provider 故障」页看它们怎么到达 provider。`)
    load()
  }

  return (
    <div className="grid2">
      <div className="card">
        <h2>发一个通知</h2>
        <p className="muted">POST /v1/notifications，必须带 Idempotency-Key。用同一个 key 再发一次，看服务返回的是不是同一个通知。</p>
        <div className="row">
          <label>channel <select value={channel} onChange={(e) => { setChannel(e.target.value); setRecipient(e.target.value === 'email' ? 'ada@example.com' : 'device-abc') }} data-testid="channel"><option>email</option><option>push</option></select></label>
          <label>template <select value={template} onChange={(e) => { setTemplate(e.target.value); setParams(JSON.stringify(TEMPLATES[e.target.value])) }} data-testid="template">{Object.keys(TEMPLATES).map((t) => <option key={t}>{t}</option>)}</select></label>
        </div>
        <label>recipient <input value={recipient} onChange={(e) => setRecipient(e.target.value)} data-testid="recipient" /></label>
        <p className="muted small">试试 <code>nobody@invalid.example</code>（邮箱不存在）或 push 的 <code>expired-123</code>（token 失效）：永久失败，不重试。</p>
        <label>params <input value={params} onChange={(e) => setParams(e.target.value)} data-testid="params" /></label>
        <label>Idempotency-Key <input value={key} onChange={(e) => setKey(e.target.value)} data-testid="idem-key" /></label>
        <div className="row">
          <button onClick={() => send()} data-testid="send">发送</button>
          <button className="secondary" onClick={() => send(key)} data-testid="send-again">用同一个 key 再发一次</button>
          <button className="secondary" onClick={() => setKey(newKey())} data-testid="new-key">换一个新 key</button>
        </div>
        {err && <div className="error">{err}</div>}
        {results.map((r, i) => (
          <div key={i} className="result" data-testid="send-result">
            <b data-testid="result-code">{r.status}</b>{' '}
            {r.replayed && <span className="badge" data-testid="replayed">Idempotent-Replayed</span>}{' '}
            key <code>{r.key}</code>{' '}
            {r.body?.id ? <a href={`/notifications/${r.body.id}`} data-testid="result-id" onClick={(e) => { e.preventDefault(); navigate(`/notifications/${r.body.id}`) }}>{r.body.id}</a> : <span className="error">{r.body?.error}</span>}
          </div>
        ))}
        <h3>一次发一批</h3>
        <div className="row">
          <button className="secondary" onClick={() => sendMany(100)} data-testid="send-100">同时发 100 个（各自新 key）</button>
        </div>
        {batch && <p className="muted" data-testid="batch-result">{batch}</p>}
      </div>
      <div className="card">
        <h2>最近的通知</h2>
        <table data-testid="recent">
          <thead><tr><th>id</th><th>channel</th><th>收件人</th><th>状态</th><th>尝试</th></tr></thead>
          <tbody>
            {recent.map((n) => (
              <tr key={n.id}>
                <td><a href={`/notifications/${n.id}`} onClick={(e) => { e.preventDefault(); navigate(`/notifications/${n.id}`) }}>{n.id.slice(0, 14)}…</a></td>
                <td>{n.channel}</td><td>{n.recipient}</td><td><Status s={n.status} /></td><td>{n.attempts}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {results[0] && <Json value={results[0].body} label="最近一次响应体" />}
      </div>
    </div>
  )
}
