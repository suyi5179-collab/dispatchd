import { useEffect, useState } from 'react'
import { api, post } from '../api'
import { Histogram, Json, type Bar } from '../components'
import { GRAFANA_URL, PROM_URL } from '../source'
import stormJitter from '../../../docs/results/storm-jitter.json'
import stormNoJitter from '../../../docs/results/storm-nojitter.json'

type Mode = { mode: string; ratio?: number; slow_ms?: number; rate_limit_rps?: number; rate_limit_burst?: number; fail_first?: Record<string, number> }
type Stats = { now_ms: number; stats: { total: number; by_status: Record<string, number>; buckets: { t_ms: number; count: number }[]; peak: number; delivered: number; duplicates: number } }

const MODES: [string, string, Mode][] = [
  ['ok', '正常', { mode: 'ok' }],
  ['fail_5xx', '临时故障 503', { mode: 'fail_5xx' }],
  ['fail_5xx_30', '三成 503', { mode: 'fail_5xx', ratio: 0.3 }],
  ['fail_429', '限流 429', { mode: 'fail_429' }],
  ['permanent', '永久失败 400', { mode: 'permanent' }],
  ['slow', '慢 800 ms', { mode: 'slow', slow_ms: 800 }],
  ['quota', '配额 20/s', { mode: 'ok', rate_limit_rps: 20, rate_limit_burst: 5 }],
]

function ProviderCard({ name }: { name: string }) {
  const [mode, setMode] = useState<Mode | null>(null)
  const [stats, setStats] = useState<Stats | null>(null)
  const [err, setErr] = useState('')
  useEffect(() => {
    const load = async () => {
      try {
        setMode(await api<Mode>(`/v1/admin/providers/${name}/mode`))
        const now = (await api<Stats>(`/v1/admin/providers/${name}/stats?bucket_ms=100&since_ms=0`)).now_ms
        setStats(await api<Stats>(`/v1/admin/providers/${name}/stats?bucket_ms=100&since_ms=${Math.max(0, now - 20000)}`))
      } catch (e: any) {
        setErr(e.message)
      }
    }
    load()
    const t = setInterval(load, 1000)
    return () => clearInterval(t)
  }, [name])
  const set = async (m: Mode) => {
    // keep the learn page's per-recipient rules
    setMode(await post<Mode>(`/v1/admin/providers/${name}/mode`, { ...m, fail_first: mode?.fail_first }))
  }
  // Pad the last 20 s so the chart has a fixed time axis.
  const bars: Bar[] = []
  if (stats) {
    const end = Math.floor(stats.now_ms / 100) * 100
    const byT = new Map(stats.stats.buckets.map((b) => [b.t_ms, b.count]))
    for (let t = end - 19900; t <= end; t += 100) bars.push({ t_ms: t, count: byT.get(t) ?? 0 })
  }
  const current = MODES.find(([, , m]) => m.mode === mode?.mode && (m.ratio ?? 0) === (mode?.ratio ?? 0) && (m.rate_limit_rps ?? 0) === (mode?.rate_limit_rps ?? 0))
  return (
    <div className="card" data-testid={`provider-${name}`}>
      <h3>{name} provider <span className="pill" data-testid={`mode-${name}`}>{current ? current[1] : mode?.mode}</span></h3>
      <div className="row">
        {MODES.map(([id, label, m]) => (
          <button key={id} className={current?.[0] === id ? '' : 'secondary'} data-testid={`set-${name}-${id}`} onClick={() => set(m)}>{label}</button>
        ))}
      </div>
      {err && <div className="error">{err}</div>}
      {stats && (
        <>
          <p className="stat-line">最近二十秒 <b data-testid={`total-${name}`}>{stats.stats.total}</b> 个请求 · 状态 {Object.entries(stats.stats.by_status).map(([k, v]) => `${k}×${v}`).join(' ') || '—'} · 最忙的 100 ms <b>{stats.stats.peak}</b> 个</p>
          <Histogram bars={bars} testid={`live-${name}`} label="最近二十秒，每 100 毫秒到达 provider 的请求数" />
        </>
      )}
      {mode && <Json value={mode} label="当前故障配置（provider 的 /admin/mode）" />}
    </div>
  )
}

function StormPanel({ run, title, testid }: { run: any; title: string; testid: string }) {
  const a = run.after_recovery
  const bars: Bar[] = (a.buckets as Bar[]).slice(0, 60)
  return (
    <div className="storm" data-testid={testid}>
      <h4>{title}</h4>
      <p className="stat-line">
        恢复后最忙的 100 ms <b data-testid={`${testid}-peak`}>{a.peak_per_100ms}</b> 个 · 被拒（429）<b data-testid={`${testid}-rejected`}>{a.rejected_429}</b> 个 · 全部送达用了 <b>{(run.all_delivered_after_ms / 1000).toFixed(1)} s</b>
      </p>
      <Histogram bars={bars} max={70} testid={`${testid}-chart`} label="provider 恢复后，每 100 毫秒的请求（红色是 429）" />
    </div>
  )
}

export default function Providers() {
  return (
    <div>
      <div className="card">
        <h2>Provider 故障注入</h2>
        <p className="muted">两个假 provider 都可以切故障模式。切完去「发送」页发一批，看这里的到达分布和「通知」页的状态。指标看 <a href={GRAFANA_URL} target="_blank" rel="noreferrer">Grafana</a> · 告警看 <a href={`${PROM_URL}/alerts`} target="_blank" rel="noreferrer">Prometheus</a>。</p>
      </div>
      <div className="grid2">
        <ProviderCard name="email" />
        <ProviderCard name="push" />
      </div>
      <div className="card" data-testid="storm">
        <h2>重试风暴 vs jitter（仓库里提交的实测）</h2>
        <p className="muted">
          两百个通知积压，worker 在 provider 宕机时开始消费，第一次尝试几乎同时失败；一秒半后 provider 恢复，但每秒只扛两百个请求。
          同一个测试跑两遍：<code>TestRetriesAfterAnOutageDoNotArriveAsOneSpike</code>，结果在 docs/results/storm-*.json（scripts/contrasts.sh 生成）。
        </p>
        <div className="grid2">
          <StormPanel run={stormNoJitter} title="关掉 jitter：所有人同一时刻回来" testid="storm-nojitter" />
          <StormPanel run={stormJitter} title="full jitter：等待时间在 0 到上限之间随机" testid="storm-jitter" />
        </div>
      </div>
    </div>
  )
}
