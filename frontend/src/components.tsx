import { useEffect, useState } from 'react'
import { marked } from 'marked'
import { sourceUrl, anchor, GIT_SHA, REPO_URL } from './source'

export function SourceLinks({ ids }: { ids: string[] }) {
  return (
    <span className="src-links">
      {ids.map((id) => {
        const a = anchor(id)
        return (
          <a key={id} href={sourceUrl(id)} target="_blank" rel="noreferrer" className="src-link" data-anchor={id} title={`${a.path} L${a.start}-L${a.end}`}>
            看源码 {a.path.split('/').pop()}#L{a.start}
          </a>
        )
      })}
    </span>
  )
}

// Plays /videos/<part>.mp4 (or an explicit src), or says the video is
// not generated yet.
export function PartVideo({ part, src: given }: { part: string; src?: string }) {
  const src = given ?? `/videos/${part}.mp4`
  const [state, setState] = useState<'checking' | 'ok' | 'missing'>('checking')
  useEffect(() => {
    let live = true
    setState('checking')
    fetch(src, { method: 'HEAD' })
      .then((r) => live && setState(r.ok && (r.headers.get('content-type') || '').includes('video') ? 'ok' : 'missing'))
      .catch(() => live && setState('missing'))
    return () => {
      live = false
    }
  }, [src])
  if (state === 'checking') return <div className="video-box">…</div>
  if (state === 'missing') return <div className="video-box missing" data-testid="video-missing">视频待生成（{part}）</div>
  return <video className="video-box" src={src} controls autoPlay data-testid="part-video" />
}

// Raw data stays one click away.
export function Json({ value, label = '查看原始数据', open = false, testid = 'raw-json' }: { value: unknown; label?: string; open?: boolean; testid?: string }) {
  return (
    <details className="raw" data-testid={testid} open={open}>
      <summary data-testid={`${testid}-toggle`}>{label}</summary>
      <pre className="json">{JSON.stringify(value, null, 2)}</pre>
    </details>
  )
}

export function Status({ s }: { s: string }) {
  return <span className={`status st-${s}`} data-testid="status">{s}</span>
}

// Requests per 100 ms as bars; the red part of a bar is the 429s.
export type Bar = { t_ms: number; count: number; rejected?: number }
export function Histogram({ bars, height = 140, max: fixedMax, testid = 'histogram', label }: { bars: Bar[]; height?: number; max?: number; testid?: string; label?: string }) {
  const W = 600, H = height, L = 34, B = 18
  const max = fixedMax ?? Math.max(10, ...bars.map((b) => b.count))
  const n = Math.max(bars.length, 1)
  const bw = (W - L - 4) / n
  const y = (v: number) => H - B - (v / max) * (H - B - 8)
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="chart" data-testid={testid}>
      {[0, max / 2, max].map((t) => (
        <g key={t}>
          <line x1={L} x2={W - 4} y1={y(t)} y2={y(t)} className="grid" />
          <text x={L - 4} y={y(t) + 4} textAnchor="end" className="axis">{Math.round(t)}</text>
        </g>
      ))}
      {bars.map((b, i) => (
        <g key={i}>
          <rect x={L + i * bw} y={y(b.count)} width={Math.max(bw - 1, 1)} height={H - B - y(b.count)} className="bar" />
          {!!b.rejected && <rect x={L + i * bw} y={y(b.rejected)} width={Math.max(bw - 1, 1)} height={H - B - y(b.rejected)} className="bar-bad" />}
        </g>
      ))}
      <text x={L} y={H - 3} className="axis">{label ?? '每 100 毫秒的请求数'}</text>
      <text x={W - 4} y={H - 3} textAnchor="end" className="axis">{((bars.length * 100) / 1000).toFixed(1)} s</text>
    </svg>
  )
}

// Render markdown from docs/. Relative links (written so they work on
// GitHub) point at the commit this page was built from; a link whose
// fragment is #anchor=<id> becomes the exact @learn line range.
export function mdToHtml(src: string, base = 'docs'): string {
  const html = marked.parse(src, { async: false }) as string
  return html.replace(/href="([^"#:]+)(#[^"]*)?"/g, (_m, rel: string, hash = '') => {
    const am = hash.match(/^#anchor=([a-z0-9-]+)$/)
    if (am) return `href="${sourceUrl(am[1])}" class="src-link-inline" target="_blank" rel="noreferrer"`
    const parts = base.split('/')
    for (const seg of rel.split('/')) {
      if (seg === '..') parts.pop()
      else if (seg !== '.') parts.push(seg)
    }
    return `href="${REPO_URL}/blob/${GIT_SHA}/${parts.join('/')}${hash}" target="_blank" rel="noreferrer"`
  })
}

export function ms(n?: number) {
  return n === undefined || n === null ? '' : `+${n.toFixed(0)} ms`
}
