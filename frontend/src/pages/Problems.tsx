import md from '../../../docs/problems.md?raw'
import src from '../../../video/problems.src.json'
import { mdToHtml, PartVideo, SourceLinks } from '../components'

// docs/problems.md rendered as collapsible items. Each "### qNN · title"
// is matched to its entry in video/problems.src.json for the source
// links and the walkthrough video (/videos/problems/<id>.mp4).
type Item = { group: string; key: string; title: string; body: string }
type Src = { id: string; title: string; subtitle: string; where?: string[]; code_from: { anchor: string }; before_from?: { anchor: string } }

function parse(text: string): Item[] {
  const out: Item[] = []
  let group = ''
  let cur: Item | null = null
  for (const line of text.split('\n')) {
    if (line.startsWith('## ')) {
      group = line.slice(3).trim()
      cur = null
    } else if (line.startsWith('### ')) {
      const [key, ...rest] = line.slice(4).split(' · ')
      cur = { group, key: key.trim(), title: rest.join(' · ').trim(), body: '' }
      out.push(cur)
    } else if (cur && !line.startsWith('<a id=')) {
      cur.body += line + '\n'
    }
  }
  return out
}

export default function Problems() {
  const items = parse(md)
  const meta = src as Src[]
  const groups = [...new Set(items.map((x) => x.group))]
  return (
    <div className="card interview" data-testid="problems">
      <h2>日常问题与解决（{items.length} 个）</h2>
      <p className="muted">
        面试常问「你遇到过什么问题、怎么定位、怎么解决」。前几个是做这个项目时真实踩过的坑，后几个是这个系统在生产上的典型事故。
        来源：docs/problems.md；每题一段逐行高亮的讲解视频。
      </p>
      {groups.map((g) => (
        <section key={g}>
          <h3>{g}</h3>
          {items.filter((x) => x.group === g).map((x) => {
            const m = meta.find((p) => p.id.startsWith(x.key + '-'))
            const ids = m ? [...new Set([...(m.where ?? []), m.code_from.anchor, ...(m.before_from ? [m.before_from.anchor] : [])])] : []
            return (
              <details key={x.key} className="qa" data-testid="problem" id={x.key}>
                <summary>{x.key} · {x.title}{m && <span className="muted"> — {m.subtitle}</span>}</summary>
                {m && <SourceLinks ids={ids} />}
                {m && <PartVideo part={m.id} src={`/videos/problems/${m.id}.mp4`} />}
                <div className="answer" dangerouslySetInnerHTML={{ __html: mdToHtml(x.body) }} />
              </details>
            )
          })}
        </section>
      ))}
    </div>
  )
}
