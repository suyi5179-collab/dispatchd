import md from '../../../docs/interview.md?raw'
import { mdToHtml } from '../components'

// docs/interview.md rendered as collapsible questions: every "### " is a
// question, and everything up to the next heading is its answer.
type QA = { group: string; q: string; a: string }

function parse(src: string): QA[] {
  const out: QA[] = []
  let group = ''
  let cur: QA | null = null
  for (const line of src.split('\n')) {
    if (line.startsWith('## ')) {
      group = line.slice(3).trim()
      cur = null
    } else if (line.startsWith('### ')) {
      cur = { group, q: line.slice(4).trim(), a: '' }
      out.push(cur)
    } else if (cur) {
      cur.a += line + '\n'
    }
  }
  return out
}

export default function Interview() {
  const qas = parse(md)
  const groups = [...new Set(qas.map((x) => x.group))]
  return (
    <div className="card interview" data-testid="interview">
      <h2>面试题（{qas.length} 题）</h2>
      <p className="muted">来源：docs/interview.md。点开一题看答案；答案里的数字都是这个仓库跑出来的。</p>
      {groups.map((g) => (
        <section key={g}>
          <h3>{g}</h3>
          {qas.filter((x) => x.group === g).map((x, i) => (
            <details key={i} className="qa" data-testid="qa">
              <summary>{x.q}</summary>
              <div className="answer" dangerouslySetInnerHTML={{ __html: mdToHtml(x.a) }} />
            </details>
          ))}
        </section>
      ))}
    </div>
  )
}
