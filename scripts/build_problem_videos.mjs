#!/usr/bin/env node
// Build video/problems.walk.json for the walkthrough renderer from
// video/problems.src.json. Code is never copied into the JSON by hand:
// each problem names @learn anchors (code_from = after the fix,
// before_from = before it) and this script cuts the lines out of the
// source. It then checks every segment:
//   - line numbers (1-based, relative to the cut) are inside the cut;
//   - narration has no code symbols and no Arabic digits;
//   - each problem has 6–14 segments in total;
// and prints each segment's narration next to the lines it highlights so
// a person can check that the words match the code.
//
//   node scripts/build_problem_videos.mjs            build + check
//   node scripts/build_problem_videos.mjs --show ID  print an anchor with line numbers
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { scan } from './learn_anchors.mjs'

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const { anchors, errors } = scan()
if (errors.length) {
  console.error(errors.join('\n'))
  process.exit(1)
}

// Cut an anchor's lines and remove the common indentation (line numbers
// are unchanged by dedenting).
function cut(from) {
  const a = anchors[from.anchor]
  if (!a) throw new Error(`unknown anchor ${from.anchor}`)
  if (from.path && from.path !== a.path) throw new Error(`anchor ${from.anchor} lives in ${a.path}, not ${from.path}`)
  const lines = fs.readFileSync(path.join(root, a.path), 'utf8').split('\n').slice(a.start - 1, a.end)
  while (lines.length && lines[lines.length - 1].trim() === '') lines.pop()
  const indent = Math.min(...lines.filter((l) => l.trim()).map((l) => l.match(/^[\t ]*/)[0].length))
  return { code: lines.map((l) => l.slice(indent)).join('\n'), source: a }
}

if (process.argv[2] === '--show') {
  const { code, source } = cut({ anchor: process.argv[3] })
  console.log(`# ${source.path} L${source.start}-L${source.end}`)
  code.split('\n').forEach((l, i) => console.log(String(i + 1).padStart(3) + '  ' + l))
  process.exit(0)
}

// Narration is spoken: no code symbols, no Arabic digits (TTS reads them
// badly; write numbers in Chinese). English words are allowed.
const SYMBOLS = /[0-9_=<>{}()[\]\\/*#`|;$&^~@%+]|->|\.\w/
const src = JSON.parse(fs.readFileSync(path.join(root, 'video/problems.src.json'), 'utf8'))
const problems = []
let bad = 0
const fail = (msg) => {
  console.error('✗ ' + msg)
  bad++
}

for (const p of src) {
  const after = cut(p.code_from)
  const before = p.before_from ? cut(p.before_from) : null
  const total = p.segments.length + (p.before_segments?.length ?? 0)
  if (total < 6 || total > 14) fail(`${p.id}: ${total} segments, want 6–14`)
  console.log(`\n=== ${p.id}  ${p.title}`)
  const check = (segs, code, label) => {
    const lines = code.split('\n')
    for (const [i, s] of segs.entries()) {
      if (SYMBOLS.test(s.text)) fail(`${p.id} ${label}[${i}]: code symbol or digit in narration: ${s.text.match(SYMBOLS)[0]} — ${s.text}`)
      for (const n of s.lines) if (n < 1 || n > lines.length) fail(`${p.id} ${label}[${i}]: line ${n} outside 1..${lines.length}`)
      console.log(`  [${label} ${i}] ${s.text}`)
      for (const n of s.lines) console.log(`        ${String(n).padStart(3)} │ ${lines[n - 1] ?? '<out of range>'}`)
    }
  }
  check(p.segments, after.code, 'after')
  if (before) check(p.before_segments || [], before.code, 'before')
  problems.push({
    id: p.id,
    title: p.title,
    subtitle: p.subtitle,
    lang: p.lang,
    code: after.code,
    segments: p.segments.map(({ lines, text, caption }) => ({ lines, text, ...(caption ? { caption } : {}) })),
    example_code: before ? before.code : '',
    example_segments: (p.before_segments || []).map(({ lines, text, caption }) => ({ lines, text, ...(caption ? { caption } : {}) })),
    // The story is 现象 → 在哪 → 定位 → 修复前 → 修复后 → 证明 → 面试. The renderer
    // plays segments, then example_segments; play_before_at says where the
    // "before" segments belong in the spoken order.
    play_before_at: p.play_before_at ?? 3,
    source: { code: after.source, before: before?.source ?? null },
  })
}
if (bad) {
  console.error(`\n${bad} problem(s); video/problems.walk.json not written`)
  process.exit(1)
}
fs.writeFileSync(path.join(root, 'video/problems.walk.json'), JSON.stringify(problems, null, 2) + '\n')
console.log(`\n✓ wrote video/problems.walk.json: ${problems.length} problems, ${problems.reduce((n, p) => n + p.segments.length + p.example_segments.length, 0)} segments`)
