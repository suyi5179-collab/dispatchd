#!/usr/bin/env node
// Check video/narration.json and video/parts.json before recording:
// narration is spoken (no code symbols, no Arabic digits), each segment's
// action is one record.mjs implements, segments of a part are contiguous,
// and every part is listed in parts.json with an existing chapter.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const root = path.resolve(here, '..')
const segs = JSON.parse(fs.readFileSync(path.join(here, 'narration.json'), 'utf8'))
const parts = JSON.parse(fs.readFileSync(path.join(here, 'parts.json'), 'utf8'))
const SYMBOLS = /[0-9_=<>{}()[\]\\/*#`|;$&^~@%+]|->|\.\w/
const ACTIONS = new Set(['open', 'select', 'source', 'click', 'goto', 'show'])
const bad = []
const seen = []
for (const s of segs) {
  if (SYMBOLS.test(s.text)) bad.push(`${s.id}: code symbol or digit "${s.text.match(SYMBOLS)[0]}" in: ${s.text}`)
  if (!ACTIONS.has(s.action.split(':')[0])) bad.push(`${s.id}: unknown action ${s.action}`)
  if (seen[seen.length - 1] !== s.part) {
    if (seen.includes(s.part)) bad.push(`${s.id}: part ${s.part} is not contiguous`)
    seen.push(s.part)
  }
}
for (const p of seen) {
  const meta = parts.find((x) => x.part === p)
  if (!meta) bad.push(`part ${p} missing from parts.json`)
  else for (const c of meta.chapters ?? [meta.chapter]) if (!fs.existsSync(path.join(root, c))) bad.push(`part ${p}: chapter ${c} does not exist`)
}
for (const p of parts) if (!seen.includes(p.part)) bad.push(`parts.json lists ${p.part} but no segment uses it`)
if (segs.length < 25 || segs.length > 45) bad.push(`${segs.length} segments, want 25–45`)
if (bad.length) {
  console.error(bad.map((b) => '✗ ' + b).join('\n'))
  process.exit(1)
}
console.log(`✓ ${segs.length} segments in ${seen.length} parts: ${seen.join(', ')}`)
