#!/usr/bin/env node
// CI gate for the learn page's source links:
//  1. anchors.json matches a fresh scan (not stale);
//  2. every anchor id the frontend references exists;
//  3. every anchor's line range lies inside its file and is not empty;
//  4. every anchor is referenced somewhere (no dead anchors).
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { scan } from './learn_anchors.mjs'

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const file = path.join(root, 'frontend/src/generated/anchors.json')
const problems = []
const { anchors, errors } = scan()
problems.push(...errors)
const committed = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : {}
if (JSON.stringify(committed) !== JSON.stringify(anchors)) problems.push('anchors.json is stale: run node scripts/learn_anchors.mjs')

// @learn:anchor-refs
const refs = new Set()
const src = path.join(root, 'frontend/src')
for (const f of fs.readdirSync(src, { recursive: true })) {
  if (!/\.(ts|tsx)$/.test(f) || f.includes('generated')) continue
  const text = fs.readFileSync(path.join(src, f), 'utf8')
  for (const m of text.matchAll(/src:\s*\[([^\]]*)\]/g)) for (const id of m[1].matchAll(/'([a-z0-9-]+)'/g)) refs.add(id[1])
  for (const m of text.matchAll(/(?:sourceUrl|anchor)\('([a-z0-9-]+)'\)/g)) refs.add(m[1])
}
// Docs link anchors as [text](../internal/x.go#anchor=id): a plain
// relative link on GitHub, an exact line range in the UI.
const docs = ['README.md', 'docs/interview.md', 'docs/problems.md', 'docs/runbook.md', ...(fs.existsSync(path.join(root, 'docs/tutorial')) ? fs.readdirSync(path.join(root, 'docs/tutorial')).map((f) => 'docs/tutorial/' + f) : [])]
for (const doc of docs) {
  const p = path.join(root, doc)
  if (!fs.existsSync(p)) continue
  for (const m of fs.readFileSync(p, 'utf8').matchAll(/\]\(([^)#\s]+)#anchor=([a-z0-9-]+)\)/g)) {
    refs.add(m[2])
    // the relative path must be the file the anchor lives in
    const target = path.relative(root, path.resolve(path.dirname(p), m[1])).split(path.sep).join('/')
    if (anchors[m[2]] && anchors[m[2]].path !== target) problems.push(`${doc}: link to ${m[1]}#anchor=${m[2]} but the anchor is in ${anchors[m[2]].path}`)
  }
}
// The problem walkthroughs (video/problems.src.json) cut code by anchor.
const psrc = path.join(root, 'video/problems.src.json')
if (fs.existsSync(psrc)) for (const p of JSON.parse(fs.readFileSync(psrc, 'utf8'))) {
  for (const f of [p.code_from, p.before_from]) if (f) refs.add(f.anchor)
  for (const id of p.where || []) refs.add(id)
}
for (const id of refs) if (!anchors[id]) problems.push(`frontend references unknown anchor ${id}`)
// @learn:end anchor-refs
for (const [id, a] of Object.entries(anchors)) {
  const lines = fs.readFileSync(path.join(root, a.path), 'utf8').split('\n')
  if (a.start > a.end) problems.push(`${id}: empty range`)
  if (a.end > lines.length) problems.push(`${id}: L${a.end} beyond end of ${a.path}`)
  if (!refs.has(id)) problems.push(`${id}: defined in ${a.path} but never referenced`)
}
if (problems.length) {
  console.error(problems.map((p) => '✗ ' + p).join('\n'))
  process.exit(1)
}
console.log(`✓ ${Object.keys(anchors).length} anchors, ${refs.size} references, all fresh`)
