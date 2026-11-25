#!/usr/bin/env node
// Record the learn page following one notification from submit to
// delivered (and a replay), then the provider page, paced by the
// narration. For every segment of video/narration.json: run its action,
// draw a red frame around its element, then hold for the segment's spoken
// duration (video/durations.json, default 4 s). Writes one webm plus
// video/timeline.json with the real start of each segment, which is what
// the audio is aligned to (an action that waits on the backend can run
// longer than its narration, so planned and real starts differ).
//
//   node video/record.mjs            (needs the stack and Vite on :5185;
//                                     stop this repo's tests first)
import { chromium } from '@playwright/test'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const base = process.env.DISPATCHD_UI_URL || 'http://localhost:5185'
const apiKey = process.env.DISPATCHD_API_KEY || 'dk_demo_local_only'
const segments = JSON.parse(fs.readFileSync(path.join(here, 'narration.json'), 'utf8'))
const durFile = path.join(here, 'durations.json')
const durations = fs.existsSync(durFile) ? JSON.parse(fs.readFileSync(durFile, 'utf8')) : {}
const outDir = path.join(here, 'out')
fs.mkdirSync(outDir, { recursive: true })

const size = { width: 1600, height: 900 }
const browser = await chromium.launch()
const context = await browser.newContext({ viewport: size, recordVideo: { dir: outDir, size }, deviceScaleFactor: 1 })
const page = await context.newPage()
const t0 = Date.now()

// The highlight is a red frame drawn over the page, not a class on the
// element: it hugs exactly the thing being discussed (a number, a span
// row, a button), follows it if the page scrolls, and pulses when it
// appears. shape "circle" draws an ellipse for small targets. Returns the
// target's and the frame's rectangles so the run can prove the frame is
// on the target; a missing or zero-size target throws instead of
// silently framing nothing.
async function frame(sel, shape = 'box') {
  if (!sel) {
    await page.evaluate(() => document.getElementById('dd-frame')?.remove())
    return null
  }
  const loc = page.locator(sel).first()
  await loc.waitFor({ state: 'visible', timeout: 15_000 })
  await loc.evaluate((el) => el.scrollIntoView({ block: 'center', inline: 'nearest' }))
  await page.waitForTimeout(120)
  await page.evaluate(({ sel, shape }) => {
    document.getElementById('dd-frame')?.remove()
    if (!document.getElementById('dd-frame-style')) {
      const st = document.createElement('style')
      st.id = 'dd-frame-style'
      st.textContent = `@keyframes dd-pulse { 0% { transform: scale(1.35); opacity: 0 } 55% { transform: scale(.96); opacity: 1 } 75% { transform: scale(1.04) } 100% { transform: scale(1) } }
        #dd-frame { position: fixed; pointer-events: none; z-index: 9998; border: 3px solid #e5383b; box-shadow: 0 0 0 6px rgba(229,56,59,.25); animation: dd-pulse .55s ease-out; }`
      document.head.appendChild(st)
    }
    const el = [...document.querySelectorAll(sel)].find((e) => e.getBoundingClientRect().width > 0)
    if (!el) throw new Error('frame target not found: ' + sel)
    const f = document.createElement('div')
    f.id = 'dd-frame'
    document.body.appendChild(f)
    const place = () => {
      const r = el.getBoundingClientRect()
      const px = shape === 'circle' ? Math.max(14, r.width * 0.22) : 5
      const py = shape === 'circle' ? Math.max(10, r.height * 0.45) : 4
      Object.assign(f.style, { left: r.left - px + 'px', top: r.top - py + 'px', width: r.width + 2 * px + 'px', height: r.height + 2 * py + 'px', borderRadius: shape === 'circle' ? '50%' : '8px' })
      return r
    }
    place()
    const follow = () => { if (f.isConnected) { place(); requestAnimationFrame(follow) } }
    requestAnimationFrame(follow)
    return true
  }, { sel, shape })
  // Measure once the pulse (a transform) has settled; measuring during
  // the animation would report the scaled-up frame.
  await page.waitForTimeout(650)
  return page.evaluate((sel) => {
    const el = [...document.querySelectorAll(sel)].find((e) => e.getBoundingClientRect().width > 0)
    const f = document.getElementById('dd-frame')
    if (!el || !f) return null
    const r = el.getBoundingClientRect(), fr = f.getBoundingClientRect()
    return { target: [r.left, r.top, r.width, r.height].map(Math.round), frame: [fr.left, fr.top, fr.width, fr.height].map(Math.round) }
  }, sel)
}

async function caption(text) {
  // Show a link's URL on screen instead of leaving the page for GitHub.
  await page.evaluate((text) => {
    let c = document.getElementById('dd-caption')
    if (!c) {
      c = document.createElement('div')
      c.id = 'dd-caption'
      c.style.cssText = 'position:fixed;left:50%;bottom:24px;transform:translateX(-50%);background:#111827;color:#fde68a;font:14px ui-monospace,monospace;padding:8px 14px;border-radius:8px;z-index:9999;max-width:90%;word-break:break-all'
      document.body.appendChild(c)
    }
    c.textContent = text
    c.style.display = text ? 'block' : 'none'
  }, text)
}

async function act(action) {
  const k = action.indexOf(':')
  const name = k < 0 ? action : action.slice(0, k)
  const arg = k < 0 ? '' : action.slice(k + 1)
  switch (name) {
    case 'open': {
      // Start from a known state: both fake providers healthy.
      for (const p of ['email', 'push'])
        await page.request.post(`${base}/v1/admin/providers/${p}/mode`, { headers: { Authorization: `Bearer ${apiKey}` }, data: { mode: 'ok' } })
      await page.goto(base + '/learn')
      return
    }
    case 'select': {
      // Wait until the backend has written this stage, frame it, click it.
      const step = page.locator(`[data-testid=step-${arg}][data-status=done]`)
      await step.waitFor({ timeout: 60_000 })
      await frame(`[data-testid=step-${arg}]`, 'box')
      await page.waitForTimeout(800)
      await step.click()
      await page.waitForTimeout(400)
      return
    }
    case 'source': {
      const a = page.locator(arg).first()
      await a.hover()
      await caption(await a.getAttribute('href'))
      return
    }
    case 'click': {
      // Frame what is about to be clicked, let the viewer see it, click.
      await frame(arg, 'box')
      await page.waitForTimeout(900)
      await page.locator(arg).first().click()
      await page.waitForTimeout(400)
      return
    }
    case 'goto':
      await page.goto(base + arg)
      await page.waitForLoadState('networkidle')
      return
    case 'show':
      return
    default:
      throw new Error(`unknown action ${action}`)
  }
}

// @learn:record-pacing
const timeline = []
for (const s of segments) {
  const startMs = Date.now() - t0
  await caption('')
  await act(s.action)
  const rects = await frame(s.highlight, s.shape || 'box')
  const want = (durations[s.id] ?? 4) * 1000
  const spent = Date.now() - t0 - startMs
  if (spent < want) await page.waitForTimeout(want - spent)
  timeline.push({ id: s.id, part: s.part, startMs, durMs: Date.now() - t0 - startMs, plannedMs: want, highlight: s.highlight, shape: s.shape || 'box', rects })
  console.log(`${s.id} ${s.part} @${(startMs / 1000).toFixed(1)}s ${s.action}`)
}
// @learn:end record-pacing
await page.waitForTimeout(800)
const video = page.video()
await context.close()
await browser.close()
const webm = path.join(outDir, 'learn.webm')
fs.renameSync(await video.path(), webm)
fs.writeFileSync(path.join(here, 'timeline.json'), JSON.stringify({ video: path.relative(path.dirname(here), webm), size, segments: timeline }, null, 2) + '\n')
console.log(`wrote ${webm} and video/timeline.json (${timeline.length} segments, ${((Date.now() - t0) / 1000).toFixed(0)} s)`)
