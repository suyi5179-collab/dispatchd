import { test, expect, type Locator } from '@playwright/test'
import fs from 'node:fs'
import path from 'node:path'

const root = path.resolve(import.meta.dirname, '..')
const SRC = /^https:\/\/github\.com\/[^/]+\/dispatchd\/blob\/([0-9a-f]{40})\/(.+)#L(\d+)-L(\d+)$/

// Every source link is pinned to a commit and points at lines that exist.
async function checkSourceLinks(scope: Locator) {
  const links = await scope.locator('a.src-link').all()
  expect(links.length).toBeGreaterThan(0)
  for (const a of links) {
    const href = (await a.getAttribute('href'))!
    const m = href.match(SRC)
    expect(m, href).not.toBeNull()
    const lines = fs.readFileSync(path.join(root, m![2]), 'utf8').split('\n').length
    expect(Number(m![3])).toBeLessThanOrEqual(Number(m![4]))
    expect(Number(m![4])).toBeLessThanOrEqual(lines)
  }
}

test('sending twice with the same key returns the same notification', async ({ page }) => {
  await page.goto('/')
  await page.getByTestId('recipient').fill(`e2e-${Date.now()}@example.com`)
  await page.getByTestId('send').click()
  const first = page.getByTestId('send-result').first()
  await expect(first.getByTestId('result-code')).toHaveText('202')
  const id = (await first.getByTestId('result-id').textContent())!
  await page.getByTestId('send-again').click()
  await expect(page.getByTestId('send-result')).toHaveCount(2)
  const second = page.getByTestId('send-result').first()
  await expect(second.getByTestId('replayed')).toBeVisible()
  await expect(second.getByTestId('result-id')).toHaveText(id)
  await second.getByTestId('result-id').click()
  await expect(page.getByTestId('notification-detail').getByTestId('status').first()).toHaveText('DELIVERED', { timeout: 30_000 })
  await expect(page.getByTestId('attempt-1')).toBeVisible()
})

test('a permanent failure is not retried and says what to do', async ({ page }) => {
  await page.goto('/')
  await page.getByTestId('recipient').fill('nobody@invalid.example')
  await page.getByTestId('new-key').click()
  await page.getByTestId('send').click()
  await page.getByTestId('send-result').first().getByTestId('result-id').click()
  await expect(page.getByTestId('notification-detail').getByTestId('status').first()).toHaveText('FAILED_PERMANENT', { timeout: 30_000 })
  await expect(page.getByTestId('error-action')).toContainText('do not retry')
  await expect(page.getByTestId('attempt-2')).toHaveCount(0)
})

test('provider page switches fault modes and shows the committed storm results', async ({ page }) => {
  await page.goto('/providers')
  await page.getByTestId('set-push-fail_5xx').click()
  await expect(page.getByTestId('mode-push')).toHaveText('临时故障 503')
  await page.getByTestId('set-push-ok').click()
  await expect(page.getByTestId('mode-push')).toHaveText('正常')
  await expect(page.getByTestId('live-email')).toBeVisible()
  const off = Number(await page.getByTestId('storm-nojitter-rejected').textContent())
  const on = Number(await page.getByTestId('storm-jitter-rejected').textContent())
  expect(off).toBeGreaterThan(on)
})

test('learn mode lights every stage in order, with the real rows behind it', async ({ page }) => {
  await page.goto('/learn')
  const ids = ['submit', 'idempotency', 'persist', 'relay', 'token', 'attempt1', 'delay', 'retry', 'delivered']
  await page.getByTestId('learn-start').click()
  for (let i = 0; i < ids.length; i++) {
    await expect(page.getByTestId(`step-${ids[i]}`)).toHaveAttribute('data-status', 'done', { timeout: 30_000 })
    for (const before of ids.slice(0, i)) await expect(page.getByTestId(`step-${before}`)).toHaveAttribute('data-status', 'done')
  }
  await expect(page.getByTestId('step-replay')).toHaveAttribute('data-status', 'pending')
  const check: Record<string, [string, RegExp][]> = {
    submit: [['submit-code', /^202$/], ['sample-id', /^ntf_/]],
    idempotency: [['idem-row-key', /^learn-/], ['idem-nid', /^ntf_/]],
    persist: [['persist-status', /^QUEUED$/], ['outbox-topic', /notifications\.email$/]],
    relay: [['kafka-partition', /^\d+$/], ['kafka-offset', /^\d+$/]],
    token: [['bucket-rate', /\/s/], ['claim-transition', /QUEUED.*SENDING/]],
    attempt1: [['attempt1-status', /^503$/], ['attempt1-kind', /^TEMPORARY$/]],
    delay: [['backoff-ceiling', /ms$/], ['backoff-delay', /ms$/], ['heap-len', /^\d+$/]],
    retry: [['attempt2-status', /^200$/], ['woken-late', /ms$/]],
    delivered: [['final-status', /^DELIVERED$/]],
  }
  for (const id of ids) {
    await page.getByTestId(`step-${id}`).click()
    for (const [tid, re] of check[id]) await expect(page.getByTestId('step-detail').getByTestId(tid)).toHaveText(re)
    await checkSourceLinks(page.getByTestId('step-detail'))
  }
  await page.getByTestId('learn-replay').click()
  await expect(page.getByTestId('step-replay')).toHaveAttribute('data-status', 'done', { timeout: 30_000 })
  await page.getByTestId('step-replay').click()
  const detail = page.getByTestId('step-detail')
  await expect(detail.getByTestId('replay-header')).toHaveText('true')
  await expect(detail.getByTestId('replay-id')).toHaveText((await page.getByTestId('learn-id').textContent())!)
  await expect(detail.getByTestId('replay-deliveries')).toHaveText('1')
  await expect(detail.getByTestId('replay-rows')).toHaveText('1')
})

test('each stage has its teaching video slot', async ({ page }) => {
  await page.goto('/learn')
  await page.getByTestId('video-delay').click()
  await expect(page.locator('[data-testid=part-video], [data-testid=video-missing]')).toBeVisible()
})

test('interview tab: open a question and read the answer', async ({ page }) => {
  await page.goto('/learn/interview')
  const qa = page.getByTestId('qa').first()
  await expect(qa).toBeVisible()
  expect(await page.getByTestId('qa').count()).toBeGreaterThanOrEqual(25)
  await qa.locator('summary').click()
  await expect(qa.locator('.answer')).toBeVisible()
  await expect(qa.locator('.answer')).toContainText('一句话答案')
})

test('problems tab: every problem has its write-up, source links and video slot', async ({ page }) => {
  await page.goto('/learn/problems')
  const items = page.getByTestId('problem')
  await expect(items.first()).toBeVisible()
  const n = await items.count()
  expect(n).toBeGreaterThanOrEqual(6)
  expect(n).toBeLessThanOrEqual(12)
  const first = items.first()
  await first.locator('summary').click()
  for (const part of ['现象', '在哪里', '怎么定位', '怎么解决', '怎么证明修好了', '面试怎么讲']) await expect(first.locator('.answer')).toContainText(part)
  await checkSourceLinks(first)
  await expect(first.locator('[data-testid=part-video], [data-testid=video-missing]')).toBeVisible()
})
