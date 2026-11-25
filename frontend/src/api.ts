// Tiny API client. The API key lives in localStorage when it is
// available (private windows may refuse it) and in memory otherwise.
// The default is the public demo key the compose stack seeds.
export const DEMO_KEY = 'dk_demo_local_only'
let memKey: string | null = null

function read(key: string): string | null {
  try {
    return localStorage.getItem(key)
  } catch {
    return null
  }
}

export function apiKey(): string {
  return memKey ?? read('dispatchd.key') ?? DEMO_KEY
}

export function setApiKey(k: string) {
  memKey = k
  try {
    localStorage.setItem('dispatchd.key', k)
  } catch {
    /* ignore */
  }
}

export class ApiError extends Error {
  constructor(public status: number, message: string, public body?: any) {
    super(message)
  }
}

export type Resp<T> = { status: number; headers: Headers; body: T }

export async function raw<T = any>(path: string, init: RequestInit = {}): Promise<Resp<T>> {
  const headers = new Headers(init.headers)
  headers.set('Authorization', `Bearer ${apiKey()}`)
  if (init.body) headers.set('Content-Type', 'application/json')
  const r = await fetch(path, { ...init, headers })
  const text = await r.text()
  let body: any = null
  try {
    body = text ? JSON.parse(text) : null
  } catch {
    body = { error: text }
  }
  return { status: r.status, headers: r.headers, body }
}

export async function api<T = any>(path: string, init: RequestInit = {}): Promise<T> {
  const r = await raw<T>(path, init)
  if (r.status >= 400) throw new ApiError(r.status, (r.body as any)?.error || `HTTP ${r.status}`, r.body)
  return r.body
}

export const post = <T = any>(path: string, body?: unknown) => api<T>(path, { method: 'POST', body: body === undefined ? undefined : JSON.stringify(body) })

export type Notification = {
  id: string; channel: string; recipient: string; template: string; params: any; status: string; attempts: number
  last_error?: any; created_at: string; updated_at: string; delivered_at?: string; next_attempt_at?: string
}
export type Attempt = { attempt: number; provider: string; started_at: string; finished_at?: string; outcome: string; http_status: number; provider_code?: string; detail?: any; latency_ms: number }
export type Event = { id: number; stage: string; from?: string; to?: string; data?: any; at: string }

export function newKey(prefix = 'ui'): string {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
}
