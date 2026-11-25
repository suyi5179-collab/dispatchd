import anchors from './generated/anchors.json'

// Source links point at the exact commit the page was built from, so the
// line numbers (generated from @learn markers, never typed by hand) match.
export const REPO_URL: string = import.meta.env.VITE_REPO_URL
export const GIT_SHA: string = import.meta.env.VITE_GIT_SHA
export const GRAFANA_URL: string = import.meta.env.VITE_GRAFANA_URL
export const PROM_URL: string = import.meta.env.VITE_PROM_URL

export type Anchor = { path: string; start: number; end: number }
const table = anchors as Record<string, Anchor>

// @learn:anchor-lookup
export function anchor(id: string): Anchor {
  const a = table[id]
  if (!a) throw new Error(`unknown @learn anchor ${id}`)
  return a
}

export function sourceUrl(id: string): string {
  const a = anchor(id)
  return `${REPO_URL}/blob/${GIT_SHA}/${a.path}#L${a.start}-L${a.end}`
}
// @learn:end anchor-lookup
