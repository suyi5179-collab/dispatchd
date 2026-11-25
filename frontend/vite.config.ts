import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import { execSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'

const root = path.resolve(__dirname, '..')
const videos = path.join(root, 'docs', 'videos')

// The one place the GitHub account is written down: every source link,
// doc link and video link in the UI is built from this.
const REPO_URL = process.env.VITE_REPO_URL || 'https://github.com/suyi5179-collab/dispatchd'

// Source links are pinned to a commit so line numbers stay right after
// the code changes. VITE_GIT_SHA overrides; otherwise use HEAD.
function gitSha(): string {
  if (process.env.VITE_GIT_SHA) return process.env.VITE_GIT_SHA
  try {
    return execSync('git rev-parse HEAD', { cwd: root }).toString().trim()
  } catch {
    return 'main'
  }
}

// Serve docs/videos at /videos in dev and copy it into dist on build, so
// the teaching videos live in one place in the repo.
function videosDir(): Plugin {
  return {
    name: 'dispatchd-videos',
    configureServer(server) {
      server.middlewares.use('/videos', (req, res, next) => {
        const file = path.join(videos, decodeURIComponent((req.url || '').split('?')[0]))
        if (!file.startsWith(videos) || !fs.existsSync(file) || fs.statSync(file).isDirectory() || !file.endsWith('.mp4')) return next()
        res.setHeader('Content-Type', 'video/mp4')
        fs.createReadStream(file).pipe(res)
      })
    },
    closeBundle() {
      if (fs.existsSync(videos)) fs.cpSync(videos, path.join(__dirname, 'dist', 'videos'), { recursive: true })
    },
  }
}

export default defineConfig({
  plugins: [react(), videosDir()],
  define: {
    'import.meta.env.VITE_GIT_SHA': JSON.stringify(gitSha()),
    'import.meta.env.VITE_REPO_URL': JSON.stringify(REPO_URL),
    'import.meta.env.VITE_GRAFANA_URL': JSON.stringify(process.env.VITE_GRAFANA_URL || 'http://localhost:3092'),
    'import.meta.env.VITE_PROM_URL': JSON.stringify(process.env.VITE_PROM_URL || 'http://localhost:9192'),
  },
  server: {
    port: Number(process.env.DISPATCHD_UI_PORT || 5185),
    strictPort: true,
    fs: { allow: [root] },
    proxy: { '/v1': 'http://localhost:8850' },
  },
})
