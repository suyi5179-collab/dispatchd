import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './style.css'
import { apiKey, DEMO_KEY } from './api'
import { navigate, usePath } from './router'
import Send from './pages/Send'
import { NotificationDetail, NotificationList } from './pages/Notifications'
import Providers from './pages/Providers'
import Learn from './pages/Learn'
import Interview from './pages/Interview'
import Problems from './pages/Problems'

function Nav({ path }: { path: string }) {
  const link = (to: string, label: string) => {
    const active = to === '/' ? path === '/' : path === to || (path.startsWith(to + '/') && to !== '/learn')
    return <a href={to} className={active ? 'active' : ''} onClick={(e) => { e.preventDefault(); navigate(to) }}>{label}</a>
  }
  return (
    <nav>
      <b className="brand">dispatchd</b>
      {link('/', '发送')}
      {link('/notifications', '通知')}
      {link('/providers', 'Provider 故障')}
      {link('/learn', '教学模式')}
      {link('/learn/interview', '面试题')}
      {link('/learn/problems', '日常问题')}
      <span className="spacer" />
      <span className="muted small" title="API key">key: <code>{apiKey() === DEMO_KEY ? `${DEMO_KEY}（demo）` : '自定义'}</code></span>
    </nav>
  )
}

function App() {
  const path = usePath()
  let page
  if (path === '/learn/interview') page = <Interview />
  else if (path === '/learn/problems') page = <Problems />
  else if (path === '/learn') page = <Learn />
  else if (path === '/providers') page = <Providers />
  else if (path === '/notifications') page = <NotificationList />
  else if (path.startsWith('/notifications/')) page = <NotificationDetail id={decodeURIComponent(path.slice('/notifications/'.length))} />
  else page = <Send />
  return (
    <>
      <Nav path={path} />
      <main key={path}>{page}</main>
    </>
  )
}

createRoot(document.getElementById('root')!).render(<StrictMode><App /></StrictMode>)
