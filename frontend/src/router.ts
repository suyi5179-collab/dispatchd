import { useEffect, useState } from 'react'

// A ten-line router: the app has six screens and no nested routes.
export function navigate(to: string) {
  if (to !== location.pathname) history.pushState(null, '', to)
  window.dispatchEvent(new Event('lab:navigate'))
}

export function usePath(): string {
  const [path, setPath] = useState(location.pathname)
  useEffect(() => {
    const on = () => setPath(location.pathname)
    window.addEventListener('popstate', on)
    window.addEventListener('lab:navigate', on)
    return () => {
      window.removeEventListener('popstate', on)
      window.removeEventListener('lab:navigate', on)
    }
  }, [])
  return path
}
