'use client'

import { useEffect, useState } from 'react'
import { usePathname, useRouter } from 'next/navigation'
import { Sidebar } from '@/components/layout/Sidebar'
import { isAuthenticated } from '@/lib/auth'
import { resetDemo } from '@/lib/demo'

const isDemo = process.env.NEXT_PUBLIC_DEMO_MODE === 'true'

const PUBLIC_PATHS = ['/login']

export function AuthGate({ children }: { children: React.ReactNode }) {
  const pathname = usePathname()
  const router = useRouter()
  const [ready, setReady] = useState(false)

  const isPublic = PUBLIC_PATHS.includes(pathname)

  useEffect(() => {
    if (isDemo) {
      window.localStorage.setItem('kafbat_token', 'demo-only-no-backend-access')
      window.localStorage.setItem('kafbat_user', JSON.stringify({ id: 'demo-user', username: 'Demo Explorer', role: 'admin' }))
      if (isPublic) router.replace('/clusters')
      setReady(true)
      return
    }
    if (isPublic) {
      setReady(true)
      return
    }
    if (!isAuthenticated()) {
      router.replace('/login')
      return
    }
    setReady(true)
  }, [isPublic, pathname, router])

  if (!ready) {
    return (
      <div className="min-h-screen flex items-center justify-center text-gray-400 text-sm">
        Loading…
      </div>
    )
  }

  if (isPublic) {
    return <>{children}</>
  }

  return (
    <div className="flex h-screen">
      <Sidebar />
      <main className="flex-1 ml-60 overflow-y-auto">
        {isDemo && <div className="sticky top-0 z-20 flex flex-wrap items-center justify-between gap-2 border-b border-amber-200 bg-amber-50 px-8 py-3 text-sm text-amber-950">
          <p><strong>Interactive demo</strong> · Sample data only. Changes stay in this browser. Do not enter real credentials.</p>
          <button type="button" onClick={resetDemo} className="font-semibold underline underline-offset-2">Reset demo</button>
        </div>}
        <div className="p-8">{children}</div>
      </main>
    </div>
  )
}
