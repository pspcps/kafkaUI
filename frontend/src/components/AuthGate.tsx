'use client'

import { useEffect, useState } from 'react'
import { usePathname, useRouter } from 'next/navigation'
import { Sidebar } from '@/components/layout/Sidebar'
import { isAuthenticated } from '@/lib/auth'

const PUBLIC_PATHS = ['/login']

export function AuthGate({ children }: { children: React.ReactNode }) {
  const pathname = usePathname()
  const router = useRouter()
  const [ready, setReady] = useState(false)

  const isPublic = PUBLIC_PATHS.includes(pathname)

  useEffect(() => {
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
        <div className="p-8">{children}</div>
      </main>
    </div>
  )
}
