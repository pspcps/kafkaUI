'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { useQuery } from '@tanstack/react-query'
import { fetchClusters } from '@/lib/api'
import { cn } from '@/lib/utils'
import {
  LayoutDashboard,
  BookOpen,
  Users,
  Activity,
  Server,
  ChevronRight,
  ChevronDown,
  Layers,
  GitMerge,
  LogOut,
} from 'lucide-react'
import { useEffect, useState } from 'react'
import { getUser, logout, type AuthUser } from '@/lib/auth'

const clusterNavItems = [
  { href: '',                 label: 'Dashboard',       icon: LayoutDashboard },
  { href: '/topics',          label: 'Topics',          icon: BookOpen },
  { href: '/consumer-groups', label: 'Consumer Groups', icon: Users },
]

export function Sidebar() {
  const pathname = usePathname()
  const [user, setUserState] = useState<AuthUser | null>(null)

  useEffect(() => {
    setUserState(getUser())
  }, [])

  // Extract active cluster id from URL: /clusters/[clusterId]/...
  const match = pathname.match(/^\/clusters\/([^/]+)/)
  const activeClusterId = match?.[1]

  const { data: clusters = [] } = useQuery({
    queryKey: ['clusters'],
    queryFn: fetchClusters,
    staleTime: 60_000,
  })

  return (
    <aside className="fixed left-0 top-0 h-screen w-60 bg-gray-900 text-white flex flex-col z-30">
      {/* Logo */}
      <div className="flex items-center gap-3 px-6 py-5 border-b border-gray-800">
        <Activity className="w-6 h-6 text-blue-400" />
        <span className="font-bold text-lg tracking-tight">KafkaUI</span>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-4 overflow-y-auto">
        {/* Clusters overview link */}
        <Link
          href="/clusters"
          className={cn(
            'flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors mb-3',
            pathname === '/clusters'
              ? 'bg-blue-600 text-white'
              : 'text-gray-400 hover:bg-gray-800 hover:text-white'
          )}
        >
          <Layers className="w-[18px] h-[18px] flex-shrink-0" />
          <span>All Clusters</span>
        </Link>

        {/* Global nav */}
        <Link
          href="/jobs"
          className={cn(
            'flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors mb-1',
            pathname.startsWith('/jobs')
              ? 'bg-blue-600 text-white'
              : 'text-gray-400 hover:bg-gray-800 hover:text-white'
          )}
        >
          <GitMerge className="w-[18px] h-[18px] flex-shrink-0" />
          <span>Replication Jobs</span>
        </Link>

        <div className="my-3 border-t border-gray-800" />

        <p className="px-3 pb-1 text-xs font-semibold text-gray-500 uppercase tracking-wider">
          Clusters
        </p>

        <div className="space-y-0.5">
          {clusters.map((cluster) => {
            const isExpanded = activeClusterId === cluster.id
            const base = `/clusters/${cluster.id}`

            return (
              <div key={cluster.id}>
                <Link
                  href={base}
                  className={cn(
                    'flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors',
                    isExpanded
                      ? 'bg-gray-800 text-white'
                      : 'text-gray-400 hover:bg-gray-800 hover:text-white'
                  )}
                >
                  <Server className="w-[18px] h-[18px] flex-shrink-0" />
                  <span className="flex-1 truncate">{cluster.name}</span>
                  {isExpanded
                    ? <ChevronDown className="w-3.5 h-3.5 opacity-60 flex-shrink-0" />
                    : <ChevronRight className="w-3.5 h-3.5 opacity-40 flex-shrink-0" />}
                </Link>

                {isExpanded && (
                  <div className="ml-3 mt-0.5 mb-1 border-l border-gray-700 pl-3 space-y-0.5">
                    {clusterNavItems.map(({ href, label, icon: Icon }) => {
                      const fullHref = `${base}${href}`
                      const isActive = href === ''
                        ? pathname === base
                        : pathname.startsWith(fullHref)
                      return (
                        <Link
                          key={fullHref}
                          href={fullHref}
                          className={cn(
                            'flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors',
                            isActive
                              ? 'bg-blue-600 text-white font-medium'
                              : 'text-gray-400 hover:bg-gray-800 hover:text-white'
                          )}
                        >
                          <Icon className="w-4 h-4 flex-shrink-0" />
                          <span>{label}</span>
                        </Link>
                      )
                    })}
                  </div>
                )}
              </div>
            )
          })}

          {clusters.length === 0 && (
            <p className="px-3 py-2 text-xs text-gray-500 italic">No clusters configured</p>
          )}
        </div>
      </nav>

      {user && (
        <div className="px-3 py-3 border-t border-gray-800">
          <div className="flex items-center gap-2 px-3 py-2 text-xs">
            <div className="flex-1 min-w-0">
              <div className="font-medium text-white truncate">{user.username}</div>
              <div className="text-gray-500 capitalize">{user.role}</div>
            </div>
            <button
              onClick={logout}
              title="Sign out"
              className="text-gray-400 hover:text-white p-1 rounded hover:bg-gray-800"
            >
              <LogOut className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}

      <div className="px-6 py-3 border-t border-gray-800 text-xs text-gray-500">
        KafkaUI v1.0.0
      </div>
    </aside>
  )
}
