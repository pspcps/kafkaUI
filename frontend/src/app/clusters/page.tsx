'use client'

import { useQuery } from '@tanstack/react-query'
import Link from 'next/link'
import { fetchClusters } from '@/lib/api'
import { Card } from '@/components/ui/Card'
import { Header } from '@/components/layout/Header'
import { Server, ChevronRight, AlertCircle, Activity } from 'lucide-react'

export default function ClustersPage() {
  const { data: clusters = [], isLoading, error } = useQuery({
    queryKey: ['clusters'],
    queryFn: fetchClusters,
  })

  return (
    <div>
      <Header title="Clusters" subtitle="Select an environment to manage" />

      {isLoading && (
        <div className="flex items-center justify-center py-20 text-gray-400">
          <Activity className="w-5 h-5 animate-pulse mr-2" />
          Loading clusters…
        </div>
      )}

      {error && (
        <div className="flex items-center gap-3 bg-red-50 border border-red-200 text-red-700 rounded-xl p-4">
          <AlertCircle className="w-5 h-5" />
          {(error as Error).message}
        </div>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {clusters.map((cluster) => (
          <Link key={cluster.id} href={`/clusters/${cluster.id}`}>
            <Card className="p-6 hover:shadow-md hover:border-blue-200 transition-all cursor-pointer group">
              <div className="flex items-start justify-between mb-4">
                <div className="p-3 bg-blue-50 rounded-lg group-hover:bg-blue-100 transition-colors">
                  <Server className="w-6 h-6 text-blue-600" />
                </div>
                <ChevronRight className="w-5 h-5 text-gray-300 group-hover:text-blue-500 transition-colors" />
              </div>
              <h3 className="text-lg font-bold text-gray-900">{cluster.name}</h3>
              <p className="text-xs text-gray-400 mt-1 font-mono">{cluster.id}</p>
              <div className="mt-3 flex flex-wrap gap-1">
                {cluster.brokers.map((b) => (
                  <span key={b} className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded font-mono">
                    {b}
                  </span>
                ))}
              </div>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  )
}
