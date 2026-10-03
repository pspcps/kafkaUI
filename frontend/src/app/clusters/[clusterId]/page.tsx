'use client'

import { useQuery } from '@tanstack/react-query'
import { useParams } from 'next/navigation'
import { fetchCluster } from '@/lib/api'
import { Header } from '@/components/layout/Header'
import { StatCard } from '@/components/cluster/StatCard'
import { BrokerTable } from '@/components/cluster/BrokerTable'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { Server, BookOpen, Users, Activity, AlertCircle } from 'lucide-react'

export default function ClusterDashboardPage() {
  const { clusterId } = useParams<{ clusterId: string }>()

  const { data, isLoading, error } = useQuery({
    queryKey: ['cluster', clusterId],
    queryFn: () => fetchCluster(clusterId),
    refetchInterval: 30_000,
  })

  return (
    <div>
      <Header title="Dashboard" subtitle="Cluster overview" />

      {isLoading && (
        <div className="flex items-center justify-center py-20 text-gray-400">
          <Activity className="w-6 h-6 animate-pulse mr-2" />
          Connecting to cluster…
        </div>
      )}

      {error && (
        <div className="flex items-center gap-3 bg-red-50 border border-red-200 text-red-700 rounded-xl p-4 mb-6">
          <AlertCircle className="w-5 h-5 flex-shrink-0" />
          <div>
            <p className="font-medium">Failed to connect to cluster</p>
            <p className="text-sm mt-0.5">{(error as Error).message}</p>
          </div>
        </div>
      )}

      {data && (
        <>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
            <StatCard label="Brokers"          value={data.brokerCount}              icon={Server}   iconColor="text-blue-600"   iconBg="bg-blue-50" />
            <StatCard label="Topics"           value={data.topicCount}               icon={BookOpen} iconColor="text-purple-600" iconBg="bg-purple-50" />
            <StatCard label="Consumer Groups"  value={data.consumerGroups}           icon={Users}    iconColor="text-green-600"  iconBg="bg-green-50" />
            <StatCard label="Controller"       value={`Broker #${data.controller}`}  icon={Activity} iconColor="text-orange-600" iconBg="bg-orange-50" />
          </div>

          <Card>
            <CardHeader><CardTitle>Brokers</CardTitle></CardHeader>
            <CardContent className="p-0 pb-0">
              <BrokerTable brokers={data.brokers} />
            </CardContent>
          </Card>
        </>
      )}
    </div>
  )
}
