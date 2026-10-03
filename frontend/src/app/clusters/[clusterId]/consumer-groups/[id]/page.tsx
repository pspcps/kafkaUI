'use client'

import { useQuery } from '@tanstack/react-query'
import { useParams, useRouter } from 'next/navigation'
import { fetchConsumerGroup } from '@/lib/api'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { Table, TableHead, TableBody, Th, Td } from '@/components/ui/Table'
import { formatNumber, stateColor } from '@/lib/utils'
import { ArrowLeft, AlertCircle } from 'lucide-react'

export default function ConsumerGroupDetailPage() {
  const { clusterId, id } = useParams<{ clusterId: string; id: string }>()
  const router = useRouter()
  const groupId = decodeURIComponent(id)

  const { data, isLoading, error } = useQuery({
    queryKey: ['consumer-group', clusterId, groupId],
    queryFn: () => fetchConsumerGroup(clusterId, groupId),
    refetchInterval: 10_000,
  })

  return (
    <div>
      <div className="flex items-center gap-3 mb-6">
        <button
          onClick={() => router.push(`/clusters/${clusterId}/consumer-groups`)}
          className="p-2 rounded-lg text-gray-500 hover:bg-gray-100"
        >
          <ArrowLeft className="w-4 h-4" />
        </button>
        <div>
          <div className="flex items-center gap-3">
            <h1 className="text-2xl font-bold text-gray-900 font-mono">{groupId}</h1>
            {data && (
              <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${stateColor(data.state)}`}>
                {data.state}
              </span>
            )}
          </div>
          {data && (
            <p className="text-sm text-gray-500 mt-0.5">
              {data.members.length} member{data.members.length !== 1 ? 's' : ''}
            </p>
          )}
        </div>
      </div>

      {isLoading && <div className="text-gray-400 py-12 text-center">Loading consumer group…</div>}

      {error && (
        <div className="flex items-center gap-3 bg-red-50 border border-red-200 text-red-700 rounded-xl p-4 mb-4">
          <AlertCircle className="w-5 h-5" />
          {(error as Error).message}
        </div>
      )}

      {data && (
        <div className="space-y-6">
          {data.members.length > 0 && (
            <Card>
              <CardHeader><CardTitle>Members ({data.members.length})</CardTitle></CardHeader>
              <CardContent className="p-0">
                <Table>
                  <TableHead>
                    <tr><Th>Member ID</Th><Th>Client ID</Th><Th>Client Host</Th></tr>
                  </TableHead>
                  <TableBody>
                    {data.members.map((m) => (
                      <tr key={m.memberId} className="hover:bg-gray-50">
                        <Td className="font-mono text-xs max-w-xs truncate">{m.memberId}</Td>
                        <Td className="font-mono text-xs">{m.clientId}</Td>
                        <Td className="font-mono text-xs">{m.clientHost}</Td>
                      </tr>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          )}

          {data.topicLag.length > 0 ? (
            data.topicLag.map((topicInfo) => (
              <Card key={topicInfo.topic}>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <CardTitle className="font-mono">{topicInfo.topic}</CardTitle>
                    <div className="text-sm text-gray-500">
                      Total lag:{' '}
                      <span className={`font-semibold ${
                        topicInfo.totalLag > 10000 ? 'text-red-600' :
                        topicInfo.totalLag > 1000  ? 'text-yellow-600' :
                        topicInfo.totalLag > 0     ? 'text-blue-600' :
                        'text-green-600'
                      }`}>
                        {formatNumber(topicInfo.totalLag)}
                      </span>
                    </div>
                  </div>
                </CardHeader>
                <CardContent className="p-0">
                  <Table>
                    <TableHead>
                      <tr>
                        <Th>Partition</Th>
                        <Th>Consumer Offset</Th>
                        <Th>End Offset</Th>
                        <Th>Lag</Th>
                        <Th>Progress</Th>
                      </tr>
                    </TableHead>
                    <TableBody>
                      {topicInfo.partitions.map((p) => {
                        const pct = p.endOffset > 0
                          ? Math.min(100, Math.round((p.consumerOffset / p.endOffset) * 100))
                          : 100
                        return (
                          <tr key={p.partitionId} className="hover:bg-gray-50">
                            <Td><span className="font-mono font-semibold">{p.partitionId}</span></Td>
                            <Td className="font-mono text-xs">{p.consumerOffset}</Td>
                            <Td className="font-mono text-xs">{p.endOffset}</Td>
                            <Td>
                              <span className={`font-semibold text-sm ${
                                p.lag > 5000 ? 'text-red-600' :
                                p.lag > 500  ? 'text-yellow-600' :
                                p.lag > 0    ? 'text-blue-600' :
                                'text-green-600'
                              }`}>
                                {formatNumber(p.lag)}
                              </span>
                            </Td>
                            <Td>
                              <div className="flex items-center gap-2">
                                <div className="flex-1 bg-gray-200 rounded-full h-2 min-w-[80px]">
                                  <div
                                    className={`h-2 rounded-full transition-all ${
                                      pct >= 99 ? 'bg-green-500' :
                                      pct >= 80 ? 'bg-blue-500' :
                                      pct >= 50 ? 'bg-yellow-500' :
                                      'bg-red-500'
                                    }`}
                                    style={{ width: `${pct}%` }}
                                  />
                                </div>
                                <span className="text-xs text-gray-500 w-10 text-right">{pct}%</span>
                              </div>
                            </Td>
                          </tr>
                        )
                      })}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            ))
          ) : (
            <Card>
              <CardContent>
                <div className="py-8 text-center text-gray-400 text-sm">
                  No committed offsets found for this consumer group.
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      )}
    </div>
  )
}
