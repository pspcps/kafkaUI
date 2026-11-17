'use client'

import { useState } from 'react'
import Link from 'next/link'
import { useQuery } from '@tanstack/react-query'
import { useParams, useRouter } from 'next/navigation'
import { fetchConsumerGroup } from '@/lib/api'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { Table, TableHead, TableBody, Th, Td } from '@/components/ui/Table'
import { formatNumber, stateColor } from '@/lib/utils'
import { ArrowLeft, AlertCircle, ChevronDown, ChevronRight, RefreshCw } from 'lucide-react'

export default function ConsumerGroupDetailPage() {
  const { clusterId, id } = useParams<{ clusterId: string; id: string }>()
  const router = useRouter()
  const groupId = decodeURIComponent(id)
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [search, setSearch] = useState('')
  const { data, isLoading, error, isFetching, dataUpdatedAt, refetch } = useQuery({
    queryKey: ['consumer-group', clusterId, groupId],
    queryFn: () => fetchConsumerGroup(clusterId, groupId),
    refetchInterval: 10_000,
  })
  const topics = data?.topicLag ?? []
  const visible = topics.filter((t) => t.topic.toLowerCase().includes(search.toLowerCase()))
  const totalLag = topics.reduce((sum, t) => sum + t.totalLag, 0)
  const uncommitted = topics.reduce(
    (sum, t) => sum + t.partitions.filter((p) => p.consumerOffset < 0).length,
    0,
  )
  const topicKey = (topic: string) => JSON.stringify([clusterId, groupId, topic])
  function toggle(topic: string) {
    topic = topicKey(topic)
    setExpanded((previous) => {
      const next = new Set(previous)
      if (next.has(topic)) next.delete(topic)
      else next.add(topic)
      return next
    })
  }
  return (
    <div>
      <div className="flex flex-wrap items-center gap-3 mb-6">
        <button
          type="button"
          aria-label="Back to consumer groups"
          onClick={() => router.push(`/clusters/${encodeURIComponent(clusterId)}/consumer-groups`)}
          className="rounded-lg p-2 text-gray-500 hover:bg-gray-100"
        >
          <ArrowLeft className="w-4 h-4" />
        </button>
        <div className="flex-1 min-w-0">
          <div className="flex flex-wrap items-center gap-3">
            <h1 className="text-2xl font-bold text-gray-900 font-mono break-all">{groupId}</h1>
            {data && (
              <span
                className={`rounded-full px-2.5 py-0.5 text-xs font-medium ${stateColor(data.state)}`}
              >
                {data.state}
              </span>
            )}
          </div>
          {data && (
            <p className="mt-1 text-sm text-gray-500">
              {data.members.length} members · {topics.length} topics · {formatNumber(totalLag)}{' '}
              total known lag
            </p>
          )}
        </div>
        <button
          type="button"
          onClick={() => refetch()}
          disabled={isFetching}
          className="flex items-center gap-2 rounded-lg border bg-white px-3 py-2 text-sm disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>
      {isLoading && <p className="py-12 text-center text-gray-400">Loading consumer group…</p>}
      {error && (
        <p
          role="alert"
          className="mb-4 flex gap-2 rounded border border-red-200 bg-red-50 p-4 text-sm text-red-700"
        >
          <AlertCircle className="h-4 w-4 shrink-0" />
          {data ? 'Refresh failed. Showing previously fetched data. ' : ''}
          {(error as Error).message}
        </p>
      )}
      {data && (
        <div className="space-y-5">
          <p className="text-xs text-gray-400">
            Updated {new Date(dataUpdatedAt).toLocaleTimeString()} · refreshes every 10 seconds
          </p>
          {uncommitted > 0 && (
            <p className="rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
              {uncommitted} partitions have no committed offset. Their lag is unknown and excluded
              from totals.
            </p>
          )}
          <Card>
            <CardHeader>
              <CardTitle>Members ({data.members.length})</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              {data.members.length === 0 ? (
                <p className="p-4 text-sm text-gray-500">
                  No active members. Committed topic offsets may still be retained.
                </p>
              ) : (
                <Table>
                  <TableHead>
                    <tr>
                      <Th>Member ID</Th>
                      <Th>Client ID</Th>
                      <Th>Client Host</Th>
                    </tr>
                  </TableHead>
                  <TableBody>
                    {data.members.map((m) => (
                      <tr key={m.memberId}>
                        <Td className="font-mono text-xs break-all">{m.memberId}</Td>
                        <Td className="font-mono text-xs">{m.clientId}</Td>
                        <Td className="font-mono text-xs">{m.clientHost}</Td>
                      </tr>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <input
              aria-label="Filter associated topics"
              placeholder="Filter associated topics…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="rounded-lg border border-gray-300 px-3 py-2 text-sm"
            />
            <div className="flex gap-3 text-sm">
              <button
                type="button"
                onClick={() => setExpanded(new Set(visible.map((t) => topicKey(t.topic))))}
                className="text-blue-600 hover:underline"
              >
                Expand all
              </button>
              <button
                type="button"
                onClick={() => setExpanded(new Set())}
                className="text-gray-600 hover:underline"
              >
                Collapse all
              </button>
            </div>
          </div>
          {visible.map((t) => {
            const open = expanded.has(topicKey(t.topic))
            const panelId = `partitions-${encodeURIComponent(t.topic)}`
            return (
              <Card key={t.topic}>
                <div className="flex flex-wrap items-center gap-3 px-5 py-4">
                  <button
                    type="button"
                    aria-expanded={open}
                    aria-controls={panelId}
                    aria-label={`${open ? 'Collapse' : 'Expand'} partitions for ${t.topic}`}
                    onClick={() => toggle(t.topic)}
                    className="rounded p-1 text-gray-500 hover:bg-gray-100"
                  >
                    {open ? (
                      <ChevronDown className="h-5 w-5" />
                    ) : (
                      <ChevronRight className="h-5 w-5" />
                    )}
                  </button>
                  <div className="flex-1 min-w-0">
                    <Link
                      href={`/clusters/${encodeURIComponent(clusterId)}/topics/${encodeURIComponent(t.topic)}`}
                      className="font-mono font-semibold text-blue-600 hover:underline break-all"
                    >
                      {t.topic}
                    </Link>
                    <p className="text-xs text-gray-400 mt-1">
                      {t.partitions.length} committed/assigned partitions
                    </p>
                  </div>
                  <span className="text-sm text-gray-500">
                    Topic lag:{' '}
                    <strong className={t.totalLag > 0 ? 'text-amber-700' : 'text-green-700'}>
                      {formatNumber(t.totalLag)}
                    </strong>
                  </span>
                </div>
                {open && (
                  <div id={panelId} className="border-t border-gray-100">
                    <Table>
                      <TableHead>
                        <tr>
                          <Th>Partition</Th>
                          <Th>Consumer Offset</Th>
                          <Th>End Offset</Th>
                          <Th>Lag</Th>
                          <Th>Status</Th>
                        </tr>
                      </TableHead>
                      <TableBody>
                        {t.partitions.map((p) => (
                          <tr key={p.partitionId}>
                            <Td className="font-mono">{p.partitionId}</Td>
                            <Td className="font-mono text-xs">
                              {p.consumerOffset < 0
                                ? 'Not committed'
                                : p.consumerOffset.toLocaleString()}
                            </Td>
                            <Td className="font-mono text-xs">{p.endOffset.toLocaleString()}</Td>
                            <Td>
                              <strong
                                className={
                                  p.consumerOffset < 0
                                    ? 'text-gray-400'
                                    : p.lag > 0
                                      ? 'text-amber-700'
                                      : 'text-green-700'
                                }
                              >
                                {p.consumerOffset < 0 ? 'Unknown' : formatNumber(p.lag)}
                              </strong>
                            </Td>
                            <Td className="text-sm">
                              {p.consumerOffset < 0
                                ? 'No committed offset'
                                : p.lag === 0
                                  ? 'Caught up'
                                  : 'Behind'}
                            </Td>
                          </tr>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </Card>
            )
          })}
          {visible.length === 0 && (
            <p className="py-8 text-center text-sm text-gray-400">
              {topics.length
                ? 'No topics match your filter.'
                : 'No committed topic offsets found for this group.'}
            </p>
          )}
        </div>
      )}
    </div>
  )
}
