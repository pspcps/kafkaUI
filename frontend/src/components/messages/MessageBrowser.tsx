'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchMessages, FetchMessagesParams } from '@/lib/api'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { formatDate } from '@/lib/utils'
import { Search, ChevronDown, ChevronRight } from 'lucide-react'

interface MessageBrowserProps {
  clusterId: string
  topic: string
  partitionCount: number
}

export function MessageBrowser({ clusterId, topic, partitionCount }: MessageBrowserProps) {
  const [params, setParams] = useState<FetchMessagesParams>({ limit: 50 })
  const [applied, setApplied] = useState<FetchMessagesParams>({ limit: 50 })
  const [expanded, setExpanded] = useState<number | null>(null)

  const { data, isLoading, isFetching, refetch } = useQuery({
    queryKey: ['messages', clusterId, topic, applied],
    queryFn: () => fetchMessages(clusterId, topic, applied),
  })

  const partitions = Array.from({ length: partitionCount }, (_, i) => i)

  return (
    <Card>
      <CardHeader>
        <CardTitle>Message Browser</CardTitle>
      </CardHeader>
      <CardContent>
        {/* Filters */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
          <select
            className="rounded-lg border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            value={params.partition ?? -1}
            onChange={(e) => setParams({ ...params, partition: parseInt(e.target.value) })}
          >
            <option value={-1}>All Partitions</option>
            {partitions.map((p) => (
              <option key={p} value={p}>Partition {p}</option>
            ))}
          </select>
          <Input
            placeholder="Offset"
            type="number"
            min={0}
            value={params.offset ?? ''}
            onChange={(e) => setParams({ ...params, offset: e.target.value ? parseInt(e.target.value) : undefined })}
          />
          <Input
            placeholder="Filter by key"
            value={params.filterKey ?? ''}
            onChange={(e) => setParams({ ...params, filterKey: e.target.value || undefined })}
          />
          <Input
            placeholder="Filter by value"
            value={params.filterValue ?? ''}
            onChange={(e) => setParams({ ...params, filterValue: e.target.value || undefined })}
          />
        </div>
        <div className="flex items-center gap-3 mb-6">
          <select
            className="rounded-lg border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            value={params.limit ?? 50}
            onChange={(e) => setParams({ ...params, limit: parseInt(e.target.value) })}
          >
            {[20, 50, 100, 200, 500].map((n) => (
              <option key={n} value={n}>{n} messages</option>
            ))}
          </select>
          <Button
            size="sm"
            onClick={() => { setApplied(params); refetch() }}
            loading={isLoading || isFetching}
          >
            <Search className="w-4 h-4" /> Fetch Messages
          </Button>
        </div>

        {/* Results */}
        {isLoading ? (
          <div className="text-center text-gray-400 py-8">Loading messages…</div>
        ) : !data || data.messages.length === 0 ? (
          <div className="text-center text-gray-400 py-8">No messages found.</div>
        ) : (
          <div className="space-y-2">
            <p className="text-xs text-gray-500 mb-3">
              Showing {data.messages.length} messages
            </p>
            {data.messages.map((msg, idx) => (
              <div key={idx} className="border border-gray-200 rounded-lg overflow-hidden">
                <button
                  className="w-full flex items-center gap-3 px-4 py-3 bg-gray-50 hover:bg-gray-100 text-left transition-colors"
                  onClick={() => setExpanded(expanded === idx ? null : idx)}
                >
                  {expanded === idx ? (
                    <ChevronDown className="w-4 h-4 text-gray-400 flex-shrink-0" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-gray-400 flex-shrink-0" />
                  )}
                  <span className="font-mono text-xs text-gray-500 w-24">P{msg.partition}@{msg.offset}</span>
                  <span className="font-mono text-xs text-gray-400 w-40">{formatDate(msg.timestamp)}</span>
                  <span className="font-mono text-xs text-gray-600 truncate flex-1">
                    {msg.key ? <span className="text-blue-600">{msg.key}</span> : <span className="italic">null</span>}
                    {' → '}
                    {msg.value.substring(0, 100)}{msg.value.length > 100 && '…'}
                  </span>
                </button>
                {expanded === idx && (
                  <div className="px-4 py-4 border-t border-gray-200 bg-white space-y-3">
                    <div>
                      <p className="text-xs font-semibold text-gray-500 mb-1">KEY</p>
                      <pre className="font-mono text-sm bg-gray-50 rounded p-2 overflow-x-auto">{msg.key || '(null)'}</pre>
                    </div>
                    <div>
                      <p className="text-xs font-semibold text-gray-500 mb-1">VALUE</p>
                      <pre className="font-mono text-sm bg-gray-50 rounded p-2 overflow-x-auto whitespace-pre-wrap break-all">
                        {(() => {
                          try { return JSON.stringify(JSON.parse(msg.value), null, 2) }
                          catch { return msg.value }
                        })()}
                      </pre>
                    </div>
                    {Object.keys(msg.headers).length > 0 && (
                      <div>
                        <p className="text-xs font-semibold text-gray-500 mb-1">HEADERS</p>
                        <pre className="font-mono text-sm bg-gray-50 rounded p-2 overflow-x-auto">
                          {JSON.stringify(msg.headers, null, 2)}
                        </pre>
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
