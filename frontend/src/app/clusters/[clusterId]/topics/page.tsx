'use client'

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import { fetchTopics, deleteTopic } from '@/lib/api'
import { Header } from '@/components/layout/Header'
import { Card, CardContent } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { Badge } from '@/components/ui/Badge'
import { Table, TableHead, TableBody, Th, Td } from '@/components/ui/Table'
import { CreateTopicModal } from '@/components/topics/CreateTopicModal'
import { formatNumber } from '@/lib/utils'
import { Plus, Trash2, ChevronRight, Search, AlertCircle } from 'lucide-react'

export default function TopicsPage() {
  const { clusterId } = useParams<{ clusterId: string }>()
  const qc = useQueryClient()
  const [search, setSearch] = useState('')
  const [showCreate, setShowCreate] = useState(false)
  const [includeInternal, setIncludeInternal] = useState(false)

  const { data: topics = [], isLoading, error } = useQuery({
    queryKey: ['topics', clusterId, includeInternal],
    queryFn: () => fetchTopics(clusterId, includeInternal),
  })

  const deleteMutation = useMutation({
    mutationFn: (name: string) => deleteTopic(clusterId, name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['topics', clusterId] }),
  })

  const filtered = topics.filter((t) =>
    t.name.toLowerCase().includes(search.toLowerCase())
  )

  return (
    <div>
      <Header
        title="Topics"
        subtitle={`${topics.length} topic${topics.length !== 1 ? 's' : ''}`}
        actions={
          <Button size="sm" onClick={() => setShowCreate(true)}>
            <Plus className="w-4 h-4" /> New Topic
          </Button>
        }
      />

      <div className="flex items-center gap-4 mb-4">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            className="w-full pl-9 pr-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            placeholder="Search topics…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
        <label className="flex items-center gap-2 text-sm text-gray-600 cursor-pointer">
          <input
            type="checkbox"
            checked={includeInternal}
            onChange={(e) => setIncludeInternal(e.target.checked)}
            className="rounded"
          />
          Show internal topics
        </label>
      </div>

      {error && (
        <div className="flex items-center gap-3 bg-red-50 border border-red-200 text-red-700 rounded-xl p-4 mb-4">
          <AlertCircle className="w-5 h-5" />
          {(error as Error).message}
        </div>
      )}

      <Card>
        <CardContent className="p-0">
          {isLoading ? (
            <div className="py-12 text-center text-gray-400">Loading topics…</div>
          ) : filtered.length === 0 ? (
            <div className="py-12 text-center text-gray-400">
              {search ? 'No topics match your search.' : 'No topics found.'}
            </div>
          ) : (
            <Table>
              <TableHead>
                <tr>
                  <Th>Name</Th>
                  <Th>Partitions</Th>
                  <Th>Replication</Th>
                  <Th>Messages</Th>
                  <Th>Type</Th>
                  <Th className="w-24">Actions</Th>
                </tr>
              </TableHead>
              <TableBody>
                {filtered.map((topic) => (
                  <tr key={topic.name} className="hover:bg-gray-50">
                    <Td>
                      <Link
                        href={`/clusters/${clusterId}/topics/${encodeURIComponent(topic.name)}`}
                        className="font-medium text-blue-600 hover:underline flex items-center gap-1"
                      >
                        {topic.name}
                        <ChevronRight className="w-3.5 h-3.5 opacity-50" />
                      </Link>
                    </Td>
                    <Td>{topic.partitions}</Td>
                    <Td>{topic.replicationFactor}</Td>
                    <Td>{formatNumber(topic.messageCount)}</Td>
                    <Td>
                      {topic.internal
                        ? <Badge variant="warning">Internal</Badge>
                        : <Badge variant="success">User</Badge>}
                    </Td>
                    <Td>
                      <button
                        onClick={() => {
                          if (confirm(`Delete topic "${topic.name}"?`)) {
                            deleteMutation.mutate(topic.name)
                          }
                        }}
                        disabled={topic.internal}
                        className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
                        title="Delete topic"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </Td>
                  </tr>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {showCreate && (
        <CreateTopicModal
          clusterId={clusterId}
          onClose={() => setShowCreate(false)}
        />
      )}
    </div>
  )
}
