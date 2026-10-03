'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import { fetchConsumerGroups } from '@/lib/api'
import { Header } from '@/components/layout/Header'
import { Card, CardContent } from '@/components/ui/Card'
import { Table, TableHead, TableBody, Th, Td } from '@/components/ui/Table'
import { stateColor, formatNumber } from '@/lib/utils'
import { Search, ChevronRight, AlertCircle } from 'lucide-react'

export default function ConsumerGroupsPage() {
  const { clusterId } = useParams<{ clusterId: string }>()
  const [search, setSearch] = useState('')

  const { data: groups = [], isLoading, error } = useQuery({
    queryKey: ['consumer-groups', clusterId],
    queryFn: () => fetchConsumerGroups(clusterId),
    refetchInterval: 15_000,
  })

  const filtered = groups.filter((g) =>
    g.groupId.toLowerCase().includes(search.toLowerCase())
  )

  return (
    <div>
      <Header
        title="Consumer Groups"
        subtitle={`${groups.length} group${groups.length !== 1 ? 's' : ''}`}
      />

      <div className="relative max-w-sm mb-4">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
        <input
          className="w-full pl-9 pr-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          placeholder="Search groups…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
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
            <div className="py-12 text-center text-gray-400">Loading consumer groups…</div>
          ) : filtered.length === 0 ? (
            <div className="py-12 text-center text-gray-400">
              {search ? 'No groups match your search.' : 'No consumer groups found.'}
            </div>
          ) : (
            <Table>
              <TableHead>
                <tr>
                  <Th>Group ID</Th>
                  <Th>State</Th>
                  <Th>Members</Th>
                  <Th>Total Lag</Th>
                  <Th className="w-16">{''}</Th>
                </tr>
              </TableHead>
              <TableBody>
                {filtered.map((group) => (
                  <tr key={group.groupId} className="hover:bg-gray-50">
                    <Td>
                      <Link
                        href={`/clusters/${clusterId}/consumer-groups/${encodeURIComponent(group.groupId)}`}
                        className="font-medium text-blue-600 hover:underline font-mono text-sm flex items-center gap-1"
                      >
                        {group.groupId}
                        <ChevronRight className="w-3.5 h-3.5 opacity-50" />
                      </Link>
                    </Td>
                    <Td>
                      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${stateColor(group.state)}`}>
                        {group.state}
                      </span>
                    </Td>
                    <Td>{group.memberCount}</Td>
                    <Td>
                      <span className={`font-semibold ${
                        group.totalLag > 10000 ? 'text-red-600' :
                        group.totalLag > 1000  ? 'text-yellow-600' :
                        group.totalLag > 0     ? 'text-blue-600' :
                        'text-green-600'
                      }`}>
                        {formatNumber(group.totalLag)}
                      </span>
                    </Td>
                    <Td>
                      <Link
                        href={`/clusters/${clusterId}/consumer-groups/${encodeURIComponent(group.groupId)}`}
                        className="text-gray-400 hover:text-blue-600"
                      >
                        <ChevronRight className="w-4 h-4" />
                      </Link>
                    </Td>
                  </tr>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
