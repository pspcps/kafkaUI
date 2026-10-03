'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useParams, useRouter } from 'next/navigation'
import { fetchTopic } from '@/lib/api'
import { Header } from '@/components/layout/Header'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { Button } from '@/components/ui/Button'
import { Table, TableHead, TableBody, Th, Td } from '@/components/ui/Table'
import { MessageBrowser } from '@/components/messages/MessageBrowser'
import { ProduceMessageModal } from '@/components/messages/ProduceMessageModal'
import { SchemaEditor } from '@/components/schema/SchemaEditor'
import { formatNumber } from '@/lib/utils'
import { ArrowLeft, Send, AlertCircle } from 'lucide-react'

const TABS = ['Partitions', 'Messages', 'Schema', 'Configuration'] as const
type Tab = typeof TABS[number]

export default function TopicDetailPage() {
  const { clusterId, name } = useParams<{ clusterId: string; name: string }>()
  const router = useRouter()
  const topicName = decodeURIComponent(name)
  const [activeTab, setActiveTab] = useState<Tab>('Partitions')
  const [showProduce, setShowProduce] = useState(false)

  const { data, isLoading, error } = useQuery({
    queryKey: ['topic', clusterId, topicName],
    queryFn: () => fetchTopic(clusterId, topicName),
  })

  return (
    <div>
      <div className="flex items-center gap-3 mb-6">
        <button
          onClick={() => router.push(`/clusters/${clusterId}/topics`)}
          className="p-2 rounded-lg text-gray-500 hover:bg-gray-100"
        >
          <ArrowLeft className="w-4 h-4" />
        </button>
        <div className="flex-1">
          <div className="flex items-center gap-3">
            <h1 className="text-2xl font-bold text-gray-900 font-mono">{topicName}</h1>
            {data && (
              <Badge variant={data.internal ? 'warning' : 'success'}>
                {data.internal ? 'Internal' : 'User'}
              </Badge>
            )}
          </div>
          {data && (
            <p className="text-sm text-gray-500 mt-0.5">
              {data.partitions.length} partition{data.partitions.length !== 1 ? 's' : ''} &middot; Replication {data.replicationFactor}
            </p>
          )}
        </div>
        <Button size="sm" onClick={() => setShowProduce(true)}>
          <Send className="w-4 h-4" /> Produce Message
        </Button>
      </div>

      {isLoading && <div className="text-gray-400 py-12 text-center">Loading topic…</div>}

      {error && (
        <div className="flex items-center gap-3 bg-red-50 border border-red-200 text-red-700 rounded-xl p-4 mb-4">
          <AlertCircle className="w-5 h-5" />
          {(error as Error).message}
        </div>
      )}

      {data && (
        <>
          <div className="border-b border-gray-200 mb-6">
            <nav className="flex gap-1">
              {TABS.map((tab) => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px ${
                    activeTab === tab
                      ? 'border-blue-600 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  }`}
                >
                  {tab}
                </button>
              ))}
            </nav>
          </div>

          {activeTab === 'Partitions' && (
            <Card>
              <CardContent className="p-0">
                <Table>
                  <TableHead>
                    <tr>
                      <Th>Partition</Th>
                      <Th>Leader</Th>
                      <Th>Replicas</Th>
                      <Th>ISR</Th>
                      <Th>Offset Begin</Th>
                      <Th>Offset End</Th>
                      <Th>Messages</Th>
                    </tr>
                  </TableHead>
                  <TableBody>
                    {data.partitions.map((p) => (
                      <tr key={p.partitionId} className="hover:bg-gray-50">
                        <Td><span className="font-mono font-semibold text-gray-900">{p.partitionId}</span></Td>
                        <Td><Badge variant="info">Broker #{p.leader}</Badge></Td>
                        <Td><span className="font-mono text-xs text-gray-600">[{p.replicas.join(', ')}]</span></Td>
                        <Td>
                          <span className={`font-mono text-xs ${p.isr.length < p.replicas.length ? 'text-red-600' : 'text-green-600'}`}>
                            [{p.isr.join(', ')}]
                          </span>
                        </Td>
                        <Td className="font-mono text-xs">{p.offsetBegin}</Td>
                        <Td className="font-mono text-xs">{p.offsetEnd}</Td>
                        <Td><span className="text-sm font-medium">{formatNumber(p.messageCount)}</span></Td>
                      </tr>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          )}

          {activeTab === 'Messages' && (
            <MessageBrowser
              clusterId={clusterId}
              topic={topicName}
              partitionCount={data.partitions.length}
            />
          )}

          {activeTab === 'Schema' && (
            <SchemaEditor clusterId={clusterId} topicName={topicName} />
          )}

          {activeTab === 'Configuration' && (
            <Card>
              <CardHeader><CardTitle>Topic Configuration (non-default values)</CardTitle></CardHeader>
              <CardContent className="p-0">
                {Object.keys(data.config).length === 0 ? (
                  <div className="py-8 text-center text-gray-400 text-sm">All values are at default settings.</div>
                ) : (
                  <Table>
                    <TableHead>
                      <tr><Th>Key</Th><Th>Value</Th></tr>
                    </TableHead>
                    <TableBody>
                      {Object.entries(data.config).map(([k, v]) => (
                        <tr key={k} className="hover:bg-gray-50">
                          <Td className="font-mono text-xs text-gray-700">{k}</Td>
                          <Td className="font-mono text-xs text-gray-600">{v}</Td>
                        </tr>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          )}
        </>
      )}

      {showProduce && data && (
        <ProduceMessageModal
          clusterId={clusterId}
          topic={topicName}
          partitionCount={data.partitions.length}
          onClose={() => setShowProduce(false)}
        />
      )}
    </div>
  )
}
