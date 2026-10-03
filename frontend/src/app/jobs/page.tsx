'use client'

import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchJobs, pauseJob, resumeJob, deleteJob,
  fetchJobErrors, resetJobOffset, fetchLiveMetrics, fetchJobLogs, scaleJobReplicas,
} from '@/lib/api'
import type { LiveMetric, LiveConsumerMember } from '@/lib/api'
import type { ReplicationJob, JobStatus } from '@/types'
import { Card, CardContent } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { CreateJobModal } from '@/components/jobs/CreateJobModal'
import {
  Plus, Play, Pause, Trash2, X, Database, Activity, Clock,
  AlertTriangle, RotateCcw, RefreshCw, Terminal,
  Info, ChevronRight,
} from 'lucide-react'

type DrawerTab = 'details' | 'logs' | 'reset'

const STATUS_VARIANT: Record<JobStatus, 'success' | 'warning' | 'danger' | 'info' | 'default'> = {
  running: 'success',
  paused:  'warning',
  pending: 'info',
  failed:  'danger',
  stopped: 'default',
}

function fmtLag(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace(/\.0$/, '') + 'M'
  if (n >= 1_000)     return (n / 1_000).toFixed(1).replace(/\.0$/, '') + 'k'
  return String(n)
}

function LagBadge({ jobId }: { jobId: string }) {
  const { data, isLoading } = useQuery({
    queryKey: ['live-metrics', jobId],
    queryFn: () => fetchLiveMetrics(jobId),
    refetchInterval: 15_000,
    staleTime: 10_000,
  })

  if (isLoading || !data) {
    return (
      <span className="w-7 h-7 rounded-full bg-gray-100 flex items-center justify-center">
        <span className="w-2 h-2 rounded-full bg-gray-300 animate-pulse" />
      </span>
    )
  }

  const lag = data.partitions.reduce((s, m) => s + m.lag, 0)
  const color =
    lag === 0        ? 'bg-green-100 text-green-700 ring-green-200' :
    lag < 1_000      ? 'bg-yellow-100 text-yellow-700 ring-yellow-200' :
                       'bg-red-100 text-red-700 ring-red-200'

  return (
    <span className={`inline-flex items-center justify-center min-w-[28px] h-7 px-1.5 rounded-full text-xs font-bold ring-1 ${color}`}>
      {fmtLag(lag)}
    </span>
  )
}

const DEST_LABELS: Record<string, string> = {
  postgresql: 'PostgreSQL',
  mysql:      'MySQL',
  sqlserver:  'SQL Server',
  mongodb:    'MongoDB',
}


// ── Tab: Reset Offset ─────────────────────────────────────────────────────────
// Only available when job is paused or failed.
// Commits new offsets to Kafka — does NOT restart the job.

function ResetTab({ job }: { job: ReplicationJob }) {
  const { data: metricsResp } = useQuery({
    queryKey: ['live-metrics', job.id],
    queryFn: () => fetchLiveMetrics(job.id),
    refetchInterval: 10000,
  })

  return (
    <div className="p-5 space-y-5">
      <div>
        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-1">Reset Kafka Offset</p>
        <p className="text-xs text-gray-500">
          Changes the committed offset for the consumer group. The job stays paused — resume it manually when ready.
        </p>
      </div>
      <ResetOffsetForm job={job} metrics={metricsResp?.partitions ?? []} />
    </div>
  )
}

function ResetOffsetForm({ job, metrics }: { job: ReplicationJob; metrics: LiveMetric[] }) {
  const qc = useQueryClient()
  const [customOpen, setCustomOpen] = useState(false)
  const [customOffsets, setCustomOffsets] = useState<Record<string, string>>({})

  const resetMut = useMutation({
    mutationFn: (args: { position: 'earliest' | 'latest' | 'custom'; offsets?: Record<string, number> }) =>
      resetJobOffset(job.id, args.position, args.offsets),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['jobs'] })
      qc.invalidateQueries({ queryKey: ['job-errors', job.id] })
      setCustomOpen(false)
      setCustomOffsets({})
    },
  })

  const openCustom = () => {
    const pre: Record<string, string> = {}
    metrics.forEach((m) => { pre[String(m.partitionId)] = String(m.currentOffset) })
    setCustomOffsets(pre)
    setCustomOpen(true)
  }

  const submitCustom = () => {
    const parsed: Record<string, number> = {}
    for (const [p, v] of Object.entries(customOffsets)) {
      const n = parseInt(v, 10)
      if (!isNaN(n)) parsed[p] = n
    }
    if (Object.keys(parsed).length === 0) return
    resetMut.mutate({ position: 'custom', offsets: parsed })
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2 flex-wrap">
        <Button size="sm" variant="secondary" loading={resetMut.isPending}
          onClick={() => resetMut.mutate({ position: 'earliest' })}>
          <RotateCcw className="w-3.5 h-3.5" /> Earliest
        </Button>
        <Button size="sm" variant="secondary" loading={resetMut.isPending}
          onClick={() => resetMut.mutate({ position: 'latest' })}>
          <RotateCcw className="w-3.5 h-3.5" /> Latest
        </Button>
        <Button size="sm" variant="secondary" onClick={openCustom}>
          Custom per partition…
        </Button>
      </div>
      <p className="text-xs text-gray-400 flex items-center gap-1">
        <Info className="w-3 h-3 flex-shrink-0" />
        Only changes Kafka committed offsets. Resume the job manually when ready.
      </p>

      {customOpen && (
        <div className="border border-amber-200 bg-amber-50 rounded-xl p-3 space-y-3">
          <p className="text-xs font-semibold text-amber-800">
            Set exact offset per partition — pre-filled from current committed offsets.
          </p>
          {metrics.length > 0 ? (
            <div className="grid grid-cols-2 gap-2">
              {metrics.map((m) => (
                <div key={m.partitionId} className="flex items-center gap-2">
                  <span className="text-xs text-gray-600 font-mono w-16 flex-shrink-0">
                    P{m.partitionId}
                    <span className="text-gray-400 ml-1">/{m.endOffset}</span>
                  </span>
                  <Input
                    type="number" min={0} placeholder="offset"
                    className="text-xs h-7 font-mono"
                    value={customOffsets[String(m.partitionId)] ?? ''}
                    onChange={(e) =>
                      setCustomOffsets((p) => ({ ...p, [String(m.partitionId)]: e.target.value }))
                    }
                  />
                </div>
              ))}
            </div>
          ) : (
            <p className="text-xs text-gray-500">
              No partition data — open Metrics tab first to load offsets, then come back here.
            </p>
          )}
          <div className="flex gap-2">
            <Button size="sm" loading={resetMut.isPending} onClick={submitCustom}>Apply Reset</Button>
            <Button size="sm" variant="secondary" onClick={() => setCustomOpen(false)}>Cancel</Button>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Replica Editor ────────────────────────────────────────────────────────────

function ReplicaEditor({ job }: { job: ReplicationJob }) {
  const qc = useQueryClient()
  const [editing, setEditing] = useState(false)
  const [value, setValue] = useState(String(job.consumerCount))

  const scaleMut = useMutation({
    mutationFn: (n: number) => scaleJobReplicas(job.id, n),
    onSuccess: (res) => {
      qc.invalidateQueries({ queryKey: ['jobs'] })
      setEditing(false)
      setValue(String(res.replicas))
    },
  })

  const apply = () => {
    const n = parseInt(value, 10)
    if (isNaN(n) || n < 1 || n > 32) return
    if (n === job.consumerCount) { setEditing(false); return }
    scaleMut.mutate(n)
  }

  if (!editing) {
    return (
      <div className="flex items-center gap-1.5">
        <span className="font-medium text-gray-800">{job.consumerCount}</span>
        <span className="text-gray-400 text-xs">({job.consumerCountMode})</span>
        <button
          onClick={() => { setValue(String(job.consumerCount)); setEditing(true) }}
          className="text-xs text-blue-600 hover:underline ml-1"
        >
          Edit
        </button>
      </div>
    )
  }

  return (
    <div className="flex items-center gap-1.5">
      <button
        onClick={() => setValue((v) => String(Math.max(1, parseInt(v, 10) - 1)))}
        disabled={parseInt(value, 10) <= 1}
        className="w-5 h-5 rounded border border-gray-300 flex items-center justify-center text-gray-600 hover:bg-gray-100 font-bold leading-none"
      >−</button>
      <input
        type="number" min={1} max={32} value={value}
        onChange={(e) => setValue(e.target.value)}
        className="w-12 h-6 text-center text-xs border border-gray-300 rounded font-mono focus:outline-none focus:ring-1 focus:ring-blue-400"
      />
      <button
        onClick={() => setValue((v) => String(Math.min(32, parseInt(v, 10) + 1)))}
        disabled={parseInt(value, 10) >= 32}
        className="w-5 h-5 rounded border border-gray-300 flex items-center justify-center text-gray-600 hover:bg-gray-100 font-bold leading-none"
      >+</button>
      <Button size="sm" loading={scaleMut.isPending} onClick={apply}>Apply</Button>
      <Button size="sm" variant="secondary" onClick={() => setEditing(false)}>Cancel</Button>
      {job.status !== 'running' && (
        <span className="text-xs text-amber-600 ml-1">takes effect on Resume</span>
      )}
    </div>
  )
}

// ── Tab: Details + Metrics (merged) ──────────────────────────────────────────

function DetailsTab({ job }: { job: ReplicationJob }) {
  const { data: errors = [] } = useQuery({
    queryKey: ['job-errors', job.id],
    queryFn: () => fetchJobErrors(job.id),
    refetchInterval: 10000,
  })

  const { data: metricsResp, dataUpdatedAt, isLoading: metricsLoading } = useQuery({
    queryKey: ['live-metrics', job.id],
    queryFn: () => fetchLiveMetrics(job.id),
    refetchInterval: 5000,
  })

  const metrics = metricsResp?.partitions ?? []
  const members = metricsResp?.members ?? []
  const groupState = metricsResp?.groupState ?? ''
  const totalLag = metrics.reduce((s, m) => s + m.lag, 0)
  const caught = metrics.filter((m) => m.lag === 0).length

  return (
    <div className="space-y-6 p-5">
      {/* Error banner */}
      {job.lastError && (
        <div className="flex items-start gap-2 bg-red-50 border border-red-200 rounded-xl px-3 py-3 text-xs text-red-700">
          <AlertTriangle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
          <div>
            <p className="font-semibold mb-0.5">
              {job.status === 'paused' ? 'Paused due to error — reset offset or resume to retry' : 'Last error'}
            </p>
            <p className="font-mono break-all leading-relaxed">{job.lastError}</p>
          </div>
        </div>
      )}

      {/* Config grid */}
      <div>
        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">Configuration</p>
        <div className="grid grid-cols-2 gap-x-8 gap-y-4 text-xs">
          <div>
            <span className="text-gray-400 block mb-0.5">Consumer group</span>
            <p className="font-mono font-medium text-gray-800 break-all">{job.consumerGroup}</p>
          </div>
          <div>
            <span className="text-gray-400 block mb-0.5">Topic</span>
            <p className="font-mono font-medium text-gray-800 break-all">{job.topicName}</p>
          </div>
          <div>
            <span className="text-gray-400 block mb-0.5">Destination type</span>
            <p className="font-medium text-gray-800">{DEST_LABELS[job.destinationType] || job.destinationType}</p>
          </div>
          <div>
            <span className="text-gray-400 block mb-0.5">Table / Collection</span>
            <p className="font-mono font-medium text-gray-800">
              {job.destinationConfig.tableName || job.destinationConfig.collection}
            </p>
          </div>
          <div>
            <span className="text-gray-400 block mb-0.5">Host</span>
            <p className="font-mono font-medium text-gray-800">
              {job.destinationConfig.host}:{job.destinationConfig.port}
            </p>
          </div>
          <div>
            <span className="text-gray-400 block mb-0.5">Database</span>
            <p className="font-mono font-medium text-gray-800">{job.destinationConfig.database}</p>
          </div>
          <div>
            <span className="text-gray-400 block mb-0.5">Start position</span>
            <p className="font-medium text-gray-800 capitalize">{job.startPosition}</p>
          </div>
          <div>
            <span className="text-gray-400 block mb-0.5">k8s namespace</span>
            <p className="font-mono font-medium text-gray-800">{job.k8sNamespace}</p>
          </div>
          {job.teamName && (
            <div>
              <span className="text-gray-400 block mb-0.5">Team</span>
              <p className="font-medium text-gray-800">{job.teamName}</p>
            </div>
          )}
          {job.alertChannel && (
            <div>
              <span className="text-gray-400 block mb-0.5">Alert channel</span>
              <p className="font-mono font-medium text-gray-800">{job.alertChannel}</p>
            </div>
          )}
          <div className="col-span-2">
            <span className="text-gray-400 block mb-1">Replicas</span>
            <ReplicaEditor job={job} />
          </div>
        </div>
      </div>

      {/* Live Partition Metrics */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide">Live Partition Metrics</p>
            {groupState && (
              <span className={`text-xs px-1.5 py-0.5 rounded font-medium ${
                groupState === 'Stable' ? 'bg-green-100 text-green-700' :
                groupState === 'Empty'  ? 'bg-gray-100 text-gray-500' :
                'bg-yellow-100 text-yellow-700'
              }`}>
                {groupState}
              </span>
            )}
          </div>
          {dataUpdatedAt > 0 && (
            <span className="text-xs text-gray-400">
              updated {new Date(dataUpdatedAt).toLocaleTimeString()} · 5s
            </span>
          )}
        </div>

        {metrics.length > 0 && (
          <div className="grid grid-cols-3 gap-3 mb-3">
            <div className="bg-gray-50 rounded-xl p-3 text-center">
              <p className="text-xs text-gray-500 mb-1">Partitions</p>
              <p className="text-xl font-bold text-gray-800">{metrics.length}</p>
            </div>
            <div className="bg-gray-50 rounded-xl p-3 text-center">
              <p className="text-xs text-gray-500 mb-1">Total Lag</p>
              <p className={`text-xl font-bold ${totalLag === 0 ? 'text-green-600' : totalLag < 1000 ? 'text-yellow-600' : 'text-red-600'}`}>
                {totalLag.toLocaleString()}
              </p>
            </div>
            <div className="bg-gray-50 rounded-xl p-3 text-center">
              <p className="text-xs text-gray-500 mb-1">Caught up</p>
              <p className="text-xl font-bold text-gray-800">{caught}/{metrics.length}</p>
            </div>
          </div>
        )}

        {metricsLoading && metrics.length === 0 ? (
          <div className="text-gray-400 text-xs py-3 text-center">Querying Kafka…</div>
        ) : metrics.length === 0 ? (
          <div className="text-gray-400 text-xs py-3 text-center">No partition data yet.</div>
        ) : (
          <div className="border border-gray-100 rounded-xl overflow-hidden">
            <table className="w-full text-xs">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-3 py-2.5 text-left text-gray-500 font-medium">Partition</th>
                  <th className="px-3 py-2.5 text-right text-gray-500 font-medium">Committed</th>
                  <th className="px-3 py-2.5 text-right text-gray-500 font-medium">End</th>
                  <th className="px-3 py-2.5 text-right text-gray-500 font-medium">Lag</th>
                  <th className="px-3 py-2.5 text-right text-gray-500 font-medium">Progress</th>
                </tr>
              </thead>
              <tbody>
                {metrics.map((m) => {
                  const pct = m.endOffset > 0 ? Math.min(100, Math.round((m.currentOffset / m.endOffset) * 100)) : 0
                  return (
                    <tr key={m.partitionId} className="border-t border-gray-100 hover:bg-gray-50">
                      <td className="px-3 py-2.5 font-mono font-semibold text-gray-700">P{m.partitionId}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-gray-600">{m.currentOffset.toLocaleString()}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-gray-600">{m.endOffset.toLocaleString()}</td>
                      <td className="px-3 py-2.5 text-right font-mono font-semibold">
                        <span className={m.lag === 0 ? 'text-green-600' : m.lag < 100 ? 'text-yellow-600' : 'text-red-600'}>
                          {m.lag.toLocaleString()}
                        </span>
                      </td>
                      <td className="px-3 py-2.5 text-right">
                        <div className="flex items-center justify-end gap-2">
                          <div className="w-16 h-1.5 bg-gray-200 rounded-full overflow-hidden">
                            <div
                              className="h-full bg-blue-500 rounded-full transition-all"
                              style={{ width: `${pct}%` }}
                            />
                          </div>
                          <span className="text-gray-500 w-8 text-right">{pct}%</span>
                        </div>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
        {/* Active Consumer Members */}
        <div className="mt-4">
          <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
            Active Consumers
            <span className="ml-1.5 font-normal normal-case text-gray-400">({members.length})</span>
          </p>
          {members.length === 0 ? (
            <div className="text-xs text-gray-400 py-2">
              No active consumers — group is {groupState || 'empty'} (job may be paused).
            </div>
          ) : (
            <div className="border border-gray-100 rounded-xl overflow-hidden">
              <table className="w-full text-xs">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-3 py-2 text-left text-gray-500 font-medium">Client ID</th>
                    <th className="px-3 py-2 text-left text-gray-500 font-medium">Host / IP</th>
                    <th className="px-3 py-2 text-left text-gray-500 font-medium">Member ID</th>
                  </tr>
                </thead>
                <tbody>
                  {members.map((m) => (
                    <tr key={m.memberId} className="border-t border-gray-100 hover:bg-gray-50">
                      <td className="px-3 py-2 font-mono text-gray-800">{m.clientId}</td>
                      <td className="px-3 py-2 font-mono text-gray-600">{m.clientHost}</td>
                      <td className="px-3 py-2 font-mono text-gray-400 text-xs truncate max-w-[180px]" title={m.memberId}>
                        {m.memberId}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* Error log */}
      {errors.length > 0 && (
        <div>
          <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
            Error Log <span className="text-red-400 font-normal normal-case">({errors.length})</span>
          </p>
          <div className="border border-red-100 rounded-xl overflow-hidden">
            <table className="w-full text-xs">
              <thead className="bg-red-50">
                <tr>
                  <th className="px-3 py-2 text-left text-red-700 font-medium">P</th>
                  <th className="px-3 py-2 text-right text-red-700 font-medium">Offset</th>
                  <th className="px-3 py-2 text-left text-red-700 font-medium">Error</th>
                  <th className="px-3 py-2 text-right text-red-700 font-medium">Time</th>
                </tr>
              </thead>
              <tbody>
                {errors.map((e) => (
                  <tr key={e.id} className="border-t border-red-50 hover:bg-red-50/50">
                    <td className="px-3 py-2 font-mono font-semibold text-gray-700">P{e.partitionId}</td>
                    <td className="px-3 py-2 text-right font-mono text-gray-600">{e.offset.toLocaleString()}</td>
                    <td className="px-3 py-2 text-red-700 max-w-xs">
                      <span className="truncate block" title={e.errorMsg}>{e.errorMsg}</span>
                    </td>
                    <td className="px-3 py-2 text-right text-gray-400 whitespace-nowrap">
                      {new Date(e.occurredAt).toLocaleTimeString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Tab: Logs ─────────────────────────────────────────────────────────────────

function LogsTab({ jobId }: { jobId: string }) {
  const { data: logs = '', isFetching, refetch } = useQuery({
    queryKey: ['job-logs', jobId],
    queryFn: () => fetchJobLogs(jobId, 300),
    staleTime: 0,
    refetchOnMount: true,
  })

  return (
    <div className="p-5 flex flex-col gap-3 h-full">
      <div className="flex items-center justify-between flex-shrink-0">
        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide">Pod Logs (last 300 lines)</p>
        <button
          onClick={() => refetch()}
          disabled={isFetching}
          className="flex items-center gap-1.5 text-xs text-gray-500 hover:text-gray-700 disabled:opacity-50"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>
      <div className="bg-gray-900 rounded-xl p-4 overflow-auto flex-1" style={{ minHeight: 300, maxHeight: 'calc(100vh - 240px)' }}>
        {isFetching && !logs ? (
          <p className="text-xs text-gray-500">Fetching logs…</p>
        ) : logs ? (
          <pre className="text-xs text-green-400 font-mono whitespace-pre-wrap break-all leading-[1.6]">
            {logs}
          </pre>
        ) : (
          <p className="text-xs text-gray-500">No logs found. Worker pod may not have started yet.</p>
        )}
      </div>
    </div>
  )
}

// ── Job Drawer ────────────────────────────────────────────────────────────────

function JobDrawer({ job, onClose }: { job: ReplicationJob; onClose: () => void }) {
  const qc = useQueryClient()
  const [activeTab, setActiveTab] = useState<DrawerTab>('details')
  // Live metrics shared across tabs via deduped query key
  const { data: liveMetricsResp } = useQuery({
    queryKey: ['live-metrics', job.id],
    queryFn: () => fetchLiveMetrics(job.id),
    refetchInterval: 5000,
  })
  const liveMetrics = liveMetricsResp?.partitions ?? []

  const pauseMut = useMutation({
    mutationFn: () => pauseJob(job.id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['jobs'] }),
  })

  const resumeMut = useMutation({
    mutationFn: () => resumeJob(job.id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['jobs'] }),
  })

  const deleteMut = useMutation({
    mutationFn: () => deleteJob(job.id),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['jobs'] }); onClose() },
  })

  const totalLag = liveMetrics.reduce((s, m) => s + m.lag, 0)

  const canReset = job.status === 'paused' || job.status === 'failed'

  const tabs: { id: DrawerTab; label: string; icon: React.ReactNode; disabled?: boolean }[] = [
    { id: 'details', label: 'Details',      icon: <Info className="w-3.5 h-3.5" /> },
    { id: 'logs',    label: 'Logs',         icon: <Terminal className="w-3.5 h-3.5" /> },
    { id: 'reset',   label: 'Reset Offset', icon: <RotateCcw className="w-3.5 h-3.5" />, disabled: !canReset },
  ]

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [onClose])

  return (
    <>
      {/* Backdrop */}
      <div className="fixed inset-0 bg-black/20 z-40" onClick={onClose} />

      {/* Drawer panel */}
      <div className="fixed right-0 top-0 h-full w-[680px] bg-white shadow-2xl z-50 flex flex-col">

        {/* ── Header ── */}
        <div className="px-5 pt-4 pb-3 border-b border-gray-100 flex-shrink-0">
          {/* Title row */}
          <div className="flex items-start justify-between mb-2">
            <div className="min-w-0 pr-3">
              <h2 className="font-bold text-gray-900 text-base leading-snug">{job.name}</h2>
              <div className="flex items-center gap-2 mt-1">
                <Badge variant={STATUS_VARIANT[job.status]}>{job.status}</Badge>
                <span className="text-xs text-gray-400 font-mono truncate">{job.topicName}</span>
              </div>
            </div>
            <button
              onClick={onClose}
              className="p-1.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg flex-shrink-0"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          {/* Quick stats */}
          <div className="flex items-center gap-4 text-xs text-gray-500 mb-3">
            <span className="flex items-center gap-1">
              <Activity className="w-3 h-3" />
              Total lag:
              <span className={`font-mono font-semibold ml-0.5 ${totalLag === 0 ? 'text-green-600' : totalLag < 1000 ? 'text-yellow-600' : 'text-red-600'}`}>
                {totalLag.toLocaleString()}
              </span>
            </span>
            <span className="flex items-center gap-1">
              <Database className="w-3 h-3" />
              {DEST_LABELS[job.destinationType] || job.destinationType}
            </span>
            <span className="flex items-center gap-1">
              <Clock className="w-3 h-3" />
              {new Date(job.createdAt).toLocaleDateString()}
            </span>
          </div>

          {/* Action buttons */}
          <div className="flex items-center gap-2">
            {job.status === 'running' && (
              <Button
                size="sm"
                variant="secondary"
                loading={pauseMut.isPending}
                onClick={() => pauseMut.mutate()}
              >
                <Pause className="w-3.5 h-3.5" /> Pause
              </Button>
            )}
            {(job.status === 'paused' || job.status === 'failed') && (
              <Button size="sm" loading={resumeMut.isPending} onClick={() => resumeMut.mutate()}>
                <Play className="w-3.5 h-3.5" /> Resume
              </Button>
            )}
            <Button
              size="sm"
              variant="danger"
              loading={deleteMut.isPending}
              onClick={() => { if (confirm(`Delete job "${job.name}"?`)) deleteMut.mutate() }}
            >
              <Trash2 className="w-3.5 h-3.5" /> Delete
            </Button>
          </div>
        </div>

        {/* ── Tabs ── */}
        <div className="flex border-b border-gray-100 flex-shrink-0">
          {tabs.map((t) => (
            <button
              key={t.id}
              disabled={t.disabled}
              onClick={() => !t.disabled && setActiveTab(t.id)}
              title={t.disabled ? 'Only available when job is paused' : undefined}
              className={`flex items-center gap-1.5 px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                t.disabled
                  ? 'border-transparent text-gray-300 cursor-not-allowed'
                  : activeTab === t.id
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:bg-gray-50'
              }`}
            >
              {t.icon}
              {t.label}
            </button>
          ))}
        </div>

        {/* ── Tab content ── */}
        <div className="flex-1 overflow-auto">
          {activeTab === 'details' && <DetailsTab job={job} />}
          {activeTab === 'logs'    && <LogsTab jobId={job.id} />}
          {activeTab === 'reset'   && <ResetTab job={job} />}
        </div>
      </div>
    </>
  )
}

// ── Job Card ──────────────────────────────────────────────────────────────────

function JobCard({ job, onClick }: { job: ReplicationJob; onClick: () => void }) {
  return (
    <div
      onClick={onClick}
      className={`bg-white border rounded-xl px-4 py-3.5 flex items-center gap-4 cursor-pointer hover:shadow-sm hover:border-gray-300 transition-all ${
        job.status === 'failed' ? 'border-red-200 bg-red-50/30' : 'border-gray-200'
      }`}
    >
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-0.5">
          <span className="font-semibold text-gray-900 text-sm">{job.name}</span>
          <Badge variant={STATUS_VARIANT[job.status]}>{job.status}</Badge>
        </div>
        <div className="flex items-center gap-3 text-xs text-gray-500 mt-0.5">
          <span className="font-mono">{job.topicName}</span>
          <span className="text-gray-300">→</span>
          <span className="flex items-center gap-1">
            <Database className="w-3 h-3" />
            {DEST_LABELS[job.destinationType] || job.destinationType}
            {(job.destinationConfig.tableName || job.destinationConfig.collection) && (
              <span className="text-gray-400">
                / {job.destinationConfig.tableName || job.destinationConfig.collection}
              </span>
            )}
          </span>
        </div>
        {job.lastError && (
          <div className="flex items-center gap-1 mt-1.5 text-xs text-red-600">
            <AlertTriangle className="w-3 h-3 flex-shrink-0" />
            <span className="truncate">{job.lastError}</span>
          </div>
        )}
      </div>
      <div className="flex items-center gap-3 flex-shrink-0">
        <div className="flex flex-col items-center gap-0.5">
          <span className="text-xs text-gray-400">lag</span>
          <LagBadge jobId={job.id} />
        </div>
        <ChevronRight className="w-4 h-4 text-gray-300" />
      </div>
    </div>
  )
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function JobsPage() {
  const [showCreate, setShowCreate] = useState(false)
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null)

  const { data: jobs = [], isLoading } = useQuery({
    queryKey: ['jobs'],
    queryFn: fetchJobs,
    refetchInterval: 10_000,
  })

  const selectedJob = jobs.find((j) => j.id === selectedJobId) ?? null

  const running = jobs.filter((j) => j.status === 'running').length
  const paused  = jobs.filter((j) => j.status === 'paused').length
  const failed  = jobs.filter((j) => j.status === 'failed').length

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Replication Jobs</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {jobs.length} job{jobs.length !== 1 ? 's' : ''}
            {running > 0 && <> · <span className="text-green-600">{running} running</span></>}
            {paused  > 0 && <> · <span className="text-yellow-600">{paused} paused</span></>}
            {failed  > 0 && <> · <span className="text-red-600">{failed} failed</span></>}
          </p>
        </div>
        <Button onClick={() => setShowCreate(true)}>
          <Plus className="w-4 h-4" /> New Job
        </Button>
      </div>

      {isLoading ? (
        <div className="text-center text-gray-400 py-12">Loading jobs…</div>
      ) : jobs.length === 0 ? (
        <Card>
          <CardContent>
            <div className="py-16 text-center">
              <Database className="w-10 h-10 text-gray-300 mx-auto mb-3" />
              <p className="text-gray-500 text-sm">No replication jobs yet.</p>
              <button
                onClick={() => setShowCreate(true)}
                className="mt-3 text-blue-600 text-sm hover:underline"
              >
                Create your first job →
              </button>
            </div>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-2">
          {jobs.map((job) => (
            <JobCard
              key={job.id}
              job={job}
              onClick={() => setSelectedJobId(job.id)}
            />
          ))}
        </div>
      )}

      {selectedJob && (
        <JobDrawer
          job={selectedJob}
          onClose={() => setSelectedJobId(null)}
        />
      )}

      {showCreate && <CreateJobModal onClose={() => setShowCreate(false)} />}
    </div>
  )
}
