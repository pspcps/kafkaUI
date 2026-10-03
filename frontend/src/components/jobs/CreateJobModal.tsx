'use client'

import { useState, useEffect } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { createJob, fetchClusters, fetchTopics } from '@/lib/api'
import type { DestinationType, ConsumerCountMode, StartPosition, DestinationConfig } from '@/types'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { X, ChevronRight } from 'lucide-react'

const DESTINATIONS: { value: DestinationType; label: string }[] = [
  { value: 'postgresql', label: 'PostgreSQL' },
  { value: 'mysql',      label: 'MySQL / MariaDB' },
  { value: 'sqlserver',  label: 'SQL Server' },
  { value: 'mongodb',    label: 'MongoDB' },
]

const DEFAULT_PORTS: Record<DestinationType, number> = {
  postgresql: 5432,
  mysql:      3306,
  sqlserver:  1433,
  mongodb:    27017,
}

const STEPS = ['Source', 'Consumer', 'Destination', 'Review'] as const
type Step = typeof STEPS[number]

interface Props { onClose: () => void }

export function CreateJobModal({ onClose }: Props) {
  const qc = useQueryClient()
  const [step, setStep] = useState<Step>('Source')

  // ── Source fields ────────────────────────────────────────────────────────────
  const [clusterId, setClusterId] = useState('')
  const [topicName, setTopicName] = useState('')
  const [serviceName, setServiceName] = useState('')
  const [consumerGroup, setConsumerGroup] = useState('')
  const [teamName, setTeamName] = useState('')
  const [alertChannel, setAlertChannel] = useState('')

  // ── Consumer options ─────────────────────────────────────────────────────────
  const [startPosition, setStartPosition] = useState<StartPosition>('latest')
  const [includeHistorical, setIncludeHistorical] = useState(false)
  const [countMode, setCountMode] = useState<ConsumerCountMode>('fixed')
  const [consumerCount, setConsumerCount] = useState(1)

  // ── Destination ──────────────────────────────────────────────────────────────
  const [destType, setDestType] = useState<DestinationType>('postgresql')
  const [destConfig, setDestConfig] = useState<Partial<DestinationConfig>>({
    host: '', port: 5432, database: '', username: '', password: '',
    tableName: '', collection: '', schema: 'public',
  })

  // ── Data loaders ─────────────────────────────────────────────────────────────
  const { data: clusters = [] } = useQuery({ queryKey: ['clusters'], queryFn: fetchClusters })
  const { data: topics = [] } = useQuery({
    queryKey: ['topics', clusterId],
    queryFn: () => fetchTopics(clusterId),
    enabled: !!clusterId,
  })

  const partitionCount = topics.find((t) => t.name === topicName)?.partitions ?? 1

  // Auto-suggest consumer group and table name
  useEffect(() => {
    if (serviceName && topicName) {
      setConsumerGroup(`${serviceName}-${topicName}`)
      const safeTable = topicName.replace(/\./g, '_').replace(/[^a-zA-Z0-9_]/g, '')
      setDestConfig((prev) => ({ ...prev, tableName: safeTable, collection: safeTable }))
    }
  }, [serviceName, topicName])

  // Update default port when dest type changes
  useEffect(() => {
    setDestConfig((prev) => ({ ...prev, port: DEFAULT_PORTS[destType] }))
  }, [destType])

  const mutation = useMutation({
    mutationFn: () => createJob({
      name: `${serviceName} → ${topicName}`,
      clusterId,
      topicName,
      consumerGroup,
      serviceName,
      destinationType: destType,
      destinationConfig: destConfig as DestinationConfig,
      startPosition,
      includeHistorical,
      consumerCountMode: countMode,
      consumerCount: resolvedCount(),
      teamName,
      alertChannel,
    }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['jobs'] })
      onClose()
    },
  })

  function resolvedCount(): number {
    if (countMode === 'per_partition') return partitionCount
    if (countMode === 'fixed') return 1
    return Math.min(consumerCount, partitionCount)
  }

  const stepIdx = STEPS.indexOf(step)
  const canNext = (() => {
    if (step === 'Source') return !!clusterId && !!topicName && !!serviceName && !!consumerGroup && !!teamName
    if (step === 'Consumer') return true
    if (step === 'Destination') return !!(destConfig.host && destConfig.database && destConfig.username && destConfig.password)
    return true
  })()

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl mx-4 flex flex-col max-h-[90vh]">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <h2 className="text-lg font-semibold text-gray-900">New Replication Job</h2>
          <button onClick={onClose} className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Step indicator */}
        <div className="flex items-center px-6 py-3 gap-2 bg-gray-50 border-b border-gray-100">
          {STEPS.map((s, i) => (
            <div key={s} className="flex items-center gap-2">
              <div className={`flex items-center justify-center w-6 h-6 rounded-full text-xs font-semibold ${
                i < stepIdx ? 'bg-green-500 text-white' :
                i === stepIdx ? 'bg-blue-600 text-white' :
                'bg-gray-200 text-gray-500'
              }`}>{i < stepIdx ? '✓' : i + 1}</div>
              <span className={`text-sm ${i === stepIdx ? 'font-semibold text-blue-600' : 'text-gray-500'}`}>{s}</span>
              {i < STEPS.length - 1 && <ChevronRight className="w-3.5 h-3.5 text-gray-300" />}
            </div>
          ))}
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-6 py-5 space-y-4">
          {/* ── STEP 1: Source ───────────────────────────────────────────── */}
          {step === 'Source' && (
            <>
              <Field label="Cluster">
                <select
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  value={clusterId}
                  onChange={(e) => { setClusterId(e.target.value); setTopicName('') }}
                >
                  <option value="">Select cluster…</option>
                  {clusters.map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
                </select>
              </Field>
              <Field label="Topic">
                <select
                  className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  value={topicName}
                  onChange={(e) => setTopicName(e.target.value)}
                  disabled={!clusterId}
                >
                  <option value="">Select topic…</option>
                  {topics.filter((t) => !t.internal).map((t) => (
                    <option key={t.name} value={t.name}>{t.name} ({t.partitions} partitions)</option>
                  ))}
                </select>
              </Field>
              <Field label="Service Name" hint="Your application or service name">
                <Input
                  placeholder="e.g. trip-analytics-service"
                  value={serviceName}
                  onChange={(e) => setServiceName(e.target.value)}
                />
              </Field>
              <Field label="Consumer Group" hint="Auto-generated from service + topic — you can edit it">
                <Input
                  value={consumerGroup}
                  onChange={(e) => setConsumerGroup(e.target.value)}
                  className="font-mono text-sm"
                />
              </Field>
              <div className="grid grid-cols-2 gap-3">
                <Field label="Team Name" hint="Team responsible for this job">
                  <Input
                    placeholder="e.g. data-engineering"
                    value={teamName}
                    onChange={(e) => setTeamName(e.target.value)}
                  />
                </Field>
                <Field label="Alert Channel" hint="Slack channel, PagerDuty key, etc.">
                  <Input
                    placeholder="e.g. #alerts-data"
                    value={alertChannel}
                    onChange={(e) => setAlertChannel(e.target.value)}
                  />
                </Field>
              </div>
            </>
          )}

          {/* ── STEP 2: Consumer options ─────────────────────────────────── */}
          {step === 'Consumer' && (
            <>
              <Field label="Start position">
                <div className="flex gap-3">
                  {([['latest', 'Latest (Deployment — continuous)'], ['earliest', 'Earliest (includes all historical data)']] as const).map(([val, lbl]) => (
                    <label key={val} className={`flex-1 cursor-pointer border-2 rounded-xl p-3 ${startPosition === val ? 'border-blue-500 bg-blue-50' : 'border-gray-200'}`}>
                      <input type="radio" className="sr-only" value={val} checked={startPosition === val}
                        onChange={() => setStartPosition(val)} />
                      <div className="font-medium text-sm text-gray-900">{val === 'latest' ? 'Latest' : 'Earliest'}</div>
                      <div className="text-xs text-gray-500 mt-0.5">{lbl.split('(')[1]?.replace(')', '') ?? ''}</div>
                    </label>
                  ))}
                </div>
              </Field>
              <label className="flex items-center gap-3 cursor-pointer">
                <input
                  type="checkbox"
                  className="w-4 h-4 rounded"
                  checked={includeHistorical}
                  onChange={(e) => setIncludeHistorical(e.target.checked)}
                />
                <div>
                  <div className="text-sm font-medium text-gray-900">Include historical data</div>
                  <div className="text-xs text-gray-500">Start from the earliest offset, replay all existing messages</div>
                </div>
              </label>
              <Field label="Consumer replicas" hint={`Topic has ${partitionCount} partition(s) — replicas cannot exceed partitions`}>
                <div className="space-y-2">
                  {([
                    ['fixed', '1 replica (default)'],
                    ['per_partition', `${partitionCount} replicas (one per partition)`],
                    ['custom', 'Custom count'],
                  ] as [ConsumerCountMode, string][]).map(([val, lbl]) => (
                    <label key={val} className={`flex items-center gap-3 border-2 rounded-xl p-3 cursor-pointer ${countMode === val ? 'border-blue-500 bg-blue-50' : 'border-gray-200'}`}>
                      <input type="radio" className="sr-only" value={val} checked={countMode === val}
                        onChange={() => setCountMode(val)} />
                      <div className="flex-1">
                        <div className="text-sm font-medium">{lbl}</div>
                      </div>
                      {val === 'custom' && countMode === 'custom' && (
                        <input
                          type="number" min={1} max={partitionCount}
                          className="w-20 border border-gray-300 rounded-lg px-2 py-1 text-sm text-right"
                          value={consumerCount}
                          onChange={(e) => setConsumerCount(Math.min(parseInt(e.target.value) || 1, partitionCount))}
                        />
                      )}
                    </label>
                  ))}
                </div>
              </Field>
            </>
          )}

          {/* ── STEP 3: Destination ──────────────────────────────────────── */}
          {step === 'Destination' && (
            <>
              <Field label="Destination type">
                <div className="grid grid-cols-2 gap-2">
                  {DESTINATIONS.map((d) => (
                    <label key={d.value} className={`border-2 rounded-xl p-3 cursor-pointer ${destType === d.value ? 'border-blue-500 bg-blue-50' : 'border-gray-200 hover:border-gray-300'}`}>
                      <input type="radio" className="sr-only" value={d.value} checked={destType === d.value}
                        onChange={() => setDestType(d.value)} />
                      <span className="text-sm font-medium">{d.label}</span>
                    </label>
                  ))}
                </div>
              </Field>
              <div className="grid grid-cols-3 gap-3">
                <div className="col-span-2">
                  <Field label="Host">
                    <Input placeholder="db.example.com" value={destConfig.host ?? ''}
                      onChange={(e) => setDestConfig((p) => ({ ...p, host: e.target.value }))} />
                  </Field>
                </div>
                <Field label="Port">
                  <Input type="number" value={destConfig.port ?? DEFAULT_PORTS[destType]}
                    onChange={(e) => setDestConfig((p) => ({ ...p, port: parseInt(e.target.value) }))} />
                </Field>
              </div>
              <Field label="Database">
                <Input placeholder="my_database" value={destConfig.database ?? ''}
                  onChange={(e) => setDestConfig((p) => ({ ...p, database: e.target.value }))} />
              </Field>
              <div className="grid grid-cols-2 gap-3">
                <Field label="Username">
                  <Input placeholder="user" value={destConfig.username ?? ''}
                    onChange={(e) => setDestConfig((p) => ({ ...p, username: e.target.value }))} />
                </Field>
                <Field label="Password">
                  <Input type="password" placeholder="••••••••" value={destConfig.password ?? ''}
                    onChange={(e) => setDestConfig((p) => ({ ...p, password: e.target.value }))} />
                </Field>
              </div>
              {destType === 'mongodb' ? (
                <Field label="Collection name" hint="Where documents will be inserted">
                  <Input value={destConfig.collection ?? ''}
                    onChange={(e) => setDestConfig((p) => ({ ...p, collection: e.target.value }))} />
                </Field>
              ) : (
                <div className="grid grid-cols-2 gap-3">
                  {destType === 'sqlserver' && (
                    <Field label="Schema">
                      <Input placeholder="dbo" value={destConfig.schema ?? 'dbo'}
                        onChange={(e) => setDestConfig((p) => ({ ...p, schema: e.target.value }))} />
                    </Field>
                  )}
                  <Field label="Table name" hint="Created automatically from topic schema">
                    <Input value={destConfig.tableName ?? ''}
                      onChange={(e) => setDestConfig((p) => ({ ...p, tableName: e.target.value }))} />
                  </Field>
                </div>
              )}
            </>
          )}

          {/* ── STEP 4: Review ───────────────────────────────────────────── */}
          {step === 'Review' && (
            <div className="space-y-3">
              <ReviewRow label="Team" value={teamName} />
              {alertChannel && <ReviewRow label="Alert channel" value={alertChannel} />}
              <ReviewRow label="Cluster" value={clusters.find((c) => c.id === clusterId)?.name ?? clusterId} />
              <ReviewRow label="Topic" value={topicName} mono />
              <ReviewRow label="Consumer group" value={consumerGroup} mono />
              <ReviewRow label="Start position" value={startPosition === 'earliest' || includeHistorical ? 'Earliest (historical)' : 'Latest'} />
              <ReviewRow label="Replicas" value={`${resolvedCount()} (${countMode})`} />
              <ReviewRow label="Destination" value={DESTINATIONS.find((d) => d.value === destType)?.label ?? destType} />
              <ReviewRow label="Host" value={`${destConfig.host}:${destConfig.port}`} mono />
              <ReviewRow label="Database" value={destConfig.database ?? ''} mono />
              <ReviewRow label={destType === 'mongodb' ? 'Collection' : 'Table'} value={destConfig.tableName || destConfig.collection || '—'} mono />
              {mutation.error && (
                <div className="bg-red-50 border border-red-200 text-red-700 rounded-xl p-3 text-sm">
                  {(mutation.error as Error).message}
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex justify-between items-center px-6 py-4 border-t border-gray-100">
          <Button variant="secondary" size="sm" onClick={() => {
            if (stepIdx === 0) onClose()
            else setStep(STEPS[stepIdx - 1])
          }}>
            {stepIdx === 0 ? 'Cancel' : '← Back'}
          </Button>
          {step !== 'Review' ? (
            <Button size="sm" disabled={!canNext} onClick={() => setStep(STEPS[stepIdx + 1])}>
              Next →
            </Button>
          ) : (
            <Button size="sm" loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Create Job
            </Button>
          )}
        </div>
      </div>
    </div>
  )
}

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1">
      <label className="text-sm font-medium text-gray-700">{label}</label>
      {children}
      {hint && <p className="text-xs text-gray-400">{hint}</p>}
    </div>
  )
}

function ReviewRow({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex justify-between py-2 border-b border-gray-100 last:border-0">
      <span className="text-sm text-gray-500">{label}</span>
      <span className={`text-sm font-medium text-gray-900 ${mono ? 'font-mono' : ''}`}>{value}</span>
    </div>
  )
}
