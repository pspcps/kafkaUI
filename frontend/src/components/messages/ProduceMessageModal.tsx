'use client'

import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { fetchSchema, produceMessage } from '@/lib/api'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { X, AlertCircle, ChevronDown, ChevronRight } from 'lucide-react'

interface ProduceMessageModalProps {
  clusterId: string
  topic: string
  partitionCount: number
  onClose: () => void
}

export function ProduceMessageModal({ clusterId, topic, partitionCount, onClose }: ProduceMessageModalProps) {
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [partition, setPartition] = useState<string>('')
  const [headerKey, setHeaderKey] = useState('')
  const [headerValue, setHeaderValue] = useState('')
  const [headers, setHeaders] = useState<Record<string, string>>({})
  const [showSchema, setShowSchema] = useState(true)

  const { data: schema } = useQuery({
    queryKey: ['schema', clusterId, topic],
    queryFn: () => fetchSchema(clusterId, topic),
  })

  const hasEnforcedSchema =
    schema && (schema.schemaFormat === 'json_schema' || schema.schemaFormat === 'avro')

  const mutation = useMutation({
    mutationFn: () =>
      produceMessage(clusterId, topic, {
        key: key || undefined,
        value,
        partition: partition ? parseInt(partition) : undefined,
        headers: Object.keys(headers).length > 0 ? headers : undefined,
      }),
    onSuccess: onClose,
  })

  const addHeader = () => {
    if (headerKey && headerValue) {
      setHeaders((prev) => ({ ...prev, [headerKey]: headerValue }))
      setHeaderKey('')
      setHeaderValue('')
    }
  }

  const partitions = Array.from({ length: partitionCount }, (_, i) => i)

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-3xl mx-4 max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 sticky top-0 bg-white">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Produce Message</h2>
            <div className="text-xs text-gray-500 mt-0.5">
              Topic: <span className="font-medium text-gray-700">{topic}</span>
              {hasEnforcedSchema && (
                <span className="ml-2 px-1.5 py-0.5 bg-blue-50 text-blue-700 rounded text-[10px] font-semibold uppercase tracking-wider">
                  {schema!.schemaFormat.replace('_', ' ')}
                </span>
              )}
            </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X className="w-5 h-5" />
          </button>
        </div>

        <form
          className="p-6 space-y-4"
          onSubmit={(e) => { e.preventDefault(); mutation.mutate() }}
        >
          {hasEnforcedSchema && (
            <div className="rounded-lg border border-gray-200 bg-gray-50 overflow-hidden">
              <button
                type="button"
                onClick={() => setShowSchema((s) => !s)}
                className="w-full flex items-center justify-between px-3 py-2 text-xs font-medium text-gray-700 hover:bg-gray-100"
              >
                <span className="flex items-center gap-1.5">
                  {showSchema ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                  Schema (your message will be validated against this)
                </span>
                <span className="text-gray-500">v{schema!.schemaVersion}</span>
              </button>
              {showSchema && (
                <pre className="px-3 pb-3 text-[11px] font-mono text-gray-700 max-h-48 overflow-auto whitespace-pre-wrap">
                  {schema!.schemaText}
                </pre>
              )}
            </div>
          )}

          <select
            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            value={partition}
            onChange={(e) => setPartition(e.target.value)}
          >
            <option value="">Auto (default partitioner)</option>
            {partitions.map((p) => (
              <option key={p} value={p}>Partition {p}</option>
            ))}
          </select>

          <Input
            label="Key (optional)"
            placeholder="message-key"
            value={key}
            onChange={(e) => setKey(e.target.value)}
          />

          <div className="flex flex-col gap-1">
            <label className="text-sm font-medium text-gray-700">Value *</label>
            <textarea
              className="rounded-lg border border-gray-300 px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 resize-y"
              rows={8}
              placeholder='{"key": "value"}'
              value={value}
              onChange={(e) => setValue(e.target.value)}
              required
            />
          </div>

          <div>
            <label className="text-sm font-medium text-gray-700 mb-2 block">Headers</label>
            {Object.entries(headers).map(([k, v]) => (
              <div key={k} className="flex items-center gap-2 mb-1">
                <span className="font-mono text-xs bg-gray-100 px-2 py-1 rounded flex-1">{k}: {v}</span>
                <button
                  type="button"
                  onClick={() => setHeaders((prev) => { const next = { ...prev }; delete next[k]; return next })}
                  className="text-gray-400 hover:text-red-500"
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              </div>
            ))}
            <div className="flex gap-2 mt-1">
              <Input placeholder="Key" value={headerKey} onChange={(e) => setHeaderKey(e.target.value)} className="flex-1" />
              <Input placeholder="Value" value={headerValue} onChange={(e) => setHeaderValue(e.target.value)} className="flex-1" />
              <Button type="button" variant="secondary" size="sm" onClick={addHeader}>Add</Button>
            </div>
          </div>

          {mutation.error && (
            <div className="rounded-lg border border-red-200 bg-red-50 p-3 flex items-start gap-2">
              <AlertCircle className="w-4 h-4 text-red-500 mt-0.5 flex-shrink-0" />
              <pre className="text-xs text-red-700 font-mono whitespace-pre-wrap break-words">
                {(mutation.error as Error).message}
              </pre>
            </div>
          )}
          {mutation.isSuccess && (
            <p className="text-sm text-green-600">Message produced successfully!</p>
          )}

          <div className="flex justify-end gap-3 pt-2">
            <Button variant="secondary" type="button" onClick={onClose}>Cancel</Button>
            <Button type="submit" loading={mutation.isPending} disabled={!value}>
              Produce Message
            </Button>
          </div>
        </form>
      </div>
    </div>
  )
}
