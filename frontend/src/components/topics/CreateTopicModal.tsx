'use client'

import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { createTopic, validateSchema } from '@/lib/api'
import type { SchemaFormat } from '@/types'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { X, ArrowLeft, ArrowRight, Check, AlertCircle } from 'lucide-react'

interface CreateTopicModalProps {
  clusterId: string
  onClose: () => void
}

type Step = 'basics' | 'schema'

const FORMAT_OPTIONS: { value: SchemaFormat; label: string; blurb: string }[] = [
  {
    value: 'none',
    label: 'Schemaless / Flexible',
    blurb: 'No validation. Any payload is accepted. Good for prototyping or external integrations.',
  },
  {
    value: 'json_schema',
    label: 'JSON Schema',
    blurb: 'Standard JSON Schema (2020-12). Human-readable, easy for prototyping, validates JSON payloads on produce.',
  },
  {
    value: 'avro',
    label: 'Avro',
    blurb: 'Apache Avro JSON schema. Strong typing, supports schema evolution, compact format. Recommended for production.',
  },
  {
    value: 'protobuf',
    label: 'Protobuf (coming soon)',
    blurb: 'Phase 2 — dynamic .proto parsing. Not yet enforced.',
  },
]

const PLACEHOLDERS: Record<SchemaFormat, string> = {
  none: '',
  manual: '',
  protobuf: `syntax = "proto3";
message Order {
  string id = 1;
  int64 amount_cents = 2;
}`,
  json_schema: `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["id", "amount"],
  "properties": {
    "id":     { "type": "string", "format": "uuid" },
    "amount": { "type": "number", "minimum": 0 },
    "tags":   { "type": "array", "items": { "type": "string" } }
  },
  "additionalProperties": false
}`,
  avro: `{
  "type": "record",
  "name": "Order",
  "fields": [
    { "name": "id", "type": "string" },
    { "name": "amount", "type": "double" },
    { "name": "tags", "type": { "type": "array", "items": "string" } }
  ]
}`,
}

export function CreateTopicModal({ clusterId, onClose }: CreateTopicModalProps) {
  const qc = useQueryClient()

  const [step, setStep] = useState<Step>('basics')
  const [name, setName] = useState('')
  const [partitions, setPartitions] = useState('3')
  const [replicationFactor, setReplicationFactor] = useState('1')
  const [retentionMs, setRetentionMs] = useState('')

  const [format, setFormat] = useState<SchemaFormat>('none')
  const [schemaText, setSchemaText] = useState('')
  const [validateState, setValidateState] = useState<
    | { kind: 'idle' }
    | { kind: 'checking' }
    | { kind: 'ok' }
    | { kind: 'err'; message: string }
  >({ kind: 'idle' })

  const mutation = useMutation({
    mutationFn: () =>
      createTopic(
        clusterId,
        {
          name,
          partitions: parseInt(partitions),
          replicationFactor: parseInt(replicationFactor),
          configs: retentionMs ? { 'retention.ms': retentionMs } : undefined,
        },
        format !== 'none' && format !== 'manual' && schemaText.trim() !== ''
          ? { format, text: schemaText }
          : undefined,
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['topics', clusterId] })
      onClose()
    },
  })

  const onValidate = async () => {
    setValidateState({ kind: 'checking' })
    const res = await validateSchema(format, schemaText)
    setValidateState(res.valid ? { kind: 'ok' } : { kind: 'err', message: res.message })
  }

  const canProceedFromBasics = !!name && parseInt(partitions) >= 1 && parseInt(replicationFactor) >= 1
  const showEditor = format !== 'none' && format !== 'protobuf'
  const editorPlaceholder = PLACEHOLDERS[format] || ''

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl mx-4 max-h-[90vh] flex flex-col">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Create Topic</h2>
            <div className="flex items-center gap-2 mt-1 text-xs text-gray-500">
              <span className={step === 'basics' ? 'font-semibold text-blue-600' : ''}>1. Basics</span>
              <span>→</span>
              <span className={step === 'schema' ? 'font-semibold text-blue-600' : ''}>2. Schema</span>
            </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X className="w-5 h-5" />
          </button>
        </div>

        <form
          className="p-6 space-y-4 overflow-y-auto"
          onSubmit={(e) => {
            e.preventDefault()
            if (step === 'basics') {
              setStep('schema')
              return
            }
            mutation.mutate()
          }}
        >
          {step === 'basics' && (
            <>
              <Input
                label="Topic Name"
                placeholder="my-topic"
                value={name}
                onChange={(e) => setName(e.target.value)}
                required
                autoFocus
              />
              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Partitions"
                  type="number"
                  min={1}
                  value={partitions}
                  onChange={(e) => setPartitions(e.target.value)}
                  required
                />
                <Input
                  label="Replication Factor"
                  type="number"
                  min={1}
                  value={replicationFactor}
                  onChange={(e) => setReplicationFactor(e.target.value)}
                  required
                />
              </div>
              <Input
                label="Retention (ms)"
                placeholder="e.g. 604800000 for 7 days"
                value={retentionMs}
                onChange={(e) => setRetentionMs(e.target.value)}
              />
            </>
          )}

          {step === 'schema' && (
            <>
              <div>
                <label className="text-sm font-medium text-gray-700 block mb-2">Schema Format</label>
                <div className="space-y-2">
                  {FORMAT_OPTIONS.map((opt) => {
                    const isSelected = format === opt.value
                    const disabled = opt.value === 'protobuf'
                    return (
                      <button
                        key={opt.value}
                        type="button"
                        disabled={disabled}
                        onClick={() => {
                          setFormat(opt.value)
                          setValidateState({ kind: 'idle' })
                          if (!schemaText && opt.value !== 'none') {
                            setSchemaText(PLACEHOLDERS[opt.value] || '')
                          }
                        }}
                        className={`w-full text-left rounded-lg border px-4 py-3 transition-colors ${
                          isSelected
                            ? 'border-blue-500 bg-blue-50'
                            : 'border-gray-200 hover:border-gray-300'
                        } ${disabled ? 'opacity-50 cursor-not-allowed' : ''}`}
                      >
                        <div className="font-medium text-sm text-gray-900">{opt.label}</div>
                        <div className="text-xs text-gray-500 mt-0.5">{opt.blurb}</div>
                      </button>
                    )
                  })}
                </div>
              </div>

              {showEditor && (
                <div>
                  <div className="flex items-center justify-between mb-2">
                    <label className="text-sm font-medium text-gray-700">
                      Schema Source
                    </label>
                    <Button
                      type="button"
                      size="sm"
                      variant="secondary"
                      onClick={onValidate}
                      disabled={!schemaText.trim() || validateState.kind === 'checking'}
                    >
                      {validateState.kind === 'checking' ? 'Validating…' : 'Validate'}
                    </Button>
                  </div>
                  <textarea
                    className="w-full rounded-lg border border-gray-300 px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 resize-y"
                    rows={12}
                    placeholder={editorPlaceholder}
                    value={schemaText}
                    onChange={(e) => {
                      setSchemaText(e.target.value)
                      setValidateState({ kind: 'idle' })
                    }}
                  />
                  {validateState.kind === 'ok' && (
                    <p className="text-xs text-green-700 mt-1 flex items-start gap-1.5">
                      <Check className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
                      Schema is valid
                    </p>
                  )}
                  {validateState.kind === 'err' && (
                    <p className="text-xs text-red-700 mt-1 flex items-start gap-1.5 whitespace-pre-wrap">
                      <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
                      {validateState.message}
                    </p>
                  )}
                </div>
              )}
            </>
          )}

          {mutation.error && (
            <p className="text-sm text-red-600 whitespace-pre-wrap">{(mutation.error as Error).message}</p>
          )}

          <div className="flex justify-between pt-2">
            {step === 'schema' ? (
              <Button variant="secondary" type="button" onClick={() => setStep('basics')}>
                <ArrowLeft className="w-4 h-4 mr-1" />
                Back
              </Button>
            ) : (
              <Button variant="secondary" type="button" onClick={onClose}>
                Cancel
              </Button>
            )}

            {step === 'basics' ? (
              <Button type="submit" disabled={!canProceedFromBasics}>
                Next
                <ArrowRight className="w-4 h-4 ml-1" />
              </Button>
            ) : (
              <Button type="submit" loading={mutation.isPending}>
                Create Topic
              </Button>
            )}
          </div>
        </form>
      </div>
    </div>
  )
}
