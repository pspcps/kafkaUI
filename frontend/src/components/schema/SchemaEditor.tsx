'use client'

import { useState, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchSchema, saveSchema } from '@/lib/api'
import type { SchemaField, FieldType, FieldSource } from '@/types'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Table, TableHead, TableBody, Th, Td } from '@/components/ui/Table'
import { Plus, Trash2, Save, GripVertical, Key, Star } from 'lucide-react'

const FIELD_TYPES: FieldType[] = ['string', 'int', 'long', 'float', 'double', 'boolean', 'timestamp', 'bytes', 'json']

const EMPTY_FIELD = (): SchemaField => ({
  name: '',
  fieldType: 'string',
  fieldSource: 'value',
  isRequired: false,
  isPrimaryKey: false,
  defaultValue: '',
  description: '',
})

interface SchemaEditorProps {
  clusterId: string
  topicName: string
}

export function SchemaEditor({ clusterId, topicName }: SchemaEditorProps) {
  const qc = useQueryClient()
  const [fields, setFields] = useState<SchemaField[]>([])
  const [schemaType, setSchemaType] = useState<'manual' | 'avro'>('manual')
  const [avroRaw, setAvroRaw] = useState('')
  const [dirty, setDirty] = useState(false)
  const [saveMsg, setSaveMsg] = useState('')

  const { data: schemaData, isLoading } = useQuery({
    queryKey: ['schema', clusterId, topicName],
    queryFn: () => fetchSchema(clusterId, topicName),
  })

  // Populate form when data loads (only once per topic)
  const [initialized, setInitialized] = useState(false)
  if (schemaData && !initialized) {
    setFields(schemaData.fields ?? [])
    setSchemaType(schemaData.schemaType ?? 'manual')
    setAvroRaw(schemaData.avroRaw ?? '')
    setInitialized(true)
  }

  const saveMutation = useMutation({
    mutationFn: () => saveSchema(clusterId, topicName, schemaType, fields, avroRaw),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['schema', clusterId, topicName] })
      setSaveMsg('Saved!')
      setDirty(false)
      setTimeout(() => setSaveMsg(''), 3000)
    },
  })

  const update = useCallback((idx: number, patch: Partial<SchemaField>) => {
    setFields((prev) => {
      const next = [...prev]
      const updated = { ...next[idx], ...patch }
      // Key-source fields are always PK; value/header fields can never be PK
      if (patch.fieldSource !== undefined) {
        updated.isPrimaryKey = patch.fieldSource === 'key'
      }
      next[idx] = updated
      return next
    })
    setDirty(true)
  }, [])

  const addField = () => {
    setFields((prev) => [...prev, EMPTY_FIELD()])
    setDirty(true)
  }

  const removeField = (idx: number) => {
    setFields((prev) => prev.filter((_, i) => i !== idx))
    setDirty(true)
  }

  if (isLoading) return <div className="text-gray-400 py-8 text-center">Loading schema…</div>

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex gap-3 items-center">
          <label className="text-sm font-medium text-gray-700">Schema type:</label>
          <select
            className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm"
            value={schemaType}
            onChange={(e) => { setSchemaType(e.target.value as 'manual' | 'avro'); setDirty(true) }}
          >
            <option value="manual">Manual (define fields below)</option>
            <option value="avro">Avro JSON</option>
          </select>
        </div>
        <div className="flex items-center gap-3">
          {saveMsg && <span className="text-sm text-green-600 font-medium">{saveMsg}</span>}
          <Button
            size="sm"
            onClick={() => saveMutation.mutate()}
            loading={saveMutation.isPending}
            disabled={!dirty}
          >
            <Save className="w-4 h-4" /> Save Schema
          </Button>
        </div>
      </div>

      {schemaType === 'avro' ? (
        <div className="space-y-2">
          <label className="text-sm font-medium text-gray-700">Avro Schema JSON</label>
          <textarea
            className="w-full font-mono text-sm border border-gray-300 rounded-lg p-3 h-64 focus:outline-none focus:ring-2 focus:ring-blue-500"
            placeholder='{"type":"record","name":"MyEvent","fields":[...]}'
            value={avroRaw}
            onChange={(e) => { setAvroRaw(e.target.value); setDirty(true) }}
          />
          <p className="text-xs text-gray-400">
            Paste your Avro schema JSON. Fields will be extracted for validation and table creation.
          </p>
        </div>
      ) : (
        <>
          {/* Legend */}
          <div className="flex gap-4 text-xs text-gray-500">
            <span className="flex items-center gap-1"><Key className="w-3 h-3 text-amber-500" /> Primary Key</span>
            <span className="flex items-center gap-1"><Star className="w-3 h-3 text-blue-500" /> Required (NOT NULL)</span>
            <span className="px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded font-mono">V</span> Message value
            <span className="px-1.5 py-0.5 bg-purple-100 text-purple-700 rounded font-mono">H</span> Kafka header
            <span className="px-1.5 py-0.5 bg-amber-100 text-amber-700 rounded font-mono">K</span> Message key (always PK)
          </div>

          {/* Field table */}
          {fields.length === 0 ? (
            <div className="border-2 border-dashed border-gray-200 rounded-xl py-10 text-center text-gray-400">
              <p className="text-sm">No fields defined yet.</p>
              <button onClick={addField} className="mt-2 text-blue-600 text-sm hover:underline">+ Add first field</button>
            </div>
          ) : (
            <div className="border border-gray-200 rounded-xl overflow-hidden">
              <Table>
                <TableHead>
                  <tr>
                    <Th className="w-8"></Th>
                    <Th>Field Name</Th>
                    <Th className="w-24">Source</Th>
                    <Th>Type</Th>
                    <Th className="w-20 text-center">Required</Th>
                    <Th className="w-20 text-center">PK</Th>
                    <Th>Default</Th>
                    <Th>Description</Th>
                    <Th className="w-10"></Th>
                  </tr>
                </TableHead>
                <TableBody>
                  {fields.map((f, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      <Td>
                        <GripVertical className="w-4 h-4 text-gray-300 cursor-grab" />
                      </Td>
                      <Td>
                        <Input
                          placeholder="field_name"
                          value={f.name}
                          onChange={(e) => update(idx, { name: e.target.value })}
                          className="font-mono text-sm"
                        />
                      </Td>
                      <Td>
                        <select
                          className="rounded-lg border border-gray-300 px-2 py-1.5 text-sm w-full focus:outline-none focus:ring-2 focus:ring-blue-500"
                          value={f.fieldSource ?? 'value'}
                          onChange={(e) => update(idx, { fieldSource: e.target.value as FieldSource })}
                        >
                          <option value="value">Value</option>
                          <option value="header">Header</option>
                          <option value="key">Key (PK)</option>
                        </select>
                      </Td>
                      <Td>
                        <select
                          className="rounded-lg border border-gray-300 px-2 py-1.5 text-sm w-full focus:outline-none focus:ring-2 focus:ring-blue-500"
                          value={f.fieldType}
                          onChange={(e) => update(idx, { fieldType: e.target.value as FieldType })}
                        >
                          {FIELD_TYPES.map((t) => (
                            <option key={t} value={t}>{t}</option>
                          ))}
                        </select>
                      </Td>
                      <Td className="text-center">
                        <button
                          onClick={() => update(idx, { isRequired: !f.isRequired })}
                          className={`w-6 h-6 rounded flex items-center justify-center mx-auto transition-colors ${
                            f.isRequired ? 'bg-blue-100 text-blue-600' : 'bg-gray-100 text-gray-300'
                          }`}
                          title="Required (NOT NULL)"
                        >
                          <Star className="w-3.5 h-3.5" />
                        </button>
                      </Td>
                      <Td className="text-center">
                        {f.fieldSource === 'key' ? (
                          <span
                            className="w-6 h-6 rounded flex items-center justify-center mx-auto bg-amber-100 text-amber-600"
                            title="Primary Key (set by Key source)"
                          >
                            <Key className="w-3.5 h-3.5" />
                          </span>
                        ) : (
                          <span
                            className="w-6 h-6 rounded flex items-center justify-center mx-auto bg-gray-100 text-gray-300 cursor-not-allowed"
                            title="PK can only be set via Key source"
                          >
                            <Key className="w-3.5 h-3.5" />
                          </span>
                        )}
                      </Td>
                      <Td>
                        <Input
                          placeholder="—"
                          value={f.defaultValue ?? ''}
                          onChange={(e) => update(idx, { defaultValue: e.target.value })}
                          className="text-sm"
                        />
                      </Td>
                      <Td>
                        <Input
                          placeholder="optional note"
                          value={f.description ?? ''}
                          onChange={(e) => update(idx, { description: e.target.value })}
                          className="text-sm"
                        />
                      </Td>
                      <Td>
                        <button
                          onClick={() => removeField(idx)}
                          className="p-1 text-gray-400 hover:text-red-500 rounded transition-colors"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </Td>
                    </tr>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}

          <Button variant="secondary" size="sm" onClick={addField}>
            <Plus className="w-4 h-4" /> Add Field
          </Button>
        </>
      )}
    </div>
  )
}
