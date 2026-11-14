import { AxiosError, AxiosHeaders, type AxiosAdapter } from 'axios'
import type { KafkaMessage, ReplicationJob, TopicSchema, TopicSummary } from '@/types'

type DemoState = {
  topics: Record<string, TopicSummary[]>
  messages: Record<string, KafkaMessage[]>
  schemas: Record<string, TopicSchema>
  jobs: ReplicationJob[]
}

const STORAGE_KEY = 'kafkaui-demo-v2'
const clusters = [
  {
    id: 'demo',
    name: 'Demo · Development',
    brokers: ['broker-1.demo:9093', 'broker-2.demo:9093', 'broker-3.demo:9093'],
  },
  { id: 'staging', name: 'Demo · Staging', brokers: ['staging-1.demo:9093'] },
]
const now = () => new Date().toISOString()
const key = (cluster: string, topic: string) => `${cluster}/${topic}`
const makeMessages = (topic: string, count: number): KafkaMessage[] =>
  Array.from({ length: count }, (_, i) => ({
    partition: i % 3,
    offset: i,
    timestamp: new Date(Date.now() - (count - i) * 45000).toISOString(),
    key: `order-${1000 + i}`,
    value: JSON.stringify({
      event: topic,
      orderId: `ORD-${1000 + i}`,
      status: i % 3 ? 'completed' : 'pending',
      amount: (i + 1) * 24.5,
      currency: 'USD',
    }),
    headers: { source: 'sample-data', 'trace-id': `demo-trace-${i}` },
  }))

function initialState(): DemoState {
  const topics = [
    {
      name: 'demo.orders',
      partitions: 6,
      replicationFactor: 3,
      messageCount: 24,
      internal: false,
    },
    {
      name: 'demo.payments',
      partitions: 3,
      replicationFactor: 3,
      messageCount: 12,
      internal: false,
    },
    {
      name: 'demo.inventory',
      partitions: 3,
      replicationFactor: 3,
      messageCount: 8,
      internal: false,
    },
    {
      name: '__consumer_offsets',
      partitions: 3,
      replicationFactor: 3,
      messageCount: 0,
      internal: true,
    },
  ]
  return {
    topics: {
      demo: topics,
      staging: [
        {
          name: 'staging.events',
          partitions: 1,
          replicationFactor: 1,
          messageCount: 5,
          internal: false,
        },
      ],
    },
    messages: Object.fromEntries([
      ...topics.map((t) => [key('demo', t.name), makeMessages(t.name, t.messageCount)]),
      [key('staging', 'staging.events'), makeMessages('staging.events', 5)],
    ]),
    schemas: {},
    jobs: [
      {
        id: 'demo-job',
        name: 'Orders → Analytics',
        clusterId: 'demo',
        topicName: 'demo.orders',
        consumerGroup: 'analytics-orders',
        serviceName: 'analytics',
        destinationType: 'postgresql',
        destinationConfig: { connectionId: 'demo-rds', tableName: 'orders' },
        startPosition: 'earliest',
        includeHistorical: true,
        consumerCountMode: 'fixed',
        consumerCount: 2,
        teamName: 'Data Platform',
        status: 'running',
        k8sNamespace: 'demo',
        createdAt: now(),
        updatedAt: now(),
      },
    ],
  }
}

let state: DemoState | undefined
function loadState() {
  if (state) return state
  if (typeof window !== 'undefined') {
    try {
      // The earlier demo allowed inline passwords. Discard that legacy browser state.
      window.localStorage.removeItem('kafkaui-demo-v1')
      const saved = window.localStorage.getItem(STORAGE_KEY)
      if (saved) state = JSON.parse(saved) as DemoState
    } catch {
      /* Storage may be disabled. */
    }
  }
  return state ?? (state = initialState())
}
function save() {
  if (typeof window !== 'undefined') {
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state))
    } catch {
      /* Keep the in-memory demo working. */
    }
  }
}
export function resetDemo() {
  state = initialState()
  save()
  window.location.assign('/clusters')
}

// This adapter never forwards requests to a network service. Demo writes affect this browser only.
export const demoAdapter: AxiosAdapter = async (config) => {
  const data = loadState()
  const path = (config.url ?? '').split('?')[0].split('/').filter(Boolean).map(decodeURIComponent)
  const method = (config.method ?? 'get').toLowerCase()
  const body =
    typeof config.data === 'string' ? JSON.parse(config.data || '{}') : (config.data ?? {})
  const params = config.params ?? {}
  const response = (value: unknown, status = 200) => ({
    data: value,
    status,
    statusText: 'OK',
    headers: new AxiosHeaders(),
    config,
  })
  const fail = (message: string, status = 400): never => {
    throw new AxiosError(message, 'DEMO_ERROR', config, undefined, {
      ...response({ message }, status),
      statusText: message,
    })
  }
  const metric = () =>
    Array.from({ length: 3 }, (_, i) => ({
      partitionId: i,
      currentOffset: 100 + i,
      endOffset: 108 + i,
      lag: 8,
    }))
  let result: unknown

  if (path[0] === 'auth') {
    const user = {
      id: 'demo-user',
      username: 'Demo Explorer',
      role: 'admin' as const,
    }
    result =
      path[1] === 'login'
        ? {
            token: 'demo-only-no-backend-access',
            expiresAt: new Date(Date.now() + 86400000).toISOString(),
            user,
          }
        : user
  } else if (path[0] === 'connections') {
    result = [
      {
        id: 'demo-rds',
        name: 'Demo analytics · RDS IAM',
        destinationType: 'postgresql',
        provider: 'rds_iam',
      },
      {
        id: 'demo-vault',
        name: 'Demo warehouse · Vault',
        destinationType: 'postgresql',
        provider: 'vault',
      },
      {
        id: 'demo-secrets',
        name: 'Demo reporting · AWS Secrets Manager',
        destinationType: 'mysql',
        provider: 'aws_secrets_manager',
      },
      {
        id: 'demo-k8s',
        name: 'Demo documents · Kubernetes Secret',
        destinationType: 'mongodb',
        provider: 'kubernetes_secret',
      },
    ]
  } else if (path[0] === 'clusters' && path.length === 1) {
    result = clusters
  } else if (path[0] === 'clusters') {
    const cluster = clusters.find((c) => c.id === path[1])
    if (!cluster) fail('Demo cluster not found', 404)
    const clusterId = path[1]
    const list = data.topics[clusterId] ?? []
    const topic = list.find((t) => t.name === path[3])
    const topicKey = key(clusterId, path[3])
    if (path[2] === 'cluster') {
      result = {
        clusterId,
        brokerCount: cluster!.brokers.length,
        topicCount: list.filter((t) => !t.internal).length,
        consumerGroups: 2,
        controller: 1,
        brokers: cluster!.brokers.map((host, i) => ({
          id: i + 1,
          host: host.split(':')[0],
          port: 9093,
          isLeader: i === 0,
          rack: `zone-${i + 1}`,
        })),
      }
    } else if (path[2] === 'topics') {
      if (path.length === 3) {
        if (method === 'post') {
          if (!body.name?.trim() || list.some((t) => t.name === body.name))
            fail('Choose a unique topic name')
          const partitions = Number(body.partitions)
          const replicationFactor = Number(body.replicationFactor)
          if (!Number.isInteger(partitions) || partitions < 1 || partitions > 24)
            fail('Demo topics support 1–24 partitions')
          if (
            !Number.isInteger(replicationFactor) ||
            replicationFactor < 1 ||
            replicationFactor > cluster!.brokers.length
          )
            fail('Replication cannot exceed the demo broker count')
          const created = {
            name: body.name,
            partitions,
            replicationFactor,
            messageCount: 0,
            internal: false,
          }
          list.push(created)
          data.messages[key(clusterId, body.name)] = []
          result = created
        } else result = list.filter((t) => params.includeInternal === true || !t.internal)
      } else if (!topic) fail('Topic not found in this demo', 404)
      else if (path[4] === 'consumer-groups') {
        result = ['analytics-orders', 'notifications-service']
          .filter((groupId) =>
            clusterId === 'staging'
              ? path[3] === 'staging.events'
              : groupId === 'analytics-orders'
                ? ['demo.orders', 'demo.payments'].includes(path[3])
                : ['demo.orders', 'demo.inventory'].includes(path[3]),
          )
          .map((groupId) => ({
            groupId,
            state: 'Stable',
            memberCount: groupId === 'analytics-orders' ? 2 : 1,
            topicLag: groupId === 'analytics-orders' ? 24 : 0,
          }))
      } else if (path[4] === 'messages') {
        const messages = data.messages[topicKey] ?? []
        if (method === 'post') {
          if (typeof body.value !== 'string') fail('A message value is required')
          const partition = Number(body.partition ?? 0)
          if (!Number.isInteger(partition) || partition < 0 || partition >= topic.partitions)
            fail('Choose a valid partition')
          const message = {
            partition,
            offset: messages.length,
            timestamp: now(),
            key: body.key ?? '',
            value: body.value,
            headers: body.headers ?? {},
          }
          messages.push(message)
          data.messages[topicKey] = messages
          topic.messageCount = messages.length
          result = message
        } else {
          const filtered = messages.filter(
            (m) =>
              (params.partition == null ||
                Number(params.partition) < 0 ||
                m.partition === Number(params.partition)) &&
              (params.offset == null || m.offset >= Number(params.offset)) &&
              (!params.filterKey || m.key.includes(params.filterKey)) &&
              (!params.filterValue || m.value.includes(params.filterValue)),
          )
          result = {
            messages: filtered.slice(0, Math.min(Number(params.limit) || 50, 500)),
            totalCount: filtered.length,
          }
        }
      } else if (path[4] === 'schema') {
        const previous = data.schemas[topicKey] ?? {
          id: `schema-${path[3]}`,
          clusterId,
          topicName: path[3],
          schemaType: 'manual',
          schemaFormat: 'none',
          schemaVersion: 0,
          fields: [],
        }
        result =
          method === 'put'
            ? (data.schemas[topicKey] = {
                ...previous,
                ...body,
                schemaVersion: (previous.schemaVersion ?? 0) + 1,
                updatedAt: now(),
              })
            : previous
      } else if (method === 'delete') {
        data.topics[clusterId] = list.filter((t) => t.name !== path[3])
        delete data.messages[topicKey]
        delete data.schemas[topicKey]
        result = {}
      } else {
        result = {
          ...topic,
          partitions: Array.from({ length: topic.partitions }, (_, i) => ({
            partitionId: i,
            leader: (i % cluster!.brokers.length) + 1,
            replicas: Array.from({ length: topic.replicationFactor }, (_, j) => j + 1),
            isr: Array.from({ length: topic.replicationFactor }, (_, j) => j + 1),
            offsetBegin: 0,
            offsetEnd: (data.messages[topicKey] ?? []).filter((m) => m.partition === i).length,
            messageCount: (data.messages[topicKey] ?? []).filter((m) => m.partition === i).length,
          })),
          config: {
            'cleanup.policy': 'delete',
            'retention.ms': '604800000',
            'compression.type': 'producer',
          },
        }
      }
    } else if (path[2] === 'consumer-groups') {
      const groupTopics =
        clusterId === 'demo'
          ? path[3] === 'notifications-service'
            ? ['demo.orders', 'demo.inventory']
            : ['demo.orders', 'demo.payments']
          : ['staging.events']
      const groups = [
        {
          groupId: 'analytics-orders',
          state: 'Stable',
          memberCount: 2,
          totalLag: 48,
        },
        {
          groupId: 'notifications-service',
          state: 'Stable',
          memberCount: 1,
          totalLag: 0,
        },
      ]
      const group = groups.find((g) => g.groupId === path[3])
      if (path.length > 3 && !group) fail('Consumer group not found', 404)
      result =
        path.length === 3
          ? groups
          : {
              ...group,
              members: Array.from({ length: group!.memberCount }, (_, i) => ({
                memberId: `${path[3]}-member-${i}`,
                clientId: `${path[3]}-consumer-${i}`,
                clientHost: `10.0.0.${12 + i}`,
              })),
              topicLag: groupTopics
                .filter((name) => list.some((t) => t.name === name))
                .map((name) => ({
                  topic: name,
                  totalLag: path[3] === 'notifications-service' ? 0 : 24,
                  partitions: metric().map((m) => ({
                    partitionId: m.partitionId,
                    consumerOffset:
                      path[3] === 'notifications-service' ? m.endOffset : m.currentOffset,
                    endOffset: m.endOffset,
                    lag: path[3] === 'notifications-service' ? 0 : m.lag,
                  })),
                })),
            }
    } else fail('This demo route is unavailable', 404)
  } else if (path[0] === 'jobs') {
    const job = data.jobs.find((j) => j.id === path[1])
    if (path.length === 1) {
      if (method === 'post') {
        if (body.destinationConfig?.password || !body.destinationConfig?.connectionId)
          fail('Select a managed demo connection; do not enter credentials')
        result = {
          ...body,
          id: crypto.randomUUID(),
          status: 'running',
          k8sNamespace: 'demo',
          createdAt: now(),
          updatedAt: now(),
        }
        data.jobs.push(result as ReplicationJob)
      } else result = data.jobs
    } else if (!job) fail('Demo job not found', 404)
    else if (path[2] === 'live-metrics')
      result = {
        groupId: job.consumerGroup,
        groupState: job.status === 'running' ? 'Stable' : 'Empty',
        partitions: metric(),
        members: [],
      }
    else if (path[2] === 'metrics')
      result = metric().map((m) => ({
        ...m,
        jobId: job.id,
        replicatedCount: 100,
        updatedAt: now(),
      }))
    else if (path[2] === 'errors') result = []
    else if (path[2] === 'logs')
      result = {
        logs: '[Demo] No worker is running. These are simulated job details.\n[Demo] Try pause, resume, and scaling without touching any real infrastructure.',
      }
    else if (path[2] === 'reset-offset')
      fail('Offset reset requires a real Kafka backend and is unavailable in this demo')
    else if (path[2] === 'replicas') {
      job.consumerCount = Math.max(1, Math.min(12, Number(body.replicas) || 1))
      result = { replicas: job.consumerCount, applied: true }
    } else if (path[2] === 'destination') {
      job.destinationType = body.destinationType
      job.destinationConfig = body.destinationConfig
      result = job
    } else if (method === 'patch') {
      job.status = body.action === 'pause' ? 'paused' : 'running'
      job.updatedAt = now()
      result = job
    } else if (method === 'delete') {
      data.jobs = data.jobs.filter((j) => j.id !== path[1])
      result = {}
    } else result = job
  } else if (path[0] === 'validate-schema')
    fail('Server schema validation is unavailable in the browser demo')
  else fail('This feature requires a live backend', 404)
  if (method !== 'get') save()
  return response(result)
}
