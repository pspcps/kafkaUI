import axios from 'axios'
import { getToken, clearAuth } from './auth'
import type {
  ClusterConfig,
  ClusterInfo,
  TopicSummary,
  TopicDetail,
  CreateTopicRequest,
  ConsumerGroupSummary,
  ConsumerGroupDetail,
  MessagesResponse,
  ProduceMessageRequest,
  TopicSchema,
  SchemaField,
  ReplicationJob,
  ConsumerMetric,
  JobError,
} from '@/types'

const BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080'

const api = axios.create({
  baseURL: `${BASE_URL}/api/v1`,
  headers: { 'Content-Type': 'application/json' },
  timeout: 15000,
})

api.interceptors.request.use((config) => {
  const token = getToken()
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

api.interceptors.response.use(
  (res) => res,
  (err) => {
    if (err.response?.status === 401 && typeof window !== 'undefined') {
      clearAuth()
      if (window.location.pathname !== '/login') {
        window.location.href = '/login'
      }
    }
    const message = err.response?.data?.message || err.message || 'Unknown error'
    return Promise.reject(new Error(message))
  }
)

// ── Auth ──────────────────────────────────────────────────────────────────────

export type LoginResponse = {
  token: string
  expiresAt: string
  user: { id: string; username: string; role: 'admin' | 'viewer' }
}

export const login = async (username: string, password: string): Promise<LoginResponse> => {
  const { data } = await api.post<LoginResponse>('/auth/login', { username, password })
  return data
}

export const fetchMe = async (): Promise<LoginResponse['user']> => {
  const { data } = await api.get<LoginResponse['user']>('/auth/me')
  return data
}

const clusterBase = (clusterId: string) => `/clusters/${encodeURIComponent(clusterId)}`

// ── Clusters ──────────────────────────────────────────────────────────────────

export const fetchClusters = async (): Promise<ClusterConfig[]> => {
  const { data } = await api.get<ClusterConfig[]>('/clusters')
  return data
}

// ── Cluster overview ──────────────────────────────────────────────────────────

export const fetchCluster = async (clusterId: string): Promise<ClusterInfo> => {
  const { data } = await api.get<ClusterInfo>(`${clusterBase(clusterId)}/cluster`)
  return data
}

// ── Topics ────────────────────────────────────────────────────────────────────

export const fetchTopics = async (clusterId: string, includeInternal = false): Promise<TopicSummary[]> => {
  const { data } = await api.get<TopicSummary[]>(`${clusterBase(clusterId)}/topics`, {
    params: { includeInternal },
  })
  return data
}

export const fetchTopic = async (clusterId: string, name: string): Promise<TopicDetail> => {
  const { data } = await api.get<TopicDetail>(
    `${clusterBase(clusterId)}/topics/${encodeURIComponent(name)}`
  )
  return data
}

export const createTopic = async (
  clusterId: string,
  req: CreateTopicRequest,
  schema?: import('@/types').SchemaInput,
): Promise<void> => {
  await api.post(`${clusterBase(clusterId)}/topics`, {
    ...req,
    ...(schema && schema.format !== 'none' ? { schema } : {}),
  })
}

export const deleteTopic = async (clusterId: string, name: string): Promise<void> => {
  await api.delete(`${clusterBase(clusterId)}/topics/${encodeURIComponent(name)}`)
}

// ── Messages ──────────────────────────────────────────────────────────────────

export interface FetchMessagesParams {
  partition?: number
  offset?: number
  limit?: number
  filterKey?: string
  filterValue?: string
}

export const fetchMessages = async (
  clusterId: string,
  topic: string,
  params: FetchMessagesParams = {}
): Promise<MessagesResponse> => {
  const { data } = await api.get<MessagesResponse>(
    `${clusterBase(clusterId)}/topics/${encodeURIComponent(topic)}/messages`,
    { params }
  )
  return data
}

export const produceMessage = async (
  clusterId: string,
  topic: string,
  req: ProduceMessageRequest
): Promise<void> => {
  await api.post(
    `${clusterBase(clusterId)}/topics/${encodeURIComponent(topic)}/messages`,
    req
  )
}

// ── Consumer Groups ───────────────────────────────────────────────────────────

export const fetchConsumerGroups = async (clusterId: string): Promise<ConsumerGroupSummary[]> => {
  const { data } = await api.get<ConsumerGroupSummary[]>(
    `${clusterBase(clusterId)}/consumer-groups`
  )
  return data
}

export const fetchConsumerGroup = async (
  clusterId: string,
  id: string
): Promise<ConsumerGroupDetail> => {
  const { data } = await api.get<ConsumerGroupDetail>(
    `${clusterBase(clusterId)}/consumer-groups/${encodeURIComponent(id)}`
  )
  return data
}

// ── Schema ────────────────────────────────────────────────────────────────────

export const fetchSchema = async (clusterId: string, topicName: string): Promise<TopicSchema> => {
  const { data } = await api.get<TopicSchema>(
    `${clusterBase(clusterId)}/topics/${encodeURIComponent(topicName)}/schema`
  )
  return data
}

export const saveSchema = async (
  clusterId: string,
  topicName: string,
  schemaType: string,
  fields: SchemaField[],
  avroRaw?: string,
): Promise<TopicSchema> => {
  const { data } = await api.put<TopicSchema>(
    `${clusterBase(clusterId)}/topics/${encodeURIComponent(topicName)}/schema`,
    { schemaType, fields, avroRaw }
  )
  return data
}

export const saveTypedSchema = async (
  clusterId: string,
  topicName: string,
  format: import('@/types').SchemaFormat,
  text: string,
): Promise<TopicSchema> => {
  const { data } = await api.put<TopicSchema>(
    `${clusterBase(clusterId)}/topics/${encodeURIComponent(topicName)}/schema`,
    { schemaFormat: format, schemaText: text }
  )
  return data
}

export type SchemaCheckResult =
  | { valid: true; format: string }
  | { valid: false; stage: 'compile' | 'sample'; message: string }

export const validateSchema = async (
  format: string,
  text: string,
  sample?: string,
): Promise<SchemaCheckResult> => {
  try {
    const { data } = await api.post<SchemaCheckResult>('/validate-schema', {
      format,
      text,
      sample,
    })
    return data
  } catch (err) {
    // axios interceptor turns the 400 body into Error(message). Reconstitute.
    return { valid: false, stage: 'compile', message: (err as Error).message }
  }
}

// ── Replication Jobs ──────────────────────────────────────────────────────────

export const fetchJobs = async (): Promise<ReplicationJob[]> => {
  const { data } = await api.get<ReplicationJob[]>('/jobs')
  return data
}

export const fetchJob = async (id: string): Promise<ReplicationJob> => {
  const { data } = await api.get<ReplicationJob>(`/jobs/${id}`)
  return data
}

export const createJob = async (payload: Omit<ReplicationJob, 'id' | 'status' | 'k8sDeploymentName' | 'k8sNamespace' | 'createdAt' | 'updatedAt' | 'metrics'>): Promise<ReplicationJob> => {
  const { data } = await api.post<ReplicationJob>('/jobs', payload)
  return data
}

export const pauseJob = async (id: string): Promise<void> => {
  await api.patch(`/jobs/${id}`, { action: 'pause' })
}

export const resumeJob = async (id: string): Promise<void> => {
  await api.patch(`/jobs/${id}`, { action: 'resume' })
}

export const deleteJob = async (id: string): Promise<void> => {
  await api.delete(`/jobs/${id}`)
}

export const fetchJobMetrics = async (id: string): Promise<ConsumerMetric[]> => {
  const { data } = await api.get<ConsumerMetric[]>(`/jobs/${id}/metrics`)
  return data
}

export const resetJobOffset = async (
  id: string,
  position: 'earliest' | 'latest' | 'custom',
  offsets?: Record<string, number>,
): Promise<void> => {
  await api.post(`/jobs/${id}/reset-offset`, { position, offsets })
}

export const updateJobDestination = async (
  id: string,
  destinationType: string,
  destinationConfig: import('@/types').DestinationConfig,
): Promise<void> => {
  await api.put(`/jobs/${id}/destination`, { destinationType, destinationConfig })
}

export const fetchJobErrors = async (id: string): Promise<JobError[]> => {
  const { data } = await api.get<JobError[]>(`/jobs/${id}/errors`)
  return data
}

export interface LiveMetric {
  partitionId: number
  currentOffset: number
  endOffset: number
  lag: number
}

export interface LiveConsumerMember {
  memberId: string
  clientId: string
  clientHost: string
}

export interface LiveMetricsResponse {
  groupId: string
  groupState: string
  partitions: LiveMetric[]
  members: LiveConsumerMember[]
}

export const fetchLiveMetrics = async (id: string): Promise<LiveMetricsResponse> => {
  const { data } = await api.get<LiveMetricsResponse>(`/jobs/${id}/live-metrics`)
  return data
}

export const scaleJobReplicas = async (id: string, replicas: number): Promise<{ replicas: number; applied: boolean }> => {
  const { data } = await api.put(`/jobs/${id}/replicas`, { replicas })
  return data
}

export const fetchJobLogs = async (id: string, lines = 200): Promise<string> => {
  const { data } = await api.get<{ logs: string }>(`/jobs/${id}/logs`, { params: { lines } })
  return data.logs
}
