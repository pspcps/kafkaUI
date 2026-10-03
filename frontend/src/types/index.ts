export interface ClusterConfig {
  id: string
  name: string
  brokers: string[]
}

export interface BrokerInfo {
  id: number
  host: string
  port: number
  isLeader: boolean
  rack?: string
}

export interface ClusterInfo {
  clusterId: string
  brokerCount: number
  topicCount: number
  consumerGroups: number
  controller: number
  brokers: BrokerInfo[]
}

export interface TopicSummary {
  name: string
  partitions: number
  replicationFactor: number
  messageCount: number
  internal: boolean
}

export interface PartitionInfo {
  partitionId: number
  leader: number
  replicas: number[]
  isr: number[]
  offsetBegin: number
  offsetEnd: number
  messageCount: number
}

export interface TopicDetail {
  name: string
  partitions: PartitionInfo[]
  replicationFactor: number
  config: Record<string, string>
  internal: boolean
}

export interface CreateTopicRequest {
  name: string
  partitions: number
  replicationFactor: number
  configs?: Record<string, string>
}

export interface PartitionLag {
  partitionId: number
  consumerOffset: number
  endOffset: number
  lag: number
}

export interface TopicLagInfo {
  topic: string
  totalLag: number
  partitions: PartitionLag[]
}

export interface ConsumerMember {
  memberId: string
  clientId: string
  clientHost: string
}

export interface ConsumerGroupSummary {
  groupId: string
  state: string
  memberCount: number
  totalLag: number
}

export interface ConsumerGroupDetail {
  groupId: string
  state: string
  members: ConsumerMember[]
  topicLag: TopicLagInfo[]
}

export interface KafkaMessage {
  partition: number
  offset: number
  timestamp: string
  key: string
  value: string
  headers: Record<string, string>
}

export interface MessagesResponse {
  messages: KafkaMessage[]
  totalCount: number
}

export interface ProduceMessageRequest {
  partition?: number
  key?: string
  value: string
  headers?: Record<string, string>
}

// ── Schema ────────────────────────────────────────────────────────────────────

export type FieldType =
  | 'string' | 'int' | 'long' | 'float' | 'double'
  | 'boolean' | 'timestamp' | 'bytes' | 'json'

export type FieldSource = 'value' | 'header' | 'key'

export interface SchemaField {
  id?: string
  schemaId?: string
  name: string
  fieldType: FieldType
  fieldSource: FieldSource
  isRequired: boolean
  isPrimaryKey: boolean
  defaultValue?: string
  description?: string
  position?: number
}

export type SchemaFormat = 'manual' | 'none' | 'json_schema' | 'avro' | 'protobuf'

export interface TopicSchema {
  id?: string
  clusterId: string
  topicName: string
  schemaType: 'manual' | 'avro'
  schemaFormat: SchemaFormat
  schemaText?: string
  schemaVersion?: number
  avroRaw?: string
  fields: SchemaField[]
  createdAt?: string
  updatedAt?: string
}

export interface SchemaInput {
  format: SchemaFormat
  text: string
}

// ── Replication Jobs ──────────────────────────────────────────────────────────

export type DestinationType = 'postgresql' | 'mysql' | 'sqlserver' | 'mongodb'

export interface DestinationConfig {
  host: string
  port: number
  database: string
  username: string
  password: string
  tableName?: string   // SQL targets
  collection?: string  // MongoDB
  schema?: string      // SQL Server schema
}

export interface JobError {
  id: string
  jobId: string
  partitionId: number
  offset: number
  errorMsg: string
  occurredAt: string
}

export type ConsumerCountMode = 'fixed' | 'per_partition' | 'custom'
export type StartPosition = 'latest' | 'earliest'
export type JobStatus = 'pending' | 'running' | 'paused' | 'failed' | 'stopped'

export interface ConsumerMetric {
  jobId: string
  partitionId: number
  currentOffset: number
  endOffset: number
  lag: number
  replicatedCount: number
  lastMessageAt?: string
  updatedAt: string
}

export interface ReplicationJob {
  id: string
  name: string
  clusterId: string
  topicName: string
  consumerGroup: string
  serviceName: string
  destinationType: DestinationType
  destinationConfig: DestinationConfig
  startPosition: StartPosition
  includeHistorical: boolean
  consumerCountMode: ConsumerCountMode
  consumerCount: number
  teamName?: string
  alertChannel?: string
  status: JobStatus
  lastError?: string
  k8sDeploymentName?: string
  k8sNamespace: string
  createdAt: string
  updatedAt: string
  metrics?: ConsumerMetric[]
}
