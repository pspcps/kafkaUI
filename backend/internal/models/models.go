package models

import (
	"encoding/json"
	"time"
)

// AuthConfig holds authentication settings for a cluster.
//
// Supported Type values:
//   ""           / "none"        — plaintext, no SASL (TLS still possible via TLS=true)
//   "iam"                        — AWS MSK IAM (SASL/OAUTHBEARER over TLS)
//   "sasl_scram" / "scram_sha512"— SASL/SCRAM-SHA-512 over TLS (Confluent Cloud, self-managed)
//   "sasl_plain"                 — SASL/PLAIN over TLS (Azure Event Hubs)
type AuthConfig struct {
	Type string `json:"type"`
	TLS  bool   `json:"tls,omitempty"` // force TLS even when SASL is disabled

	// SASL/SCRAM and SASL/PLAIN credentials.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`

	// AWS MSK IAM.
	Region          string `json:"region,omitempty"`
	AccessKeyID     string `json:"accessKeyId,omitempty"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
	SessionToken    string `json:"sessionToken,omitempty"`
}

// ClusterConfig is a configured Kafka cluster (environment).
type ClusterConfig struct {
	ID      string      `json:"id"`
	Name    string      `json:"name"`
	Brokers []string    `json:"brokers"`
	Auth    *AuthConfig `json:"auth,omitempty"`
}

// ClusterInfo represents Kafka cluster overview
type ClusterInfo struct {
	ClusterID      string       `json:"clusterId"`
	BrokerCount    int          `json:"brokerCount"`
	TopicCount     int          `json:"topicCount"`
	ConsumerGroups int          `json:"consumerGroups"`
	Controller     int32        `json:"controller"`
	Brokers        []BrokerInfo `json:"brokers"`
}

// BrokerInfo represents a single Kafka broker
type BrokerInfo struct {
	ID       int32  `json:"id"`
	Host     string `json:"host"`
	Port     int32  `json:"port"`
	IsLeader bool   `json:"isLeader"`
	Rack     string `json:"rack,omitempty"`
}

// TopicSummary is used in the topics list
type TopicSummary struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replicationFactor"`
	MessageCount      int64  `json:"messageCount"`
	Internal          bool   `json:"internal"`
}

// TopicDetail includes full partition info and config
type TopicDetail struct {
	Name              string            `json:"name"`
	Partitions        []PartitionInfo   `json:"partitions"`
	ReplicationFactor int               `json:"replicationFactor"`
	Config            map[string]string `json:"config"`
	Internal          bool              `json:"internal"`
}

// PartitionInfo holds per-partition metadata
type PartitionInfo struct {
	PartitionID int32   `json:"partitionId"`
	Leader      int32   `json:"leader"`
	Replicas    []int32 `json:"replicas"`
	ISR         []int32 `json:"isr"`
	OffsetBegin int64   `json:"offsetBegin"`
	OffsetEnd   int64   `json:"offsetEnd"`
	MessageCount int64  `json:"messageCount"`
}

// CreateTopicRequest is the payload to create a topic
type CreateTopicRequest struct {
	Name              string            `json:"name" binding:"required"`
	Partitions        int32             `json:"partitions" binding:"required,min=1"`
	ReplicationFactor int16             `json:"replicationFactor" binding:"required,min=1"`
	Configs           map[string]string `json:"configs"`
}

// ConsumerGroupSummary is used in the consumer groups list
type ConsumerGroupSummary struct {
	GroupID    string `json:"groupId"`
	State      string `json:"state"`
	MemberCount int   `json:"memberCount"`
	TotalLag   int64  `json:"totalLag"`
}

// ConsumerGroupDetail has per-topic-partition lag info
type ConsumerGroupDetail struct {
	GroupID  string              `json:"groupId"`
	State    string              `json:"state"`
	Members  []ConsumerMember    `json:"members"`
	TopicLag []TopicLagInfo      `json:"topicLag"`
}

// ConsumerMember represents a group member
type ConsumerMember struct {
	MemberID   string `json:"memberId"`
	ClientID   string `json:"clientId"`
	ClientHost string `json:"clientHost"`
}

// TopicLagInfo holds lag for a topic within a consumer group
type TopicLagInfo struct {
	Topic      string          `json:"topic"`
	TotalLag   int64           `json:"totalLag"`
	Partitions []PartitionLag  `json:"partitions"`
}

// PartitionLag holds lag for a single partition
type PartitionLag struct {
	PartitionID     int32 `json:"partitionId"`
	ConsumerOffset  int64 `json:"consumerOffset"`
	EndOffset       int64 `json:"endOffset"`
	Lag             int64 `json:"lag"`
}

// KafkaMessage represents a single Kafka message
type KafkaMessage struct {
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Timestamp time.Time         `json:"timestamp"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers"`
}

// ProduceMessageRequest is the payload to produce a message.
// Value accepts any JSON value (string, object, array, number, bool).
// It is forwarded as raw bytes to Kafka.
type ProduceMessageRequest struct {
	Partition *int32            `json:"partition"`
	Key       string            `json:"key"`
	Value     json.RawMessage   `json:"value" binding:"required"`
	Headers   map[string]string `json:"headers"`
}

// MessagesResponse wraps a page of messages
type MessagesResponse struct {
	Messages   []KafkaMessage `json:"messages"`
	TotalCount int            `json:"totalCount"`
}

// ErrorResponse is the standard error envelope
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}
