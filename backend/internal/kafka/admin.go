package kafka

import (
	"fmt"
	"sort"
	"strings"

	"github.com/IBM/sarama"
	"github.com/prakash/kafbat-ui/internal/models"
)

// AdminClient wraps sarama ClusterAdmin for Kafka management operations
type AdminClient struct {
	brokers []string
	config  *sarama.Config
	admin   sarama.ClusterAdmin
	client  sarama.Client
}

// NewAdminClient creates a new AdminClient connected to the given brokers.
// auth may be nil for PLAINTEXT clusters.
func NewAdminClient(brokers []string, auth *models.AuthConfig) (*AdminClient, error) {
	cfg := buildSaramaConfig(auth)

	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client: %w", err)
	}

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		admin.Close()
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &AdminClient{
		brokers: brokers,
		config:  cfg,
		admin:   admin,
		client:  client,
	}, nil
}

// Close closes the admin client and underlying connections
func (a *AdminClient) Close() {
	if a.client != nil {
		a.client.Close()
	}
	if a.admin != nil {
		a.admin.Close()
	}
}

// GetClusterInfo returns high-level cluster overview
func (a *AdminClient) GetClusterInfo() (*models.ClusterInfo, error) {
	brokers := a.client.Brokers()
	controller, err := a.client.Controller()
	if err != nil {
		return nil, fmt.Errorf("failed to get controller: %w", err)
	}

	brokerInfos := make([]models.BrokerInfo, 0, len(brokers))
	for _, b := range brokers {
		if err := b.Open(a.config); err != nil && err != sarama.ErrAlreadyConnected {
			continue
		}
		brokerInfos = append(brokerInfos, models.BrokerInfo{
			ID:       b.ID(),
			Host:     b.Addr(),
			Port:     0,
			IsLeader: b.ID() == controller.ID(),
		})
	}

	topics, err := a.admin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}

	groups, err := a.admin.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	return &models.ClusterInfo{
		ClusterID:      strings.Join(a.brokers, ","),
		BrokerCount:    len(brokers),
		TopicCount:     len(topics),
		ConsumerGroups: len(groups),
		Controller:     controller.ID(),
		Brokers:        brokerInfos,
	}, nil
}

// ListTopics returns summaries of all non-internal topics (plus internal if requested)
func (a *AdminClient) ListTopics(includeInternal bool) ([]models.TopicSummary, error) {
	topicsMeta, err := a.admin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}

	names := make([]string, 0, len(topicsMeta))
	for name := range topicsMeta {
		names = append(names, name)
	}
	sort.Strings(names)

	// DescribeTopics gives us IsInternal + partition metadata
	metadata, err := a.admin.DescribeTopics(names)
	if err != nil {
		return nil, fmt.Errorf("failed to describe topics: %w", err)
	}
	metaMap := make(map[string]*sarama.TopicMetadata, len(metadata))
	for _, m := range metadata {
		metaMap[m.Name] = m
	}

	summaries := make([]models.TopicSummary, 0, len(names))
	for _, name := range names {
		meta, ok := metaMap[name]
		if !ok {
			continue
		}
		if !includeInternal && meta.IsInternal {
			continue
		}
		replication := 0
		if len(meta.Partitions) > 0 {
			replication = len(meta.Partitions[0].Replicas)
		}
		summaries = append(summaries, models.TopicSummary{
			Name:              name,
			Partitions:        len(meta.Partitions),
			ReplicationFactor: replication,
			Internal:          meta.IsInternal,
		})
	}

	return summaries, nil
}

// GetTopicDetail returns full partition metadata and config for a topic
func (a *AdminClient) GetTopicDetail(topicName string) (*models.TopicDetail, error) {
	// DescribeTopics returns partition metadata including leader, replicas, ISR, IsInternal
	metadata, err := a.admin.DescribeTopics([]string{topicName})
	if err != nil || len(metadata) == 0 {
		return nil, fmt.Errorf("failed to describe topic %q: %w", topicName, err)
	}
	meta := metadata[0]
	if meta.Err != sarama.ErrNoError {
		return nil, fmt.Errorf("topic %q not found: %w", topicName, meta.Err)
	}

	// Fetch offsets per partition
	partitions := make([]models.PartitionInfo, 0, len(meta.Partitions))
	for _, partMeta := range meta.Partitions {
		partID := partMeta.ID
		oldest, err := a.client.GetOffset(topicName, partID, sarama.OffsetOldest)
		if err != nil {
			oldest = 0
		}
		newest, err := a.client.GetOffset(topicName, partID, sarama.OffsetNewest)
		if err != nil {
			newest = 0
		}
		msgCount := newest - oldest
		if msgCount < 0 {
			msgCount = 0
		}

		replicas := make([]int32, len(partMeta.Replicas))
		copy(replicas, partMeta.Replicas)
		isr := make([]int32, len(partMeta.Isr))
		copy(isr, partMeta.Isr)

		partitions = append(partitions, models.PartitionInfo{
			PartitionID:  partID,
			Leader:       partMeta.Leader,
			Replicas:     replicas,
			ISR:          isr,
			OffsetBegin:  oldest,
			OffsetEnd:    newest,
			MessageCount: msgCount,
		})
	}

	// Sort partitions by ID
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].PartitionID < partitions[j].PartitionID
	})

	// Fetch topic config
	configEntries, err := a.admin.DescribeConfig(sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: topicName,
	})
	configMap := make(map[string]string)
	if err == nil {
		for _, entry := range configEntries {
			if !entry.Default {
				configMap[entry.Name] = entry.Value
			}
		}
	}

	replication := 0
	if len(meta.Partitions) > 0 {
		replication = len(meta.Partitions[0].Replicas)
	}

	return &models.TopicDetail{
		Name:              topicName,
		Partitions:        partitions,
		ReplicationFactor: replication,
		Config:            configMap,
		Internal:          meta.IsInternal,
	}, nil
}

// CreateTopic creates a new Kafka topic
func (a *AdminClient) CreateTopic(req *models.CreateTopicRequest) error {
	detail := &sarama.TopicDetail{
		NumPartitions:     req.Partitions,
		ReplicationFactor: req.ReplicationFactor,
	}

	if len(req.Configs) > 0 {
		detail.ConfigEntries = make(map[string]*string)
		for k, v := range req.Configs {
			val := v
			detail.ConfigEntries[k] = &val
		}
	}

	if err := a.admin.CreateTopic(req.Name, detail, false); err != nil {
		return fmt.Errorf("failed to create topic %q: %w", req.Name, err)
	}
	return nil
}

// DeleteTopic deletes a Kafka topic
func (a *AdminClient) DeleteTopic(topicName string) error {
	if err := a.admin.DeleteTopic(topicName); err != nil {
		return fmt.Errorf("failed to delete topic %q: %w", topicName, err)
	}
	return nil
}

// ListConsumerGroups returns summaries of all consumer groups
func (a *AdminClient) ListConsumerGroups() ([]models.ConsumerGroupSummary, error) {
	groups, err := a.admin.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	groupIDs := make([]string, 0, len(groups))
	for id := range groups {
		groupIDs = append(groupIDs, id)
	}
	sort.Strings(groupIDs)

	// Describe groups to get state and member count
	descriptions, err := a.admin.DescribeConsumerGroups(groupIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to describe consumer groups: %w", err)
	}

	summaries := make([]models.ConsumerGroupSummary, 0, len(descriptions))
	for _, desc := range descriptions {
		summaries = append(summaries, models.ConsumerGroupSummary{
			GroupID:     desc.GroupId,
			State:       desc.State,
			MemberCount: len(desc.Members),
		})
	}

	return summaries, nil
}

// ResetConsumerGroupOffsets resets committed offsets for a consumer group on a topic.
// position must be "earliest" or "latest".
// The consumer group must have no active members (scale to 0 first).
func (a *AdminClient) ResetConsumerGroupOffsets(group, topic, position string) error {
	partitions, err := a.client.Partitions(topic)
	if err != nil {
		return fmt.Errorf("get partitions for %q: %w", topic, err)
	}

	// Build partition→target offset map.
	targetOffsets := make(map[int32]int64, len(partitions))
	for _, p := range partitions {
		var saramaOffset int64
		if position == "earliest" {
			saramaOffset = sarama.OffsetOldest
		} else {
			saramaOffset = sarama.OffsetNewest
		}
		off, err := a.client.GetOffset(topic, p, saramaOffset)
		if err != nil {
			return fmt.Errorf("get %s offset for partition %d: %w", position, p, err)
		}
		targetOffsets[p] = off
	}

	// Commit the target offsets via an OffsetManager.
	om, err := sarama.NewOffsetManagerFromClient(group, a.client)
	if err != nil {
		return fmt.Errorf("create offset manager for group %q: %w", group, err)
	}
	defer om.Close()

	for p, off := range targetOffsets {
		pom, err := om.ManagePartition(topic, p)
		if err != nil {
			return fmt.Errorf("manage partition %d: %w", p, err)
		}
		pom.MarkOffset(off, "admin-reset")
		pom.Close()
	}

	om.Commit()
	return nil
}

// ResetConsumerGroupOffsetsCustom commits exact offsets per partition.
// Only the partitions present in the map are touched; others are left unchanged.
func (a *AdminClient) ResetConsumerGroupOffsetsCustom(group, topic string, partitionOffsets map[int32]int64) error {
	om, err := sarama.NewOffsetManagerFromClient(group, a.client)
	if err != nil {
		return fmt.Errorf("create offset manager for group %q: %w", group, err)
	}
	defer om.Close()

	for p, off := range partitionOffsets {
		pom, err := om.ManagePartition(topic, p)
		if err != nil {
			return fmt.Errorf("manage partition %d: %w", p, err)
		}
		pom.MarkOffset(off, "admin-custom-reset")
		pom.Close()
	}

	om.Commit()
	return nil
}

// LiveMetric holds live per-partition lag data queried directly from Kafka.
type LiveMetric struct {
	PartitionID   int32 `json:"partitionId"`
	CurrentOffset int64 `json:"currentOffset"`
	EndOffset     int64 `json:"endOffset"`
	Lag           int64 `json:"lag"`
}

// LiveConsumerMember holds metadata about one active consumer group member.
type LiveConsumerMember struct {
	MemberID   string `json:"memberId"`
	ClientID   string `json:"clientId"`
	ClientHost string `json:"clientHost"`
}

// LiveMetricsResponse is the full response for the live-metrics endpoint.
type LiveMetricsResponse struct {
	GroupID    string               `json:"groupId"`
	GroupState string               `json:"groupState"`
	Partitions []LiveMetric         `json:"partitions"`
	Members    []LiveConsumerMember `json:"members"`
}

// GetLiveMetrics queries Kafka directly for committed offsets and end offsets
// for the given consumer group / topic combination. Unlike DB-cached metrics
// this always reflects the true real-time position of the consumer group.
func (a *AdminClient) GetLiveMetrics(group, topic string) (*LiveMetricsResponse, error) {
	// Fetch the partition list first — sarama requires an explicit non-nil slice.
	parts, err := a.client.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("list partitions for %q: %w", topic, err)
	}

	offsets, err := a.admin.ListConsumerGroupOffsets(group, map[string][]int32{topic: parts})
	if err != nil {
		return nil, fmt.Errorf("list offsets for group %q: %w", group, err)
	}

	partBlocks := offsets.Blocks[topic]

	partitions := make([]LiveMetric, 0, len(parts))
	for _, p := range parts {
		end, _ := a.client.GetOffset(topic, p, sarama.OffsetNewest)

		var current int64
		if partBlocks != nil {
			if block, ok := partBlocks[p]; ok && block.Err == sarama.ErrNoError && block.Offset >= 0 {
				current = block.Offset
			}
		}

		lag := end - current
		if lag < 0 {
			lag = 0
		}
		partitions = append(partitions, LiveMetric{
			PartitionID: p, CurrentOffset: current, EndOffset: end, Lag: lag,
		})
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].PartitionID < partitions[j].PartitionID })

	// Fetch active consumer group members.
	groupState := "Unknown"
	members := []LiveConsumerMember{}
	if descs, err := a.admin.DescribeConsumerGroups([]string{group}); err == nil && len(descs) > 0 {
		desc := descs[0]
		groupState = desc.State
		for _, m := range desc.Members {
			members = append(members, LiveConsumerMember{
				MemberID:   m.MemberId,
				ClientID:   m.ClientId,
				ClientHost: m.ClientHost,
			})
		}
	}

	return &LiveMetricsResponse{
		GroupID:    group,
		GroupState: groupState,
		Partitions: partitions,
		Members:    members,
	}, nil
}

// GetConsumerGroupDetail returns full detail including per-partition lag
func (a *AdminClient) GetConsumerGroupDetail(groupID string) (*models.ConsumerGroupDetail, error) {
	descriptions, err := a.admin.DescribeConsumerGroups([]string{groupID})
	if err != nil || len(descriptions) == 0 {
		return nil, fmt.Errorf("failed to describe group %q: %w", groupID, err)
	}
	desc := descriptions[0]

	members := make([]models.ConsumerMember, 0, len(desc.Members))
	for _, m := range desc.Members {
		members = append(members, models.ConsumerMember{
			MemberID:   m.MemberId,
			ClientID:   m.ClientId,
			ClientHost: m.ClientHost,
		})
	}

	// Get committed offsets
	offsets, err := a.admin.ListConsumerGroupOffsets(groupID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get offsets for group %q: %w", groupID, err)
	}

	topicLagMap := make(map[string]*models.TopicLagInfo)
	for topic, partOffsets := range offsets.Blocks {
		lagInfo := &models.TopicLagInfo{
			Topic:      topic,
			Partitions: []models.PartitionLag{},
		}

		for partID, block := range partOffsets {
			if block.Err != sarama.ErrNoError {
				continue
			}
			endOffset, err := a.client.GetOffset(topic, partID, sarama.OffsetNewest)
			if err != nil {
				endOffset = 0
			}
			lag := endOffset - block.Offset
			if lag < 0 {
				lag = 0
			}
			lagInfo.TotalLag += lag
			lagInfo.Partitions = append(lagInfo.Partitions, models.PartitionLag{
				PartitionID:    partID,
				ConsumerOffset: block.Offset,
				EndOffset:      endOffset,
				Lag:            lag,
			})
		}

		sort.Slice(lagInfo.Partitions, func(i, j int) bool {
			return lagInfo.Partitions[i].PartitionID < lagInfo.Partitions[j].PartitionID
		})

		topicLagMap[topic] = lagInfo
	}

	topicLags := make([]models.TopicLagInfo, 0, len(topicLagMap))
	for _, tl := range topicLagMap {
		topicLags = append(topicLags, *tl)
	}
	sort.Slice(topicLags, func(i, j int) bool {
		return topicLags[i].Topic < topicLags[j].Topic
	})

	return &models.ConsumerGroupDetail{
		GroupID: desc.GroupId,
		State:   desc.State,
		Members: members,
		TopicLag: topicLags,
	}, nil
}
