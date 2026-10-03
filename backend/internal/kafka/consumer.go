package kafka

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/prakash/kafbat-ui/internal/models"
)

// MessageConsumer reads messages from a topic
type MessageConsumer struct {
	brokers []string
	config  *sarama.Config
}

// NewMessageConsumer creates a consumer for reading messages.
// auth may be nil for PLAINTEXT clusters.
func NewMessageConsumer(brokers []string, auth *models.AuthConfig) *MessageConsumer {
	cfg := buildSaramaConfig(auth)
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return &MessageConsumer{brokers: brokers, config: cfg}
}

// FetchOptions controls what messages are returned.
type FetchOptions struct {
	Partition   int32
	Offset      int64
	Limit       int
	FilterKey   string
	FilterValue string
	FromTime    *time.Time
	ToTime      *time.Time
}

func (mc *MessageConsumer) FetchMessages(topic string, opts FetchOptions) (*models.MessagesResponse, error) {
	// Single client for both offset lookups and consuming.
	client, err := sarama.NewClient(mc.brokers, mc.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	// Determine partitions to read.
	var partitions []int32
	if opts.Partition >= 0 {
		partitions = []int32{opts.Partition}
	} else {
		partitions, err = consumer.Partitions(topic)
		if err != nil {
			return nil, fmt.Errorf("failed to get partitions: %w", err)
		}
	}

	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}

	messages := make([]models.KafkaMessage, 0, opts.Limit)

	for _, partID := range partitions {
		if len(messages) >= opts.Limit {
			break
		}

		newestOffset, err := client.GetOffset(topic, partID, sarama.OffsetNewest)
		if err != nil || newestOffset == 0 {
			// Partition is empty or unreachable.
			continue
		}

		// Determine the start offset for this partition.
		startOffset := opts.Offset
		if startOffset < 0 {
			startOffset = sarama.OffsetOldest
		}

		if opts.FromTime != nil {
			if tsOffset, err := client.GetOffset(topic, partID, opts.FromTime.UnixMilli()); err == nil {
				startOffset = tsOffset
			}
		}

		// lastAvailable is the offset of the last written message.
		lastAvailable := newestOffset - 1

		pc, err := consumer.ConsumePartition(topic, partID, startOffset)
		if err != nil {
			continue
		}

		// 2-second safety timeout — should never fire because we stop at lastAvailable.
		timeout := time.After(2 * time.Second)
	loop:
		for len(messages) < opts.Limit {
			select {
			case msg, ok := <-pc.Messages():
				if !ok {
					break loop
				}

				// Apply time filter.
				if opts.ToTime != nil && msg.Timestamp.After(*opts.ToTime) {
					break loop
				}

				// Apply key/value filter.
				keyStr := string(msg.Key)
				valStr := string(msg.Value)
				if opts.FilterKey != "" && keyStr != opts.FilterKey {
					if msg.Offset >= lastAvailable {
						break loop
					}
					continue
				}
				if opts.FilterValue != "" {
					found := false
					for i := 0; i <= len(valStr)-len(opts.FilterValue); i++ {
						if valStr[i:i+len(opts.FilterValue)] == opts.FilterValue {
							found = true
							break
						}
					}
					if !found {
						if msg.Offset >= lastAvailable {
							break loop
						}
						continue
					}
				}

				headers := make(map[string]string)
				for _, h := range msg.Headers {
					headers[string(h.Key)] = string(h.Value)
				}

				messages = append(messages, models.KafkaMessage{
					Partition: msg.Partition,
					Offset:    msg.Offset,
					Timestamp: msg.Timestamp,
					Key:       keyStr,
					Value:     valStr,
					Headers:   headers,
				})

				// Stop once we've consumed the last available message.
				if msg.Offset >= lastAvailable {
					break loop
				}

			case <-timeout:
				break loop
			}
		}

		pc.Close()
	}

	return &models.MessagesResponse{
		Messages:   messages,
		TotalCount: len(messages),
	}, nil
}
