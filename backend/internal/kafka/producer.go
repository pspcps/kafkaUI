package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/prakash/kafbat-ui/internal/models"
)

// ProduceMessage sends a single message to a Kafka topic.
// auth may be nil for PLAINTEXT clusters.
func ProduceMessage(brokers []string, topic string, req *models.ProduceMessageRequest, auth *models.AuthConfig) error {
	cfg := buildSaramaConfig(auth)
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(req.Value),
	}

	if req.Key != "" {
		msg.Key = sarama.StringEncoder(req.Key)
	}

	if req.Partition != nil {
		cfg.Producer.Partitioner = sarama.NewManualPartitioner
		msg.Partition = *req.Partition
	}

	for k, v := range req.Headers {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}
