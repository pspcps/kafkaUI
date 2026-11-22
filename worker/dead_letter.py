"""Acknowledged Kafka dead-letter writes. DLQ and source commits are not atomic."""
import json
from confluent_kafka import Producer
from delivery import DeadLetterUnavailable


class KafkaDeadLetter:
    def __init__(self, brokers, topic, source_topic, timeout=10):
        if not 1 <= timeout <= 10:
            raise ValueError('Dead-letter acknowledgment timeout must be 1–10 seconds')
        if not topic or topic == source_topic:
            raise ValueError('A distinct, operator-provisioned DLQ topic is required')
        self.topic, self.timeout = topic, timeout
        self.producer = Producer({'bootstrap.servers': brokers, 'enable.idempotence': True,
                                  'acks': 'all', 'allow.auto.create.topics': False,
                                  'delivery.timeout.ms': int(timeout * 1000)})

    def __call__(self, envelope):
        acknowledgments = []
        def delivered(error, message):
            acknowledgments.append(error)
        self.producer.produce(self.topic, key=envelope['event_id'].encode(),
                              value=json.dumps(envelope, separators=(',', ':')).encode(), on_delivery=delivered)
        pending = self.producer.flush(self.timeout + 1)
        if pending or not acknowledgments or acknowledgments[0] is not None:
            self.producer.purge(in_queue=True, in_flight=True)
            self.producer.poll(0)
            raise DeadLetterUnavailable('Dead-letter acknowledgment unavailable; source offset unchanged')

    def close(self):
        self.producer.purge(in_queue=True, in_flight=True)
        self.producer.poll(0)
