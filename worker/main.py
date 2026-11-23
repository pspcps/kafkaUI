#!/usr/bin/env python3
"""Serial, fail-closed Kafka worker. See docs/DELIVERY_CONTRACT.md."""
import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from delivery import DeliveryBlocked, LeaseLost, Processor, RetryPolicy
from destinations import get_destination

logger = logging.getLogger('worker')
_shutdown = False


def handle_signal(sig, frame):
    global _shutdown
    _shutdown = True


def run_worker(job, *, brokers=None, emit=None, report_error=None, report_metric=None,
               stopped=None, after_write=None, before_process=None, assignment=None,
               stop_offsets=None, policy=None, dlq_topic=None):
    """The lab/replay tools call the same loop; hooks are not environment failpoints."""
    stopped = stopped or (lambda: _shutdown)
    brokers = brokers or os.environ.get('KAFKA_BROKERS', 'kafka:9093')
    emit = emit or (lambda event: logger.info(json.dumps(event, sort_keys=True)))
    retries = RetryPolicy(int(os.environ.get('DELIVERY_MAX_ATTEMPTS', '3')),
                          float(os.environ.get('DELIVERY_RETRY_BASE_SECONDS', '.5')),
                          float(os.environ.get('DELIVERY_RETRY_CAP_SECONDS', '5')))
    policy = policy or os.environ.get('DELIVERY_FAILURE_POLICY', 'pause')
    if policy not in ('pause', 'dead_letter'):
        raise ValueError('DELIVERY_FAILURE_POLICY must be pause or dead_letter')
    destination = get_destination(job['destination_type'], job['destination_config'], job['fields'])
    # A startup failure happens before subscription; no offsets can advance.
    try:
        destination.ensure_table()
    except Exception:
        destination.close()
        emit({'event': 'startup_blocked', 'stage': 'destination_schema', 'offsets_advanced': False})
        raise
    dlq = None
    if policy == 'dead_letter':
        from dead_letter import KafkaDeadLetter
        dlq = KafkaDeadLetter(brokers, dlq_topic or os.environ.get('DELIVERY_DLQ_TOPIC'), job['topic_name'], timeout=float(os.environ.get('DELIVERY_DLQ_TIMEOUT_SECONDS','10')))
    consumer = Consumer({
        'bootstrap.servers': brokers, 'group.id': job['consumer_group'],
        'auto.offset.reset': 'error',
        'enable.auto.commit': False, 'enable.auto.offset.store': False,
        'allow.auto.create.topics': False, 'session.timeout.ms': 10000,
        'max.poll.interval.ms': 300000, 'partition.assignment.strategy': 'range',
    })
    owned, epoch = set(), [0]
    counts = {}
    completed = set()
    last_metrics = time.monotonic()

    def assigned(client, partitions):
        epoch[0] += 1
        owned.clear()
        owned.update((tp.topic, tp.partition) for tp in partitions)
        if assignment is None:
            committed = client.committed(partitions, timeout=5)
            by_partition = {(tp.topic, tp.partition): tp for tp in committed}
            for tp in partitions:
                previous = by_partition.get((tp.topic, tp.partition))
                if previous is None:
                    raise RuntimeError('Committed offset response is incomplete')
                if previous.error is not None:
                    raise KafkaException(previous.error)
                low, high = client.get_watermark_offsets(tp, timeout=3)
                if previous.offset >= 0:
                    if not low <= previous.offset <= high:
                        raise RuntimeError('Committed offsets are outside retention; explicit recovery is required')
                    tp.offset = previous.offset
                else:
                    tp.offset = low if job.get('include_historical') or job.get('start_position') == 'earliest' else high
        client.assign(partitions)
        emit({'event': 'assigned', 'partitions': [tp.partition for tp in partitions], 'epoch': epoch[0]})

    def revoked(client, partitions):
        epoch[0] += 1
        for tp in partitions:
            owned.discard((tp.topic, tp.partition))
        # Never commit an unacknowledged/prefetched position during a rebalance.
        emit({'event': 'revoked', 'partitions': [tp.partition for tp in partitions], 'epoch': epoch[0]})

    def commit(identity):
        fenced = (KafkaError.REBALANCE_IN_PROGRESS, KafkaError.ILLEGAL_GENERATION,
                  KafkaError.UNKNOWN_MEMBER_ID, KafkaError.FENCED_INSTANCE_ID)
        try:
            result = consumer.commit(offsets=[TopicPartition(identity.topic, identity.partition, identity.offset + 1)], asynchronous=False)
        except KafkaException as error:
            if error.args and error.args[0].code() in fenced:
                raise LeaseLost('Consumer generation changed before commit') from error
            raise
        for tp in result or []:
            if tp.error is not None:
                if tp.error.code() in fenced:
                    raise LeaseLost('Consumer generation changed before commit')
                raise KafkaException(tp.error)
        if not result:
            raise RuntimeError('Source offset acknowledgment unavailable')

    def metrics():
        if report_metric is None:
            return
        partitions = consumer.assignment()
        if not partitions:
            return
        try:
            committed = consumer.committed(partitions, timeout=2)
            for tp in committed:
                if tp.offset < 0:
                    continue  # unknown is not offset zero
                _, end = consumer.get_watermark_offsets(tp, timeout=2)
                report_metric(job_id=job['id'], partition_id=tp.partition, current_offset=tp.offset,
                              end_offset=end, replicated_count=counts.get(tp.partition, 0))
        except Exception as error:
            emit({'event': 'metrics_unavailable', 'error_type': type(error).__name__})

    def interrupted_sleep(delay):
        deadline = time.monotonic() + delay
        while not stopped() and time.monotonic() < deadline:
            time.sleep(min(.1, max(0, deadline - time.monotonic())))

    processor = Processor(destination, commit, policy=policy, retries=retries, dead_letter=dlq,
                          emit=emit, sleep=interrupted_sleep, stopped=stopped, after_write=after_write)
    try:
        if assignment is not None:
            assigned(consumer, assignment)
        else:
            consumer.subscribe([job['topic_name']], on_assign=assigned, on_revoke=revoked, on_lost=revoked)
        while not stopped():
            message = consumer.poll(1)
            if message is None:
                if time.monotonic() - last_metrics >= 5:
                    metrics(); last_metrics = time.monotonic()
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    emit({'event': 'kafka_error', 'code': message.error().code()})
                    if message.error().fatal() or message.error().code() in (KafkaError._AUTO_OFFSET_RESET, KafkaError.OFFSET_OUT_OF_RANGE):
                        raise KafkaException(message.error())
                continue
            partition_key = (message.topic(), message.partition())
            lease = epoch[0]
            owns = lambda: epoch[0] == lease and partition_key in owned
            if not owns():
                continue
            if stop_offsets is not None and message.offset() >= stop_offsets.get(message.partition(), 0):
                consumer.pause([TopicPartition(message.topic(), message.partition())])
                completed.add(message.partition())
                if completed == set(stop_offsets):
                    return True
                continue
            if before_process:
                before_process(message)
            try:
                outcome = processor.process(message, job['cluster_id'], owns=owns)
            except LeaseLost:
                emit({'event': 'lease_lost', 'topic': message.topic(), 'partition': message.partition(), 'offset': message.offset()})
                # Drop this old lease, not the record: rejoin from broker-confirmed
                # offsets. The new owner checks the ledger before any business write.
                owned.clear()
                if assignment is not None:
                    raise RuntimeError('Replay lost its assignment; rerun the selected range')
                consumer.unsubscribe()
                consumer.subscribe([job['topic_name']], on_assign=assigned, on_revoke=revoked, on_lost=revoked)
                continue
            except DeliveryBlocked as blocked:
                event = dict(event='record_paused', **blocked.identity.fields(), stage=blocked.stage,
                             attempts=blocked.attempts, error_type=blocked.error_type,
                             sqlstate=blocked.sqlstate, offsets_advanced=False, resume='restart_worker')
                emit(event)
                if report_error:
                    report_error(job['id'], blocked.identity.partition, blocked.identity.offset, json.dumps(event))
                # Deliberately pause the whole worker, not just a poison partition. Do not
                # process or commit records returned from already buffered fetches.
                while not stopped():
                    current = consumer.assignment()
                    if current:
                        consumer.pause(current)
                    consumer.poll(.5)  # serve rebalance callbacks/heartbeats; no writes/commits
                    if time.monotonic() - last_metrics >= 5:
                        metrics(); last_metrics = time.monotonic()
                return
            if outcome == 'applied':
                counts[message.partition()] = counts.get(message.partition(), 0) + 1
            if stop_offsets is not None and message.offset() + 1 >= stop_offsets[message.partition()]:
                completed.add(message.partition())
                consumer.pause([TopicPartition(message.topic(), message.partition())])
                if completed == set(stop_offsets):
                    return True
            if time.monotonic() - last_metrics >= 5:
                metrics(); last_metrics = time.monotonic()
    finally:
        # Both automatic commit and automatic offset storage are disabled.
        try:
            consumer.close()
        finally:
            try:
                destination.close()
            finally:
                if dlq:
                    dlq.close()
                emit({'event': 'worker_closed', 'group': job['consumer_group']})


def main():
    import config
    import errors
    import metrics
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, handle_signal)
    run_worker(config.get_job(os.environ['JOB_ID']), report_error=errors.report, report_metric=metrics.report)


if __name__ == '__main__':
    main()
