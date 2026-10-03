#!/usr/bin/env python3
"""
main.py — Kafbat replication worker.

Environment variables (injected by the k8s Deployment):
  JOB_ID         UUID of the replication job in the management DB
  MGMT_DB_DSN    PostgreSQL DSN for the management database
  MGMT_API_URL   Base URL of the kafbat backend (for metrics reporting)
  KAFKA_BROKERS  Comma-separated broker list (e.g. kafka:9093)
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

import config as cfg_module
import errors as errors_module
import metrics as metrics_module
from destinations import get_destination

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("worker")

# ── signal handling ──────────────────────────────────────────────────────────
_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    logger.info("Received signal %s — shutting down…", sig)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── helpers ──────────────────────────────────────────────────────────────────

def parse_message_value(raw_bytes: bytes) -> dict:
    """Try to parse bytes as JSON; fall back to a plain string dict."""
    try:
        val = json.loads(raw_bytes.decode("utf-8"))
        if isinstance(val, dict):
            return val
        # scalar JSON (string / number) — wrap it
        return {"value": val}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {"value": raw_bytes.decode("utf-8", errors="replace")}


def kafka_timestamp_to_dt(ts_type, ts_ms) -> datetime | None:
    if ts_type == 0 or ts_ms < 0:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    job_id = os.environ["JOB_ID"]
    brokers = os.environ.get("KAFKA_BROKERS", "kafka:9093")

    logger.info("Starting worker for job %s", job_id)

    # Load job configuration from management DB
    job = cfg_module.get_job(job_id)
    logger.info(
        "Job: %s | topic: %s | destination: %s | start: %s",
        job["name"], job["topic_name"], job["destination_type"], job["start_position"],
    )

    # Build destination adapter
    dest = get_destination(
        job["destination_type"],
        job["destination_config"],
        job["fields"],
    )
    dest.ensure_table()

    # Build Kafka consumer config
    auto_offset = "earliest" if job["start_position"] == "earliest" or job["include_historical"] else "latest"
    consumer_conf = {
        "bootstrap.servers":        brokers,
        "group.id":                 job["consumer_group"],
        "auto.offset.reset":        auto_offset,
        "enable.auto.commit":       False,
        "session.timeout.ms":       30_000,
        "max.poll.interval.ms":     300_000,
        # Force IPv4 — k8s pods can't reach host.docker.internal over IPv6
        "broker.address.family":    "v4",
        # Retry topic subscription in case broker just started
        "topic.metadata.refresh.interval.ms": 5_000,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([job["topic_name"]])
    logger.info("Subscribed to topic %s (group: %s)", job["topic_name"], job["consumer_group"])

    # Per-partition counters for metrics reporting
    partition_offsets: dict[int, int]   = {}
    partition_counts:  dict[int, int]   = {}
    partition_last_ts: dict[int, datetime] = {}
    last_metrics_flush = time.monotonic()
    METRICS_INTERVAL   = 5  # seconds

    try:
        while not _shutdown:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # Flush metrics even when idle
                if time.monotonic() - last_metrics_flush >= METRICS_INTERVAL:
                    _flush_metrics(job_id, consumer, job["topic_name"],
                                   partition_offsets, partition_counts, partition_last_ts)
                    last_metrics_flush = time.monotonic()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("End of partition %d at offset %d", msg.partition(), msg.offset())
                else:
                    logger.error("Kafka error: %s", msg.error())
                    if msg.error().fatal():
                        raise KafkaException(msg.error())
                continue

            # Parse and insert
            record    = parse_message_value(msg.value() or b"{}")
            # Kafka key — decoded as string (may be JSON for composite keys)
            raw_key   = msg.key()
            record["__key__"]     = raw_key.decode("utf-8", errors="replace") if raw_key else None
            # Kafka headers — flat string map
            headers   = {k: v.decode("utf-8", errors="replace") if isinstance(v, bytes) else v
                         for k, v in (msg.headers() or [])}
            record["__headers__"] = headers
            pub_dt    = kafka_timestamp_to_dt(*msg.timestamp())
            partition = msg.partition()
            offset    = msg.offset()

            try:
                dest.insert(record, partition, offset, pub_dt)
                consumer.commit(message=msg, asynchronous=False)

                partition_offsets[partition] = offset + 1
                partition_counts[partition]  = partition_counts.get(partition, 0) + 1
                if pub_dt:
                    partition_last_ts[partition] = pub_dt

            except Exception as exc:
                # Build a detailed error string that includes type info useful for debugging.
                err_detail = (
                    f"{type(exc).__name__}: {exc}  "
                    f"[key={record.get('__key__')!r}  "
                    f"value_keys={list(k for k in record if not k.startswith('__'))}]"
                )
                logger.error(
                    "Insert failed — partition=%d offset=%d — %s",
                    partition, offset, err_detail,
                )
                # Report to the management API so the UI shows the exact failure context.
                errors_module.report(job_id, partition, offset, err_detail)
                # Commit the offset anyway so the worker does not get stuck on this record.
                # The user can inspect the error in the UI and reset to this offset to retry,
                # or reset to offset+1 to skip it.
                consumer.commit(message=msg, asynchronous=False)
                partition_offsets[partition] = offset + 1

            # Periodic metrics flush
            if time.monotonic() - last_metrics_flush >= METRICS_INTERVAL:
                _flush_metrics(job_id, consumer, job["topic_name"],
                               partition_offsets, partition_counts, partition_last_ts)
                last_metrics_flush = time.monotonic()

    finally:
        logger.info("Closing consumer and destination…")
        consumer.close()
        dest.close()


def _flush_metrics(job_id, consumer, topic, partition_offsets, partition_counts, partition_last_ts):
    """Report current lag to the management backend for each assigned partition."""
    try:
        assignment = consumer.assignment()
        if not assignment:
            return

        # Get end offsets for all assigned partitions
        end_offsets = consumer.get_watermark_offsets
        for tp in assignment:
            try:
                low, high = consumer.get_watermark_offsets(tp, timeout=2.0)
            except Exception:
                continue

            current = partition_offsets.get(tp.partition, tp.offset if tp.offset >= 0 else 0)
            count   = partition_counts.get(tp.partition, 0)
            last_ts = partition_last_ts.get(tp.partition)

            metrics_module.report(
                job_id=job_id,
                partition_id=tp.partition,
                current_offset=current,
                end_offset=high,
                replicated_count=count,
                last_message_at=last_ts,
            )
    except Exception as exc:
        logger.warning("Metrics flush failed: %s", exc)


if __name__ == "__main__":
    main()
