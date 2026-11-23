"""Record-level delivery contract. No implicit offset storage or commit-on-error.

Default: bounded transient retries, then pause until operator restart. Optional
DLQ applies only to invalid records, never destination outages or commit errors.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
import base64
import hashlib
import json
import time


@dataclass(frozen=True)
class Identity:
    cluster: str
    topic: str
    partition: int
    offset: int

    @property
    def key(self):
        raw = json.dumps([self.cluster, self.topic, self.partition, self.offset], separators=(',', ':'))
        return hashlib.sha256(raw.encode()).hexdigest()

    def fields(self):
        return dict(cluster=self.cluster, topic=self.topic, partition=self.partition, offset=self.offset)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_seconds: float = 0.5
    max_seconds: float = 5.0

    def __post_init__(self):
        if not 1 <= self.max_attempts <= 5 or not 0 <= self.base_seconds <= 5 or not 0 <= self.max_seconds <= 10:
            raise ValueError('Retries require 1–5 attempts, base delay 0–5s and maximum delay 0–10s')

    def delay(self, attempt):
        return min(self.max_seconds, self.base_seconds * 2 ** (attempt - 1))


class DeliveryBlocked(Exception):
    def __init__(self, identity, stage, attempts, error):
        self.identity, self.stage, self.attempts = identity, stage, attempts
        self.error = error
        self.error_type = type(error).__name__
        self.sqlstate = getattr(error, 'pgcode', None) or getattr(error, 'sqlstate', None)
        # No raw driver strings, connection strings or record values in logs/callbacks.
        super().__init__(f'{stage} blocked at {identity.topic}/{identity.partition}/{identity.offset}: {self.error_type}')


class LeaseLost(Exception):
    pass


class DeadLetterUnavailable(Exception):
    pass


def error_kind(error):
    code = getattr(error, 'pgcode', None) or getattr(error, 'sqlstate', None) or ''
    if code[:2] in ('22', '23'):
        return 'invalid_record'
    if code[:2] in ('08', '40', '53', '55') or code in ('57P01', '57P02', '57P03'):
        return 'transient'
    if code:
        return 'infrastructure'  # auth/schema/config errors must not empty a topic into a DLQ
    if isinstance(error, (ValueError, TypeError, UnicodeError)):
        return 'invalid_record'
    if isinstance(error, (TimeoutError, ConnectionError)) or type(error).__name__ in ('OperationalError', 'InterfaceError'):
        return 'transient'
    return 'infrastructure'


def decode_message(message):
    raw = message.value()
    if raw is None:
        raise ValueError('Tombstones require an explicit delete mapping; delivery is paused by default')
    record = json.loads(raw.decode('utf-8'))
    if not isinstance(record, dict):
        record = {'value': record}
    if any(key.startswith('__') for key in record):
        raise ValueError('Double-underscore fields are reserved for worker metadata')
    key = message.key()
    record['__key__'] = key.decode('utf-8', errors='replace') if key is not None else None
    record['__headers__'] = {name: value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value for name, value in message.headers() or []}
    _, timestamp = message.timestamp()
    published = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc) if timestamp is not None and timestamp >= 0 else None
    return record, published


def dead_letter_envelope(message, identity, error, stage):
    def encoded(value):
        if value is None:
            return None
        return base64.b64encode(value if isinstance(value, bytes) else str(value).encode()).decode('ascii')
    return {
        'version': 1, 'event_id': identity.key, 'source': identity.fields(),
        'failure': {'stage': stage, 'type': type(error).__name__, 'sqlstate': getattr(error, 'pgcode', None)},
        'record': {'key_base64': encoded(message.key()), 'value_base64': encoded(message.value()),
                   'headers': [{'key': key, 'value_base64': encoded(value)} for key, value in message.headers() or []],
                   'timestamp': list(message.timestamp())},
    }


class Processor:
    def __init__(self, destination, commit, *, policy='pause', retries=None, dead_letter=None,
                 emit=None, sleep=time.sleep, stopped=lambda: False, after_write=None):
        if policy not in ('pause', 'dead_letter') or (policy == 'dead_letter' and dead_letter is None):
            raise ValueError('Choose pause or configure an acknowledged dead-letter destination')
        self.destination, self.commit, self.policy = destination, commit, policy
        self.retries, self.dead_letter = retries or RetryPolicy(), dead_letter
        self.emit, self.sleep, self.stopped = emit or (lambda event: None), sleep, stopped
        self.after_write = after_write or (lambda identity: None)

    def event(self, name, identity, **values):
        self.emit(dict(event=name, **identity.fields(), **values))

    def attempt(self, function, identity, stage, owns, retry_any=False):
        for attempt in range(1, self.retries.max_attempts + 1):
            if self.stopped() or not owns():
                raise LeaseLost('Stop or assignment change; no offset will be committed')
            try:
                return function()
            except LeaseLost:
                raise
            except Exception as error:
                can_retry = retry_any or error_kind(error) == 'transient'
                self.event('record_error', identity, stage=stage, attempt=attempt, error_type=type(error).__name__, kind=error_kind(error))
                if not can_retry or attempt == self.retries.max_attempts:
                    raise DeliveryBlocked(identity, stage, attempt, error) from error
                delay = self.retries.delay(attempt)
                self.event('record_retry', identity, stage=stage, attempt=attempt, delay_seconds=delay)
                self.sleep(delay)

    def process(self, message, cluster, owns=lambda: True):
        identity = Identity(cluster, message.topic(), message.partition(), message.offset())
        started = time.monotonic()
        try:
            record, published = decode_message(message)
        except (ValueError, TypeError, UnicodeError) as error:
            blocked = DeliveryBlocked(identity, 'validation', 1, error)
            return self.failure(message, blocked, owns, started)
        try:
            outcome = self.attempt(lambda: self.destination.write(record, identity, published), identity, 'destination', owns)
        except DeliveryBlocked as blocked:
            return self.failure(message, blocked, owns, started)
        # Test hooks are injected by the integration harness, never via production env.
        self.after_write(identity)
        if not owns() or self.stopped():
            raise LeaseLost('Assignment changed after write; replay is required')
        self.finish(identity, message, outcome, owns, started)
        return outcome

    def failure(self, message, blocked, owns, started):
        cause = blocked.__cause__ or blocked.error
        eligible = blocked.stage == 'validation' or (cause is not None and error_kind(cause) == 'invalid_record')
        if self.policy != 'dead_letter' or not eligible:
            raise blocked
        envelope = dead_letter_envelope(message, blocked.identity, cause or blocked, blocked.stage)
        self.attempt(lambda: self.dead_letter(envelope), blocked.identity, 'dead_letter', owns, retry_any=True)
        if not owns() or self.stopped():
            raise LeaseLost('Assignment changed after DLQ acknowledgment; replay may repeat the DLQ event')
        self.finish(blocked.identity, message, 'dead_lettered', owns, started)
        return 'dead_lettered'

    def finish(self, identity, message, outcome, owns, started):
        # Commit failures are not destination failures and can never be sent to a DLQ.
        self.attempt(lambda: self.commit(identity), identity, 'offset_commit', owns, retry_any=True)
        self.event('record_processed', identity, outcome=outcome, next_offset=identity.offset + 1,
                   source_timestamp_ms=message.timestamp()[1], committed_at_ns=time.time_ns(),
                   processing_seconds=time.monotonic() - started)
