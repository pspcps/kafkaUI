"""
base.py — abstract base class for all replication destinations.
"""
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone


# Map schema field types to SQL column types
SQL_TYPE_MAP = {
    "string":    "TEXT",
    "int":       "INTEGER",
    "long":      "BIGINT",
    "float":     "FLOAT",
    "double":    "DOUBLE PRECISION",
    "boolean":   "BOOLEAN",
    "timestamp": "TIMESTAMPTZ",
    "bytes":     "BYTEA",
    "json":      "JSONB",
}

# Metadata columns appended to every replicated row
METADATA_COLS = [
    ("_kafka_partition",      "INTEGER",     False),
    ("_kafka_offset",         "BIGINT",      False),
    ("_kafka_published_at",   "TIMESTAMPTZ", False),
    ("_kafka_replicated_at",  "TIMESTAMPTZ", False),
]


def extract_field(record: dict, field: dict):
    """Extract a field value from the correct source: value body, header, or message key.

    For 'key' source the raw key string is returned directly (the field maps to the
    entire Kafka message key).  If the key happens to be JSON and the field name is
    NOT '__kafka_key__', we try to parse it and pull a named sub-field — enabling
    composite / structured keys.
    """
    source = field.get("field_source", "value")
    if source == "header":
        return record.get("__headers__", {}).get(field["name"])
    if source == "key":
        raw_key = record.get("__key__")
        # Try to parse as JSON for composite/structured keys
        if raw_key:
            try:
                import json as _json
                parsed = _json.loads(raw_key)
                if isinstance(parsed, dict):
                    # Structured key — extract named sub-field if present
                    if field["name"] in parsed:
                        return parsed[field["name"]]
            except (ValueError, TypeError):
                pass
        # Plain string key — return as-is (whole key maps to this field)
        return raw_key
    # default: 'value' source
    return record.get(field["name"])


def parse_value(raw, field_type: str):
    """Coerce a string value from the Kafka message into the right Python type."""
    if raw is None:
        return None
    if field_type in ("int", "long"):
        return int(raw)
    if field_type in ("float", "double"):
        return float(raw)
    if field_type == "boolean":
        if isinstance(raw, bool):
            return raw
        return str(raw).lower() in ("true", "1", "yes")
    if field_type == "timestamp":
        if isinstance(raw, datetime):
            return raw
        from dateutil import parser as dp
        return dp.parse(str(raw))
    if field_type == "json" and isinstance(raw, (dict, list)):
        return json.dumps(raw)
    return str(raw) if raw is not None else None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class BaseDestination(ABC):
    def __init__(self, config: dict, fields: list):
        self.config = config
        self.fields = fields  # list of dicts from schema_fields table

    @abstractmethod
    def ensure_table(self):
        """Create the target table / collection if it does not exist."""

    @abstractmethod
    def insert(self, record: dict, partition: int, offset: int, published_at):
        """Write one record to the destination."""

    def close(self):
        pass
