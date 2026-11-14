"""
mongodb.py — replication destination for MongoDB.
Direct document insert (schema optional, used for field coercion only).
"""
from credentials import connection_credentials
import logging
from datetime import timezone

from pymongo import MongoClient, ASCENDING

from .base import BaseDestination, extract_field, parse_value, now_utc

logger = logging.getLogger(__name__)


class MongoDBDestination(BaseDestination):
    def __init__(self, config: dict, fields: list):
        super().__init__(config, fields)
        c = connection_credentials(config)
        self.collection_name = c.get("collection") or c.get("tableName", "kafka_messages")
        port = int(c.get("port", 27017))

        tls_options = {'tlsCAFile': c['sslRootCert']} if c.get('sslRootCert') else {}
        self._client = MongoClient(host=c['host'], port=port, username=c['username'], password=c['password'],
                                   authSource='admin', tls=True, serverSelectionTimeoutMS=10_000, **tls_options)
        self._db = self._client[c["database"]]
        self._col = self._db[self.collection_name]

    def ensure_table(self):
        """Create indexes for PK fields and Kafka metadata."""
        pk_fields = [f["name"] for f in self.fields if f["is_primary_key"]]
        if pk_fields:
            index = [(name, ASCENDING) for name in pk_fields]
            self._col.create_index(index, unique=True, sparse=True)
        # Index for dedup / replay support
        self._col.create_index(
            [("_kafka_partition", ASCENDING), ("_kafka_offset", ASCENDING)],
            unique=True,
        )
        logger.info("Collection %s indexes ensured", self.collection_name)

    def insert(self, record: dict, partition: int, offset: int, published_at):
        doc = {}

        # Coerce values using schema field types (if schema is defined)
        if self.fields:
            for f in self.fields:
                raw = extract_field(record, f)
                doc[f["name"]] = parse_value(raw, f["field_type"])
        else:
            # No schema — insert raw document (excluding internal __headers__ key)
            doc.update({k: v for k, v in record.items() if k != "__headers__"})

        pub_utc = published_at.astimezone(timezone.utc) if published_at else None
        doc["_kafka_partition"]     = partition
        doc["_kafka_offset"]        = offset
        doc["_kafka_published_at"]  = pub_utc
        doc["_kafka_replicated_at"] = now_utc()

        # UPSERT: match on PK fields (or kafka coords as fallback)
        pk_fields = [f["name"] for f in self.fields if f.get("is_primary_key")]
        if pk_fields:
            filter_doc = {name: doc[name] for name in pk_fields if name in doc}
        else:
            filter_doc = {"_kafka_partition": partition, "_kafka_offset": offset}

        self._col.replace_one(filter_doc, doc, upsert=True)

    def close(self):
        self._client.close()
