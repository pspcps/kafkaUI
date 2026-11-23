"""PostgreSQL business write + source-identity ledger in one local transaction.

The ledger guards the business write itself (including transactional triggers),
not merely the final row count. It is not a distributed Kafka/DB transaction.
"""
import hashlib
import logging
from datetime import timezone

import psycopg2
from psycopg2 import sql
from credentials import connection_credentials
from .base import BaseDestination, SQL_TYPE_MAP, METADATA_COLS, extract_field, parse_value, now_utc

logger = logging.getLogger(__name__)
SOURCE_COLS = [('_kafka_cluster', 'TEXT', False), ('_kafka_topic', 'TEXT', False)]
ALL_METADATA = SOURCE_COLS + METADATA_COLS
LEDGER = '_kafkaui_delivery_ledger'


class DestinationConfigurationError(RuntimeError):
    """Credential/configuration errors are never invalid source records."""




class PostgreSQLDestination(BaseDestination):
    def __init__(self, config, fields):
        super().__init__(config, fields)
        self.table = config.get('tableName') or 'kafka_messages'
        self.schema = config.get('schema') or 'public'
        if self.table == LEDGER or any(field['name'].startswith('_kafka') for field in fields):
            raise ValueError('Kafka metadata and delivery ledger names are reserved')
        self.target_id = hashlib.sha256(f'{self.schema}\0{self.table}'.encode()).hexdigest()
        self._conn = None
        self._connect()

    def _connect(self):
        if self._conn is not None:
            self._conn.close()
        try:
            c = connection_credentials(self.config)
        except (ValueError, TypeError, KeyError):
            # Secret rotation can introduce invalid JSON/fields. Do not route
            # healthy source records to a DLQ because the credential mount broke.
            raise DestinationConfigurationError('Destination credentials are unavailable or invalid') from None
        self._conn = psycopg2.connect(host=c['host'], port=c.get('port', 5432), dbname=c['database'],
            user=c['username'], password=c['password'], sslmode='verify-full', sslrootcert=c['sslRootCert'],
            connect_timeout=10, keepalives=1, keepalives_idle=10, keepalives_interval=3, keepalives_count=3,
            options='-c statement_timeout=10000 -c lock_timeout=10000')
        self._conn.autocommit = False

    @property
    def qualified_table(self):
        return sql.Identifier(self.schema, self.table)

    @property
    def qualified_ledger(self):
        return sql.Identifier(self.schema, LEDGER)

    def ensure_table(self):
        columns = [sql.SQL('{} {}{}').format(sql.Identifier(f['name']), sql.SQL(SQL_TYPE_MAP.get(f['field_type'], 'TEXT')),
                   sql.SQL(' NOT NULL' if f['is_required'] else '')) for f in self.fields]
        columns += [sql.SQL('{} {} NOT NULL').format(sql.Identifier(name), sql.SQL(kind)) for name, kind, _ in ALL_METADATA if name != '_kafka_published_at']
        columns += [sql.SQL('_kafka_published_at TIMESTAMPTZ')]
        pk = [f['name'] for f in self.fields if f.get('is_primary_key')]
        if pk:
            columns.append(sql.SQL('PRIMARY KEY ({})').format(sql.SQL(',').join(map(sql.Identifier, pk))))
        with self._conn:
            with self._conn.cursor() as cursor:
                cursor.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {} ({})').format(self.qualified_table, sql.SQL(',').join(columns)))
                cursor.execute('SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s', (self.schema, self.table))
                if not {name for name, _, _ in ALL_METADATA}.issubset({row[0] for row in cursor.fetchall()}):
                    raise ValueError('Destination predates source identities; use a new target or a reviewed migration')
                cursor.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {} (target_id TEXT NOT NULL, source_cluster TEXT NOT NULL, source_topic TEXT NOT NULL, source_partition INTEGER NOT NULL, source_offset BIGINT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(target_id,source_cluster,source_topic,source_partition,source_offset))').format(self.qualified_ledger))
                if not pk:
                    index = f'_kafkaui_coords_{self.target_id[:16]}'
                    cursor.execute(sql.SQL('CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (_kafka_cluster,_kafka_topic,_kafka_partition,_kafka_offset)').format(sql.Identifier(index), self.qualified_table))
        logger.info('Replay-safe destination schema ready')

    def write(self, record, identity, published_at):
        if self._conn.closed:
            self._connect()
        names = [field['name'] for field in self.fields]
        values = []
        for field in self.fields:
            value = extract_field(record, field)
            if value is None and field.get('is_required'):
                raise ValueError('A required mapped field is missing')
            values.append(parse_value(value, field['field_type']))
        names += [name for name, _, _ in ALL_METADATA]
        values += [identity.cluster, identity.topic, identity.partition, identity.offset,
                   published_at.astimezone(timezone.utc) if published_at else None, now_utc()]
        pk = [field['name'] for field in self.fields if field.get('is_primary_key')]
        insert = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(self.qualified_table,
                 sql.SQL(',').join(map(sql.Identifier, names)), sql.SQL(',').join(sql.Placeholder() for _ in values))
        if pk:
            updates = [name for name in names if name not in pk]
            insert += sql.SQL(' ON CONFLICT ({}) DO UPDATE SET {}').format(sql.SQL(',').join(map(sql.Identifier, pk)),
                sql.SQL(',').join(sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(name), sql.Identifier(name)) for name in updates))
        else:
            insert += sql.SQL(' ON CONFLICT (_kafka_cluster,_kafka_topic,_kafka_partition,_kafka_offset) DO NOTHING')
        try:
            with self._conn:
                with self._conn.cursor() as cursor:
                    cursor.execute(sql.SQL('INSERT INTO {} (target_id,source_cluster,source_topic,source_partition,source_offset) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING source_offset').format(self.qualified_ledger),
                                   (self.target_id, identity.cluster, identity.topic, identity.partition, identity.offset))
                    if cursor.fetchone() is None:
                        return 'duplicate'
                    cursor.execute(insert, values)
            return 'applied'
        except Exception:
            # On a network/commit ambiguity, discard the connection. A fresh attempt
            # checks the committed ledger before any repeated business effect.
            self.close()
            raise

    def insert(self, record, partition, offset, published_at):
        raise RuntimeError('Source cluster and topic identity are required; call write()')

    def close(self):
        if self._conn is not None:
            self._conn.close()
