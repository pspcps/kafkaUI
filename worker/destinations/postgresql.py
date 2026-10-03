"""
postgresql.py — replication destination for PostgreSQL.
"""
import logging
from datetime import timezone

import psycopg2
import psycopg2.extras

from .base import BaseDestination, SQL_TYPE_MAP, METADATA_COLS, extract_field, parse_value, now_utc

logger = logging.getLogger(__name__)


class PostgreSQLDestination(BaseDestination):
    def __init__(self, config: dict, fields: list):
        super().__init__(config, fields)
        self.table = config.get("tableName") or config.get("table_name", "kafka_messages")
        self.schema = config.get("schema", "public")
        self._conn = None
        self._connect()

    def _connect(self):
        c = self.config
        dsn = (
            f"host={c['host']} port={c.get('port', 5432)} "
            f"dbname={c['database']} user={c['username']} password={c['password']} "
            f"sslmode=prefer"
        )
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False

    def ensure_table(self):
        pk_fields = [f for f in self.fields if f["is_primary_key"]]
        col_defs = []
        for f in self.fields:
            sql_type = SQL_TYPE_MAP.get(f["field_type"], "TEXT")
            nullable = "" if f["is_required"] else " NULL"
            col_defs.append(f'    "{f["name"]}" {sql_type}{nullable}')

        # Metadata cols
        for col_name, col_type, _ in METADATA_COLS:
            col_defs.append(f'    "{col_name}" {col_type}')

        # Primary key constraint
        if pk_fields:
            pk_names = ", ".join(f'"{f["name"]}"' for f in pk_fields)
            col_defs.append(f"    PRIMARY KEY ({pk_names})")

        cols_sql = ",\n".join(col_defs)
        ddl = f'CREATE TABLE IF NOT EXISTS "{self.schema}"."{self.table}" (\n{cols_sql}\n);'

        with self._conn.cursor() as cur:
            cur.execute(ddl)
            # When there are no user-defined PK fields, deduplicate on kafka coords.
            if not pk_fields:
                cur.execute(
                    f'CREATE UNIQUE INDEX IF NOT EXISTS '
                    f'"{self.table}_kafka_dedup" '
                    f'ON "{self.schema}"."{self.table}" (_kafka_partition, _kafka_offset)'
                )
        self._conn.commit()
        logger.info("Table %s.%s ensured", self.schema, self.table)

    def insert(self, record: dict, partition: int, offset: int, published_at):
        col_names = [f'"{f["name"]}"' for f in self.fields]
        col_names += [f'"{c}"' for c, _, _ in METADATA_COLS]

        values = []
        for f in self.fields:
            raw = extract_field(record, f)
            values.append(parse_value(raw, f["field_type"]))

        pub_utc = published_at.astimezone(timezone.utc) if published_at else None
        values += [partition, offset, pub_utc, now_utc()]

        placeholders = ", ".join(["%s"] * len(values))
        col_list = ", ".join(col_names)

        pk_fields = [f for f in self.fields if f.get("is_primary_key")]
        if pk_fields:
            # UPSERT: on PK conflict update all non-PK columns so we get latest values
            pk_clause = ", ".join(f'"{f["name"]}"' for f in pk_fields)
            non_pk_updates = [
                f'"{f["name"]}" = EXCLUDED."{f["name"]}"'
                for f in self.fields if not f.get("is_primary_key")
            ]
            non_pk_updates += [f'"{c}" = EXCLUDED."{c}"' for c, _, _ in METADATA_COLS]
            update_clause = ", ".join(non_pk_updates) if non_pk_updates else f'"{pk_fields[0]["name"]}" = EXCLUDED."{pk_fields[0]["name"]}"'
            sql = (
                f'INSERT INTO "{self.schema}"."{self.table}" ({col_list})\n'
                f"VALUES ({placeholders})\n"
                f"ON CONFLICT ({pk_clause}) DO UPDATE SET {update_clause}"
            )
        else:
            # No PK defined — deduplicate on kafka offset (idempotent replay)
            sql = (
                f'INSERT INTO "{self.schema}"."{self.table}" ({col_list})\n'
                f"VALUES ({placeholders})\n"
                f'ON CONFLICT ("_kafka_partition", "_kafka_offset") DO NOTHING'
            )

        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, values)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def close(self):
        if self._conn:
            self._conn.close()
