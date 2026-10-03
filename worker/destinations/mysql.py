"""
mysql.py — replication destination for MySQL / MariaDB.
"""
import logging
from datetime import timezone

import pymysql
import pymysql.cursors

from .base import BaseDestination, SQL_TYPE_MAP, METADATA_COLS, extract_field, parse_value, now_utc

logger = logging.getLogger(__name__)

MYSQL_TYPE_MAP = {
    **SQL_TYPE_MAP,
    "double":    "DOUBLE",
    "bytes":     "BLOB",
    "json":      "JSON",
    "timestamp": "DATETIME",
    "boolean":   "TINYINT(1)",
}


class MySQLDestination(BaseDestination):
    def __init__(self, config: dict, fields: list):
        super().__init__(config, fields)
        self.table = config.get("tableName", "kafka_messages")
        self._conn = pymysql.connect(
            host=config["host"],
            port=int(config.get("port", 3306)),
            database=config["database"],
            user=config["username"],
            password=config["password"],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    def ensure_table(self):
        pk_fields = [f for f in self.fields if f["is_primary_key"]]
        col_defs = []
        for f in self.fields:
            sql_type = MYSQL_TYPE_MAP.get(f["field_type"], "TEXT")
            nullable = "NOT NULL" if f["is_required"] else "NULL"
            col_defs.append(f"`{f['name']}` {sql_type} {nullable}")

        for col_name, col_type, _ in METADATA_COLS:
            mysql_type = col_type.replace("TIMESTAMPTZ", "DATETIME").replace("BYTEA", "BLOB").replace("JSONB", "JSON")
            col_defs.append(f"`{col_name}` {mysql_type}")

        if pk_fields:
            pk_names = ", ".join(f"`{f['name']}`" for f in pk_fields)
            col_defs.append(f"PRIMARY KEY ({pk_names})")

        cols_sql = ",\n  ".join(col_defs)
        ddl = f"CREATE TABLE IF NOT EXISTS `{self.table}` (\n  {cols_sql}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

        with self._conn.cursor() as cur:
            cur.execute(ddl)
        self._conn.commit()
        logger.info("Table %s ensured", self.table)

    def insert(self, record: dict, partition: int, offset: int, published_at):
        col_names = [f"`{f['name']}`" for f in self.fields]
        col_names += [f"`{c}`" for c, _, _ in METADATA_COLS]

        values = [parse_value(extract_field(record, f), f["field_type"]) for f in self.fields]
        pub_utc = published_at.astimezone(timezone.utc) if published_at else None
        values += [partition, offset, pub_utc, now_utc()]

        placeholders = ", ".join(["%s"] * len(values))
        col_list = ", ".join(col_names)

        pk_fields = [f for f in self.fields if f.get("is_primary_key")]
        if pk_fields:
            # UPSERT: update all non-PK columns on duplicate key
            non_pk_cols = [f"`{f['name']}`" for f in self.fields if not f.get("is_primary_key")]
            non_pk_cols += [f"`{c}`" for c, _, _ in METADATA_COLS]
            update_parts = [f"{c} = VALUES({c})" for c in non_pk_cols] if non_pk_cols else [f"`{pk_fields[0]['name']}` = VALUES(`{pk_fields[0]['name']}`)"]
            sql = (
                f"INSERT INTO `{self.table}` ({col_list}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {', '.join(update_parts)}"
            )
        else:
            sql = f"INSERT IGNORE INTO `{self.table}` ({col_list}) VALUES ({placeholders})"

        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, values)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def close(self):
        self._conn.close()
