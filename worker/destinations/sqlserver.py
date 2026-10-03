"""
sqlserver.py — replication destination for Microsoft SQL Server.
"""
import logging
from datetime import timezone

import pyodbc

from .base import BaseDestination, SQL_TYPE_MAP, METADATA_COLS, extract_field, parse_value, now_utc

logger = logging.getLogger(__name__)

MSSQL_TYPE_MAP = {
    **SQL_TYPE_MAP,
    "double":    "FLOAT",
    "long":      "BIGINT",
    "bytes":     "VARBINARY(MAX)",
    "json":      "NVARCHAR(MAX)",
    "timestamp": "DATETIMEOFFSET",
    "boolean":   "BIT",
    "string":    "NVARCHAR(MAX)",
}


class SQLServerDestination(BaseDestination):
    def __init__(self, config: dict, fields: list):
        super().__init__(config, fields)
        self.table = config.get("tableName", "kafka_messages")
        self.schema = config.get("schema", "dbo")
        c = config
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={c['host']},{c.get('port', 1433)};"
            f"DATABASE={c['database']};"
            f"UID={c['username']};PWD={c['password']};"
            f"TrustServerCertificate=yes;"
        )
        self._conn = pyodbc.connect(conn_str, autocommit=False)

    def ensure_table(self):
        pk_fields = [f for f in self.fields if f["is_primary_key"]]
        col_defs = []
        for f in self.fields:
            sql_type = MSSQL_TYPE_MAP.get(f["field_type"], "NVARCHAR(MAX)")
            nullable = "NOT NULL" if f["is_required"] else "NULL"
            col_defs.append(f"[{f['name']}] {sql_type} {nullable}")

        for col_name, col_type, _ in METADATA_COLS:
            ms_type = col_type.replace("TIMESTAMPTZ", "DATETIMEOFFSET").replace("BYTEA", "VARBINARY(MAX)").replace("JSONB", "NVARCHAR(MAX)")
            col_defs.append(f"[{col_name}] {ms_type} NULL")

        if pk_fields:
            pk_names = ", ".join(f"[{f['name']}]" for f in pk_fields)
            col_defs.append(f"CONSTRAINT PK_{self.table} PRIMARY KEY ({pk_names})")

        cols_sql = ",\n  ".join(col_defs)
        ddl = (
            f"IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA='{self.schema}' AND TABLE_NAME='{self.table}')\n"
            f"CREATE TABLE [{self.schema}].[{self.table}] (\n  {cols_sql}\n);"
        )
        cursor = self._conn.cursor()
        cursor.execute(ddl)
        self._conn.commit()
        logger.info("Table [%s].[%s] ensured", self.schema, self.table)

    def insert(self, record: dict, partition: int, offset: int, published_at):
        col_names = [f"[{f['name']}]" for f in self.fields]
        col_names += [f"[{c}]" for c, _, _ in METADATA_COLS]

        values = [parse_value(extract_field(record, f), f["field_type"]) for f in self.fields]
        pub_utc = published_at.astimezone(timezone.utc) if published_at else None
        values += [partition, offset, pub_utc, now_utc()]

        placeholders = ", ".join(["?"] * len(values))
        sql = f"INSERT INTO [{self.schema}].[{self.table}] ({', '.join(col_names)}) VALUES ({placeholders})"
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql, values)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def close(self):
        self._conn.close()
