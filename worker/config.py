"""
config.py — loads job configuration from the management database.
"""
import os
import json
import psycopg2
import psycopg2.extras


def get_job(job_id: str) -> dict:
    """Fetch replication job + schema fields from the management DB."""
    dsn = os.environ["MGMT_DB_DSN"]
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT j.*,
                       ts.schema_type,
                       ts.avro_raw
                FROM   replication_jobs j
                LEFT JOIN topic_schemas ts
                       ON ts.cluster_id = j.cluster_id
                      AND ts.topic_name = j.topic_name
                WHERE  j.id = %s
                """,
                (job_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Job {job_id} not found")
            job = dict(row)

            # destination_config is JSONB — already a dict via RealDictCursor
            if isinstance(job["destination_config"], str):
                job["destination_config"] = json.loads(job["destination_config"])

            # Fetch schema fields
            schema_id_row = None
            cur.execute(
                """
                SELECT id FROM topic_schemas
                WHERE cluster_id = %s AND topic_name = %s
                """,
                (job["cluster_id"], job["topic_name"]),
            )
            schema_id_row = cur.fetchone()

            job["fields"] = []
            if schema_id_row:
                cur.execute(
                    """
                    SELECT name, field_type,
                           COALESCE(field_source, 'value') AS field_source,
                           is_required, is_primary_key,
                           default_value, position
                    FROM   schema_fields
                    WHERE  schema_id = %s
                    ORDER  BY position
                    """,
                    (schema_id_row["id"],),
                )
                job["fields"] = [dict(r) for r in cur.fetchall()]

            return job
    finally:
        conn.close()
