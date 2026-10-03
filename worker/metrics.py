"""
metrics.py — reports consumer partition metrics back to the management API.
"""
import os
import logging
import requests

logger = logging.getLogger(__name__)

_API_BASE = os.environ.get("MGMT_API_URL", "http://kafbat-backend:8080")


def report(job_id: str, partition_id: int, current_offset: int,
           end_offset: int, replicated_count: int, last_message_at=None):
    """POST partition metrics to the management backend."""
    payload = {
        "partitionId":     partition_id,
        "currentOffset":   current_offset,
        "endOffset":       end_offset,
        "replicatedCount": replicated_count,
    }
    if last_message_at is not None:
        payload["lastMessageAt"] = last_message_at.isoformat() + "Z"

    try:
        resp = requests.post(
            f"{_API_BASE}/api/v1/jobs/{job_id}/metrics",
            json=payload,
            timeout=5,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to report metrics for job %s partition %d: %s", job_id, partition_id, exc)
