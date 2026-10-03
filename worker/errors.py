"""
errors.py — reports per-message processing errors back to the management API.
"""
import logging
import os

import requests

logger = logging.getLogger(__name__)

_API_BASE = os.environ.get("MGMT_API_URL", "http://kafbat-backend:8080")


def report(job_id: str, partition_id: int, offset: int, error_msg: str):
    """POST an error event (with partition + offset context) to the management backend."""
    payload = {
        "partitionId": partition_id,
        "offset":      offset,
        "errorMsg":    error_msg,
    }
    try:
        resp = requests.post(
            f"{_API_BASE}/api/v1/jobs/{job_id}/errors",
            json=payload,
            timeout=5,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(
            "Failed to report error for job %s partition %d offset %d: %s",
            job_id, partition_id, offset, exc,
        )
