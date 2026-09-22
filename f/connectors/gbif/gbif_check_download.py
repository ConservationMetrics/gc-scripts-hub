# requirements:
# requests

"""Perform one GBIF download status check for a Flow polling loop."""

from datetime import datetime, timezone

import requests

_API = "https://api.gbif.org/v1/occurrence/download"
_PENDING = frozenset({"PREPARING", "RUNNING", "SUSPENDED"})
_TERMINAL = frozenset({"CANCELLED", "KILLED", "FAILED", "FILE_ERASED"})


def main(download_key: str, deadline: str) -> dict:
    """Return one status result or fail for terminal, invalid, or expired work.

    Parameters
    ----------
    download_key : str
        Key returned by the GBIF submission endpoint.
    deadline : str
        ISO-8601 absolute deadline returned by the submission step.
    """
    try:
        expires_at = datetime.fromisoformat(deadline.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ValueError("deadline must be an ISO-8601 timestamp.") from exc
    if datetime.now(timezone.utc) >= expires_at:
        raise TimeoutError(
            f"GBIF download {download_key} exceeded its polling deadline."
        )
    try:
        response = requests.get(f"{_API}/{download_key}", timeout=(10, 60))
    except requests.RequestException as exc:
        raise RuntimeError(
            f"retryable GBIF status check failure for {download_key}: {exc}"
        ) from exc
    if response.status_code == 429 or response.status_code >= 500:
        raise RuntimeError(
            f"retryable GBIF status check failure for {download_key}: HTTP {response.status_code}."
        )
    response.raise_for_status()
    payload = response.json()
    status = payload.get("status")
    if status in _TERMINAL:
        raise RuntimeError(f"GBIF download {download_key} ended with status {status}.")
    if status not in _PENDING and status != "SUCCEEDED":
        raise RuntimeError(
            f"GBIF download {download_key} returned unknown status {status!r}."
        )
    return {
        "download_key": download_key,
        "status": status,
        "succeeded": status == "SUCCEEDED",
    }
