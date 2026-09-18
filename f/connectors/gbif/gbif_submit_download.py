# requirements:
# python-dateutil
# requests

"""Submit one asynchronous GBIF SIMPLE_CSV occurrence download."""

from datetime import datetime, timedelta, timezone
from typing import TypedDict

import requests

from f.common_logic.date_utils import calculate_cutoff_date
from f.common_logic.geo_utils import bounding_box_to_wkt

_API = "https://api.gbif.org/v1/occurrence/download"
_MAX_AREA_KM2 = 15_000


class c_gbif(TypedDict):
    """Windmill GBIF account resource."""

    username: str
    password: str


def main(
    gbif_account: c_gbif,
    bounding_box: list | str,
    max_months_lookback: int | None = None,
    max_wait_seconds: int = 86400,
) -> dict:
    """Validate filters and submit exactly one GBIF download request.

    Parameters
    ----------
    gbif_account : c_gbif
        Resource containing a GBIF username and password.
    bounding_box : list or str
        ``[[west, south], [east, north]]`` in longitude/latitude order.
    max_months_lookback : int, optional
        Include records interpreted on or after the cutoff month's first day.
    max_wait_seconds : int
        Absolute maximum Flow wait after submission, from 1 through 86,400.
    """
    if isinstance(max_months_lookback, bool) or (
        max_months_lookback is not None
        and (not isinstance(max_months_lookback, int) or max_months_lookback < 0)
    ):
        raise ValueError("max_months_lookback must be a non-negative integer or null.")
    if (
        isinstance(max_wait_seconds, bool)
        or not isinstance(max_wait_seconds, int)
        or not 1 <= max_wait_seconds <= 86_400
    ):
        raise ValueError("max_wait_seconds must be an integer from 1 through 86400.")
    if not gbif_account.get("username") or not gbif_account.get("password"):
        raise ValueError("gbif_account must contain a username and password.")

    wkt = bounding_box_to_wkt(bounding_box, max_area_km2=_MAX_AREA_KM2)
    predicate: dict = {"type": "within", "geometry": wkt}
    cutoff = calculate_cutoff_date(max_months_lookback)
    if cutoff is not None:
        year, month = cutoff
        predicate = {
            "type": "and",
            "predicates": [
                predicate,
                {
                    "type": "greaterThanOrEquals",
                    "key": "LAST_INTERPRETED",
                    "value": f"{year}-{month:02d}-01",
                },
            ],
        }
    response = requests.post(
        f"{_API}/request",
        json={
            "format": "SIMPLE_CSV",
            "sendNotification": False,
            "predicate": predicate,
        },
        auth=(gbif_account["username"], gbif_account["password"]),
        timeout=(10, 60),
    )
    response.raise_for_status()
    download_key = response.text.strip().strip('"')
    if not download_key or len(download_key) > 200:
        raise RuntimeError("GBIF did not return a usable download key.")
    submitted_at = datetime.now(timezone.utc)
    return {
        "download_key": download_key,
        "deadline": (submitted_at + timedelta(seconds=max_wait_seconds)).isoformat(),
        "submitted_at": submitted_at.isoformat(),
    }
