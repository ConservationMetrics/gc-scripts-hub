# requirements:
# psycopg[binary]
# pyproj
# python-dateutil
# requests~=2.32

import logging
import re
from datetime import date
from pathlib import Path
from typing import Any, Literal, NamedTuple

import requests

from f.common_logic.date_utils import parse_observation_date
from f.common_logic.db_operations import (
    DynSelect_existing_db_table_name,
    existing_db_table_name as list_dataset_tables,
    postgresql,
    resolve_db_table_name,
)
from f.common_logic.file_operations import save_data_to_file
from f.common_logic.geo_utils import geojson_point, parse_lon_lat
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

_API_ROOT = "https://api.ebird.org/v2"
_OBSERVATION_TYPES = (
    "recent_region",
    "recent_notable_region",
    "recent_nearby",
    "recent_notable_nearby",
    "historic_date",
)
_REGION_TYPES = frozenset({"recent_region", "recent_notable_region"})
_NOTABLE_TYPES = frozenset({"recent_notable_region", "recent_notable_nearby"})
_HISTORIC_DATE = "historic_date"
_REGION_CODE_RE = re.compile(r"^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$")
# eBird's historic endpoint accepts years from 1800 through the present.
_EARLIEST_OBSERVATION_DATE = date(1800, 1, 1)
# eBird simple observations have no obsId. Notable `detail=full` responses do.
_FIELD_MAP = (
    ("speciesCode", "species_code"),
    ("comName", "common_name"),
    ("sciName", "scientific_name"),
    ("locId", "loc_id"),
    ("locName", "loc_name"),
    ("obsDt", "observed_at"),
    ("howMany", "how_many"),
    ("lat", "lat"),
    ("lng", "lng"),
    ("obsValid", "obs_valid"),
    ("obsReviewed", "obs_reviewed"),
    ("locationPrivate", "location_private"),
    ("subId", "sub_id"),
    ("subnational1Code", "subnational1_code"),
    ("subnational1Name", "subnational1_name"),
    ("subnational2Code", "subnational2_code"),
    ("subnational2Name", "subnational2_name"),
    ("countryCode", "country_code"),
    ("countryName", "country_name"),
    ("userDisplayName", "user_display_name"),
    ("obsId", "obs_id"),
    ("checklistId", "checklist_id"),
    ("presenceNoted", "presence_noted"),
    ("hasComments", "has_comments"),
    ("firstName", "observer_first_name"),
    ("lastName", "observer_last_name"),
    ("hasRichMedia", "has_rich_media"),
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class _ObservationsRequest(NamedTuple):
    observation_type: str
    url: str
    params: dict[str, Any]
    region_code: str | None
    search_lon: float | None
    search_lat: float | None
    observation_date: str | None


def existing_db_table_name(db: postgresql | None = None, **_):
    """Windmill dynamic select. Defined here so the `db` resource is resolved."""
    return list_dataset_tables(db)


def parse_region_code(region_code: str | None) -> str:
    """Return a stripped eBird region code, or raise if it is not usable.

    Parameters
    ----------
    region_code : str or None
        Country, subnational1, subnational2, or location code.

    Returns
    -------
    str
        The region code with surrounding whitespace removed.

    Raises
    ------
    ValueError
        If the code is empty or contains characters other than letters,
        digits, and hyphens.
    """
    code = region_code.strip() if isinstance(region_code, str) else ""
    if not _REGION_CODE_RE.fullmatch(code):
        raise ValueError(
            "region_code must be an eBird region code such as US-NY or L295658."
        )
    return code


def main(
    observation_type: str,
    api_token: str,
    db: postgresql,
    region_code: str | None = None,
    observation_date: str | None = None,
    lon_lat: str | None = None,
    db_table_name: str | None = None,
    attachment_root: str = "/persistent-storage/datalake",
    destination_action: Literal[
        "create_new_dataset", "use_existing_dataset"
    ] = "create_new_dataset",
    existing_db_table_name: DynSelect_existing_db_table_name | None = None,
):
    """Fetch recent eBird observations and write them to the datalake and PostgreSQL.

    Parameters
    ----------
    observation_type : str
        ``recent_region``, ``recent_notable_region``, ``recent_nearby``,
        ``recent_notable_nearby``, or ``historic_date``.
    api_token : str
        eBird API token. Sent as the ``X-eBirdApiToken`` header.
    db : postgresql
        Database connection configuration.
    region_code : str, optional
        eBird region code. Required for region and historic observation types.
    observation_date : str, optional
        ``YYYY-MM-DD``. Required when ``observation_type`` is ``historic_date``.
    lon_lat : str, optional
        ``"lon, lat"``. Required for the two nearby observation types.
    db_table_name : str, optional
        Table name and datalake subdirectory. Used when
        ``destination_action`` is ``"create_new_dataset"``. Creates the
        dataset if needed; updates it if it already exists.
    attachment_root : str
        Root directory for persisted files.
    destination_action : str
        ``"use_existing_dataset"`` or ``"create_new_dataset"``. The form
        shows these as "Use existing dataset" and "Enter dataset name".
    existing_db_table_name : str, optional
        Public table to append to. Used when ``destination_action`` is
        ``"use_existing_dataset"``.
    """
    token = _require_api_token(api_token)
    request = build_observations_request(
        observation_type, region_code, lon_lat, observation_date
    )
    table_name = resolve_db_table_name(
        db, destination_action, db_table_name, existing_db_table_name
    )
    observations = download_observations(token, request.url, request.params)
    geojson = transform_observations_to_geojson(
        observations,
        observation_type=request.observation_type,
        region_code=request.region_code,
        search_lon=request.search_lon,
        search_lat=request.search_lat,
        observation_date=request.observation_date,
    )
    write_observations(observations, geojson, db, table_name, attachment_root)


def build_observations_request(
    observation_type: str,
    region_code: str | None = None,
    lon_lat: str | None = None,
    observation_date: str | None = None,
) -> _ObservationsRequest:
    """Resolve a dropdown choice to an eBird URL and query parameters.

    Region and historic choices require ``region_code``. Historic choices
    also require ``observation_date``. Nearby choices require ``lon_lat``.
    Notable choices add ``detail=full``. The unused input is ignored, so a
    hidden form field cannot fail the run.
    """
    if observation_type not in _OBSERVATION_TYPES:
        joined = ", ".join(_OBSERVATION_TYPES)
        raise ValueError(
            f"Invalid observation_type {observation_type!r}. Expected one of: {joined}."
        )

    params: dict[str, Any] = {}
    if observation_type in _NOTABLE_TYPES:
        params["detail"] = "full"

    if observation_type == _HISTORIC_DATE:
        code = parse_region_code(region_code)
        observed = parse_observation_date(
            observation_date, earliest=_EARLIEST_OBSERVATION_DATE
        )
        url = (
            f"{_API_ROOT}/data/obs/{code}/historic/"
            f"{observed.year}/{observed.month}/{observed.day}"
        )
        return _ObservationsRequest(
            observation_type, url, params, code, None, None, observed.isoformat()
        )

    if observation_type in _REGION_TYPES:
        code = parse_region_code(region_code)
        url = f"{_API_ROOT}/data/obs/{code}/recent"
        if observation_type in _NOTABLE_TYPES:
            url += "/notable"
        return _ObservationsRequest(
            observation_type, url, params, code, None, None, None
        )

    lon, lat = parse_lon_lat(lon_lat)
    url = f"{_API_ROOT}/data/obs/geo/recent"
    if observation_type in _NOTABLE_TYPES:
        url += "/notable"
    params["lng"] = lon
    params["lat"] = lat
    return _ObservationsRequest(observation_type, url, params, None, lon, lat, None)


def download_observations(
    api_token: str, url: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    """GET one eBird observations list.

    These endpoints return a single JSON array. There is no page cursor.

    Parameters
    ----------
    api_token : str
        Value of the ``X-eBirdApiToken`` header.
    url : str
        Absolute observations URL.
    params : dict
        Query parameters such as ``detail``, ``lat``, and ``lng``.

    Returns
    -------
    list of dict
        Observation records, which may be empty.

    Raises
    ------
    ValueError
        If the response body is not a JSON list.
    requests.HTTPError
        If eBird responds with an HTTP error status.
    """
    response = requests.get(
        url,
        params=params,
        headers={"X-eBirdApiToken": api_token},
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("eBird observations response was not a JSON list.")
    logger.info(
        "Downloaded %s eBird observation(s) from %s", len(payload), response.url
    )
    return payload


def transform_observations_to_geojson(
    observations: list[dict],
    *,
    observation_type: str,
    region_code: str | None = None,
    search_lon: float | None = None,
    search_lat: float | None = None,
    observation_date: str | None = None,
) -> dict:
    """Convert eBird observation dicts into a GeoJSON FeatureCollection.

    Parameters
    ----------
    observations : list of dict
        Records from an eBird ``data/obs`` response.
    observation_type : str
        Dropdown value that produced this pull. Stored on each feature.
    region_code : str, optional
        Region code sent for a region pull.
    search_lon : float, optional
        Longitude sent for a nearby pull.
    search_lat : float, optional
        Latitude sent for a nearby pull.
    observation_date : str, optional
        ``YYYY-MM-DD`` sent for a historic pull.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection. Each feature id is ``obsId`` when the
        record has one, otherwise ``{subId}_{speciesCode}``.
    """
    features = []
    seen: set[str] = set()
    for observation in observations:
        feature_id = _observation_id(observation)
        if feature_id in seen:
            raise ValueError(f"Duplicate observation id {feature_id}.")
        seen.add(feature_id)
        properties = {column: observation.get(source) for source, column in _FIELD_MAP}
        properties["data_source"] = "eBird"
        properties["observation_type"] = observation_type
        if region_code is not None:
            properties["region_code"] = region_code
        if search_lon is not None and search_lat is not None:
            properties["search_lon"] = search_lon
            properties["search_lat"] = search_lat
        if observation_date is not None:
            properties["observation_date"] = observation_date
        features.append(
            {
                "type": "Feature",
                "id": feature_id,
                "geometry": geojson_point(
                    observation.get("lng"), observation.get("lat")
                ),
                "properties": properties,
            }
        )

    logger.info("Formatted %s observation(s) as GeoJSON features.", len(features))
    return {"type": "FeatureCollection", "features": features}


def write_observations(
    observations: list[dict],
    geojson: dict,
    db: postgresql,
    db_table_name: str,
    attachment_root: str,
) -> None:
    """Save the API response and GeoJSON, then write features to PostgreSQL."""
    save_path = Path(attachment_root) / db_table_name
    save_data_to_file(
        observations,
        f"{db_table_name}_observations",
        save_path,
        file_type="json",
    )
    if geojson["features"]:
        save_data_to_file(geojson, db_table_name, save_path, file_type="geojson")
        save_geojson_to_postgres(
            db,
            db_table_name,
            str(Path(db_table_name) / f"{db_table_name}.geojson"),
            attachment_root,
            delete_geojson_file=False,
        )
        logger.info(
            "eBird observations written to database table: [%s]",
            db_table_name,
        )
    else:
        logger.warning(
            "No observations returned; skipping database write for table: [%s]",
            db_table_name,
        )


def _require_api_token(api_token: str | None) -> str:
    if not isinstance(api_token, str) or not api_token.strip():
        raise ValueError("api_token is required.")
    return api_token


def _observation_id(observation: dict) -> str:
    obs_id = observation.get("obsId")
    if obs_id:
        return str(obs_id)
    sub_id = observation.get("subId")
    species_code = observation.get("speciesCode")
    if sub_id and species_code:
        return f"{sub_id}_{species_code}"
    raise ValueError(
        "Observation is missing obsId, so it needs both subId and speciesCode."
    )
