# requirements:
# psycopg[binary]
# requests~=2.32

import logging
import time
from pathlib import Path
from typing import Any

import requests

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

BASE_URL = "https://api.inaturalist.org/v1"
_PAGE_SIZE = 200
_PAGE_DELAY_S = 1.1  # stay at or below iNaturalist's requested 60 req/min

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    project_id: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    """
    Fetch public iNaturalist project observations and write them to the datalake
    and PostgreSQL.

    Parameters
    ----------
    project_id : str
        Project numeric ID or slug.
    db : postgresql
        Database connection configuration.
    db_table_name : str
        Database table name and datalake subdirectory.
    attachment_root : str
        Root directory for persisted files.
    """
    save_path = Path(attachment_root) / db_table_name

    project = download_project_metadata(project_id, db_table_name, attachment_root)
    if project:
        logger.info(
            "Fetched project metadata for '%s' (id=%s)",
            project.get("title") or project_id,
            project.get("id", project_id),
        )

    observations = download_observations(project_id)
    save_data_to_file(
        observations,
        f"{db_table_name}_observations",
        save_path,
        file_type="json",
    )

    geojson = transform_observations_to_geojson(observations, project_id)

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
            "iNaturalist observations written to database table: [%s]",
            db_table_name,
        )
    else:
        logger.warning(
            "No observations returned; skipping database write for table: [%s]",
            db_table_name,
        )


def download_project_metadata(
    project_id: str, db_table_name: str, attachment_root: str
) -> dict | None:
    """Fetch project metadata and save it to disk as JSON.

    Parameters
    ----------
    project_id : str
        Project numeric ID or slug.
    db_table_name : str
        Used as the subdirectory name under attachment_root.
    attachment_root : str
        Root directory for persisted files.

    Returns
    -------
    dict or None
        The first project result, or None if the request fails or returns nothing.
    """
    resp = requests.get(f"{BASE_URL}/projects/{project_id}", timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    results = payload.get("results") or []
    if not results:
        logger.warning("No project metadata found for '%s'", project_id)
        return None

    project = results[0]
    save_path = Path(attachment_root) / db_table_name
    save_data_to_file(project, f"{db_table_name}_project", save_path, "json")
    logger.info(
        "Project metadata saved to %s/%s_project.json", save_path, db_table_name
    )
    return project


def download_observations(project_id: str) -> list[dict[str, Any]]:
    """Fetch all publicly accessible observations for a project.

    Uses observation IDs as a pagination cursor (``id_above``) instead of page
    numbers, per iNaturalist API guidance for large result sets.

    Parameters
    ----------
    project_id : str
        Project numeric ID or slug.

    Returns
    -------
    list of dict
        All observation dicts returned by the API.
    """
    observations: list[dict[str, Any]] = []
    last_id: int | None = None
    params: dict[str, Any] = {
        "project_id": project_id,
        "per_page": _PAGE_SIZE,
        "order_by": "id",
        "order": "asc",
    }

    with requests.Session() as session:
        while True:
            if last_id is not None:
                params["id_above"] = last_id

            resp = session.get(
                f"{BASE_URL}/observations", params=params, timeout=60
            )
            resp.raise_for_status()
            payload = resp.json()
            batch = payload.get("results") or []

            if not batch:
                break

            observations.extend(batch)
            new_last_id = max(observation["id"] for observation in batch)

            if new_last_id == last_id:
                raise RuntimeError("Pagination cursor did not advance")

            last_id = new_last_id
            logger.info(
                "[%s] Fetched %s of %s observations",
                project_id,
                len(observations),
                payload.get("total_results", "unknown"),
            )

            if len(batch) < params["per_page"]:
                break

            time.sleep(_PAGE_DELAY_S)

    logger.info("[%s] Downloaded %s total observations.", project_id, len(observations))
    return observations


def _photo_url(observation: dict) -> str | None:
    """Return the first photo URL, preferring medium over square size."""
    photos = observation.get("photos") or []
    if not photos:
        return None
    url = photos[0].get("url")
    if not url:
        return None
    return url.replace("/square.", "/medium.")


def transform_observations_to_geojson(
    observations: list[dict], project_id: str
) -> dict:
    """Convert raw observation dicts into a GeoJSON FeatureCollection.

    Parameters
    ----------
    observations : list of dict
        Raw observation dicts from the iNaturalist API.
    project_id : str
        Project numeric ID or slug used for the pull.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection with flattened properties.
    """
    features = []
    for observation in observations:
        taxon = observation.get("taxon") or {}
        user = observation.get("user") or {}
        features.append(
            {
                "type": "Feature",
                "id": observation["id"],
                "geometry": observation.get("geojson"),
                "properties": {
                    "observation_id": observation["id"],
                    "observed_on": observation.get("observed_on"),
                    "quality_grade": observation.get("quality_grade"),
                    "species_guess": observation.get("species_guess"),
                    "taxon_id": taxon.get("id"),
                    "scientific_name": taxon.get("name"),
                    "common_name": taxon.get("preferred_common_name"),
                    "observer": user.get("login"),
                    "uri": observation.get("uri"),
                    "license_code": observation.get("license_code"),
                    "photo_url": _photo_url(observation),
                    "data_source": "iNaturalist",
                    "project_id": project_id,
                },
            }
        )

    logger.info("Formatted %s observation(s) as GeoJSON features.", len(features))
    return {"type": "FeatureCollection", "features": features}
