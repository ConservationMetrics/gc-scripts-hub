# requirements:
# psycopg[binary]
# requests~=2.32

import logging
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

BASE_URL = "https://api.inaturalist.org/v1"
_PAGE_SIZE = 200
# https://www.inaturalist.org/pages/developers — max 100 req/min; please stay ≤60
# https://www.inaturalist.org/pages/api+recommended+practices — ~1 req/sec
_PAGE_DELAY_S = 1.1
_PHOTO_DELAY_S = 0.2
_VALID_SOURCES = frozenset({"project", "user"})

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    source: str,
    slug: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    """
    Fetch public iNaturalist observations for a project or user and write them
    to the datalake and PostgreSQL.

    Parameters
    ----------
    source : str
        ``"project"`` or ``"user"``.
    slug : str
        Project numeric ID/slug when ``source`` is ``"project"``, or username
        when ``source`` is ``"user"``.
    db : postgresql
        Database connection configuration.
    db_table_name : str
        Database table name and datalake subdirectory.
    attachment_root : str
        Root directory for persisted files.
    """
    if source not in _VALID_SOURCES:
        raise ValueError(
            f"Invalid source '{source}'. Expected one of: {sorted(_VALID_SOURCES)}"
        )

    project_id = None
    user_id = None

    if source == "project":
        project_id = slug
        project = download_project_metadata(slug, db_table_name, attachment_root)
        if project:
            logger.info(
                "Fetched project metadata for '%s' (id=%s)",
                project.get("title") or slug,
                project.get("id", slug),
            )
        filter_params = {"project_id": slug}
    else:
        user_id = slug
        filter_params = {"user_id": slug}

    observations = download_observations(filter_params)
    write_observations(
        observations,
        db,
        db_table_name,
        attachment_root,
        project_id=project_id,
        user_id=user_id,
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


def download_observations(filter_params: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch publicly accessible observations with cursor pagination.

    Uses observation IDs as a pagination cursor (``id_above``) instead of page
    numbers, per iNaturalist API guidance for large result sets.

    Parameters
    ----------
    filter_params : dict
        Extra query parameters such as ``project_id`` or ``user_id``.

    Returns
    -------
    list of dict
        All observation dicts returned by the API.
    """
    observations: list[dict[str, Any]] = []
    last_id: int | None = None
    params: dict[str, Any] = {
        **filter_params,
        "per_page": _PAGE_SIZE,
        "order_by": "id",
        "order": "asc",
    }
    label = (
        filter_params.get("project_id")
        or filter_params.get("user_id")
        or "observations"
    )

    with requests.Session() as session:
        while True:
            if last_id is not None:
                params["id_above"] = last_id

            resp = session.get(f"{BASE_URL}/observations", params=params, timeout=60)
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
                label,
                len(observations),
                payload.get("total_results", "unknown"),
            )

            if len(batch) < params["per_page"]:
                break

            time.sleep(_PAGE_DELAY_S)

    logger.info("[%s] Downloaded %s total observations.", label, len(observations))
    return observations


def write_observations(
    observations: list[dict],
    db: postgresql,
    db_table_name: str,
    attachment_root: str,
    *,
    project_id: str | None = None,
    user_id: str | None = None,
) -> None:
    """Save raw JSON + GeoJSON to the datalake and write features to PostgreSQL."""
    save_path = Path(attachment_root) / db_table_name
    save_data_to_file(
        observations,
        f"{db_table_name}_observations",
        save_path,
        file_type="json",
    )

    download_observation_photos(observations, db_table_name, attachment_root)

    geojson = transform_observations_to_geojson(
        observations, project_id=project_id, user_id=user_id
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
            "iNaturalist observations written to database table: [%s]",
            db_table_name,
        )
    else:
        logger.warning(
            "No observations returned; skipping database write for table: [%s]",
            db_table_name,
        )


def _sized_photo_url(url: str, size: str) -> str:
    """Rewrite an iNaturalist square thumbnail URL to another size variant."""
    return url.replace("/square.", f"/{size}.")


def _photo_filename(photo: dict) -> str | None:
    """Return a stable local filename ``{photo_id}{ext}`` for a photo dict."""
    photo_id = photo.get("id")
    url = photo.get("url")
    if photo_id is None or not url:
        return None
    ext = Path(urlparse(url).path).suffix or ".jpg"
    return f"{photo_id}{ext}"


def _first_photo(observation: dict) -> dict | None:
    photos = observation.get("photos") or []
    return photos[0] if photos else None


def _photo_url(observation: dict) -> str | None:
    """Return the first photo URL, preferring medium over square size."""
    photo = _first_photo(observation)
    if not photo:
        return None
    url = photo.get("url")
    if not url:
        return None
    return _sized_photo_url(url, "medium")


def _photo_filename_for_observation(observation: dict) -> str | None:
    """Return the local filename for the observation's first photo, if any."""
    photo = _first_photo(observation)
    return _photo_filename(photo) if photo else None


def download_observation_photos(
    observations: list[dict],
    db_table_name: str,
    attachment_root: str,
) -> None:
    """Download observation photos to ``{attachment_root}/{db_table_name}/attachments/``.

    Uses the original-size CDN URL. Files already on disk are skipped.
    """
    photo_count = sum(len(obs.get("photos") or []) for obs in observations)
    logger.info(
        "Starting photo downloads: %s photo(s) across %s observation(s).",
        photo_count,
        len(observations),
    )
    if photo_count == 0:
        logger.info("No photos to download.")
        return

    skipped = 0
    downloaded = 0
    failed = 0

    for observation in observations:
        for photo in observation.get("photos") or []:
            url = photo.get("url")
            filename = _photo_filename(photo)
            if not url or not filename:
                continue

            save_path = Path(attachment_root) / db_table_name / "attachments" / filename
            if save_path.exists():
                logger.debug("Photo already exists, skipping: %s", save_path)
                skipped += 1
                continue

            download_url = _sized_photo_url(url, "original")
            resp = requests.get(download_url, timeout=60)
            if resp.status_code == 200:
                save_path.parent.mkdir(parents=True, exist_ok=True)
                save_path.write_bytes(resp.content)
                downloaded += 1
                logger.debug("Downloaded photo: %s", filename)
            else:
                failed += 1
                logger.error(
                    "Failed to download photo '%s' (HTTP %s)",
                    filename,
                    resp.status_code,
                )

            time.sleep(_PHOTO_DELAY_S)

    logger.info(
        "Finished photo downloads: %s downloaded, %s skipped (already on disk), "
        "%s failed.",
        downloaded,
        skipped,
        failed,
    )


def transform_observations_to_geojson(
    observations: list[dict],
    *,
    project_id: str | None = None,
    user_id: str | None = None,
) -> dict:
    """Convert raw observation dicts into a GeoJSON FeatureCollection.

    Feature ``properties`` are an opinionated subset chosen for mapping and
    tabular use in Guardian Connector: identity, timing, taxon names, observer,
    license, photo linkage, and the pull filter (``project_id`` / ``user_id``).
    Nested API payloads (identifications, annotations, all photos, etc.) are
    omitted here; the complete observation records are still written to the
    datalake as ``{db_table_name}_observations.json``.

    Parameters
    ----------
    observations : list of dict
        Raw observation dicts from the iNaturalist API.
    project_id : str, optional
        Project numeric ID or slug used for the pull.
    user_id : str, optional
        Username used for the pull.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection with flattened properties.
    """
    features = []
    for observation in observations:
        taxon = observation.get("taxon") or {}
        user = observation.get("user") or {}
        properties = {
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
            "photo_filename": _photo_filename_for_observation(observation),
            "data_source": "iNaturalist",
        }
        if project_id is not None:
            properties["project_id"] = project_id
        if user_id is not None:
            properties["user_id"] = user_id

        features.append(
            {
                "type": "Feature",
                "id": observation["id"],
                "geometry": observation.get("geojson"),
                "properties": properties,
            }
        )

    logger.info("Formatted %s observation(s) as GeoJSON features.", len(features))
    return {"type": "FeatureCollection", "features": features}
