# requirements:
# psycopg[binary]
# requests~=2.32

import json
import logging
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

_API_V1 = "https://api.inaturalist.org/v1"
_API_V2 = "https://api.inaturalist.org/v2"
_PAGE_SIZE = 200
# https://www.inaturalist.org/pages/developers — max 100 req/min; please stay ≤60
# https://www.inaturalist.org/pages/api+recommended+practices — ~1 req/sec
_PAGE_DELAY_S = 1.1
_MEDIA_DELAY_S = 0.2
_VALID_SOURCES = frozenset({"project", "user"})
_BBOX_RANGES = {
    "swlat": (-90, 90),
    "nelat": (-90, 90),
    "swlng": (-180, 180),
    "nelng": (-180, 180),
}

# v2 silently drops unknown field names (HTTP 200, no error). This spec is the
# single source of truth for both the request and the transform.
_OBSERVATION_FIELDS = {
    "id": True,
    "uuid": True,
    "uri": True,
    "updated_at": True,
    "observed_on": True,
    "time_observed_at": True,
    "observed_time_zone": True,
    "quality_grade": True,
    "captive": True,
    "mappable": True,
    "geojson": True,
    "place_guess": True,
    "positional_accuracy": True,
    "public_positional_accuracy": True,
    "obscured": True,
    "geoprivacy": True,
    "taxon_geoprivacy": True,
    "species_guess": True,
    "description": True,
    "license_code": True,
    "identifications_count": True,
    "community_taxon_id": True,
    "num_identification_agreements": True,
    "num_identification_disagreements": True,
    "outlinks": {"source": True, "url": True},
    "taxon": {
        "id": True,
        "name": True,
        "rank": True,
        "rank_level": True,
        "preferred_common_name": True,
        "iconic_taxon_name": True,
    },
    "user": {"id": True, "login": True, "name": True, "orcid": True},
    "photos": {
        "id": True,
        "url": True,
        "license_code": True,
        "attribution": True,
    },
    "sounds": {
        "id": True,
        "file_url": True,
        "license_code": True,
        "file_content_type": True,
    },
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _encode_fields(spec: dict) -> str:
    """Serialize a nested field spec to iNaturalist API v2 ``fields`` syntax."""
    return (
        "("
        + ",".join(
            f"{k}:{_encode_fields(v) if isinstance(v, dict) else '!t'}"
            for k, v in spec.items()
        )
        + ")"
    )


def _field_spec_paths(spec: dict, prefix: str = "") -> set[str]:
    """Return dotted paths for every key in a nested field spec."""
    paths: set[str] = set()
    for key, value in spec.items():
        path = f"{prefix}.{key}" if prefix else key
        paths.add(path)
        if isinstance(value, dict):
            paths.update(_field_spec_paths(value, path))
    return paths


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    return text.strip() or None


def parse_bounding_box(bounding_box: str | list | None) -> dict[str, float] | None:
    """Parse a GFW-style JSON string ``[[west, south], [east, north]]``."""
    if bounding_box is None or bounding_box == "" or bounding_box == []:
        return None
    if isinstance(bounding_box, str):
        bounding_box = bounding_box.strip()
        if not bounding_box:
            return None
        try:
            bounding_box = json.loads(bounding_box)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "bounding_box must be a JSON list: [[west, south], [east, north]]."
            ) from exc
    if not isinstance(bounding_box, (list, tuple)):
        raise ValueError("bounding_box must be [[west, south], [east, north]].")
    if len(bounding_box) == 0:
        return None
    if len(bounding_box) != 2:
        raise ValueError(
            "bounding_box must contain exactly two [longitude, latitude] corners."
        )

    corners: list[tuple[float, float]] = []
    for point in bounding_box:
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            raise ValueError(
                "bounding_box must contain exactly two [longitude, latitude] corners."
            )
        try:
            corners.append((float(point[0]), float(point[1])))
        except (TypeError, ValueError) as exc:
            raise ValueError("bounding_box coordinates must be numeric.") from exc

    (lng_a, lat_a), (lng_b, lat_b) = corners
    parsed = {
        "swlng": min(lng_a, lng_b),
        "swlat": min(lat_a, lat_b),
        "nelng": max(lng_a, lng_b),
        "nelat": max(lat_a, lat_b),
    }
    for key, (low, high) in _BBOX_RANGES.items():
        if not low <= parsed[key] <= high:
            raise ValueError(f"bounding_box {key} must be between {low} and {high}.")
    if parsed["swlat"] >= parsed["nelat"]:
        raise ValueError("bounding_box swlat must be less than nelat.")
    if parsed["swlng"] >= parsed["nelng"]:
        raise ValueError("bounding_box swlng must be less than nelng.")
    return parsed


def main(
    source: str | None,
    slug: str | None,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
    bounding_box: str | list | None = None,
):
    """
    Fetch public iNaturalist observations for a project, user, and/or bounding
    box and write them to the datalake and PostgreSQL.

    Parameters
    ----------
    source : str, optional
        ``"project"`` or ``"user"``. Required when ``slug`` is provided.
    slug : str, optional
        Project numeric ID/slug when ``source`` is ``"project"``, or username
        when ``source`` is ``"user"``. Either ``slug`` or ``bounding_box``
        must be provided.
    db : postgresql
        Database connection configuration.
    db_table_name : str
        Database table name and datalake subdirectory.
    attachment_root : str
        Root directory for persisted files.
    bounding_box : str or list, optional
        JSON string ``[[west, south], [east, north]]`` (lng/lat), same shape
        as GFW. Combined with ``slug`` when both are provided.
    """
    source = _optional_text(source)
    slug = _optional_text(slug)
    bbox = parse_bounding_box(bounding_box)
    if slug is None and bbox is None:
        raise ValueError("Either `slug` or `bounding_box` must be provided.")

    filter_params: dict[str, Any] = {}
    project_id = None
    user_id = None

    if slug is not None:
        if source not in _VALID_SOURCES:
            raise ValueError(
                f"Invalid source '{source}'. Expected one of: {sorted(_VALID_SOURCES)}"
            )
        if source == "project":
            project_id = slug
            project = download_project_metadata(slug, db_table_name, attachment_root)
            if project:
                logger.info(
                    "Fetched project metadata for '%s' (id=%s)",
                    project.get("title") or slug,
                    project.get("id", slug),
                )
            filter_params["project_id"] = slug
        else:
            user_id = slug
            filter_params["user_id"] = slug

    if bbox is not None:
        filter_params.update(bbox)

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
    resp = requests.get(f"{_API_V1}/projects/{project_id}", timeout=60)
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
    numbers, per iNaturalist API guidance for large result sets. Requests only
    the fields in ``_OBSERVATION_FIELDS``.

    Parameters
    ----------
    filter_params : dict
        Extra query parameters such as ``project_id``, ``user_id``, or
        ``swlat`` / ``swlng`` / ``nelat`` / ``nelng``.

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
        "fields": _encode_fields(_OBSERVATION_FIELDS),
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

            resp = session.get(f"{_API_V2}/observations", params=params, timeout=60)
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
    """Save JSON + GeoJSON to the datalake and write features to PostgreSQL."""
    save_path = Path(attachment_root) / db_table_name
    save_data_to_file(
        observations,
        f"{db_table_name}_observations",
        save_path,
        file_type="json",
    )

    download_observation_media(observations, db_table_name, attachment_root)

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


def _media_filename(
    media: dict, url_key: str = "url", default_ext: str = ""
) -> str | None:
    """Return a stable local filename ``{media_id}{ext}`` for a photo or sound."""
    media_id = media.get("id")
    url = media.get(url_key)
    if media_id is None or not url:
        return None
    ext = Path(urlparse(url).path).suffix or default_ext
    return f"{media_id}{ext}"


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


def _joined_filenames(
    items: list[dict], url_key: str = "url", default_ext: str = ""
) -> str | None:
    names = [
        name for item in items if (name := _media_filename(item, url_key, default_ext))
    ]
    return ", ".join(names) if names else None


def _gbif_occurrence_id(observation: dict) -> str | None:
    """Extract the GBIF occurrence ID from ``outlinks``, if present."""
    for link in observation.get("outlinks") or []:
        if link.get("source") != "GBIF":
            continue
        url = link.get("url") or ""
        occurrence_id = url.rstrip("/").rsplit("/", 1)[-1]
        return occurrence_id or None
    return None


def _iter_media(observations: list[dict]):
    """Yield ``(download_url, filename)`` for every photo and sound."""
    for observation in observations:
        for photo in observation.get("photos") or []:
            url = photo.get("url")
            filename = _media_filename(photo, default_ext=".jpg")
            if url and filename:
                yield _sized_photo_url(url, "original"), filename
        for sound in observation.get("sounds") or []:
            url = sound.get("file_url")
            filename = _media_filename(sound, "file_url")
            if url and filename:
                yield url, filename


def download_observation_media(
    observations: list[dict],
    db_table_name: str,
    attachment_root: str,
) -> None:
    """Download photos and sounds to ``{attachment_root}/{db_table_name}/attachments/``.

    Photo URLs are rewritten to original size. Sound ``file_url`` values are
    used as-is. Files already on disk are skipped.
    """
    media = list(_iter_media(observations))
    photo_count = sum(len(obs.get("photos") or []) for obs in observations)
    sound_count = sum(len(obs.get("sounds") or []) for obs in observations)
    logger.info(
        "Starting media downloads: %s photo(s) and %s sound(s) across %s "
        "observation(s).",
        photo_count,
        sound_count,
        len(observations),
    )
    if not media:
        logger.info("No media to download.")
        return

    skipped = 0
    downloaded = 0
    failed = 0
    dest_dir = Path(attachment_root) / db_table_name / "attachments"

    with requests.Session() as session:
        for url, filename in media:
            save_path = dest_dir / filename
            if save_path.exists():
                logger.debug("Media already exists, skipping: %s", save_path)
                skipped += 1
                continue

            resp = session.get(url, timeout=60)
            if resp.status_code == 200:
                save_path.parent.mkdir(parents=True, exist_ok=True)
                save_path.write_bytes(resp.content)
                downloaded += 1
                logger.debug("Downloaded media: %s", filename)
            else:
                failed += 1
                logger.error(
                    "Failed to download media '%s' (HTTP %s)",
                    filename,
                    resp.status_code,
                )

            time.sleep(_MEDIA_DELAY_S)

    logger.info(
        "Finished media downloads: %s downloaded, %s skipped (already on disk), "
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
    """Convert observation dicts into a GeoJSON FeatureCollection.

    Feature ``properties`` are the fields in ``_OBSERVATION_FIELDS``, flattened
    for mapping and tabular use. The same curated records are written to the
    datalake as ``{db_table_name}_observations.json``.

    Parameters
    ----------
    observations : list of dict
        Observation dicts from the iNaturalist API v2 field spec.
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
        photos = observation.get("photos") or []
        sounds = observation.get("sounds") or []
        first_photo = photos[0] if photos else None
        properties = {
            "observation_id": observation["id"],
            "uuid": observation.get("uuid"),
            "updated_at": observation.get("updated_at"),
            "observed_on": observation.get("observed_on"),
            "time_observed_at": observation.get("time_observed_at"),
            "observed_time_zone": observation.get("observed_time_zone"),
            "quality_grade": observation.get("quality_grade"),
            "captive": observation.get("captive"),
            "species_guess": observation.get("species_guess"),
            "description": observation.get("description"),
            "taxon_id": taxon.get("id"),
            "scientific_name": taxon.get("name"),
            "common_name": taxon.get("preferred_common_name"),
            "taxon_rank": taxon.get("rank"),
            "taxon_rank_level": taxon.get("rank_level"),
            "iconic_taxon_name": taxon.get("iconic_taxon_name"),
            "observer": user.get("login"),
            "observer_id": user.get("id"),
            "observer_name": user.get("name"),
            "observer_orcid": user.get("orcid"),
            "uri": observation.get("uri"),
            "license_code": observation.get("license_code"),
            "place_guess": observation.get("place_guess"),
            "positional_accuracy": observation.get("positional_accuracy"),
            "public_positional_accuracy": observation.get("public_positional_accuracy"),
            "obscured": observation.get("obscured"),
            "geoprivacy": observation.get("geoprivacy"),
            "taxon_geoprivacy": observation.get("taxon_geoprivacy"),
            "mappable": observation.get("mappable"),
            "identifications_count": observation.get("identifications_count"),
            "community_taxon_id": observation.get("community_taxon_id"),
            "num_identification_agreements": observation.get(
                "num_identification_agreements"
            ),
            "num_identification_disagreements": observation.get(
                "num_identification_disagreements"
            ),
            "photo_count": len(photos),
            "sound_count": len(sounds),
            "photo_url": _photo_url(observation),
            "photo_filename": (
                _media_filename(first_photo, default_ext=".jpg")
                if first_photo
                else None
            ),
            "photo_filenames": _joined_filenames(photos, default_ext=".jpg"),
            "photo_license_code": (
                first_photo.get("license_code") if first_photo else None
            ),
            "photo_attribution": (
                first_photo.get("attribution") if first_photo else None
            ),
            "sound_filenames": _joined_filenames(sounds, "file_url"),
            "gbif_occurrence_id": _gbif_occurrence_id(observation),
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
