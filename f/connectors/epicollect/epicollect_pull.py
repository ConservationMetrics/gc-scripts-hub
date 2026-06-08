# requirements:
# psycopg[binary]
# requests~=2.32

import logging
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql
from f.common_logic.file_operations import save_data_to_file


BASE_URL = "https://five.epicollect.net"
_MEDIA_TYPES = frozenset({"photo", "audio", "video"})
_MEDIA_FORMAT = {"photo": "entry_original", "audio": "audio", "video": "video"}
_PAGE_SIZE = 250
_PAGE_DELAY_S = 1.0

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    project_slug: str,
    db: postgresql,
    db_table_name: str,
    client_id: int | None = None,
    client_secret: str | None = None,
    attachment_root: str = "/persistent-storage/datalake",
):
    """
    API auth is optional, as public EpiCollect5 projects work without credentials.
    """
    if client_id is not None and client_secret is not None:
        token = _get_access_token(client_id, client_secret)
        headers = {"Authorization": f"Bearer {token}"}
    else:
        headers = {}

    project_metadata = download_project_metadata(
        project_slug, headers, db_table_name, attachment_root
    )
    form_name = _extract_form_name(project_metadata)
    media_fields = _extract_media_fields(project_metadata)

    logo_url = project_metadata.get("data", {}).get("project", {}).get("logo_url", "")
    _download_project_logo(project_slug, logo_url, headers, db_table_name, attachment_root)

    entries = download_entries(
        project_slug, headers, db_table_name, attachment_root, media_fields
    )

    transformed = transform_epicollect_entries(entries, form_name=form_name)

    writer = StructuredDBWriter(
        conninfo(db),
        db_table_name,
        use_mapping_table=True,
        reverse_properties_separated_by="/",
    )
    writer.handle_output(transformed)
    logger.info(
        f"EpiCollect5 entries written to database table: [{db_table_name}]"
    )


def _get_access_token(client_id: int, client_secret: str) -> str:
    """Exchange client credentials for a Bearer token (valid 2 hours)."""
    resp = requests.post(
        f"{BASE_URL}/api/oauth/token",
        headers={"Content-Type": "application/vnd.api+json"},
        json={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def download_project_metadata(
    project_slug: str, headers: dict, db_table_name: str, attachment_root: str
) -> dict:
    """Fetch project definition and save it to disk as JSON.

    Parameters
    ----------
    project_slug : str
        The slugified project name.
    headers : dict
        Authorization headers.
    db_table_name : str
        Used as the subdirectory name under attachment_root.
    attachment_root : str
        Root directory for persisted files.

    Returns
    -------
    dict
        The raw project metadata response.
    """
    resp = requests.get(
        f"{BASE_URL}/api/export/project/{project_slug}",
        headers=headers,
    )
    resp.raise_for_status()
    metadata = resp.json()

    save_path = Path(attachment_root) / db_table_name
    save_data_to_file(metadata, f"{db_table_name}_metadata", save_path, "json")
    logger.info(f"Project metadata saved to {save_path}/{db_table_name}_metadata.json")

    return metadata


def _extract_form_name(project_metadata: dict) -> str | None:
    """Return the name of the first form in the project, or None."""
    try:
        return project_metadata["data"]["project"]["forms"][0]["name"]
    except (KeyError, IndexError):
        return None


def _extract_media_fields(project_metadata: dict) -> dict[str, str]:
    """Return a mapping of entry field name → media type for all media inputs.

    Uses the default project mapping to resolve input refs to entry field names.

    Parameters
    ----------
    project_metadata : dict
        The raw project metadata response.

    Returns
    -------
    dict
        ``{entry_field_name: media_type}`` for photo/audio/video inputs.
    """
    # Build input_ref -> type for media inputs across all forms
    ref_to_type: dict[str, str] = {}
    for form in project_metadata["data"]["project"]["forms"]:
        for inp in form["inputs"]:
            if inp["type"] in _MEDIA_TYPES:
                ref_to_type[inp["ref"]] = inp["type"]

    # Resolve refs to entry field names via the default mapping
    project_mappings = project_metadata["meta"]["project_mapping"]
    default_map = next(
        (m for m in project_mappings if m.get("is_default")), project_mappings[0]
    )

    media_fields: dict[str, str] = {}
    for _form_ref, form_fields in default_map["forms"].items():
        for input_ref, field_data in form_fields.items():
            if input_ref in ref_to_type:
                media_fields[field_data["map_to"]] = ref_to_type[input_ref]

    return media_fields


def _download_project_logo(
    project_slug: str,
    logo_url: str,
    headers: dict,
    db_table_name: str,
    attachment_root: str,
) -> None:
    """Download the project logo thumbnail if available.

    ``logo_url`` comes from ``data.project.logo_url`` in the project metadata.
    An empty string means no logo has been set — skip silently.
    """
    if not logo_url:
        return

    save_path = Path(attachment_root) / db_table_name / "logo.jpg"
    if save_path.exists():
        logger.debug(f"Project logo already exists, skipping: {save_path}")
        return

    resp = requests.get(
        f"{BASE_URL}/api/export/media/{project_slug}",
        headers=headers,
        params={"type": "photo", "format": "project_thumb", "name": "logo.jpg"},
    )
    if resp.status_code == 200:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(resp.content)
        logger.info(f"Project logo saved to {save_path}")
    else:
        logger.info(
            f"No project logo available for '{project_slug}' (HTTP {resp.status_code})"
        )


def _download_entry_media(
    project_slug: str,
    entry: dict,
    media_fields: dict[str, str],
    headers: dict,
    db_table_name: str,
    attachment_root: str,
) -> int:
    """Download and save media attachments for a single entry.

    Parameters
    ----------
    project_slug : str
        The project slug, used in the media API endpoint.
    entry : dict
        A single entry dict from the entries response.
    media_fields : dict
        ``{entry_field_name: media_type}`` mapping.
    headers : dict
        Authorization headers.
    db_table_name : str
        Used to construct the save path.
    attachment_root : str
        Root directory for persisted files.

    Returns
    -------
    int
        Number of files skipped (already on disk).
    """
    skipped = 0
    for field_name, media_type in media_fields.items():
        value = entry.get(field_name)
        if not value or not isinstance(value, str):
            continue

        # Public projects return the full media URL as the field value
        # (e.g. "https://five.epicollect.net/api/media/project?type=photo&...&name=uuid_ts.jpg").
        # Private projects return just the bare filename (e.g. "uuid_ts.jpg").
        # In the full-URL case we download directly and extract the filename
        # from the `name` query parameter for the local save path.
        if value.startswith("http"):
            qs = parse_qs(urlparse(value).query)
            filename = qs.get("name", [value])[0]
            # Normalise the entry field to the bare filename so the DB stores
            # just the name rather than the full URL that public projects return.
            entry[field_name] = filename
            download_kwargs: dict = {"url": value, "headers": headers}
        else:
            filename = value
            download_url = f"{BASE_URL}/api/export/media/{project_slug}"
            download_kwargs = {
                "url": download_url,
                "headers": headers,
                "params": {
                    "type": media_type,
                    "format": _MEDIA_FORMAT[media_type],
                    "name": filename,
                },
            }

        save_path = Path(attachment_root) / db_table_name / "attachments" / filename
        if save_path.exists():
            logger.debug(f"File already exists, skipping: {save_path}")
            skipped += 1
            continue

        resp = requests.get(**download_kwargs)
        if resp.status_code == 200:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_bytes(resp.content)
            logger.debug(f"Downloaded {media_type}: {filename}")
        else:
            logger.error(
                f"Failed to download {media_type} '{filename}' (HTTP {resp.status_code})"
            )

    return skipped


def download_entries(
    project_slug: str,
    headers: dict,
    db_table_name: str,
    attachment_root: str,
    media_fields: dict[str, str],
) -> list[dict]:
    """Download all entries across pages and save media attachments.

    Parameters
    ----------
    project_slug : str
        The project slug.
    headers : dict
        Authorization headers.
    db_table_name : str
        Used to construct attachment save paths.
    attachment_root : str
        Root directory for persisted files.
    media_fields : dict
        ``{entry_field_name: media_type}`` mapping.

    Returns
    -------
    list
        All entry dicts from the project.
    """
    all_entries: list[dict] = []
    page = 1

    while True:
        resp = requests.get(
            f"{BASE_URL}/api/export/entries/{project_slug}",
            headers=headers,
            params={
                "per_page": _PAGE_SIZE,
                "page": page,
                "sort_by": "created_at",
                "sort_order": "ASC",
            },
        )
        resp.raise_for_status()
        data = resp.json()

        meta = data["meta"]
        entries = data["data"]["entries"]
        all_entries.extend(entries)

        logger.info(
            f"[{project_slug}] Fetched {len(entries)} entries "
            f"(page {meta['current_page']}/{meta['last_page']})"
        )

        skipped = sum(
            _download_entry_media(
                project_slug, entry, media_fields, headers, db_table_name, attachment_root
            )
            for entry in entries
        )
        if skipped:
            logger.info(f"Skipped {skipped} already-downloaded media file(s).")

        if meta["current_page"] >= meta["last_page"]:
            break

        page += 1
        time.sleep(_PAGE_DELAY_S)

    logger.info(
        f"[{project_slug}] Downloaded {len(all_entries)} total entries."
    )
    return all_entries


def _extract_location(entry: dict) -> list[float] | None:
    """Return ``[lon, lat]`` from the first valid location field in the entry.

    EpiCollect5 location fields are dicts with ``latitude`` / ``longitude`` keys.
    Empty-string values indicate no GPS fix and are skipped.
    """
    for value in entry.values():
        if not isinstance(value, dict):
            continue
        if "latitude" not in value or "longitude" not in value:
            continue
        try:
            lat = float(value["latitude"])
            lon = float(value["longitude"])
            return [lon, lat]
        except (ValueError, TypeError):
            continue
    return None


def transform_epicollect_entries(
    entries: list[dict], form_name: str | None = None
) -> list[dict]:
    """Add metadata fields and extract geometry for DB insertion.

    Parameters
    ----------
    entries : list
        Raw entry dicts from the EpiCollect5 API.
    form_name : str, optional
        Human-readable form name. If provided, added as ``dataset_name``.

    Returns
    -------
    list
        Transformed entries ready for StructuredDBWriter.
    """
    for entry in entries:
        entry["_id"] = entry.pop("ec5_uuid")
        entry["data_source"] = "EpiCollect5"
        if form_name:
            entry["dataset_name"] = form_name

        coordinates = _extract_location(entry)
        if coordinates:
            entry.update({"g__type": "Point", "g__coordinates": coordinates})

    return entries
