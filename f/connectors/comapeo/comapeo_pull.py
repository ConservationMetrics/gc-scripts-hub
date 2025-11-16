# requirements:
# psycopg2-binary
# requests~=2.32

import json
import logging
import mimetypes
from collections import Counter
from os import listdir
from pathlib import Path
from typing import TypedDict

import requests

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file
from f.common_logic.identifier_utils import (
    normalize_and_snakecase_keys,
    normalize_identifier,
)
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres


class comapeo_server(TypedDict):
    server_url: str
    access_token: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    comapeo: comapeo_server,
    comapeo_project_blocklist: list,
    db: postgresql,
    db_table_prefix: str = "comapeo",
    attachment_root: str = "/persistent-storage/datalake",
):
    server_url = comapeo["server_url"]
    access_token = comapeo["access_token"]

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    comapeo_projects = fetch_comapeo_projects(
        server_url, session, comapeo_project_blocklist
    )

    # Run culminates in success if there were no projects returned by the API
    if len(comapeo_projects) == 0:
        logger.info(
            "No projects fetched. Skipping data processing and database writing."
        )
        return

    logger.info(f"Fetched {len(comapeo_projects)} projects.")

    comapeo_projects_geojson, stats = download_and_transform_comapeo_data(
        server_url,
        session,
        comapeo_projects,
        attachment_root,
    )

    logger.info(
        f"Downloaded CoMapeo data for {len(comapeo_projects_geojson)} project data types. Saving data..."
    )

    for (project_name, data_type), geojson in comapeo_projects_geojson.items():
        storage_path = Path(attachment_root) / db_table_prefix / project_name
        table_suffix = f"_{data_type}"
        rel_geojson_path = (
            Path(db_table_prefix)
            / project_name
            / f"{project_name}{table_suffix}.geojson"
        )

        if geojson.get("features"):
            logger.info(
                f"Saving {data_type} for project {project_name}...",
            )
            save_data_to_file(
                geojson,
                f"{project_name}{table_suffix}",
                storage_path,
                file_type="geojson",
            )

            save_geojson_to_postgres(
                db,
                db_table_prefix + "_" + project_name + table_suffix,
                rel_geojson_path,
                attachment_root,
                False,
            )  # Do not delete the file after saving to Postgres
        else:
            logger.info(
                f"No {data_type} features found in project {project_name}. Nothing to save."
            )

    if stats["attachment_failed"] > 0:
        raise RuntimeError("Some attachments failed to download.")


def fetch_comapeo_projects(server_url, session, comapeo_project_blocklist):
    """
    Fetches a list of projects from the CoMapeo API, excluding any projects
    specified in the blocklist.

    Parameters
    ----------
    server_url : str
        The base URL of the CoMapeo server.
    session : requests.Session
        A requests session with authentication headers configured.
    comapeo_project_blocklist : list
        A list of project IDs to be excluded from the fetched results.

    Returns
    -------
    list
        A list of dictionaries, each containing the 'project_id' and 'project_name'
        of a project fetched from the CoMapeo API, excluding those in the blocklist.
    """

    url = f"{server_url}/projects"
    logger.info("Fetching projects from CoMapeo API...")
    response = session.get(url)

    response.raise_for_status()
    results = response.json().get("data", [])

    comapeo_projects = [
        {
            "project_id": res.get("projectId"),
            "project_name": res.get("name"),
        }
        for res in results
    ]

    if comapeo_project_blocklist:
        logger.info(f"Blocked projects found: {comapeo_project_blocklist}")
        comapeo_projects = [
            project
            for project in comapeo_projects
            if project["project_id"] not in comapeo_project_blocklist
        ]

    return comapeo_projects


def build_existing_file_set(directory):
    """
    Builds a set of existing file names (without extensions) in the specified directory.
    This is used to check for existing files before downloading new ones.
    """
    files = listdir(directory) if Path(directory).exists() else []
    return {Path(f).stem for f in files}  # just base names, no extensions


def download_file(url, session, save_path, existing_file_stems):
    """
    Downloads a file from a specified URL and saves it to a given path.

    Parameters
    ----------
    url : str
        The URL of the file to be downloaded.
    session : requests.Session
        A requests session with authentication headers configured.
    save_path : str
        The file system path where the downloaded file will be saved (without extension).
    existing_file_stems : set
        A set of existing file names (without extensions) to check against before downloading.

    Returns
    -------
    tuple
        A tuple containing two values:
        - The name of the file if the download is successful, or None if an error occurs.
        - The number of files skipped due to already existing on disk.

    Notes
    -----
    If the file already exists at the specified path, the function will skip downloading the file.

    The function attempts to determine the file extension based on the 'Content-Type'
    header of the HTTP response. If the 'Content-Type' is not recognized,
    the file will be saved without an extension.

    The function intentionally does not raise exceptions. Instead, it logs errors and returns None,
    allowing the caller to handle the download failure gracefully.
    """
    skipped_count = 0
    base_name = Path(save_path).name
    base_stem = Path(save_path).stem

    if base_stem in existing_file_stems:
        logger.debug(f"{base_stem} already exists, skipping download.")
        skipped_count += 1
        # Try to find matching full filename (with extension)
        full_path = next(
            (f for f in Path(save_path).parent.glob(f"{base_stem}.*")), None
        )
        return (full_path.name if full_path else base_name), skipped_count

    try:
        response = session.get(url)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")
        extension = mimetypes.guess_extension(content_type) if content_type else None

        # PNG is the default image type for icons c.f. https://github.com/digidem/comapeo-core/blob/main/src/icon-api.js
        if not extension:
            extension = ".png"

        file_name = base_name + extension
        save_path = Path(str(save_path) + extension)

        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(response.content)
        logger.info("Download completed.")
        return file_name, skipped_count

    except Exception as e:
        logger.error(f"Exception during download: {e}")
        return None, 0


def _fetch_comapeo_data(server_url, session, project_id, endpoint, data_type):
    """Fetch data from a CoMapeo API endpoint.

    Parameters
    ----------
    server_url : str
        The base URL of the CoMapeo server.
    session : requests.Session
        A requests session with authentication headers configured.
    project_id : str
        The unique identifier of the project.
    endpoint : str
        The endpoint path (e.g., "observation" or "track").
    data_type : str
        The type of data being fetched (e.g., "observations" or "tracks") for logging.

    Returns
    -------
    list
        A list of data items from the API response.
    """
    url = f"{server_url}/projects/{project_id}/{endpoint}"

    logger.info(f"Fetching {data_type} for project (ID: {project_id})...")
    response = session.get(url)

    try:
        data = response.json().get("data", [])
    except requests.exceptions.JSONDecodeError:
        logger.error("Failed to decode JSON from response.")
        logger.info("Response received: ", response.text)
        raise ValueError("Invalid JSON response received from server.")

    return data


def download_project_observations(
    server_url, session, project_id, project_name, attachment_root
):
    """Download observations and their attachments for a specific project from the CoMapeo API.

    Parameters
    ----------
    server_url : str
        The base URL of the CoMapeo server.
    session : requests.Session
        A requests session with authentication headers configured.
    project_id : str
        The unique identifier of the project.
    project_name : str
        The name of the project.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    tuple
        A tuple containing (observations, stats) where stats is a Counter with:
        - 'skipped_attachments': The number of attachments skipped due to already existing on disk.
        - 'attachment_failed': The number of attachment downloads that failed.
    """
    observations = _fetch_comapeo_data(
        server_url, session, project_id, "observation", "observations"
    )

    # Download attachments for all observations
    sanitized_project_name = normalize_identifier(project_name)
    attachment_dir = (
        Path(attachment_root) / "comapeo" / sanitized_project_name / "attachments"
    )
    existing_file_stems = build_existing_file_set(attachment_dir)

    stats = Counter()

    for observation in observations:
        if "attachments" in observation:
            filenames = []
            for attachment in observation["attachments"]:
                if "url" in attachment:
                    file_name, skipped = download_file(
                        attachment["url"],
                        session,
                        str(attachment_dir / Path(attachment["url"]).name),
                        existing_file_stems,
                    )
                    stats["skipped_attachments"] += skipped
                    if file_name is not None:
                        filenames.append(file_name)
                    else:
                        logger.error(
                            f"Attachment download failed for URL: {attachment['url']}. Skipping attachment."
                        )
                        stats["attachment_failed"] += 1

            observation["attachments"] = ", ".join(filenames)

    return observations, stats


def download_project_tracks(server_url, session, project_id):
    """Download tracks for a specific project from the CoMapeo API.

    Note: Tracks do not have attachments, so no attachment downloading is performed.

    Parameters
    ----------
    server_url : str
        The base URL of the CoMapeo server.
    session : requests.Session
        A requests session with authentication headers configured.
    project_id : str
        The unique identifier of the project.

    Returns
    -------
    list
        A list of track data from the CoMapeo API.
    """
    return _fetch_comapeo_data(server_url, session, project_id, "track", "tracks")


def fetch_preset(
    server_url,
    session,
    project_id,
    preset_doc_id,
    icon_dir=None,
    existing_icon_stems=None,
):
    """Fetch a preset from the CoMapeo API and optionally download its icon.

    Parameters
    ----------
    server_url : str
        The base URL of the CoMapeo server.
    session : requests.Session
        A requests session with authentication headers configured.
    project_id : str
        The unique identifier of the project.
    preset_doc_id : str
        The document ID of the preset to fetch.
    icon_dir : Path, optional
        Directory where icons should be saved. If provided, icons will be downloaded.
    existing_icon_stems : set, optional
        Set of existing icon file names (without extensions) to check against before downloading.

    Returns
    -------
    tuple
        A tuple containing:
        - preset_data (dict or None): The preset data if successful, None otherwise.
        - skipped (int): Number of icons skipped (0 or 1).
        - failed (bool): Whether icon download failed.
    """
    url = f"{server_url}/projects/{project_id}/preset/{preset_doc_id}"

    try:
        response = session.get(url)
        response.raise_for_status()
        preset_data = response.json().get("data")

        # Download icon if preset data exists and icon_dir is provided
        if preset_data and icon_dir and existing_icon_stems is not None:
            icon_ref = preset_data.get("iconRef")
            if icon_ref and "url" in icon_ref:
                preset_name = preset_data.get("name", "")
                if preset_name:
                    sanitized_name = normalize_identifier(preset_name)
                    icon_save_path = icon_dir / sanitized_name
                    file_name, skipped = download_file(
                        icon_ref["url"],
                        session,
                        str(icon_save_path),
                        existing_icon_stems,
                    )
                    if file_name is None:
                        logger.error(
                            f"Icon download failed for URL: {icon_ref['url']}. Preset: {preset_name}."
                        )
                        return preset_data, 0, True
                    return preset_data, skipped, False
        return preset_data, 0, False
    except (
        requests.exceptions.RequestException,
        requests.exceptions.JSONDecodeError,
    ) as e:
        logger.warning(f"Failed to fetch preset {preset_doc_id}: {e}")
        return None, 0, False


def fetch_all_presets(server_url, session, project_id):
    """Fetch all presets for a project from the CoMapeo API.

    Parameters
    ----------
    server_url : str
        The base URL of the CoMapeo server.
    session : requests.Session
        A requests session with authentication headers configured.
    project_id : str
        The unique identifier of the project.

    Returns
    -------
    list
        A list of preset data dictionaries from the API response.
    """
    url = f"{server_url}/projects/{project_id}/preset"
    logger.info(f"Fetching all presets for project (ID: {project_id})...")

    try:
        response = session.get(url)
        response.raise_for_status()
        data = response.json().get("data", [])
        logger.info(f"Fetched {len(data)} presets for project {project_id}.")
        return data
    except (
        requests.exceptions.RequestException,
        requests.exceptions.JSONDecodeError,
    ) as e:
        logger.warning(f"Failed to fetch presets for project {project_id}: {e}")
        return []


def download_preset_icons(presets, icon_dir, session):
    """Download all icons from a list of presets.

    Parameters
    ----------
    presets : list
        A list of preset data dictionaries.
    icon_dir : Path
        Directory where icons should be saved.
    session : requests.Session
        A requests session with authentication headers configured.

    Returns
    -------
    tuple
        A tuple containing:
        - stats (Counter): Statistics with 'skipped_icons' and 'icon_failed' counts.
        - icon_filenames (dict): Mapping of preset docId to actual downloaded filename (e.g., "camp.png").
    """
    stats = Counter()
    icon_filenames = {}
    existing_icon_stems = build_existing_file_set(icon_dir)

    for preset in presets:
        preset_doc_id = preset.get("docId")
        icon_ref = preset.get("iconRef")
        preset_name = preset.get("name", "")

        if icon_ref and "url" in icon_ref and preset_name and preset_doc_id:
            sanitized_name = normalize_identifier(preset_name)
            icon_save_path = icon_dir / sanitized_name
            file_name, skipped = download_file(
                icon_ref["url"],
                session,
                str(icon_save_path),
                existing_icon_stems,
            )
            stats["skipped_icons"] += skipped
            if file_name is None:
                logger.error(
                    f"Icon download failed for URL: {icon_ref['url']}. Preset: {preset_name}."
                )
                stats["icon_failed"] += 1
            else:
                # Store the actual filename with extension
                icon_filenames[preset_doc_id] = file_name
                # Add to existing set for subsequent downloads in the same batch
                existing_icon_stems.add(sanitized_name)

    return stats, icon_filenames


def build_preset_mapping(presets, icon_filenames=None):
    """Build a mapping from preset docId to relevant fields for observations.

    Parameters
    ----------
    presets : list
        A list of preset data dictionaries.
    icon_filenames : dict, optional
        A mapping of preset docId to actual icon filename (e.g., "camp.png").
        If not provided, icon_filename will be set to just the sanitized name.

    Returns
    -------
    dict
        A dictionary mapping preset docId to a dict containing:
        - 'name': The preset name (used as category)
        - 'terms': Comma-separated terms string
        - 'color': The preset color
        - 'icon_filename': The actual icon filename (e.g., 'camp.png')
    """
    mapping = {}

    for preset in presets:
        doc_id = preset.get("docId")
        if not doc_id:
            continue

        preset_data = {}

        # Extract name (used as category)
        if "name" in preset:
            preset_data["name"] = preset["name"]
            # Use actual icon filename if available, otherwise use sanitized name
            if icon_filenames and doc_id in icon_filenames:
                preset_data["icon_filename"] = icon_filenames[doc_id]
            else:
                preset_data["icon_filename"] = normalize_identifier(preset["name"])

        # Extract terms as comma-separated string
        if "terms" in preset and isinstance(preset["terms"], list):
            preset_data["terms"] = ", ".join(preset["terms"])

        # Extract color
        if "color" in preset:
            preset_data["color"] = preset["color"]

        mapping[doc_id] = preset_data

    return mapping


def _apply_preset_data(item, preset_ref, server_url, session, project_id):
    """Apply preset data to an item if preset reference is available.

    Note: This function does not download icons. For observations, use the inline
    preset fetching in transform_comapeo_observations to enable icon downloading.

    Parameters
    ----------
    item : dict
        The item to apply preset data to (will be modified in place).
    preset_ref : dict or None
        The preset reference from the item.
    server_url : str
        The base URL of the CoMapeo server.
    session : requests.Session
        A requests session with authentication headers configured.
    project_id : str
        The unique identifier of the project.
    """
    if preset_ref and server_url and session and project_id:
        preset_doc_id = preset_ref.get("docId")
        if preset_doc_id:
            preset_data, _, _ = fetch_preset(
                server_url, session, project_id, preset_doc_id
            )
            if preset_data:
                # Add name as category
                if "name" in preset_data:
                    item["category"] = preset_data["name"]
                # Add terms as comma-separated string
                if "terms" in preset_data and isinstance(preset_data["terms"], list):
                    item["terms"] = ", ".join(preset_data["terms"])
                # Add color
                if "color" in preset_data:
                    item["color"] = preset_data["color"]
                # NOTE: presetRef returns much more than this (c.f. SAMPLE_PRESETS in tests/assets/server_responses.py)


def _add_project_metadata(item, project_name, project_id):
    """Add project-specific metadata to an item.

    Parameters
    ----------
    item : dict
        The item to add metadata to (will be modified in place).
    project_name : str
        The name of the project.
    project_id : str or None
        The unique identifier of the project.
    """
    item["project_name"] = project_name
    if project_id is not None:
        item["project_id"] = project_id
    item["data_source"] = "CoMapeo"


def transform_comapeo_observations(
    observations,
    project_name,
    project_id=None,
    preset_mapping=None,
):
    """Transform CoMapeo observations into GeoJSON features with proper metadata and geometry formatting.

    Parameters
    ----------
    observations : list
        A list of raw observation data from the CoMapeo API.
    project_name : str
        The name of the project these observations belong to.
    project_id : str, optional
        The unique identifier of the project. If not provided, this field will be omitted from the output.
    preset_mapping : dict, optional
        A dictionary mapping preset docId to preset data (name, terms, color, icon_filename).
        If not provided, preset data will not be added to observations.

    Returns
    -------
    features : list
        A list of GeoJSON Feature objects with transformed properties and geometry.
    """
    features = []

    for observation in observations:
        observation = observation.copy()

        # Flatten tags into observation
        for key, value in observation.pop("tags", {}).items():
            observation[key] = value

        # Extract and flatten metadata fields
        metadata = observation.pop("metadata", {})
        if metadata:
            observation["manual_location"] = metadata.get("manualLocation")
            position = metadata.get("position", {})
            if position:
                observation["position_timestamp"] = position.get("timestamp")
                coords = position.get("coords", {})
                if coords:
                    observation["altitude"] = coords.get("altitude")
                    observation["altitude_accuracy"] = coords.get("altitudeAccuracy")
                    observation["heading"] = coords.get("heading")
                    observation["speed"] = coords.get("speed")
                    observation["accuracy"] = coords.get("accuracy")
                observation["mocked"] = position.get("mocked")

        # Apply preset data from mapping
        preset_ref = observation.pop("presetRef", None)
        if preset_ref and preset_mapping:
            preset_doc_id = preset_ref.get("docId")
            if preset_doc_id and preset_doc_id in preset_mapping:
                preset_data = preset_mapping[preset_doc_id]
                # Add name as category
                if "name" in preset_data:
                    observation["category"] = preset_data["name"]
                # Add terms as comma-separated string
                if "terms" in preset_data:
                    observation["terms"] = preset_data["terms"]
                # Add color
                if "color" in preset_data:
                    observation["color"] = preset_data["color"]
                # Add icon filename as category_icon
                if "icon_filename" in preset_data:
                    observation["category_icon"] = preset_data["icon_filename"]

        # Convert all keys (except docId) from camelCase to snake_case
        special_case_keys = {"docId"}
        observation = normalize_and_snakecase_keys(observation, special_case_keys)

        # Add project-specific information
        _add_project_metadata(observation, project_name, project_id)

        # Create GeoJSON Feature
        # Currently, only Point observations with lat and lon fields are returned by the CoMapeo API
        # Other geometry types and formats may be added in the future
        feature = {
            "type": "Feature",
            # docId is the unique identifier for the observation. We'll use it as the ID for the feature
            # and also include it in the properties
            "id": observation["docId"],
            "properties": {
                k: str(v) for k, v in observation.items() if k not in ["lat", "lon"]
            },
            "geometry": {
                "type": "Point",
                "coordinates": [observation["lon"], observation["lat"]],
            }
            if "lat" in observation and "lon" in observation
            else None,
        }

        features.append(feature)

    return features


def transform_comapeo_tracks(
    tracks, project_name, project_id=None, server_url=None, session=None
):
    """Transform CoMapeo tracks into GeoJSON features with LineString geometry and timestamps.

    Parameters
    ----------
    tracks : list
        A list of raw track data from the CoMapeo API.
    project_name : str
        The name of the project these tracks belong to.
    project_id : str, optional
        The unique identifier of the project. If not provided, this field will be omitted from the output.
    server_url : str, optional
        The base URL of the CoMapeo server. Required for preset fetching.
    session : requests.Session, optional
        A requests session with authentication headers configured. Required for preset fetching.

    Returns
    -------
    list
        A list of GeoJSON Feature objects with transformed properties, LineString geometry, and timestamps.
    """
    features = []

    for track in tracks:
        track = track.copy()

        # Flatten tags into track
        for key, value in track.pop("tags", {}).items():
            track[key] = value

        # Extract locations and build coordinates and timestamps arrays
        locations = track.pop("locations", [])
        coordinates = []
        timestamps = []

        for location in locations:
            coords = location.get("coords", {})
            if "latitude" in coords and "longitude" in coords:
                coordinates.append([coords["longitude"], coords["latitude"]])
                timestamps.append(location.get("timestamp"))

        # Fetch and extract preset data
        preset_ref = track.pop("presetRef", None)
        _apply_preset_data(track, preset_ref, server_url, session, project_id)

        # Convert all keys (except docId) from camelCase to snake_case
        special_case_keys = {"docId"}
        track = normalize_and_snakecase_keys(track, special_case_keys)

        # Add project-specific information
        _add_project_metadata(track, project_name, project_id)

        # Add timestamps as a JSON string array in properties
        track["timestamps"] = json.dumps(timestamps) if timestamps else "[]"

        # Create GeoJSON Feature with LineString geometry
        feature = {
            "type": "Feature",
            # docId is the unique identifier for the track. We'll use it as the ID for the feature
            # and also include it in the properties
            "id": track["docId"],
            "properties": {k: str(v) for k, v in track.items()},
            "geometry": {
                "type": "LineString",
                "coordinates": coordinates,
            }
            if coordinates
            else None,
        }

        features.append(feature)

    return features


def download_and_transform_comapeo_data(
    server_url,
    session,
    comapeo_projects,
    attachment_root,
):
    """
    Downloads and transforms CoMapeo project observations and tracks from the API, converting them into GeoJSON FeatureCollection format and downloading any associated attachments.

    Parameters
    ----------
    server_url : str
        The base URL of the CoMapeo server.
    session : requests.Session
        A requests session with authentication headers configured.
    comapeo_projects : list
        A list of dictionaries, each containing 'project_id' and 'project_name' for the projects to be processed.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    tuple
        A tuple containing:
        - comapeo_data : dict
            A dictionary where keys are tuples of (project_name, data_type) and values are GeoJSON FeatureCollections.
            data_type is either "observations" or "tracks".
        - stats : Counter
            A Counter containing aggregated statistics across all projects:
            - 'skipped_attachments': Total number of attachments skipped.
            - 'attachment_failed': Total number of attachment downloads that failed.
            - 'skipped_icons': Total number of icons skipped.
            - 'icon_failed': Total number of icon downloads that failed.
    """

    comapeo_data = {}
    stats = Counter()

    for index, project in enumerate(comapeo_projects):
        project_id = project["project_id"]
        project_name = project["project_name"]
        sanitized_project_name = normalize_identifier(project_name)

        # Set up project directories
        project_dir = Path(attachment_root) / "comapeo" / sanitized_project_name
        icon_dir = project_dir / "icons"

        # Fetch all presets for this project
        presets = fetch_all_presets(server_url, session, project_id)

        # Save presets.json to disk
        if presets:
            save_data_to_file(
                {"data": presets},
                "presets",
                project_dir,
                file_type="json",
            )

        # Download all preset icons
        icon_stats, icon_filenames = download_preset_icons(presets, icon_dir, session)

        # Build preset mapping for use in transformations (with actual icon filenames)
        preset_mapping = build_preset_mapping(presets, icon_filenames)

        # Download all observations and attachments for this project
        observations, attachment_stats = download_project_observations(
            server_url, session, project_id, project_name, attachment_root
        )

        # Transform observations to GeoJSON features
        observation_features = transform_comapeo_observations(
            observations, project_name, project_id, preset_mapping
        )

        # Store observations as a GeoJSON FeatureCollection
        comapeo_data[(sanitized_project_name, "observations")] = {
            "type": "FeatureCollection",
            "features": observation_features,
        }

        # Download tracks for this project
        tracks = download_project_tracks(server_url, session, project_id)

        # Transform tracks to GeoJSON features
        track_features = transform_comapeo_tracks(
            tracks, project_name, project_id, server_url, session
        )

        # Store tracks as a GeoJSON FeatureCollection
        comapeo_data[(sanitized_project_name, "tracks")] = {
            "type": "FeatureCollection",
            "features": track_features,
        }

        # Aggregate statistics
        stats["skipped_attachments"] += attachment_stats["skipped_attachments"]
        stats["attachment_failed"] += attachment_stats["attachment_failed"]
        stats["skipped_icons"] += icon_stats["skipped_icons"]
        stats["icon_failed"] += icon_stats["icon_failed"]

        # Log failures (not skips, as skips are expected behavior)
        if attachment_stats["attachment_failed"] > 0:
            logger.warning(
                f"{attachment_stats['attachment_failed']} attachment download(s) failed for project {project_name}."
            )

        if icon_stats["icon_failed"] > 0:
            logger.warning(
                f"{icon_stats['icon_failed']} icon download(s) failed for project {project_name}."
            )

        logger.info(
            f"Project {index + 1} (ID: {project_id}, name: {project_name}): Processed {len(observations)} observation(s) and {len(tracks)} track(s)."
        )
    return comapeo_data, stats
