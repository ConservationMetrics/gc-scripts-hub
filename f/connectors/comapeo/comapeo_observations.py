# requirements:
# psycopg2-binary
# requests~=2.32

import logging
import mimetypes
import re
from os import listdir
from pathlib import Path
from typing import TypedDict

import requests

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file
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

    comapeo_projects = fetch_comapeo_projects(
        server_url, access_token, comapeo_project_blocklist
    )

    # Run culminates in success if there were no projects returned by the API
    if len(comapeo_projects) == 0:
        logger.info(
            "No projects fetched. Skipping data processing and database writing."
        )
        return

    logger.info(f"Fetched {len(comapeo_projects)} projects.")

    comapeo_projects_geojson, attachment_failed = download_and_transform_comapeo_data(
        server_url,
        access_token,
        comapeo_projects,
        attachment_root,
    )

    logger.info(
        f"Downloaded CoMapeo data for {len(comapeo_projects_geojson)} projects. Saving data..."
    )

    for project_name, geojson in comapeo_projects_geojson.items():
        storage_path = Path(attachment_root) / db_table_prefix / project_name
        rel_geojson_path = (
            Path(db_table_prefix) / project_name / f"{project_name}.geojson"
        )

        if geojson.get("features"):
            logger.info(f"Saving data for project {project_name}...")
            save_data_to_file(geojson, project_name, storage_path, file_type="geojson")

            save_geojson_to_postgres(
                db,
                db_table_prefix + "_" + project_name,
                rel_geojson_path,
                attachment_root,
                False,
            )  # Do not delete the file after saving to Postgres
        else:
            logger.info(
                f"No features found in project {project_name}. Nothing to save."
            )

    if attachment_failed:
        raise RuntimeError("Some attachments failed to download.")


def fetch_comapeo_projects(server_url, access_token, comapeo_project_blocklist):
    """
    Fetches a list of projects from the CoMapeo API, excluding any projects
    specified in the blocklist.

    Parameters
    ----------
    server: comapeo_server
        For authenticating with the CoMapeo API
    comapeo_project_blocklist : list
        A list of project IDs to be excluded from the fetched results.

    Returns
    -------
    list
        A list of dictionaries, each containing the 'project_id' and 'project_name'
        of a project fetched from the CoMapeo API, excluding those in the blocklist.
    """

    url = f"{server_url}/projects"
    headers = {"Authorization": f"Bearer {access_token}"}
    payload = {}
    logger.info("Fetching projects from CoMapeo API...")
    response = requests.request("GET", url, headers=headers, data=payload)

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


def download_attachment(url, headers, save_path, existing_file_stems):
    """
    Downloads a file from a specified URL and saves it to a given path.

    Parameters
    ----------
    url : str
        The URL of the file to be downloaded.
    headers : dict
        A dictionary of HTTP headers to send with the request, such as authentication tokens.
    save_path : str
        The file system path where the downloaded file will be saved.
    existing_file_stems : set
        A set of existing file names (without extensions) to check against before downloading.

    Returns
    -------
    tuple
        A tuple containing two values:
        - The name of the file if the download is successful, or None if an error occurs.
        - The number of attachments skipped due to already existing on disk.

    Notes
    -----
    If the file already exists at the specified path, the function will skip downloading the file.

    The function attempts to determine the file extension based on the 'Content-Type'
    header of the HTTP response from the CoMapeo Server. If the 'Content-Type' is not recognized,
    the file will be saved without an extension.

    The function intentionally does not raise exceptions. Instead, it logs errors and returns None,
    allowing the caller to handle the download failure gracefully.

    """
    skipped_attachments = 0
    base_name = Path(save_path).name

    if base_name in existing_file_stems:
        logger.debug(f"{base_name} already exists, skipping download.")
        skipped_attachments += 1
        # Try to find matching full filename (with extension)
        full_path = next(
            (f for f in Path(save_path).parent.glob(f"{base_name}.*")), None
        )
        return (full_path.name if full_path else base_name), skipped_attachments

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")
        extension = mimetypes.guess_extension(content_type) or ""

        file_name = base_name + extension
        save_path = Path(str(save_path) + extension)

        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(response.content)
        logger.info("Download completed.")
        return file_name, skipped_attachments

    except Exception as e:
        logger.error(f"Exception during download: {e}")
        return None, 1


def normalize_and_snakecase_keys(dictionary, special_case_keys=None):
    """
    Converts the keys of a dictionary from camelCase to snake_case, handling key collisions and truncating long keys.
    Optionally leaves specified keys unchanged.

    Parameters
    ----------
    dictionary : dict
        The dictionary whose keys are to be converted.
    special_case_keys : set, optional
        A set of keys that should not be converted.

    Returns
    -------
    dict
        A new dictionary with the keys converted to snake_case and truncated if necessary.
    """
    if special_case_keys is None:
        special_case_keys = set()

    new_dict = {}
    items = list(dictionary.items())
    for key, value in items:
        if key in special_case_keys:
            final_key = key
        else:
            new_key = (
                re.sub("([a-z0-9])([A-Z])", r"\1_\2", key).replace("-", "_").lower()
            )
            base_key = new_key[:61] if len(new_key) > 63 else new_key
            final_key = base_key
            if len(new_key) > 63:
                final_key = f"{base_key}_1"

            counter = 1
            while final_key in new_dict:
                counter += 1
                final_key = f"{base_key}_{counter}"

        new_dict[final_key] = value
    return new_dict


def download_project_observations_and_attachments(
    server_url, access_token, project_id, project_name, attachment_root
):
    """Download observations and their attachments for a specific project from the CoMapeo API.

    Parameters
    ----------
    server_url : str
        The base URL of the CoMapeo server.
    access_token : str
        The access token for authentication.
    project_id : str
        The unique identifier of the project.
    project_name : str
        The name of the project.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    tuple
        A tuple containing (observations, skipped_attachments, attachment_failed).
    """
    url = f"{server_url}/projects/{project_id}/observations"
    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    logger.info(f"Fetching project (ID: {project_id})...")
    response = requests.get(url, headers=headers)

    try:
        observations = response.json().get("data", [])
    except requests.exceptions.JSONDecodeError:
        logger.error("Failed to decode JSON from response.")
        logger.info("Response received: ", response.text)
        raise ValueError("Invalid JSON response received from server.")

    # Download attachments for all observations
    sanitized_project_name = re.sub(r"\W+", "_", project_name).lower()
    attachment_dir = (
        Path(attachment_root) / "comapeo" / sanitized_project_name / "attachments"
    )
    existing_file_stems = build_existing_file_set(attachment_dir)

    skipped_attachments = 0
    attachment_failed = False

    for observation in observations:
        if "attachments" in observation:
            filenames = []
            for attachment in observation["attachments"]:
                if "url" in attachment:
                    file_name, skipped = download_attachment(
                        attachment["url"],
                        headers,
                        str(attachment_dir / Path(attachment["url"]).name),
                        existing_file_stems,
                    )
                    skipped_attachments += skipped
                    if file_name is not None:
                        filenames.append(file_name)
                    else:
                        logger.error(
                            f"Attachment download failed for URL: {attachment['url']}. Skipping attachment."
                        )
                        attachment_failed = True

            observation["attachments"] = ", ".join(filenames)

    return observations, skipped_attachments, attachment_failed


def transform_comapeo_observations(observations, project_name, project_id=None):
    """Transform CoMapeo observations into GeoJSON features with proper metadata and geometry formatting.

    Parameters
    ----------
    observations : list
        A list of raw observation data from the CoMapeo API.
    project_name : str
        The name of the project these observations belong to.
    project_id : str, optional
        The unique identifier of the project. If not provided, this field will be omitted from the output.

    Returns
    -------
    list
        A list of GeoJSON Feature objects with transformed properties and geometry.
    """
    features = []

    for observation in observations:
        # Create k/v pairs for each tag
        for key, value in observation.pop("tags", {}).items():
            observation[key] = value

        # Convert all keys (except docId) from camelCase to snake_case, handling key collisions and char limits
        special_case_keys = set(["docId"])
        observation = normalize_and_snakecase_keys(observation, special_case_keys)

        # Add project-specific information to properties
        observation["project_name"] = project_name
        if project_id is not None:
            observation["project_id"] = project_id
        observation["data_source"] = "CoMapeo"

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


def download_and_transform_comapeo_data(
    server_url,
    access_token,
    comapeo_projects,
    attachment_root,
):
    """
    Downloads and transforms CoMapeo project data from the API, converting it into a GeoJSON FeatureCollection format and downloading any associated attachments.

    Parameters
    ----------
    comapeo_server : dict
        A dictionary containing the 'server_url' and 'access_token' keys for the CoMapeo server.
    comapeo_projects : list
        A list of dictionaries, each containing 'project_id' and 'project_name' for the projects to be processed.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    tuple
        A tuple containing:
        - comapeo_data : dict
            A dictionary where keys are project names and values are GeoJSON FeatureCollections.
        - attachment_failed : bool
            A flag indicating if any attachment downloads failed.
    """

    comapeo_data = {}
    attachment_failed = False

    for index, project in enumerate(comapeo_projects):
        project_id = project["project_id"]
        project_name = project["project_name"]

        # Download all observations and attachments for this project
        observations, skipped_attachments, project_attachment_failed = (
            download_project_observations_and_attachments(
                server_url, access_token, project_id, project_name, attachment_root
            )
        )

        # Transform observations to GeoJSON features
        features = transform_comapeo_observations(
            observations, project_name, project_id
        )

        # Store observations as a GeoJSON FeatureCollection
        sanitized_project_name = re.sub(r"\W+", "_", project_name).lower()
        comapeo_data[sanitized_project_name] = {
            "type": "FeatureCollection",
            "features": features,
        }

        if skipped_attachments > 0:
            logger.info(
                f"Skipped downloading {skipped_attachments} media attachment(s)."
            )

        if project_attachment_failed:
            attachment_failed = True

        logger.info(
            f"Project {index + 1} (ID: {project_id}, name: {project_name}): Processed {len(observations)} observation(s)."
        )
    return comapeo_data, attachment_failed
