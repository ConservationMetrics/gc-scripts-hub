# requirements:
# psycopg2-binary
# requests~=2.32

import logging
from pathlib import Path
from typing import TypedDict
from urllib.parse import urlparse

import requests

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql
from f.common_logic.file_operations import save_data_to_file
from f.common_logic.identifier_utils import normalize_identifier


# https://hub.windmill.dev/resource_types/336/localcontexts_project
class local_contexts_project(TypedDict):
    server_url: str
    api_key: str
    project_id: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    localcontexts: local_contexts_project,
    db: postgresql,
    attachment_root: str = "/persistent-storage/datalake",
):
    server_url = localcontexts["server_url"]
    api_key = localcontexts["api_key"]
    project_id = localcontexts["project_id"]

    project_data = fetch_project_data(server_url, api_key, project_id)

    project_title = project_data.get("title", "unknown_project")
    normalized_title = normalize_identifier(project_title)

    storage_path = Path(attachment_root) / "localcontexts" / normalized_title
    save_data_to_file(
        project_data,
        "project",
        storage_path,
        file_type="json",
    )
    logger.info(f"Project data saved to {storage_path}/project.json")

    download_label_attachments(
        project_data,
        normalized_title,
        attachment_root,
    )

    label_rows = transform_labels_for_db(project_data, normalized_title)

    if not label_rows:
        logger.info("No labels found in project. Skipping database write.")
        return

    db_table_name = f"localcontexts_{normalized_title}"
    logger.info(
        f"Writing {len(label_rows)} label(s) to database table [{db_table_name}]."
    )

    labels_writer = StructuredDBWriter(
        conninfo(db),
        db_table_name,
    )
    labels_writer.handle_output(label_rows)

    logger.info("Labels saved successfully.")


def fetch_project_data(server_url: str, api_key: str, project_id: str):
    """
    Fetch project data from the Local Contexts API.

    Parameters
    ----------
    server_url : str
        The base URL of the Local Contexts server.
    api_key : str
        API key for authenticating with the Local Contexts API.
    project_id : str
        The unique identifier of the project.

    Returns
    -------
    dict
        The project data from the API.
    """
    url = f"{server_url}/api/v2/projects/{project_id}"
    headers = {"X-Api-Key": api_key}

    logger.info(f"Fetching project data from Local Contexts API (ID: {project_id})...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    project_data = response.json()
    logger.info(f"Successfully fetched project: {project_data.get('title', 'Unknown')}")

    return project_data


def download_label_attachments(
    project_data: dict,
    normalized_title: str,
    attachment_root: str,
):
    """
    Download all media attachments (images, SVGs, audio) for labels.

    Parameters
    ----------
    project_data : dict
        The project data containing bc_labels and tk_labels.
    normalized_title : str
        The normalized project title for file path construction.
    attachment_root : str
        The root directory where attachments will be saved.
    """
    labels_dir = Path(attachment_root) / "localcontexts" / normalized_title / "labels"
    labels_dir.mkdir(parents=True, exist_ok=True)

    all_labels = project_data.get("bc_labels", []) + project_data.get("tk_labels", [])

    for label in all_labels:
        label_name = label.get("name", "unknown_label")
        normalized_name = normalize_identifier(label_name)

        # Download img_url
        if label.get("img_url"):
            _download_attachment(label["img_url"], labels_dir, normalized_name)

        # Download svg_url
        if label.get("svg_url"):
            _download_attachment(label["svg_url"], labels_dir, normalized_name)

        # Download audiofile
        if label.get("audiofile"):
            _download_attachment(label["audiofile"], labels_dir, normalized_name)


def _download_attachment(url: str, save_dir: Path, label_name: str):
    """
    Download a single attachment file.

    Parameters
    ----------
    url : str
        The URL of the file to download.
    save_dir : Path
        The directory where the file will be saved.
    label_name : str
        The normalized label name for organizing files.
    """
    try:
        parsed_url = urlparse(url)
        filename = Path(parsed_url.path).name

        if not filename:
            logger.warning(f"Could not extract filename from URL: {url}")
            return

        save_path = save_dir / filename

        if save_path.exists():
            logger.debug(f"File already exists, skipping download: {save_path}")
            return

        response = requests.get(url)
        response.raise_for_status()

        with open(save_path, "wb") as f:
            f.write(response.content)

        logger.info(f"Downloaded: {filename}")

    except Exception as e:
        logger.error(f"Failed to download attachment from {url}: {e}")


def transform_labels_for_db(project_data: dict, normalized_title: str):
    """
    Transform bc_labels and tk_labels into database rows.

    Parameters
    ----------
    project_data : dict
        The project data containing bc_labels and tk_labels.
    normalized_title : str
        The normalized project title.

    Returns
    -------
    list
        A list of dictionaries representing label rows for the database.
    """
    label_rows = []

    for label in project_data.get("bc_labels", []):
        row = _create_label_row(label, "BC", project_data, normalized_title)
        label_rows.append(row)

    for label in project_data.get("tk_labels", []):
        row = _create_label_row(label, "TK", project_data, normalized_title)
        label_rows.append(row)

    logger.info(f"Transformed {len(label_rows)} label(s) for database storage.")
    return label_rows


def _create_label_row(
    label: dict, label_category: str, project_data: dict, normalized_title: str
):
    """
    Create a database row from a label object.

    This function adds translations to the row as additional columns with
    language code suffixes. For example, if a label has a Wayana (way) translation:
    - `name_way` : translated name in Wayana
    - `label_text_way` : translated text in Wayana
    We expect the user of these labels to know what these language codes
    refer to; but, the `project.json` file that we will write to disk will
    have the full list of languages and their codes, if needed.

    Parameters
    ----------
    label : dict
        The label object from the API.
    label_category : str
        Either "BC" or "TK" to indicate the label category.
    project_data : dict
        The full project data for context.
    normalized_title : str
        The normalized project title.

    Returns
    -------
    dict
        A dictionary representing a database row.
    """
    community = label.get("community", {})
    community_id = community.get("id")
    community_name = community.get("name")

    row = {
        "_id": label.get("unique_id"),
        "project_title": normalized_title,
        "label_category": label_category,
        "name": label.get("name"),
        "label_type": label.get("label_type"),
        "language_tag": label.get("language_tag"),
        "language": label.get("language"),
        "label_text": label.get("label_text"),
        "label_page": label.get("label_page"),
        "img_url": label.get("img_url"),
        "svg_url": label.get("svg_url"),
        "audiofile": label.get("audiofile"),
        "community_id": community_id,
        "community_name": community_name,
        "created": label.get("created"),
        "updated": label.get("updated"),
        "data_source": "Local Contexts",
    }

    translations = label.get("translations", [])
    for translation in translations:
        lang_code = translation.get("language_tag")
        if lang_code:
            if translation.get("translated_name"):
                row[f"name_{lang_code}"] = translation["translated_name"]
            if translation.get("translated_text"):
                row[f"label_text_{lang_code}"] = translation["translated_text"]
            if translation.get("language"):
                row[f"language_{lang_code}"] = translation["language"]

    return row
