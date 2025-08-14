# requirements:
# psycopg2-binary
# requests~=2.32

import ast
import hashlib
import json
import logging
from pathlib import Path
from typing import TypedDict

import requests

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql
from f.common_logic.file_operations import save_data_to_file


# https://hub.windmill.dev/resource_types/193/kobotoolbox_account
class kobotoolbox(TypedDict):
    server_url: str
    api_key: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    kobotoolbox: kobotoolbox,
    form_id: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    kobo_server_base_url = kobotoolbox["server_url"]
    kobo_api_key = kobotoolbox["api_key"]

    headers = {
        "Authorization": f"Token {kobo_api_key}",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    form_metadata = download_form_metadata(
        kobo_server_base_url, headers, form_id, db_table_name, attachment_root
    )

    form_labels = extract_form_labels(form_metadata)

    form_responses, form_name, form_languages = download_form_responses_and_attachments(
        headers, form_metadata, db_table_name, attachment_root
    )

    transformed_form_responses = transform_kobotoolbox_form_data(
        form_responses,
        form_name=form_name,
        form_languages=form_languages,
    )

    kobo_response_writer = StructuredDBWriter(
        conninfo(db),
        db_table_name,
        use_mapping_table=True,
        reverse_properties_separated_by="/",
    )
    kobo_response_writer.handle_output(transformed_form_responses)
    logger.info(
        f"KoboToolbox responses successfully written to database table: [{db_table_name}]"
    )

    # If form_labels is empty, there were no translatable labels found in metadata
    if form_labels:
        kobo_translations_writer = StructuredDBWriter(
            conninfo(db), db_table_name, suffix="labels"
        )
        kobo_translations_writer.handle_output(form_labels)
        logger.info(
            f"KoboToolbox form labels successfully written to database table: [{db_table_name}__labels]"
        )


def download_form_metadata(
    server_base_url, headers, form_id, db_table_name, attachment_root
):
    """
    Downloads form metadata from the KoboToolbox server and saves it to a JSON file.

    The metadata is saved to disk intentionally (not as a temporary artifact),
    as it may have value for future processes beyond immediate label extraction.
    The saved file includes additional metadata information about the form such as sector,
    country, field properties, and more.

    This file does **not** contain sensitive secrets. The only identifiers it includes are the
    KoboToolbox username (typically public-facing) and the form ID.

    Parameters
    ----------
    server_base_url : str
        The base URL of the KoboToolbox server.
    headers : dict
        HTTP headers for the request, including authorization.
    form_id : str
        The unique identifier of the form.
    db_table_name : str
        The name of the database table where metadata will be associated.
    attachment_root : str
        The root directory path where the metadata JSON file will be saved.

    Returns
    -------
    dict
        The form metadata as a dictionary.
    """
    form_uri = f"{server_base_url}/api/v2/assets/{form_id}/"
    form_metadata_response = requests.get(form_uri, headers=headers)
    form_metadata_response.raise_for_status()
    form_metadata = form_metadata_response.json()

    # Save the form metadata to a JSON file on the datalake
    save_path = Path(attachment_root) / db_table_name
    save_data_to_file(
        form_metadata,
        f"{db_table_name}_metadata",
        save_path,
        "json",
    )

    logger.info
    f"Form metadata extracted, and saved to: {save_path}/{db_table_name}_metadata.json"

    return form_metadata


def extract_form_labels(form_metadata):
    """
    Extracts and prepares normalized labels for form questions and choices from the provided form metadata.
    This function is designed to create a lookup table for form translations.

    If no labels are found in the metadata, returns an empty list.

    Parameters
    ----------
    form_metadata : dict
        A dictionary containing metadata of the form, including content and translations.

    Returns
    -------
    list of dict
        A list of dictionaries, each representing a label for a form item with the following keys:
        - '_id': A unique identifier for the item-language tuple, generated using an MD5 hash.
        - 'type': The type of the item, either 'survey' or 'choices'.
        - 'name': The name of the form item.
        - 'language': The language code of the label (e.g., 'en', 'es').
        - 'label': The label text in the specified language.
    """
    content = form_metadata.get("content", {})
    translations = content.get("translations", [])

    rows = []

    if not translations or translations == [None]:
        # Single-language form, only one label per item
        for section in ["survey", "choices"]:
            for item in content.get(section, []):
                label = item.get("label", [None])[0]
                row = {
                    "type": section,
                    "name": item.get("name", item.get("$autoname")),
                    "language": None,
                    "label": label,
                }
                hash_input = json.dumps(row, sort_keys=True).encode("utf-8")
                row["_id"] = hashlib.md5(hash_input).hexdigest()
                rows.append(row)
        return rows

    # Parse language codes from translations (assumes format "Name (xx)")
    lang_codes = [
        lang[lang.find("(") + 1 : lang.find(")")]
        for lang in translations
        if isinstance(lang, str) and "(" in lang and ")" in lang
    ]

    for section in ["survey", "choices"]:
        for item in content.get(section, []):
            labels = item.get("label", [])
            for i, code in enumerate(lang_codes):
                if i < len(labels):
                    row = {
                        "type": section,
                        "name": item.get("name", item.get("$autoname")),
                        "language": code,
                        "label": labels[i],
                    }
                    hash_input = json.dumps(row, sort_keys=True).encode("utf-8")
                    row["_id"] = hashlib.md5(hash_input).hexdigest()
                    rows.append(row)

    return rows


def _download_submission_attachments(
    submission, db_table_name, attachment_root, headers
):
    """Download and save attachments from a form submission.

    Parameters
    ----------
    submission : dict
        The form submission data
    attachment_root : str
        The base directory where attachments will be stored.
    headers : dict
        HTTP headers required for downloading the attachments.

    Returns
    -------
    int
        The number of attachments skipped due to already existing on disk.

    Notes
    -----
    If the file already exists at the specified path, the function will skip downloading the file.
    """
    skipped_attachments = 0
    for attachment in submission["_attachments"]:
        if "download_url" in attachment:
            file_name = attachment["filename"]
            save_path = (
                Path(attachment_root)
                / db_table_name
                / "attachments"
                / Path(file_name).name
            )
            if save_path.exists():
                logger.debug(f"File already exists, skipping download: {save_path}")
                skipped_attachments += 1
                continue

            response = requests.get(attachment["download_url"], headers=headers)
            if response.status_code == 200:
                save_path.parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, "wb") as file:
                    file.write(response.content)
                logger.debug(f"Download completed: {attachment['download_url']}")
            else:
                logger.error(
                    f"Failed downloading attachment: {attachment['download_url']}"
                )
    return skipped_attachments


def download_form_responses_and_attachments(
    headers, form_metadata, db_table_name, attachment_root
):
    """Download form responses and attachments from the KoboToolbox API.

    Parameters
    ----------
    headers : dict
        HTTP headers required for authentication and content type.
    form_metadata : dict
        Metadata of the form, including the data URI and translations.
    db_table_name : str
        The name of the database table where the form responses will be stored.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    tuple
        A tuple containing:
        - list: Raw form submissions downloaded from the KoboToolbox API
        - int: Number of attachments skipped due to already existing on disk
        - str: Form name from metadata
        - str: Comma-separated form languages (or None if single language)
    """
    # Extract the data URI, form name, and translations from the metadata
    data_uri = form_metadata["data"]
    form_name = form_metadata.get("name")
    languages = form_metadata.get("content", {}).get("translations", [])
    form_languages = ",".join(filter(None, languages)) if languages != [None] else None

    # Next download the form responses.
    # FIXME: need to paginate. Maximum results per page is 30000.
    form_data_response = requests.get(data_uri, headers=headers)
    form_data_response.raise_for_status()

    form_submissions = form_data_response.json()["results"]

    skipped_attachments = 0

    # Download attachments for each submission, if they exist
    for submission in form_submissions:
        if "_attachments" in submission:
            skipped_attachments += _download_submission_attachments(
                submission, db_table_name, attachment_root, headers
            )

    if skipped_attachments > 0:
        logger.info(f"Skipped downloading {skipped_attachments} media attachment(s).")

    logger.info(f"[Form {form_name}] Downloaded {len(form_submissions)} submission(s).")
    return form_submissions, form_name, form_languages


def transform_kobotoolbox_form_data(form_data, form_name=None, form_languages=None):
    """Transform KoboToolbox form data by adding metadata fields and formatting geometry for SQL database insertion.

    Parameters
    ----------
    form_data : list
        A list of form submissions downloaded from the KoboToolbox API.
    form_name : str, optional
        The name of the form from metadata. If provided, adds 'dataset_name' field to each submission.
    form_languages : str, optional
        Comma-separated list of form languages. If provided, adds 'form_translations' field to each submission.

    Returns
    -------
    list
        A list of transformed form submissions with added metadata fields and formatted geometry.
    """
    for submission in form_data:
        # Add metadata fields if provided
        if form_name:
            submission["dataset_name"] = form_name
        submission["data_source"] = "KoboToolbox"
        if form_languages:
            submission["form_translations"] = form_languages

        # Transform geometry fields for GeoJSON compliance
        if "_geolocation" in submission:
            value = submission.pop("_geolocation")
            coordinates = None

            # Handle different input formats:
            # - From CSV: _geolocation is a string like "[36.97012, -122.0109429]"
            # - From server API: _geolocation is already a list like [36.97012, -122.0109429]
            if isinstance(value, str):
                value = value.strip()
                # Skip empty strings or invalid values
                if value and value not in ("[]", '""'):
                    try:
                        # Parse string representation of list to actual list
                        coords = ast.literal_eval(value)
                        if isinstance(coords, list) and len(coords) == 2:
                            coordinates = coords
                    except Exception:
                        # Skip invalid coordinate strings
                        pass
            elif isinstance(value, list) and len(value) == 2:
                coordinates = value

            # Convert [lat, lon] to [lon, lat] for GeoJSON compliance and add geometry fields
            if coordinates:
                coordinates = [float(coordinates[1]), float(coordinates[0])]
                submission.update({"g__type": "Point", "g__coordinates": coordinates})

    return form_data
