# requirements:
# psycopg2-binary
# requests~=2.32

import hashlib
import json
import logging
from pathlib import Path

import requests

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql
from f.common_logic.save_disk import save_data_to_file

# type names that refer to Windmill Resources
c_kobotoolbox_account = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    kobotoolbox: c_kobotoolbox_account,
    form_id: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    kobo_server_base_url = kobotoolbox["server_url"]
    kobo_api_key = kobotoolbox["api_key"]

    form_data, form_translations = download_form_data_and_attachments(
        kobo_server_base_url, kobo_api_key, form_id, db_table_name, attachment_root
    )

    transformed_form_data = format_geometry_fields(form_data)

    kobo_response_writer = StructuredDBWriter(
        conninfo(db),
        db_table_name,
        use_mapping_table=True,
        sanitize_keys=True,
        reverse_properties_separated_by="/",
    )
    kobo_response_writer.handle_output(transformed_form_data)
    logger.info(
        f"KoboToolbox responses successfully written to database table: [{db_table_name}]"
    )

    if form_translations:
        kobo_translations_writer = StructuredDBWriter(
            conninfo(db), f"{db_table_name}__translations"
        )
        kobo_translations_writer.handle_output(form_translations)
        logger.info(
            f"KoboToolbox translations successfully written to database table: [{db_table_name}__translations]"
        )


def _extract_form_translations(form_metadata):
    """
    Extracts translated labels for form questions and choices. The purpose is to
    prepare a lookup table for form translations.

    Parameters
    ----------
    form_metadata : dict

    Returns
    -------
    list of dict
        Each dict has keys: '_id', 'type', 'name', and 'translation_<lang_code>'.
    """
    content = form_metadata.get("content", {})
    translations = content.get("translations", [])

    if not translations:
        return []

    lang_codes = [
        lang[lang.find("(") + 1 : lang.find(")")]
        for lang in translations
        if "(" in lang and ")" in lang
    ]

    rows = []

    for section in ["survey", "choices"]:
        for item in content.get(section, []):
            row = {
                "type": section,
                "name": item["name"],
            }
            labels = item.get("label", [])
            for i, code in enumerate(lang_codes):
                if i < len(labels):
                    row[f"translation_{code}"] = labels[i]

            # Deterministic _id based on row content
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


def download_form_data_and_attachments(
    server_base_url, kobo_api_key, form_id, db_table_name, attachment_root
):
    """Fetch and store form metadata, responses, and attachments from the KoboToolbox API.

    This function retrieves form metadata, including translations, and saves it to a specified directory.
    It then downloads form responses and their associated attachments, storing them in the specified
    directory structure. If attachments already exist, they are skipped.

    Parameters
    ----------
    server_base_url : str
        The base URL of the KoboToolbox server.
    kobo_api_key : str
        The API key for authenticating requests to the KoboToolbox server.
    form_id : str
        The unique identifier of the form to download.
    db_table_name : str
        The name of the database table where the form responses will be stored.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    tuple
        A tuple containing a list of form submissions data and a list of form translations.
    """
    headers = {
        "Authorization": f"Token {kobo_api_key}",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    # First let's download the form metadata. The form metadata contains the form
    # name and the data URI that we need to download the form responses.
    # We also want to save the form metadata itself and compile the translations
    # for the form.
    form_uri = f"{server_base_url}/api/v2/assets/{form_id}/"
    form_metadata_response = requests.get(form_uri, headers=headers)
    form_metadata_response.raise_for_status()

    # Save the form metadata to a JSON file on the datalake
    save_path = Path(attachment_root) / db_table_name
    save_data_to_file(
        form_metadata_response.json(),
        f"{db_table_name}_metadata",
        save_path,
        "json",
    )

    # Extract the data URI, form name, and translations from the metadata
    data_uri = form_metadata_response.json()["data"]
    form_name = form_metadata_response.json().get("name")
    languages = form_metadata_response.json().get("content", {}).get("translations", [])
    form_languages = ",".join(filter(None, languages)) if languages != [None] else None

    form_metadata = form_metadata_response.json()
    form_translations = _extract_form_translations(form_metadata)

    # Next download the form responses.
    # FIXME: need to paginate. Maximum results per page is 30000.
    form_data_response = requests.get(data_uri, headers=headers)
    form_data_response.raise_for_status()

    form_submissions = form_data_response.json()["results"]

    skipped_attachments = 0

    for submission in form_submissions:
        submission["dataset_name"] = form_name
        submission["data_source"] = "KoboToolbox"
        if form_languages:
            submission["form_translations"] = form_languages

        # Download attachments for each submission, if they exist
        if "_attachments" in submission:
            skipped_attachments += _download_submission_attachments(
                submission, db_table_name, attachment_root, headers
            )

    if skipped_attachments > 0:
        logger.info(f"Skipped downloading {skipped_attachments} media attachment(s).")

    logger.info(f"[Form {form_id}] Downloaded {len(form_submissions)} submission(s).")
    return form_submissions, form_translations


def format_geometry_fields(form_data):
    """Transform KoboToolbox form data by formatting geometry fields for SQL database insertion.

    Parameters
    ----------
    form_data : list
        A list of form submissions downloaded from the KoboToolbox API.

    Returns
    -------
    list
        A list of transformed form submissions.
    """
    for submission in form_data:
        if "_geolocation" in submission:
            # Convert [lat, lon] to [lon, lat] for GeoJSON compliance
            coordinates = submission.pop("_geolocation")[::-1]
            submission.update({"g__type": "Point", "g__coordinates": coordinates})

    return form_data
