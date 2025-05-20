# requirements:
# psycopg2-binary
# requests~=2.32

import logging
from pathlib import Path

import requests

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql

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

    form_data = download_form_responses_and_attachments(
        kobo_server_base_url, kobo_api_key, form_id, db_table_name, attachment_root
    )

    transformed_form_data = format_geometry_fields(form_data)

    db_writer = StructuredDBWriter(
        conninfo(db),
        db_table_name,
        use_mapping_table=True,
        sanitize_keys=True,
        reverse_properties_separated_by="/",
    )
    db_writer.handle_output(transformed_form_data)
    logger.info(
        f"KoboToolbox responses successfully written to database table: [{db_table_name}]"
    )


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
    server_base_url, kobo_api_key, form_id, db_table_name, attachment_root
):
    """Download form responses and their attachments from the KoboToolbox API.

    Parameters
    ----------
    server_base_url : str
        The base URL of the KoboToolbox server.
    kobo_api_key : str
        The API key for authenticating requests to the KoboToolbox server.
    form_id : str
        The unique identifier of the form to download.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    list
        A list of form submissions data.
    """
    headers = {
        "Authorization": f"Token {kobo_api_key}",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    # First get the name of the form. You have to hit a different endpoint just for this.
    form_uri = f"{server_base_url}/api/v2/assets/{form_id}/"
    response = requests.get(form_uri, headers=headers)
    response.raise_for_status()
    data_uri = response.json()["data"]
    form_name = response.json().get("name")

    # Next download the form questions & metadata
    # FIXME: need to paginate. Maximum results per page is 30000.
    response = requests.get(data_uri, headers=headers)
    response.raise_for_status()

    form_submissions = response.json()["results"]

    skipped_attachments = 0

    for submission in form_submissions:
        submission["dataset_name"] = form_name
        submission["data_source"] = "KoboToolbox"

        # Download attachments for each submission, if they exist
        if "_attachments" in submission:
            skipped_attachments += _download_submission_attachments(
                submission, db_table_name, attachment_root, headers
            )

    if skipped_attachments > 0:
        logger.info(f"Skipped downloading {skipped_attachments} media attachment(s).")

    logger.info(f"[Form {form_id}] Downloaded {len(form_submissions)} submission(s).")
    return form_submissions


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
