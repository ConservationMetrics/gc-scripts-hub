# requirements:
# psycopg2-binary
# requests
# pyodk~=0.3.0

import logging
import tempfile
from pathlib import Path

from pyodk.client import Client

# type names that refer to Windmill Resources
postgresql = dict
c_odk_config = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conninfo(db: postgresql):
    """Convert a `postgresql` Windmill Resources to psycopg-style connection string"""
    # password is optional
    password_part = f" password={db['password']}" if "password" in db else ""
    conn = "dbname={dbname} user={user} host={host} port={port}".format(**db)
    return conn + password_part


def get_temp_config(odk_config: c_odk_config) -> Path:
    """Create a temporary config file for PyODK and return its path."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".toml")
    temp_file.write(f"""
[central]
base_url = "{odk_config["base_url"]}"
username = "{odk_config["username"]}"
password = "{odk_config["password"]}"
default_project_id = {odk_config["project_id"]}
""")
    temp_file.close()
    return Path(temp_file.name)


def main(
    odk_config: c_odk_config,
    form_id: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    config_path = get_temp_config(odk_config)

    try:
        project_id = odk_config["project_id"]
        client = Client(config_path=str(config_path))

        form_data = download_form_responses_and_attachments(
            client, project_id, form_id, db_table_name, attachment_root
        )

        logger.info(f"Inserting {len(form_data)} records into {db_table_name}...")

    finally:
        config_path.unlink(missing_ok=True)


def _download_submission_attachments(
    client, project_id, form_id, submission, db_table_name, attachment_root
):
    """Download and save attachments from a form submission.

    Parameters
    ----------
    client : pyodk.client.Client
        The PyODK client object.
    project_id : int
        The unique identifier of the project where the form is located.
    form_id : str
        The unique identifier (xmlFormId) of the form to download
    submission : dict
        The form submission data
    db_table_name : str
        The name of the database table where the form submissions will be stored.
    attachment_root : str
        The base directory where attachments will be stored.

    Returns
    -------
    int
        The number of attachments skipped due to already existing on disk.

    Notes
    -----
    If the file already exists at the specified path, the function will skip downloading the file.
    """
    skipped_attachments = 0

    uuid = submission.get("__id")
    attachments_path = (
        f"projects/{project_id}/forms/{form_id}/submissions/{uuid}/attachments"
    )

    response = client.get(attachments_path)
    response.raise_for_status()

    attachments = response.json()

    for attachment in attachments:
        file_name = attachment["name"]
        save_path = (
            Path(attachment_root) / db_table_name / "attachments" / Path(file_name).name
        )
        if save_path.exists():
            logger.debug(f"File already exists, skipping download: {save_path}")
            skipped_attachments += 1
            continue

        file_path = f"{attachments_path}/{file_name}"
        response = client.get(file_path)

        if response.status_code == 200:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "wb") as file:
                file.write(response.content)
            logger.debug(f"Download completed: {file_path}")
        else:
            logger.error(f"Failed downloading attachment: {file_path}")
    return skipped_attachments


def download_form_responses_and_attachments(
    client, project_id, form_id, db_table_name, attachment_root
):
    """Download form responses and their attachments from the ODK Central API.

    Parameters
    ----------
    client : pyodk.client.Client
        The PyODK client object.
    project_id : int
        The unique identifier of the project where the form is located.
    form_id : str
        The unique identifier (xmlFormId) of the form to download.
    db_table_name : str
        The name of the database table where the form submissions will be stored.
    attachment_root : str
        The root directory where attachments will be saved.

    Returns
    -------
    list
        A list of form submissions data.
    """

    # get_table returns {'value': []} so we need to access what's inside the value key
    form_submissions = client.submissions.get_table(form_id)["value"]

    skipped_attachments = 0

    for submission in form_submissions:
        submission["dataset_name"] = form_id
        submission["data_source"] = "ODK"

        # Download attachments for each submission, if they exist
        if submission.get("__system", {}).get("attachmentsPresent", 0) != 0:
            skipped_attachments += _download_submission_attachments(
                client, project_id, form_id, submission, db_table_name, attachment_root
            )

    if skipped_attachments > 0:
        logger.info(f"Skipped downloading {skipped_attachments} media attachment(s).")

    logger.info(f"[Form {form_id}] Downloaded {len(form_submissions)} submission(s).")
    return form_submissions
