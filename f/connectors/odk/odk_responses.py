# requirements:
# psycopg2-binary
# pyodk~=0.3.0
# requests~=2.28.1

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
        client = Client(config_path=str(config_path))

        # Get the form metadata
        metadata = client.forms.get(form_id)
        logger.info(f"Forms metadata: {metadata}")

        # Get the form data
        submissions = client.submissions.get_table(form_id)
        logger.info(f"Submissions: {submissions}")

        # Get attachments
        attachments = client.get(
            f"projects/1/forms/{form_id}/submissions/uuid:24951a9e-db46-4e22-9bce-910377c9dd22/attachments"
        )
        logger.info(f"Attachments: {attachments.json()}")

    finally:
        config_path.unlink(missing_ok=True)
