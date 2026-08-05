# requirements:
# psycopg[binary]
# requests~=2.32

import logging

from f.common_logic.db_operations import postgresql
from f.connectors.inaturalist.inaturalist_pull_project import (
    download_observations,
    write_observations,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    username: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    """
    Fetch public iNaturalist observations for a username and write them to the
    datalake and PostgreSQL.

    Parameters
    ----------
    username : str
        iNaturalist username (profile slug), e.g. from
        ``https://www.inaturalist.org/people/{username}``.
    db : postgresql
        Database connection configuration.
    db_table_name : str
        Database table name and datalake subdirectory.
    attachment_root : str
        Root directory for persisted files.
    """
    observations = download_observations({"user_id": username})
    write_observations(
        observations,
        db,
        db_table_name,
        attachment_root,
        user_id=username,
    )
