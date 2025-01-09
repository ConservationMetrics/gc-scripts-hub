# requirements:
# psycopg2-binary

import logging

# type names that refer to Windmill Resources
postgresql = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conninfo(db: postgresql):
    """Convert a `postgresql` Windmill Resources to psycopg-style connection string"""
    # password is optional
    password_part = f" password={db['password']}" if "password" in db else ""
    conn = "dbname={dbname} user={user} host={host} port={port}".format(**db)
    return conn + password_part


def main(
    db: postgresql,
    db_table_name: str,
    locusmap_tmp_path: str,
    attachment_root: str = "/frizzle-persistent-storage/datalake/",
):
    transformed_locusmap_points = transform_locusmap_points(locusmap_tmp_path)

    copy_locusmap_attachments(locusmap_tmp_path, attachment_root)

    db_writer = LocusMapDbWriter(conninf(db), db_table_name)
    db_writer.handle_output(transformed_locusmap_points)

    delete_locusmap_tmp_files(locusmap_tmp_path)
