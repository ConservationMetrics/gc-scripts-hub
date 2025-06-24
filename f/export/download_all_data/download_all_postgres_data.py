import logging
import shutil
import tempfile
from pathlib import Path

import psycopg2

from f.common_logic.db_operations import check_if_table_exists, conninfo, postgresql
from f.export.postgres_to_file.postgres_to_csv import main as postgres_to_csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    db: postgresql,
    storage_path: str = "/persistent-storage/datalake/exports/",
):
    db_connection_string = conninfo(db)

    tables = fetch_tables_from_postgres(db_connection_string)

    if not tables:
        logger.warning("No tables found in the database.")
        return

    logger.info(f"Found {len(tables)} tables: {tables}")

    zip_path = export_tables_to_zip(db, db_connection_string, tables, storage_path)
    logger.info(f"Export completed. Archive saved to {zip_path}")


def fetch_tables_from_postgres(db_connection_string: str):
    """
    Fetch all table names from the public schema of the PostgreSQL database.

    Parameters
    ----------
    db_connection_string : str
        The connection string for the PostgreSQL database.

    Returns
    -------
    list
        A list of table names from the public schema.
    """
    try:
        conn = psycopg2.connect(db_connection_string)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return tables
    except Exception as e:
        logger.error(f"Error fetching tables: {e}")
        return []


def export_tables_to_zip(
    db: postgresql,
    db_connection_string: str,
    tables: list[str],
    storage_path: str,
) -> Path:
    """
    Export specified tables from the PostgreSQL database to CSV files,
    and compress them into a zip archive.

    Parameters
    ----------
    db : postgresql
        A database connection for a PostgreSQL database.
    db_connection_string : str
        The connection string for the PostgreSQL database.
    tables : list of str
        A list of table names to be exported.
    storage_path : str
        The path to the directory where the zip archive will be stored.

    Returns
    -------
    Path
        The path to the created zip archive containing the exported CSV files.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)

        for table in tables:
            if check_if_table_exists(db_connection_string, table):
                logger.debug(f"Exporting table {table} to CSV")
                postgres_to_csv(db, table, str(tmp_dir_path))
            else:
                logger.warning(f"Table {table} does not exist.")

        zip_path = Path(storage_path) / "all_database_content.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.make_archive(zip_path.with_suffix(""), "zip", tmp_dir_path)

        return zip_path
