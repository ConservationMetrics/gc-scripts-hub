# requirements:
# pandas~=2.2
# psycopg2-binary
# azure-storage-blob

import logging
import shutil
import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
from azure.storage.blob import BlobServiceClient

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql
from f.common_logic.db_transformations import camel_to_snake

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    blob_connection_string: str,
    container_name: str,
    blob_name: str,
    db: postgresql,
    db_table_prefix: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    """
    Downloads a Timelapse ZIP file from Azure Blob Storage, extracts it, and processes the data.

    Parameters
    ----------
    blob_connection_string : str
        Azure Storage connection string
    container_name : str
        Name of the Azure Blob Storage container
    blob_name : str
        Name of the blob (ZIP file) to download
    db : postgresql
        Database connection configuration
    db_table_prefix : str
        Prefix for database table names
    attachment_root : str
        Root directory for persistent storage
    """
    base_storage_path = Path(attachment_root) / "Timelapse"

    # Download ZIP file from Azure Blob Storage
    timelapse_zip_path = download_blob_to_temp(
        blob_connection_string, container_name, blob_name
    )

    try:
        actual_storage_path = extract_timelapse_archive(
            timelapse_zip_path, base_storage_path
        )

        timelapse_tables = read_timelapse_db_tables(
            actual_storage_path, db_table_prefix
        )

        for table_name, rows in timelapse_tables.items():
            db_writer = StructuredDBWriter(
                conninfo(db),
                table_name,
                use_mapping_table=False,
                reverse_properties_separated_by=None,
            )
            db_writer.handle_output(rows)
            logger.info(
                f"Timelapse data from table '{table_name}' successfully written to the database."
            )

        return actual_storage_path

    finally:
        # Clean up the temporary ZIP file
        if timelapse_zip_path.exists():
            timelapse_zip_path.unlink()
            logger.info(f"Deleted temporary ZIP file: {timelapse_zip_path}")


def download_blob_to_temp(
    connection_string: str, container_name: str, blob_name: str
) -> Path:
    """
    Downloads a blob from Azure Blob Storage to a temporary file.

    Parameters
    ----------
    connection_string : str
        Azure Storage connection string
    container_name : str
        Name of the container
    blob_name : str
        Name of the blob to download

    Returns
    -------
    Path
        Path to the temporary file containing the downloaded blob
    """
    try:
        # Create BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

        # Create temporary file with the same name as the blob
        temp_dir = Path(tempfile.gettempdir())
        temp_path = temp_dir / blob_name

        # Download blob to temporary file
        with open(temp_path, "wb") as download_file:
            blob_data = blob_client.download_blob()
            blob_data.readinto(download_file)

        logger.info(
            f"Downloaded blob '{blob_name}' from container '{container_name}' to {temp_path}"
        )
        return temp_path

    except Exception as e:
        logger.error(
            f"Failed to download blob '{blob_name}' from container '{container_name}': {e}"
        )
        raise


def extract_timelapse_archive(
    timelapse_zip_path: Path,
    storage_path: Path,
) -> Path:
    """
    Extracts a Timelapse ZIP archive to a uniquely named subdirectory under `storage_path`.

    The final target path is: storage_path / <zip_stem>[_n]
    where `_n` ensures uniqueness if a folder with the same name already exists.

    Parameters
    ----------
    timelapse_zip_path : Path
        The path to the Timelapse ZIP file.
    storage_path : Path
        The path to the root directory where the ZIP file will be extracted.

    Returns
    -------
    Path
        The full path to the directory where the archive was extracted.
    """

    with tempfile.TemporaryDirectory() as tmpdir:
        extract_to = Path(tmpdir)

        try:
            shutil.unpack_archive(
                timelapse_zip_path,
                extract_to,
            )
            logger.info(f"Extracted Timelapse archive: {timelapse_zip_path}")
        except shutil.ReadError as e:
            raise ValueError(f"Unable to extract archive: {e}")

        zip_name = timelapse_zip_path.stem
        base_target_path = storage_path / zip_name
        final_target_path = base_target_path
        counter = 1

        # Guarantee a clean, non-conflicting destination folder e.g. "timelapse_export"
        # or "timelapse_export_1" if the first one already exists.
        while final_target_path.exists():
            final_target_path = storage_path / f"{zip_name}_{counter}"
            counter += 1

        final_target_path.mkdir(parents=True)

        for file in extract_to.rglob("*"):
            # Skip files in the "Backups" directory, as these are redundant
            if file.is_file() and "Backups" not in file.parts:
                # relative_path represents the file's full internal path *within the ZIP archive*
                # This is preserved so we don't flatten or lose folder structure during extraction
                relative_path = file.relative_to(extract_to)
                target_path = final_target_path / relative_path
                # This mkdir with exist_ok=True is safe:
                # multiple files may share parent directories within the archive (e.g. Station1/Deployment1a/)
                # so we *do* want to reuse existing folders during file copy
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(file, target_path)

        logger.info(f"Copied contents of Timelapse archive to: {final_target_path}")
        return final_target_path


def _transform_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a DataFrame by renaming its columns. Specifically, it changes any column named 'id'
    to '_id' and converts other column names from camelCase to snake_case.

    TODO: convert id to uuid?
    TODO: Does this transformation need to maintain column mapping over time?
    """
    new_columns = df.columns.map(
        lambda col: "_id" if col.lower() == "id" else camel_to_snake(col)
    )

    if len(new_columns) != len(set(new_columns)):
        raise ValueError("Column name collision detected")

    df.columns = new_columns
    return df


def read_timelapse_db_tables(storage_path: str, db_table_prefix: str):
    """
    Reads tables from a SQLite database file located at the specified storage path and returns
    a dictionary where keys are table names prefixed with the given database table prefix and
    values are lists of dictionaries representing the transformed rows of each table.

    Parameters
    ----------
    storage_path : str
        The path to the directory containing the SQLite database file 'TimelapseData.ddb'.
    db_table_prefix : str
        The prefix to be added to the keys in the output dictionary.

    Returns
    -------
    dict
        A dictionary where keys are table names prefixed with `db_table_prefix` and values are
        lists of dictionaries, each representing a row in the corresponding transformed DataFrame.
    """
    logger.info(f"Reading Timelapse database from {storage_path}")

    timelapse_db = Path(storage_path) / "TimelapseData.ddb"

    if not timelapse_db.exists():
        raise FileNotFoundError(f"The database file {timelapse_db} does not exist.")

    conn = sqlite3.connect(timelapse_db)

    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table';", conn
    )["name"].tolist()

    output = {}

    if "DataTable" in tables:
        output[f"{db_table_prefix}_data"] = _transform_df(
            pd.read_sql_query("SELECT * FROM DataTable", conn)
        )

    if "TemplateTable" in tables:
        df = pd.read_sql_query("SELECT * FROM TemplateTable", conn)
        filtered = df[["Id", "Type", "Label", "DataLabel", "Tooltip", "List"]]
        output[f"{db_table_prefix}_data_template"] = _transform_df(filtered)

    if "FolderDataInfo" in tables:
        output[f"{db_table_prefix}_folder_metadata"] = _transform_df(
            pd.read_sql_query("SELECT * FROM FolderDataInfo", conn)
        )

    if "FolderDataTemplateTable" in tables:
        df = pd.read_sql_query("SELECT * FROM FolderDataTemplateTable", conn)
        filtered = df[["Id", "Level", "Type", "Label", "DataLabel", "Tooltip", "List"]]
        output[f"{db_table_prefix}_folder_metadata_template"] = _transform_df(filtered)

    for table in tables:
        if table.startswith("Level"):
            try:
                level_num = int(table.replace("Level", ""))
                output[f"{db_table_prefix}_level_{level_num}"] = _transform_df(
                    pd.read_sql_query(f"SELECT * FROM {table}", conn)
                )
            except ValueError:
                continue

    conn.close()

    logger.info(f"Extracted tables: {list(output.keys())}")

    return {
        table_name: df.to_dict(orient="records") for table_name, df in output.items()
    }
