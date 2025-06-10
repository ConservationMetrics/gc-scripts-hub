# requirements:
# pandas~=2.2
# psycopg2-binary

import logging
import shutil
import sqlite3
import tempfile
from pathlib import Path

import pandas as pd

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql
from f.common_logic.db_transformations import camel_to_snake

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    timelapse_zip: str,
    db: postgresql,
    db_table_prefix: str,
    delete_timelapse_zip: bool = True,
    attachment_root: str = "/persistent-storage/datalake",
):
    storage_path = Path(attachment_root) / "Timelapse" / db_table_prefix

    timelapse_zip_path = Path(timelapse_zip)
    extract_timelapse_archive(timelapse_zip_path, storage_path)

    timelapse_tables = read_timelapse_db_tables(storage_path, db_table_prefix)

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

    if delete_timelapse_zip:
        # Now that we've extracted the archive and written the data to the database,
        # we can delete the original ZIP file.
        timelapse_zip_path.unlink()
        logger.info(f"Deleted Timelapse archive: {timelapse_zip_path}")


def extract_timelapse_archive(
    timelapse_zip_path: Path,
    storage_path: str,
):
    """
    Extracts a Timelapse ZIP archive to a specified root directory.

    Parameters
    ----------
    timelapse_zip_path : Path
        The path to the Timelapse ZIP file.
    storage_path : str
        The path to the root directory where the ZIP file will be extracted.
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

        storage_path = Path(storage_path)
        storage_path.mkdir(parents=True, exist_ok=True)

        for file in extract_to.rglob("*"):
            # Skip files in the "Backups" directory, as these are redundant
            if file.is_file() and "Backups" not in file.parts:
                relative_path = file.relative_to(extract_to)
                target_path = storage_path / relative_path
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(file, target_path)

        logger.info(f"Copied contents of Timelapse archive to: {storage_path}")


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

    logger.error(f"New columns: {new_columns}")
    logger.error(f"Original columns: {df.columns}")

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
