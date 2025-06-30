# requirements:
# psycopg2-binary

import csv
import logging
import shutil
from pathlib import Path

from f.common_logic.db_operations import (
    StructuredDBWriter,
    check_if_table_exists,
    conninfo,
    postgresql,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    auditor2_zip: str,
    db: postgresql,
    project_name: str,
    delete_auditor2_zip: bool = True,
    attachment_root: str = "/persistent-storage/datalake",
):
    raise_if_project_name_exists(db, project_name)

    storage_path = Path(attachment_root) / "Auditor2" / project_name
    auditor2_zip_path = Path(auditor2_zip)

    actual_storage_path = extract_auditor2_archive(auditor2_zip_path, storage_path)

    auditor2_data = read_auditor2_csvs(storage_path)

    transformed_auditor2_data = transform_auditor2_data(auditor2_data, project_name)

    for table_name, rows in transformed_auditor2_data.items():
        db_writer = StructuredDBWriter(
            conninfo(db),
            table_name,
            use_mapping_table=False,
            reverse_properties_separated_by=None,
        )
        db_writer.handle_output(rows)
        logger.info(
            f"Auditor 2 data from table '{table_name}' successfully written to the database."
        )

    if delete_auditor2_zip:
        # Now that we've extracted the archive and written the data to the database,
        # we can delete the original ZIP file.
        auditor2_zip_path.unlink()
        logger.info(f"Deleted Timelapse archive: {auditor2_zip_path}")

    return actual_storage_path


def raise_if_project_name_exists(db: postgresql, project_name: str) -> None:
    """
    Checks if the Auditor 2 tables already exist in the database for the given project name.

    Parameters
    ----------
    project_name : str
        The name of the project to check for existing Auditor 2 tables.

    Raises
    ------
    ValueError
        If any of the required Auditor 2 tables already exist.
    """
    logger.debug(
        f"Checking if Auditor 2 project name '{project_name}' already exists in the database."
    )

    required_tables = [
        f"auditor2_{project_name}_deployments",
        f"auditor2_{project_name}_human_readable_labels",
        f"auditor2_{project_name}_labels",
        f"auditor2_{project_name}_sites",
        f"auditor2_{project_name}_sound_file_summary",
    ]

    for table in required_tables:
        if check_if_table_exists(conninfo(db), table):
            raise ValueError(f"Auditor2 project name already in usage in '{table}'.")


def extract_auditor2_archive(
    auditor2_zip_path: Path,
    storage_path: Path,
) -> None:
    """
    Extracts a Auditor 2 ZIP archive to `storage_path`.

    Parameters
    ----------
    auditor2_zip_path : Path
        The path to the Auditor 2 ZIP file.
    storage_path : str
        The path to the root directory where the ZIP file will be extracted.
    """
    logger.debug(
        f"Extracting Auditor 2 archive from {auditor2_zip_path} to {storage_path}"
    )
    if not auditor2_zip_path.exists():
        raise FileNotFoundError(f"Auditor 2 ZIP file not found: {auditor2_zip_path}")
    try:
        shutil.unpack_archive(auditor2_zip_path, storage_path)
        logger.info(f"Extracted Auditor 2 archive to: {storage_path}")
        return storage_path
    except shutil.ReadError as e:
        raise ValueError(f"Unable to extract archive: {e}")


def read_auditor2_csvs(storage_path: Path) -> dict[str, list[dict[str, str]]]:
    """
    Reads specific Auditor 2 CSVs from the extracted files and returns them as a dictionary.

    The extracted directory should contain 5 CSV files, each with names including these substrings:
    - deployments
    - human_readable_labels
    - labels
    - sites
    - sound_file_summary

    Parameters
    ----------
    storage_path : Path
        The path to the directory where the Auditor 2 files were extracted.

    Returns
    -------
    dict[str, list[dict[str, str]]]
        A dictionary where keys are the CSV file identifiers and values are lists of dictionaries containing the CSV data.
    """
    required_keys = [
        "deployments",
        "human_readable_labels",
        "labels",
        "sites",
        "sound_file_summary",
    ]

    logger.debug(
        f"Reading Auditor 2 CSV files from: {storage_path}, looking for keys: {required_keys}"
    )

    csv_files = list(storage_path.glob("*.csv"))
    auditor2_tables = {}
    found_keys = {}

    for file in csv_files:
        for key in required_keys:
            if key in file.stem:
                with file.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                auditor2_tables[key] = rows
                found_keys[key] = True
                break

    missing = set(required_keys) - found_keys.keys()
    if missing:
        raise ValueError(
            f"Missing required CSV file(s) for: {', '.join(sorted(missing))}"
        )

    logger.info(f"Found {len(auditor2_tables)} Auditor 2 CSV files.")
    return auditor2_tables


def transform_auditor2_data(
    auditor2_data: dict[str, list[dict[str, str]]], project_name: str
) -> dict[str, list[dict[str, str]]]:
    """
    Transforms the Auditor 2 CSV data by assigning an `_id` field to each row.

    Parameters
    ----------
    auditor2_data : dict[str, list[dict[str, str]]]
        The raw CSV data read from the files.
    project_name : str
        The name of the project, used to construct table names.

    Returns
    -------
    dict[str, list[dict[str, str]]]
        A dictionary where keys are table names and values are lists of dictionaries containing the transformed CSV data.
    """
    id_fields = {
        "deployments": "deployment_id",
        "human_readable_labels": None,
        "labels": None,
        "sites": "site_id",
        "sound_file_summary": "deployment_id",
    }

    logger.debug(
        f"Transforming Auditor 2 data for project '{project_name}' with ID fields: {id_fields}"
    )

    transformed_data = {}

    # Process each table in the Auditor 2 data
    for table_key, rows in auditor2_data.items():
        table_name = f"auditor2_{project_name}_{table_key}"
        id_field = id_fields.get(table_key)

        for index, row in enumerate(rows):
            if id_field:
                row["_id"] = row.get(id_field, "").strip()
            else:
                row["_id"] = str(index)

            # Add GuardianConnector-compliant geo fields for `sites`
            # (Currently, these are `g__` fields that used to
            # construct GeoJSON objects on the front end. If we
            # ever switch to using something like PostGIS, this
            # logic will need to change.)
            if table_key == "sites":
                try:
                    lat = float(row.get("latitude", "").strip())
                    lon = float(row.get("longitude", "").strip())
                    row["g__coordinates"] = f"[{lon}, {lat}]"
                    row["g__type"] = "Point"
                except (ValueError, TypeError):
                    row["g__coordinates"] = None
                    row["g__type"] = None

        transformed_data[table_name] = rows

    logger.info(f"Transformed Auditor 2 data with {len(transformed_data)} tables.")
    return transformed_data
