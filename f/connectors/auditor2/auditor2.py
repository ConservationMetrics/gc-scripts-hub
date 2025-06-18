# requirements:
# pandas~=2.2
# psycopg2-binary

import csv
import logging
import shutil
import tempfile
from pathlib import Path

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    auditor2_zip: str,
    db: postgresql,
    db_table_prefix: str,
    delete_auditor2_zip: bool = True,
    attachment_root: str = "/persistent-storage/datalake",
):
    base_storage_path = Path(attachment_root) / "Auditor2" / db_table_prefix

    auditor2_zip_path = Path(auditor2_zip)
    actual_storage_path = extract_auditor2_archive(auditor2_zip_path, base_storage_path)

    auditor2_data = read_auditor2_csvs(actual_storage_path)

    transformed_auditor2_data = transform_auditor2_data(auditor2_data, db_table_prefix)

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


def extract_auditor2_archive(
    auditor2_zip_path: Path,
    storage_path: Path,
) -> Path:
    """
    Extracts a Auditor 2 ZIP archive to a uniquely named subdirectory under `storage_path`.

    The final target path is: storage_path / <zip_stem>[_n]
    where `_n` ensures uniqueness if a folder with the same name already exists.

    Parameters
    ----------
    auditor2_zip_path : Path
        The path to the Auditor 2 ZIP file.
    storage_path : str
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
                auditor2_zip_path,
                extract_to,
            )
            logger.info(f"Extracted Auditor 2 archive: {auditor2_zip_path}")
        except shutil.ReadError as e:
            raise ValueError(f"Unable to extract archive: {e}")

        zip_name = auditor2_zip_path.stem
        base_target_path = storage_path / zip_name
        final_target_path = base_target_path
        counter = 1

        # Guarantee a clean, non-conflicting destination folder e.g. "auditor2_export"
        # or "auditor2_export_1" if the first one already exists.
        while final_target_path.exists():
            final_target_path = storage_path / f"{zip_name}_{counter}"
            counter += 1

        final_target_path.mkdir(parents=True)

        shutil.copytree(extract_to, final_target_path, dirs_exist_ok=True)

        logger.info(f"Copied contents of Auditor 2 archive to: {final_target_path}")
        return final_target_path


def read_auditor2_csvs(base_storage_path: Path) -> dict[str, list[dict[str, str]]]:
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
    base_storage_path : Path
        The path to the directory where the Auditor 2 files were extracted.

    Returns
    -------
    dict[str, list[dict[str, str]]]
        A dictionary where keys are file stems and values are lists of dictionaries containing the CSV data.
    """
    required_keys = [
        "deployments",
        "human_readable_labels",
        "labels",
        "sites",
        "sound_file_summary",
    ]

    csv_files = list(base_storage_path.glob("*.csv"))
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
    auditor2_data: dict[str, list[dict[str, str]]], db_table_prefix: str
) -> dict[str, list[dict[str, str]]]:
    """
    Transforms the Auditor 2 CSV data by assigning an `_id` field to each row.

    Parameters
    ----------
    auditor2_data : dict[str, list[dict[str, str]]]
        The raw CSV data read from the files.
    db_table_prefix : str
        The prefix to use for the database table names.

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

    transformed_data = {}

    for table_key, rows in auditor2_data.items():
        table_name = f"{db_table_prefix}_{table_key}"
        id_field = id_fields.get(table_key)

        for index, row in enumerate(rows):
            if id_field:
                row["_id"] = row.get(id_field, "").strip()
            else:
                row["_id"] = str(index)

            # Add geo fields for `sites`
            if table_key == "sites":
                try:
                    lat = float(row.get("Latitude", "").strip())
                    lon = float(row.get("Longitude", "").strip())
                    row["g__coordinates"] = f"[{lon}, {lat}]"
                    row["g__type"] = "Point"
                except (ValueError, TypeError):
                    row["g__coordinates"] = None
                    row["g__type"] = None

        transformed_data[table_name] = rows

    logger.info(f"Transformed Auditor 2 data with {len(transformed_data)} tables.")
    return transformed_data
