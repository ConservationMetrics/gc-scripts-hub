import csv
import json
import logging
import shutil
from io import StringIO
from pathlib import Path

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_uploaded_file_to_temp
from f.connectors.comapeo.comapeo_observations import transform_comapeo_observations
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres
from f.connectors.kobotoolbox.kobotoolbox_responses import (
    transform_kobotoolbox_form_data,
)
from f.connectors.odk.odk_responses import transform_odk_form_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_csv_to_list(csv_path):
    """
    Read CSV file and return as list of dictionaries.

    Parameters
    ----------
    csv_path : str or pathlib.Path
        Path to the CSV file to read.

    Returns
    -------
    list of dict
        List of dictionaries representing CSV rows with column headers as keys.
    """
    logger.info(f"Reading CSV file: {csv_path}")
    with open(csv_path, "r", encoding="utf-8") as f:
        data = list(csv.DictReader(f))
    logger.info(f"Read {len(data)} rows from CSV file")
    return data


def list_to_csv_string(data):
    """
    Convert list of dictionaries to CSV string.

    Parameters
    ----------
    data : list of dict
        List of dictionaries to convert to CSV format.

    Returns
    -------
    str
        CSV-formatted string with headers and data rows.
        Returns empty string if input data is empty.
    """
    if not data:
        return ""

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()


def _save_transformed_file(data, filename, file_format, tmp_dir):
    """
    Save transformed data to temp directory and return file path.

    Parameters
    ----------
    data : list or dict
        Transformed data to save (list for CSV, dict for GeoJSON).
    filename : str
        Name of the file to save.
    file_format : str
        Format of the file ('csv' or 'geojson').
    tmp_dir : pathlib.Path
        Temporary directory path where file will be saved.

    Returns
    -------
    str
        Full path to the saved file.
    """
    if file_format == "geojson":
        file_data = json.dumps(data)
    else:  # csv
        file_data = list_to_csv_string(data)

    saved = save_uploaded_file_to_temp(
        [{"name": filename, "data": file_data}],
        is_base64=False,
        tmp_dir=str(tmp_dir),
    )
    return saved["file_paths"][0]


def _copy_to_datalake(source_path, datalake_dir, output_filename, file_type):
    """
    Copy file to datalake directory.

    Parameters
    ----------
    source_path : str or pathlib.Path
        Path to the source file to copy.
    datalake_dir : pathlib.Path
        Destination directory in the datalake.
    output_filename : str
        Name of the file in the datalake.
    file_type : str
        Type description for logging ('original', 'parsed', 'transformed').
    """
    datalake_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, datalake_dir / output_filename)
    logger.info(f"Copied {file_type} file to datalake: {output_filename}")


def _apply_transformation(data, data_source, dataset_name, output_format):
    """
    Apply transformation based on data source and return transformed data and status.

    Parameters
    ----------
    data : list or dict
        Raw data to transform (list for CSV, dict for GeoJSON).
    data_source : str
        Source system identifier ('comapeo', 'kobotoolbox', 'odk', etc.).
    dataset_name : str
        Human-readable name of the dataset.
    output_format : str
        Format of the data ('csv' or 'geojson').

    Returns
    -------
    tuple
        A tuple containing (transformed_data, transformation_applied):
        - transformed_data : list or dict
            Data after transformation (same type as input).
        - transformation_applied : bool
            Whether any transformation was applied.
    """
    transformations = {
        ("geojson", "comapeo"): (
            transform_comapeo_observations,
            "Comapeo transformation applied",
        ),
        ("csv", "kobotoolbox"): (
            transform_kobotoolbox_form_data,
            "Kobotoolbox transformation applied",
        ),
        ("csv", "odk"): (transform_odk_form_data, "ODK transformation applied"),
    }

    transform_key = (output_format, data_source)
    if transform_key in transformations:
        transform_func, log_msg = transformations[transform_key]
        transformed_data = transform_func(data, dataset_name)
        logger.info(log_msg)
        return transformed_data, True
    else:
        if output_format == "geojson":
            # TODO: support locus_map, mapeo transformations
            pass
        logger.info("No transformation applied for this data source")
        return data, False


def main(
    db: postgresql,
    filename_original,
    filename_parsed,
    output_format,
    data_source,
    dataset_name,
    valid_sql_name,
):
    """
    Apply transformations and store data in database and data lake.

    Processes parsed data files by applying data source-specific transformations,
    storing data in PostgreSQL, and organizing all files in the data lake.

    Processing Steps:
        1. Load parsed data from temporary storage
        2. Apply data source-specific transformations if available
        3. Save transformed data to temporary files (if transformation applied)
        4. Store data in PostgreSQL database table
        5. Copy original, parsed, and transformed files to data lake
        6. Clean up temporary directories

    Parameters
    ----------
    db : postgresql
        Database connection object for PostgreSQL operations.
    filename_original : str
        Name of the original uploaded file.
    filename_parsed : str
        Name of the parsed/converted file from previous step.
    output_format : str
        Format of the parsed file ('csv' or 'geojson').
    data_source : str
        Source system identifier ('comapeo', 'kobotoolbox', 'odk', etc.).
    dataset_name : str
        Human-readable name of the dataset.
    valid_sql_name : str
        SQL-safe name used for database tables and directories.
    """
    tmp_dir = Path(f"/persistent-storage/tmp/{valid_sql_name}")
    datalake_dir = Path(f"/persistent-storage/datalake/{valid_sql_name}")
    uploaded_path = tmp_dir / filename_parsed
    original_path = tmp_dir / filename_original

    try:
        logger.info(f"Processing {output_format.upper()} file")

        # Load data based on format
        if output_format == "geojson":
            with open(uploaded_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:  # csv
            data = read_csv_to_list(uploaded_path)

        # Apply transformation based on data source
        data, transformed = _apply_transformation(
            data, data_source, dataset_name, output_format
        )

        # Handle transformed vs original file
        if transformed:
            output_filename = f"{uploaded_path.stem}_transformed.{output_format}"
            file_path = _save_transformed_file(
                data, output_filename, output_format, tmp_dir
            )
            logger.info("Transformed file saved to temp directory")
        else:
            file_path = str(uploaded_path)
            logger.info(f"Using original file: {file_path}")

        # Save to PostgreSQL
        if output_format == "geojson":
            save_geojson_to_postgres(
                db=db, db_table_name=valid_sql_name, geojson_path=file_path
            )
            logger.info(f"GeoJSON saved to PostgreSQL table: {valid_sql_name}")
        else:  # csv
            # TODO: There is no CSV to Postgres connector. What to do?
            # save_csv_to_postgres(db=db, db_table_name=dataset_name, csv_path=csv_path)
            # logger.info(f"CSV saved to PostgreSQL table: {dataset_name}")
            pass

        # Copy parsed/transformed file to datalake
        if transformed:
            if output_format == "geojson":
                datalake_filename = f"{uploaded_path.stem}_transformed.geojson"
            else:  # csv
                datalake_filename = filename_parsed.replace("_parsed", "_transformed")
        else:
            datalake_filename = filename_parsed

        _copy_to_datalake(
            file_path,
            datalake_dir,
            datalake_filename,
            "transformed" if transformed else "parsed",
        )

        # Save originally uploaded file to the same directory as parsed/transformed files
        if original_path.exists():
            # Copy original file to datalake directory
            _copy_to_datalake(
                original_path, datalake_dir, filename_original, "original"
            )
        else:
            logger.warning(f"Original file not found: {original_path}")

    except Exception as e:
        logger.error(f"Error during dataset import process: {e}")
        raise

    finally:
        # Clean up dataset-specific temp directory
        if tmp_dir.exists():
            try:
                shutil.rmtree(tmp_dir)
                logger.info(f"Removed dataset temp directory: {tmp_dir}")
            except Exception as e:
                logger.warning(
                    f"Failed to remove dataset temp directory {tmp_dir}: {e}"
                )
        else:
            logger.debug(
                f"Dataset temp directory not found (already removed?): {tmp_dir}"
            )
