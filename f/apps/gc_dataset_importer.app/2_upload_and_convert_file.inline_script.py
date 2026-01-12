import csv
import json
import logging
from io import StringIO
from pathlib import Path

from f.common_logic.data_conversion import convert_data, detect_structured_data_type
from f.common_logic.db_operations import (
    postgresql,
    summarize_new_rows_updates_and_columns,
)
from f.common_logic.file_operations import save_uploaded_file_to_temp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(uploaded_file, dataset_name, table_exists, table_name, db: postgresql):
    """
    Process uploaded file and convert to standardized format.

    Takes an uploaded file, detects its format, and converts it to either CSV or GeoJSON
    depending on the data type. Saves both original and converted files to a dataset-specific
    temporary directory for further processing.

    Parameters
    ----------
    uploaded_file : object or list
        File object or list containing uploaded file data.
    dataset_name : str
        Name of the dataset, used for creating temp directory paths.
    table_exists: bool
        True if table exists, False if not.
    table_name: str
        Name of the table.
    db: postgresql
        Database connection object.

    Returns
    -------
    tuple[bool, str | None, str | None, str | None, int | None, int | None, int | None]
        A tuple containing (success, error_message, output_filename, output_format,
                           new_rows, updates, new_columns):
        - success : bool
            True if processing completed successfully, False if an error occurred.
        - error_message : str or None
            Error message if success is False, None if success is True.
        - output_filename : str or None
            Name of the converted file with '_parsed' suffix if successful, None if failed.
        - output_format : str or None
            Format of converted file ('csv' or 'geojson') if successful, None if failed.
        - new_rows : int or None
            Number of new rows that will be added (only if table_exists).
        - updates : int or None
            Number of rows that will be updated (only if table_exists).
        - new_columns : int or None
            Number of new columns that will be added (only if table_exists).
    """
    new_rows = None
    updates = None
    new_columns = None
    data_for_comparison = []

    try:
        logger.info(f"Starting file upload and conversion for dataset: {dataset_name}")

        # Use table_name (valid SQL name) for temp directory to match what Step 4 expects
        temp_dir = Path(f"/persistent-storage/tmp/{table_name}")
        temp_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created dataset temp directory: {temp_dir}")

        saved_input = save_uploaded_file_to_temp(uploaded_file, tmp_dir=str(temp_dir))
        input_path = saved_input["file_paths"][0]
        logger.info(f"Saved original file to: {input_path}")

        file_format = detect_structured_data_type(input_path)
        logger.info(f"Detected file format: {file_format}")

        converted_data, output_format = convert_data(input_path, file_format)
        logger.info(f"Converted to format: {output_format}")

        output_filename = f"{Path(input_path).stem}_parsed.{output_format}"

        if output_format == "csv":
            output = StringIO()
            writer = csv.writer(output)
            writer.writerows(converted_data)
            csv_data = output.getvalue()

            file_to_save = [{"name": output_filename, "data": csv_data}]

            # Convert CSV to list of dicts for analysis (comparison or counting)
            if converted_data:
                reader = csv.DictReader(StringIO(csv_data))
                rows = list(reader)
                # Add _id if not present (use existing _id or auto-increment)
                data_for_comparison = []
                for idx, row in enumerate(rows, 1):
                    if "_id" not in row or not row["_id"]:
                        # No _id in CSV, use auto-incrementing
                        row_with_id = {"_id": str(idx), **row}
                    else:
                        # CSV already has _id, use it
                        row_with_id = row
                    data_for_comparison.append(row_with_id)
        else:  # geojson
            file_to_save = [
                {"name": output_filename, "data": json.dumps(converted_data)}
            ]

            # Extract features as list of dicts for analysis (comparison or counting)
            if isinstance(converted_data, dict) and "features" in converted_data:
                # GeoJSON FeatureCollection - extract properties from features
                # Add _id if not present (use existing _id or auto-increment)
                data_for_comparison = []
                for idx, feature in enumerate(converted_data.get("features", []), 1):
                    properties = feature.get("properties", {}).copy()
                    if "_id" not in properties or not properties["_id"]:
                        # No _id in properties, use auto-incrementing
                        properties = {"_id": str(idx), **properties}
                    data_for_comparison.append(properties)

        saved_output = save_uploaded_file_to_temp(
            file_to_save, is_base64=False, tmp_dir=str(temp_dir)
        )
        output_path = saved_output["file_paths"][0]
        logger.info(f"Saved parsed file to: {output_path}")

        # Analyze data: compare if table exists, count if new dataset
        if data_for_comparison:
            if table_exists:
                logger.info(
                    f"Analyzing changes for {len(data_for_comparison)} rows against existing table"
                )
                new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
                    db, table_name=table_name, new_data=data_for_comparison
                )
                logger.info(
                    f"Impact analysis complete: {new_rows} new rows, {updates} updates, {new_columns} new columns"
                )
            else:
                # New dataset - count rows and columns
                logger.info("Counting rows and columns for new dataset")
                new_rows = len(data_for_comparison)
                updates = 0
                # Count unique columns across all rows
                all_columns = set()
                for row in data_for_comparison:
                    all_columns.update(row.keys())
                new_columns = len(all_columns)
                logger.info(
                    f"New dataset will have {new_rows} rows and {new_columns} columns"
                )
        else:
            logger.warning("⚠️  No data to analyze!")

        return (
            True,
            None,
            output_filename,
            output_format,
            new_rows,
            updates,
            new_columns,
        )

    except Exception as e:
        error_msg = f"Error during file upload and conversion: {e}"
        logger.error(error_msg)
        return False, error_msg, None, None, None, None, None
