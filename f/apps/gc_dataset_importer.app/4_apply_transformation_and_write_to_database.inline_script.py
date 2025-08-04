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
    """Read CSV file and return as list of dictionaries."""
    logger.info(f"Reading CSV file: {csv_path}")
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        data = list(reader)
    logger.info(f"Read {len(data)} rows from CSV file")
    return data


def list_to_csv_string(data):
    """Convert list of dictionaries to CSV string."""
    if not data:
        return ""

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()


def main(
    db: postgresql,
    filename_original,
    filename_parsed,
    output_format,
    data_source,
    dataset_name,
    valid_sql_name,
):
    # Construct uploaded_path from filename_parsed using dataset-specific temp directory
    uploaded_path = f"/persistent-storage/tmp/{valid_sql_name}/{filename_parsed}"

    try:
        if output_format == "geojson":
            logger.info("Processing GeoJSON file")
            with open(uploaded_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if data_source == "comapeo":
                data = transform_comapeo_observations(data, dataset_name)
                logger.info("Comapeo transformation applied")
                transformed = True
            else:  # TODO: support locus_map, mapeo transformations
                logger.info("No transformation applied for this data source")
                transformed = False

            if transformed:
                output_filename = f"{Path(uploaded_path).stem}_transformed.geojson"
                saved = save_uploaded_file_to_temp(
                    [{"name": output_filename, "data": json.dumps(data)}],
                    is_base64=False,
                    tmp_dir=f"/persistent-storage/tmp/{valid_sql_name}",
                )
                geojson_path = saved["file_paths"][0]
                logger.info(f"Transformed file saved to: {geojson_path}")
            else:
                geojson_path = uploaded_path
                logger.info(f"Using original file: {geojson_path}")

            logger.info(f"Saving GeoJSON to PostgreSQL table: {valid_sql_name}")
            save_geojson_to_postgres(
                db=db, db_table_name=valid_sql_name, geojson_path=geojson_path
            )

            # Copy parsed/transformed file to datalake
            datalake_dir = Path(f"/persistent-storage/datalake/{valid_sql_name}/")
            datalake_dir.mkdir(parents=True, exist_ok=True)
            output_filename = (
                f"{Path(uploaded_path).stem}_transformed.geojson"
                if transformed
                else filename_parsed
            )
            shutil.copy2(geojson_path, datalake_dir / output_filename)
            logger.info(
                f"Copied {'transformed' if transformed else 'parsed'} file to datalake: {output_filename}"
            )

        elif output_format == "csv":
            logger.info("Processing CSV file")
            data = read_csv_to_list(uploaded_path)

            if data_source == "kobotoolbox":
                data = transform_kobotoolbox_form_data(data, dataset_name)
                logger.info("Kobotoolbox transformation applied")
                transformed = True
            elif data_source == "odk":
                data = transform_odk_form_data(data, dataset_name)
                logger.info("ODK transformation applied")
                transformed = True
            else:
                logger.info("No transformation applied for this data source")
                transformed = False

            if transformed:
                output_filename = f"{Path(uploaded_path).stem}_transformed.csv"
                csv_str = list_to_csv_string(data)

                saved = save_uploaded_file_to_temp(
                    [{"name": output_filename, "data": csv_str}],
                    is_base64=False,
                    tmp_dir=f"/persistent-storage/tmp/{valid_sql_name}",
                )
                csv_path = saved["file_paths"][0]
                logger.info(f"Transformed file saved to: {csv_path}")
            else:
                csv_path = uploaded_path
                logger.info(f"Using original file: {csv_path}")

            # TODO: There is no CSV to Postgres connector. What to do?
            # save_csv_to_postgres(db=db, db_table_name=dataset_name, csv_path=csv_path)

            # Copy parsed/transformed file to datalake
            datalake_dir = Path(f"/persistent-storage/datalake/{valid_sql_name}/")
            datalake_dir.mkdir(parents=True, exist_ok=True)
            output_filename = (
                filename_parsed.replace("_parsed", "_transformed")
                if transformed
                else filename_parsed
            )
            shutil.copy2(csv_path, datalake_dir / output_filename)
            logger.info(
                f"Copied {'transformed' if transformed else 'parsed'} file to datalake: {output_filename}"
            )

        # Save filename_original to the same directory as parsed/transformed files
        original_file_path = (
            f"/persistent-storage/tmp/{valid_sql_name}/{filename_original}"
        )
        if Path(original_file_path).exists():
            # Copy original file to datalake directory
            datalake_dir = Path(f"/persistent-storage/datalake/{valid_sql_name}/")
            datalake_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(original_file_path, datalake_dir / filename_original)
            logger.info(f"Original file copied to: {datalake_dir / filename_original}")
        else:
            logger.warning(f"Original file not found: {original_file_path}")

    except Exception as e:
        logger.error(f"Error during dataset import process: {e}")
        raise

    finally:
        # Clean up dataset-specific temp directory
        temp_dir = Path(f"/persistent-storage/tmp/{valid_sql_name}")
        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
                logger.info(f"Removed dataset temp directory: {temp_dir}")
            except Exception as e:
                logger.warning(
                    f"Failed to remove dataset temp directory {temp_dir}: {e}"
                )
        else:
            logger.debug(
                f"Dataset temp directory not found (already removed?): {temp_dir}"
            )
