import csv
import json
import logging
from io import StringIO
from pathlib import Path

from f.common_logic.data_conversion import convert_data, detect_structured_data_type
from f.common_logic.file_operations import save_uploaded_file_to_temp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(uploaded_file, dataset_name):
    logger.info(f"Starting file upload and conversion for dataset: {dataset_name}")

    # Create dataset-specific temp directory
    temp_dir = Path(f"/persistent-storage/tmp/{dataset_name}")
    temp_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created dataset temp directory: {temp_dir}")

    # Save the original uploaded file to the dataset temp path
    saved_input = save_uploaded_file_to_temp(uploaded_file, tmp_dir=str(temp_dir))
    input_path = saved_input["file_paths"][0]
    logger.info(f"Saved original file to: {input_path}")

    # Detect the file type (e.g., GPX, KML, XLS, CSV, GeoJSON, etc.)
    file_format = detect_structured_data_type(input_path)
    logger.info(f"Detected file format: {file_format}")

    # Convert the file into either CSV or GeoJSON
    converted_data, output_format = convert_data(input_path, file_format)
    logger.info(f"Converted to format: {output_format}")

    # Build a new filename with the same stem and converted output file format
    output_filename = f"{Path(input_path).stem}_parsed.{output_format}"

    if output_format == "csv":
        # Convert list of lists to CSV string
        output = StringIO()
        writer = csv.writer(output)
        writer.writerows(converted_data)
        csv_data = output.getvalue()

        file_to_save = [{"name": output_filename, "data": csv_data}]
    else:
        # For GeoJSON, save as JSON
        file_to_save = [{"name": output_filename, "data": json.dumps(converted_data)}]

    # Save the converted data to the dataset temp path
    saved_output = save_uploaded_file_to_temp(
        file_to_save, is_base64=False, tmp_dir=str(temp_dir)
    )
    output_path = saved_output["file_paths"][0]
    logger.info(f"Saved parsed file to: {output_path}")

    return output_filename, output_format
