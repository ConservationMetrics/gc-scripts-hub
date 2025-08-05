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

    Returns
    -------
    tuple
        A tuple containing (output_filename, output_format):
        - output_filename : str
            Name of the converted file with '_parsed' suffix.
        - output_format : str
            Format of converted file ('csv' or 'geojson').
    """
    logger.info(f"Starting file upload and conversion for dataset: {dataset_name}")

    temp_dir = Path(f"/persistent-storage/tmp/{dataset_name}")
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
    else:  # geojson
        file_to_save = [{"name": output_filename, "data": json.dumps(converted_data)}]

    saved_output = save_uploaded_file_to_temp(
        file_to_save, is_base64=False, tmp_dir=str(temp_dir)
    )
    output_path = saved_output["file_paths"][0]
    logger.info(f"Saved parsed file to: {output_path}")

    return output_filename, output_format
