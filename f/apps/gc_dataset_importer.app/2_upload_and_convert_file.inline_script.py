import json
from pathlib import Path

from f.common_logic.data_conversion import convert_data, detect_structured_data_type
from f.common_logic.file_operations import save_uploaded_file_to_temp


def main(uploaded_file):
    # Save the original uploaded file to a temp path
    saved_input = save_uploaded_file_to_temp(uploaded_file)
    input_path = saved_input["file_paths"][0]

    # Detect the file type (e.g., csv, geojson, etc.)
    file_format = detect_structured_data_type(input_path)

    # Convert the file into a normalized structure
    converted_data, output_format = convert_data(input_path, file_format)

    # Build a new filename with the same stem and file format
    output_filename = f"{Path(input_path).stem}_parsed.{output_format}"
    file_to_save = [{"name": output_filename, "data": json.dumps(converted_data)}]

    # Save the converted data to a temp path
    saved_output = save_uploaded_file_to_temp(file_to_save, is_base64=False)
    output_path = saved_output["file_paths"][0]

    return output_path
