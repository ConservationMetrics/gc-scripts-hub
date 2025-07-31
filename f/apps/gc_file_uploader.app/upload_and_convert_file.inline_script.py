import json

from f.common_logic.file_operations import save_uploaded_file_to_temp
from f.common_logic.data_conversion import detect_structured_data_type, convert_data
from pathlib import Path

def main(uploaded_file):
    # Save the original uploaded file to a temp path
    saved_input = save_uploaded_file_to_temp(uploaded_file)
    input_path = saved_input['file_paths'][0]

    # Detect the file type (e.g., csv, geojson, etc.)
    file_type = detect_structured_data_type(input_path)

    # Convert the file into a normalized structure
    converted_data = convert_data(input_path, file_type)

    # Build a new filename with the same stem and file type
    output_filename = f"{Path(input_path).stem}_converted.{file_type}"
    file_to_save = [{
        "name": output_filename,
        "data": json.dumps(converted_data)
    }]

    # Save the converted data to a temp path
    saved_output = save_uploaded_file_to_temp(file_to_save)
    output_path = saved_output['file_paths'][0]

    return output_path