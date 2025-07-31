from f.common_logic.file_operations import save_uploaded_file_to_temp
from f.common_logic.data_conversion import detect_structured_data_type, convert_data

def main(uploaded_file):
    file_paths = save_uploaded_file_to_temp(uploaded_file)
    
    file_path = file_paths['file_paths'][0]

    file_type = detect_structured_data_type(file_path)

    converted_data = convert_data(file_path, file_type)

    print(converted_data)
   