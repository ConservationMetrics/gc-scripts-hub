import logging
import base64
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(uploaded_file, tmp_dir: str = "/frizzle-persistent-storage/tmp"):
    result = {}
    try:
        filename = uploaded_file[0]['name']
        encoded_data = uploaded_file[0]['data'].encode()
        decoded_data = base64.b64decode(encoded_data)

        tmp_dir_path = Path(tmp_dir)
        tmp_dir_path.mkdir(parents=True, exist_ok=True)
        file_path = tmp_dir_path / filename

        with open(file_path, "wb") as f:
            f.write(decoded_data)

        logger.info(f"Saved file to {file_path}")

        result = {
            "db_table_name": Path(filename).stem,
            "file_path": str(file_path)
        }
    except Exception as e:
        result = {
            "error": str(e)
        }

    return result