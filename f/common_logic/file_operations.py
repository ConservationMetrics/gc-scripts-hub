import base64
import csv
import json
import logging
import zipfile
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_safe_file_path(storage_path: str, db_table_name: str, file_type: str):
    """
    Construct a safe file path for storing data, ensuring it remains within the specified storage directory;
    otherwise, raises a ValueError.
    """
    storage_path = Path(storage_path).resolve()
    file_path = (storage_path / f"{db_table_name}.{file_type}").resolve()

    if not file_path.is_relative_to(storage_path):
        raise ValueError("Invalid path: possible path traversal detected.")

    return file_path


def save_data_to_file(data, filename: str, storage_path: str, file_type: str = "json"):
    """
    Saves the provided data to a file in the specified format and storage path.

    Parameters
    ----------
    data : list or dict
        The data to be saved. For CSV, should be a list of rows including a header.
    filename : str
        The name of the file to save the data to, without extension.
    storage_path : str
        The directory path where the file will be saved.
    file_type : str
        The format to save the file as: "json", "geojson", or "csv".
    """
    storage_path = Path(storage_path)
    storage_path.mkdir(parents=True, exist_ok=True)

    if not data or (
        # Extra check for geojson files to avoid saving empty files
        file_type in {"geojson"} and isinstance(data, dict) and not data.get("features")
    ):
        logger.warning(f"No data to save for file: {filename}.{file_type}")
        return

    file_path = get_safe_file_path(storage_path, filename, file_type)

    if file_type in {"geojson", "json"}:
        with file_path.open("w") as f:
            json.dump(data, f)
    elif file_type == "csv":
        with file_path.open("w", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerows(data)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    logger.info(f"{file_type.upper()} file saved to {file_path}")


def save_uploaded_file_to_temp(uploaded_file, tmp_dir: str = "/persistent-storage/tmp"):
    """
    Saves an uploaded file to a temp directory and extracts it if it's a zip.

    Parameters
    ----------
    uploaded_file : list of dict
        List with one dict: {"name": str, "data": base64-encoded str}
        (This is the format in which files are uploaded to a Windmill app.)
    tmp_dir : str
        Directory to save file(s). Default: /persistent-storage/tmp

    Returns
    -------
        dict
            On success:
                - file_paths: list of one or more saved file paths (extracted if zip)
            On failure:
                - error: string error message
    """
    try:
        file_info = uploaded_file[0]
        filename = file_info["name"]
        raw_data = base64.b64decode(file_info["data"].encode())

        tmp_dir_path = Path(tmp_dir)
        tmp_dir_path.mkdir(parents=True, exist_ok=True)

        file_path = tmp_dir_path / filename
        with open(file_path, "wb") as f:
            f.write(raw_data)

        logger.debug(f"Saved file to temp: {file_path}")

        if zipfile.is_zipfile(file_path):
            extract_dir = file_path.with_suffix("")
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
                file_paths = [str(p) for p in extract_dir.rglob("*") if p.is_file()]
                logger.debug(f"Extracted {len(file_paths)} files to {extract_dir}")
            file_path.unlink()
        else:
            file_paths = [str(file_path)]

        return {"file_paths": file_paths}

    except Exception as e:
        logger.error(f"Error processing uploaded file: {e}")
        return {"error": str(e)}
