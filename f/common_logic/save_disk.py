import csv
import json
import logging
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
