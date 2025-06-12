import csv
import json
import logging
from pathlib import Path

import pandas as pd
from lxml import etree

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


def detect_file_type(file_path: str) -> str:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File does not exist: {file_path}")

    def is_json(path):
        with path.open() as f:
            json.load(f)
        return True

    def is_geojson(path):
        with path.open() as f:
            data = json.load(f)
        return isinstance(data, dict) and data.get("type") == "FeatureCollection"

    def is_csv(path):
        with path.open(newline="") as f:
            sample = f.read(1024)
            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                return False
            f.seek(0)
            reader = csv.reader(f, dialect)
            headers = next(reader, None)
            return headers is not None and all(h.strip() for h in headers)

    def is_excel(path):
        pd.read_excel(path)
        return True

    def is_gpx(p):
        tree = etree.parse(str(p))
        root = tree.getroot()
        return "http://www.topografix.com/GPX/1/1" in root.nsmap.values()

    def is_kml(p):
        tree = etree.parse(str(p))
        root = tree.getroot()
        return "http://www.opengis.net/kml/2.2" in root.nsmap.values()

    validators = [
        ("geojson", is_geojson),
        ("json", is_json),
        ("xls", is_excel),
        ("gpx", is_gpx),
        ("kml", is_kml),
        ("csv", is_csv),
    ]

    for format, checker in validators:
        try:
            if checker(path):
                return format
        except Exception:
            continue

    return "unknown"
