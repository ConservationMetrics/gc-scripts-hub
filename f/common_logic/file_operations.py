"""
Utilities for validating, detecting, and saving geospatial or tabular data files.
"""

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


def detect_structured_data_type(file_path: str) -> str:
    """
    Detects the type of a structured data file, focusing on geospatial and tabular formats.

    Only files with recognized extensions are considered. The function attempts to
    parse the file to confirm it matches the expected structure. If the file type is
    unsupported or the content does not validate, 'unsupported' is returned.

    This function is intended to identify structured datasets used in environmental,
    geographic, and tabular data pipelines. It supports:
    - Geospatial formats: geojson, kml, gpx
    - Tabular formats: csv, xls, xlsx
    - General structured formats: json

    TODO: Add support for ESRI shapefiles

    Parameters
    ----------
    file_path : str
        Path to the file to inspect.

    Returns
    -------
    str
        The validated file type (e.g., "csv", "geojson", etc.), or "unsupported" if
        the file does not conform to expectations.
    """

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File does not exist: {file_path}")

    # Only allow files with known extensions
    suffix_map = {
        ".geojson": "geojson",
        ".json": "json",
        ".xlsx": "xlsx",
        ".xls": "xls",
        ".gpx": "gpx",
        ".kml": "kml",
        ".csv": "csv",
    }

    suffix = path.suffix.lower()
    if suffix not in suffix_map:
        return "unsupported"

    expected_type = suffix_map[suffix]

    def get_namespace(tag: str) -> str:
        if tag.startswith("{"):
            return tag[1:].split("}")[0]
        return ""

    # Try validators in priority order; first match wins
    def is_json(path):
        with path.open() as f:
            json.load(f)
        return True

    def is_geojson(path):
        with path.open() as f:
            data = json.load(f)
        return isinstance(data, dict) and data.get("type") == "FeatureCollection"

    def is_excel(path):
        pd.read_excel(path)
        return True

    def is_gpx(path):
        # https://www.topografix.com/gpx/1/1/#SchemaProperties
        # We don't care about the schema version, just that it's a GPX file.
        tree = etree.parse(str(path))
        ns = get_namespace(tree.getroot().tag)
        return ns.startswith("http://www.topografix.com/GPX")

    def is_kml(path):
        # https://developers.google.com/kml/documentation/kmlreference
        # We don't care about the schema version, just that it's a KML file.
        tree = etree.parse(str(path))
        ns = get_namespace(tree.getroot().tag)
        return ns.startswith("http://www.opengis.net/kml")

    def is_csv(path):
        # CSV detection is a bit more complex than the other formats,
        # because it's overly permissive.
        # We need to check that the file is actually a CSV,
        # and not just a file with commas in it.
        try:
            with path.open(newline="") as f:
                reader = csv.reader(f)

                # Read up to 3 rows from the file to test consistency
                rows = []
                for _ in range(3):
                    try:
                        rows.append(next(reader))
                    except StopIteration:
                        break

                # Ensure we have at least a header and one data row
                if len(rows) < 2:
                    return False

                # Ensure all rows have the same number of columns
                width = len(rows[0])
                if width < 2 or any(len(row) != width for row in rows):
                    return False
                return True

        except Exception:
            return False

    validators = {
        "geojson": is_geojson,
        "json": is_json,
        "xlsx": is_excel,
        "xls": is_excel,
        "gpx": is_gpx,
        "kml": is_kml,
        "csv": is_csv,
    }

    try:
        if validators[expected_type](path):
            return expected_type
    except Exception:
        return "unsupported"

    return "unsupported"
