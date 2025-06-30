import csv
import json
import logging
from pathlib import Path

import pandas as pd
from lxml import etree

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def detect_structured_data_type(file_path: str) -> str:
    """
    Detects the type of a structured data file, focusing on geospatial and tabular formats.

    Only files with recognized extensions are considered. The function attempts to
    parse the file to confirm it matches the expected structure. If the file type is
    unsupported or the content does not validate, 'unsupported' is returned.

    This function is intended to identify structured datasets used in environmental,
    geographic, and tabular data pipelines. It supports:
    - Spatial formats: geojson, kml, gpx
    - Tabular formats: csv, xls, xlsx
    - General structured formats: json

    TODO: Add support for ESRI shapefiles, which requires a different approach as a
    collection of files rather than a single file.

    Validation is intentionally lightweight. We avoid fully loading large or malformed
    files where possible. For tabular data, we use pandas selectively (e.g., Excel files)
    and simpler parsers for CSV. In the future, we could consider a more robust approach
    by consistently using `pandas` for tabular formats and something like `fiona` for
    spatial vector formats, including shapefiles. For now, this function errs on the side
    of speed and minimal side effects.

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

    logger.debug(f"Checking file {file_path} with suffix {suffix}")

    if suffix not in suffix_map:
        return "unsupported"

    expected_type = suffix_map[suffix]

    def get_namespace(tag: str) -> str:
        if tag.startswith("{"):
            return tag[1:].split("}")[0]
        return ""

    def is_json(path):
        with path.open() as f:
            json.load(f)
        return True

    def is_geojson(path):
        with path.open() as f:
            data = json.load(f)
        return isinstance(data, dict) and data.get("type") == "FeatureCollection"

    def is_excel(path):
        # Excel is a binary format and there's no lightweight parser,
        # so we use pd.read_excel despite its overhead. If needed,
        # we could explore faster or more granular validation later.
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
        # CSV detection is a bit more complex than the other formats
        # because it's overly permissive. We use csv.reader to cheaply
        # validate structure without reading the full file into memory.
        # In the future, we could consider using pd.read_csv for more
        # robust validation at the cost of performance and tolerance.
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

    # Map file suffix to (type name, validator function)
    type_map = {
        ".geojson": ("geojson", is_geojson),
        ".json": ("json", is_json),
        ".xlsx": ("xlsx", is_excel),
        ".xls": ("xls", is_excel),
        ".gpx": ("gpx", is_gpx),
        ".kml": ("kml", is_kml),
        ".csv": ("csv", is_csv),
    }

    suffix = path.suffix.lower()

    logger.debug(f"Checking file {file_path} with suffix {suffix}")

    if suffix not in type_map:
        return "unsupported"

    expected_type, validator = type_map[suffix]

    try:
        if validator(path):
            logger.info(f"File {file_path.name} validated as {expected_type}")
            return expected_type
    except Exception:
        logger.warning(
            f"File {file_path.name} has extension '{suffix}' but failed {expected_type} validation — possibly malformed or misnamed."
        )
        return "unsupported"
