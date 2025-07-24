import csv
import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path

import fiona

# pandas requires openpyxl installed separately to read .xlsx files
import openpyxl  # noqa: F401
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
        # because it's overly permissive. Currently, we use csv.reader
        # to cheaply validate structure without reading the full file
        # into memory.
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
            logger.info(f"File {path.name} validated as {expected_type}")
            return expected_type
    except Exception:
        logger.warning(
            f"File {path.name} has extension '{suffix}' but failed {expected_type} validation — possibly malformed or misnamed."
        )
        return "unsupported"


def handle_file_errors(func):
    """
    Decorator to handle file-related errors for functions that process files.
    """

    def wrapper(path: Path):
        try:
            return func(path)
        except FileNotFoundError:
            raise ValueError(f"File not found: {path}")
        except ET.ParseError as e:
            raise ValueError(f"Malformed XML in {path}: {e}")
        except ValueError as e:
            raise e
        except Exception as e:
            raise ValueError(f"Failed to read file {path}: {e}")

    return wrapper


def convert_data(file_path: str, file_format: str):
    """
    Converts a structured input file into a standard tabular or spatial format.

    NOTE: Right now, we are assuming that spatial data (e.g. GPX, KML) will only
    be converted into GeoJSON, and not CSV. And that tabular data (e.g. Excel,
    JSON) will only be converted into CSV, and not GeoJSON.
    In the future, we will want to de-couple the output format from the input format e.g.
    1. Transform spatial CSV (with lat, lon columns) into GeoJSON
    2. Convert GPX to Spatial CSV (with lat, lon columns)
    3. Convert spatial GeoJSON to tabular CSV
    In that case a better functional API might be a suite of `read_*()` fns that return
    some intermediate representation, and a suite of `to_*()` fns that takes that intermediate
    representation.

    NOTE: We assume that the input file has one layer only. In the future, we might
    consider an extension where the number of layers might be > 1. e.g multiple sheets in Excel.
    And/or carrying thru metadata from the source layer (e.g. Excel sheet name).

    Parameters
    ----------
    file_path : str
        Path to the input file.
    file_format : str
        Validated file format: one of 'csv', 'xlsx', 'xls', 'json',
        'gpx', 'kml', 'geojson'.

    Returns
    -------
    Union[list[list[str]], dict]
        Converted data as CSV (list of lists) or GeoJSON (dict).
    """
    path = Path(file_path)
    logger.debug(f"Converting {file_path} with format {file_format}")

    match file_format:
        case "csv":
            return read_csv(path)
        case "xlsx" | "xls":
            return excel_to_csv(path)
        case "json":
            return json_to_csv(path)
        case "geojson":
            return read_geojson(path)
        case "gpx":
            return gpx_to_geojson(path)
        case "kml":
            return kml_to_geojson(path)
        case _:
            raise ValueError(f"Unsupported file format: {file_format}")


@handle_file_errors
def read_csv(path: Path):
    """
    Reads a CSV file and returns its content as a list of lists. The first row is treated
    as the header.
    Raises ValueError if the file is empty or contains no data.

    Returns
    -------
    list[list[str]]
        The content of the CSV file as a list of lists.
    """
    with path.open(encoding="utf-8", newline="") as f:
        # Read a sample to detect delimiter and check for emptiness
        sample = f.read(1024)
        if not sample.strip():
            raise ValueError("CSV file is empty or contains only whitespace")
        f.seek(0)

        # Auto-detect delimiter using csv.Sniffer
        dialect = csv.Sniffer().sniff(sample)
        reader = csv.reader(f, dialect)

        # Parse all rows
        rows = list(reader)
        if len(rows) <= 1:
            raise ValueError("CSV file contains no data")
        return rows


@handle_file_errors
def excel_to_csv(path: Path):
    """
    Reads an Excel file and returns its content as a list of lists. The first row is treated
    as the header.
    Raises ValueError if the file is empty or contains multiple sheets.

    Returns
    -------
    list[list[str]]
        The content of the Excel file as a list of lists.
    """
    excel = pd.ExcelFile(path)
    if len(excel.sheet_names) > 1:
        raise ValueError(
            "Excel file contains multiple sheets; only single-sheet files are supported at the moment."
        )
    df = excel.parse(sheet_name=0)

    # Strip whitespace from all cells in the DataFrame
    df = df.astype(str).apply(lambda col: col.str.strip())
    rows = [df.columns.tolist()] + df.values.tolist()

    if len(rows) <= 1:
        raise ValueError("Excel file contains no data")

    return rows


@handle_file_errors
def read_geojson(path: Path):
    """
    Reads a GeoJSON file and validates its structure. Returns the parsed GeoJSON as a
    dictionary.
    Raises ValueError if the file is not a valid FeatureCollection or if it
    contains no features.

    NOTE: this function uses manual parsing for better validation and error messages.
    Both fiona.open() and geojson.geometry.is_valid() are too permissive with invalid
    GeoJSON files - they focus on data extraction rather than format compliance (e.g. accept
    empty features, missing properties, null geometries).

    Returns
    -------
    dict
        The parsed GeoJSON data as a dictionary.
    """
    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        raise ValueError("Invalid GeoJSON: must be a FeatureCollection object")
    if not data.get("features"):
        raise ValueError("GeoJSON contains no features")

    features = data["features"]
    if not isinstance(features, list):
        raise ValueError("GeoJSON 'features' must be a list")

    for i, feature in enumerate(features):
        if not isinstance(feature, dict):
            raise ValueError(f"Feature at index {i} is not a dictionary")
        if feature.get("type") != "Feature":
            raise ValueError(f"Feature at index {i} must have type 'Feature'")
        if "geometry" not in feature or feature["geometry"] is None:
            raise ValueError(f"Feature at index {i} missing geometry")
        if not isinstance(feature["geometry"].get("coordinates"), list):
            raise ValueError(f"Feature at index {i} has invalid geometry coordinates")
        if "properties" not in feature or feature["properties"] is None:
            raise ValueError(f"Feature at index {i} missing properties")

    return data


@handle_file_errors
def json_to_csv(path: Path):
    """
    Reads a JSON file and returns its content as a list of lists. The first row is treated
    as the header.
    Raises ValueError if the file is empty or not a list of records.

    Returns
    -------
    list[list[str]]
        The content of the JSON file as a list of lists.
    """
    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected a list of dictionaries")
    if not data:
        raise ValueError("JSON file contains no records")
    if not all(isinstance(row, dict) and row for row in data):
        raise ValueError("Each record must be a non-empty dictionary")

    # Collect all unique field names across all records and create a header row
    # Also ensure all values are strings and strip whitespace to avoid issues with CSV formatting.
    fieldnames = sorted(set(k for row in data for k in row))
    output = [fieldnames]
    for row in data:
        output.append([str(row.get(field, "")).strip() for field in fieldnames])
    return output


@handle_file_errors
def gpx_to_geojson(path: Path):
    """
    Converts a GPX file to GeoJSON format using Fiona.
    Reads all GPX layers (waypoints, tracks, etc.) and returns a FeatureCollection.

    Includes specialized business logic to handle Locus Map GPX exports:
    - Locus Map creates multiple separate link fields (link_1_href, link_2_href, etc.)
      that need to be consolidated into a single 'link' property for better usability
    - Standard Fiona GPX reading doesn't handle this consolidation automatically
    - This preprocessing ensures consistent output regardless of GPX source application

    Returns
    -------
    dict
        The converted GPX data as a GeoJSON dictionary.
    """
    features = []

    # Read all available layers in the GPX file
    layers = fiona.listlayers(path)

    for layer in layers:
        with fiona.open(path, layer=layer, driver="GPX") as collection:
            for feature in collection:
                properties = (
                    dict(feature["properties"]) if feature["properties"] else {}
                )

                # Handle Locus Map link fields - combine all link-related fields
                # NOTE: This is lossy - we lose individual field names (link_1_href, link_2_href, etc.)
                # and merge into a single comma-separated string.
                link_fields = [k for k in properties.keys() if "link" in k.lower()]
                links = []
                for field in sorted(link_fields):  # Sort for consistent output
                    if properties[field]:
                        links.append(str(properties[field]))
                        del properties[field]

                if links:
                    properties["link"] = ", ".join(links)

                features.append(
                    {
                        "type": "Feature",
                        "geometry": dict(feature["geometry"]),
                        "properties": properties,
                    }
                )

    if not features:
        raise ValueError("No valid features found in input file")

    return {"type": "FeatureCollection", "features": features}


@handle_file_errors
def kml_to_geojson(path: Path):
    """
    Converts a KML file to GeoJSON format using a hybrid approach.
    Uses Fiona for reliable geometry parsing and XML for comprehensive property extraction.

    Implements specialized business logic for comprehensive KML parsing:
    - Fiona/GDAL only reads basic KML elements (Name, Description, geometry) but ignores
      ExtendedData and custom elements that are commonly used by mapping applications
    - Locus Map KML exports include custom attachment elements and extensive ExtendedData
      that would be lost with standard Fiona-only parsing
    - This hybrid approach combines Fiona's robust GDAL-based geometry handling with
      manual XML parsing to capture all available metadata and custom fields
    - Property name normalization ensures consistent output (Name -> name, etc.)
    - Handles Locus Map-specific attachment references by extracting filename from paths
    - Applies selective lossy transformations to balance data preservation with output consistency

    Returns
    -------
    dict
        The converted KML data as a GeoJSON dictionary.
    """
    # Enable KML support in Fiona (not enabled by default)
    # See: https://github.com/geopandas/geopandas/issues/2481
    fiona.supported_drivers["KML"] = "rw"

    features = []

    # Hybrid approach: Fiona only reads basic KML elements (Name, Description, geometry)
    # but ignores ExtendedData and custom elements. We use XML parsing for comprehensive
    # property extraction while leveraging Fiona's robust geometry handling via GDAL/OGR.
    tree = ET.parse(path)
    root = tree.getroot()
    namespace = {
        "kml": "http://www.opengis.net/kml/2.2",
        "gx": "http://www.google.com/kml/ext/2.2",
    }

    # Extract all placemark properties by matching with Fiona features
    placemark_properties = []

    for placemark in root.findall(".//kml:Placemark", namespace):
        properties = {}

        # Extract basic properties
        for el in placemark:
            tag = el.tag.split("}")[-1]
            if tag in {"name", "description", "visibility", "styleUrl"} and el.text:
                properties[tag] = el.text.strip()
            elif tag == "LookAt":
                for look_el in el:
                    look_tag = look_el.tag.split("}")[-1]
                    if look_el.text:
                        properties[f"lookat_{look_tag}"] = look_el.text.strip()

        # Extract extended data (Fiona doesn't read these)
        for data_el in placemark.findall(".//kml:ExtendedData/kml:Data", namespace):
            key = data_el.attrib.get("name")
            val = data_el.findtext("kml:value", default="", namespaces=namespace)
            if key:
                properties[key] = val.strip()

        # Extract Locus Map attachments (custom elements)
        attachments = [
            el.text.strip().split("/")[-1]
            for el in placemark.findall(".//{*}attachment")
            if el.text
        ]
        if attachments:
            properties["attachments"] = ", ".join(attachments)

        placemark_properties.append(properties)

    # Use Fiona for reliable geometry parsing via GDAL/OGR
    with fiona.open(path, driver="KML") as collection:
        for i, feature in enumerate(collection):
            # Get Fiona's basic properties and normalize names
            fiona_props = dict(feature["properties"]) if feature["properties"] else {}

            # Normalize Fiona property names for consistency (Name -> name, etc.)
            normalized_fiona_props = {}
            for key, value in fiona_props.items():
                if key == "Name" and value:
                    normalized_fiona_props["name"] = value
                elif key == "Description" and value:
                    normalized_fiona_props["description"] = value
                elif value not in (None, "", "None"):
                    normalized_fiona_props[key.lower()] = value

            # Merge properties (XML takes precedence for comprehensive data)
            if i < len(placemark_properties):
                final_properties = {**normalized_fiona_props, **placemark_properties[i]}
            else:
                final_properties = normalized_fiona_props

            # Clean up empty properties
            final_properties = {
                k: v for k, v in final_properties.items() if v not in (None, "", "None")
            }

            features.append(
                {
                    "type": "Feature",
                    "geometry": dict(feature["geometry"]),
                    "properties": final_properties,
                }
            )

    if not features:
        raise ValueError("No valid features found in input file")

    return {"type": "FeatureCollection", "features": features}
