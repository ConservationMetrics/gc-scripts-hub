import csv
import json
import logging
import re
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import filetype
import fiona

# pandas requires openpyxl installed separately to read .xlsx files
# it has to be imported in this module despite also being listed in
# data_conversion.script.lock; otherwise, the script will complain about
# "Missing optional dependency 'openpyxl'"
import openpyxl  # noqa: F401
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def detect_structured_data_type(file_path: str) -> str:
    """
    Lightweight detection of structured data file types using MIME type analysis and extension
    fallbacks, focusing on geospatial and tabular formats.

    This function identifies structured datasets used in environmental, geographic, and
    tabular data pipelines. It supports:
    - Spatial formats: geojson, kml, gpx
    - Tabular formats: csv, xls, xlsx
    - General structured formats: json

    TODO: Add support for ESRI shapefiles, which requires a different approach as a
    collection of files rather than a single file.

    Format detection is intentionally lightweight. Comprehensive validation is expected
    to occur later in the data processing pipeline (e.g., in convert_data).

    Parameters
    ----------
    file_path : str
        Path to the file to inspect.

    Returns
    -------
    str
        The detected file type (e.g., "csv", "geojson", etc.), or "unsupported" if
        the file does not match any recognized format.
    """

    def _detect_by_extension(path: Path) -> str:
        """Fallback extension-based detection for text files."""
        extension_map = {
            ".csv": "csv",
            ".json": "json",
            ".geojson": "geojson",
            ".gpx": "gpx",
            ".kml": "kml",
            ".xml": "xml",
        }
        detected_type = extension_map.get(path.suffix.lower(), "unsupported")
        if detected_type != "unsupported":
            # For JSON files, check if they're actually GeoJSON
            if detected_type == "json":
                return _detect_json_subtype(path)
            # For XML files, check if they're SMART patrol XML
            if detected_type == "xml":
                return _detect_xml_subtype(path)
            logger.info(f"File {path.name} detected as {detected_type} (by extension)")
        return detected_type

    def _detect_json_subtype(path: Path) -> str:
        """Distinguish between JSON and GeoJSON using extension and content sniffing."""
        if path.suffix.lower() == ".geojson":
            logger.info(f"File {path.name} detected as geojson (by extension)")
            return "geojson"

        # For .json files, check content to detect GeoJSON
        if path.suffix.lower() == ".json":
            try:
                with path.open(encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict) and data.get("type") == "FeatureCollection":
                    logger.info(f"File {path.name} detected as geojson (by content)")
                    return "geojson"
            except (json.JSONDecodeError, UnicodeDecodeError, OSError):
                # If we can't parse it, fall back to json
                pass

        logger.info(f"File {path.name} detected as json")
        return "json"

    def _detect_xml_subtype(path: Path) -> str:
        """Distinguish between GPX, KML, SMART XML based on extension and content."""
        suffix = path.suffix.lower()
        if suffix == ".gpx":
            logger.info(f"File {path.name} detected as gpx")
            return "gpx"
        elif suffix == ".kml":
            logger.info(f"File {path.name} detected as kml")
            return "kml"
        elif suffix == ".xml":
            # Check if it's a SMART patrol XML by looking for SMART namespace
            try:
                tree = ET.parse(path)
                root = tree.getroot()
                # SMART patrol XMLs have a specific namespace
                if "smartconservationsoftware.org" in (root.tag or ""):
                    logger.info(f"File {path.name} detected as smart")
                    return "smart"
                # Check for SMART namespace in any child elements
                for elem in root.iter():
                    if elem.tag and "smartconservationsoftware.org" in str(elem.tag):
                        logger.info(f"File {path.name} detected as smart")
                        return "smart"
            except (ET.ParseError, OSError):
                pass
            logger.info(f"File {path.name} detected as xml (generic)")
            return "xml"
        return "unsupported"

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File does not exist: {file_path}")

    logger.debug(f"Checking file {file_path}")

    # Try MIME type detection first
    kind = filetype.guess(file_path)

    if kind is None:
        # Fallback to extension-based detection for text files
        return _detect_by_extension(path)

    mime = kind.mime
    logger.debug(f"Detected MIME type: {mime}")

    # Direct MIME type mappings
    mime_map = {
        "text/csv": "csv",
        "application/vnd.ms-excel": "xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    }

    if mime in mime_map:
        detected_type = mime_map[mime]
        logger.info(f"File {path.name} detected as {detected_type}")
        return detected_type

    # Handle ambiguous cases
    if mime == "application/json":
        return _detect_json_subtype(path)

    if mime in ("application/xml", "text/xml"):
        return _detect_xml_subtype(path)

    # For generic XML files detected by MIME but with .xml extension
    if path.suffix.lower() == ".xml":
        return _detect_xml_subtype(path)

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
    Exception: SMART XML contains spatial data but is converted to CSV format with
    geometry columns (waypoint_x, waypoint_y, g__type, g__coordinates) for
    compatibility with StructuredDBWriter.
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
        'gpx', 'kml', 'geojson', 'smart'.

    Returns
    -------
    tuple
        A tuple containing (converted_data, output_format) where:
        - converted_data: Union[list[list[str]], list[dict], dict] - Converted data as CSV (list of lists or list of dicts) or GeoJSON (dict)
        - output_format: str - The output format ('csv' for tabular data, 'geojson' for spatial data)
    """
    path = Path(file_path)
    logger.debug(f"Converting {file_path} with format {file_format}")

    match file_format:
        case "csv":
            return read_csv(path), "csv"
        case "xlsx" | "xls":
            return excel_to_csv(path), "csv"
        case "json":
            return json_to_csv(path), "csv"
        case "geojson":
            return read_geojson(path), "geojson"
        case "gpx":
            return gpx_to_geojson(path), "geojson"
        case "kml":
            return kml_to_geojson(path), "geojson"
        case "smart":
            return smart_xml_to_geojson(path), "geojson"
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
    empty features, missing properties).

    Features with null geometry are accepted as-is, as upstream sources sometimes
    provide features with null geometry which should be preserved in the output.

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
        if "geometry" not in feature:
            raise ValueError(f"Feature at index {i} missing geometry field")

        # Accept null geometry as-is (upstream sources sometimes provide this)
        geometry = feature["geometry"]
        if geometry is not None:
            if not isinstance(geometry.get("coordinates"), list):
                raise ValueError(
                    f"Feature at index {i} has invalid geometry coordinates"
                )

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
    Converts a GPX file to GeoJSON format using a hybrid approach.
    Uses Fiona for reliable geometry parsing and XML for comprehensive property extraction.

    Implements specialized business logic for comprehensive GPX parsing:
    - Fiona/GDAL only reads basic GPX elements but ignores custom extensions
    - OsmAnd GPX exports include custom extension elements that would be lost with standard Fiona-only parsing
    - This hybrid approach combines Fiona's robust GDAL-based geometry handling with
      manual XML parsing to capture all available metadata and custom fields
    - Handles Locus Map link fields consolidation for better usability
    - Captures OsmAnd extensions like visited_date, amenity_subtype, address, etc.
    - Applies selective lossy transformations to balance data preservation with output consistency

    Returns
    -------
    dict
        The converted GPX data as a GeoJSON dictionary.
    """
    features = []

    # Hybrid approach: Use XML parsing for comprehensive property extraction
    # while leveraging Fiona's robust geometry handling via GDAL/OGR
    tree = ET.parse(path)
    root = tree.getroot()
    namespace = {
        "gpx": "http://www.topografix.com/GPX/1/1",
        "osmand": "https://osmand.net/docs/technical/osmand-file-formats/osmand-gpx",
    }

    # Extract all waypoint properties by matching with Fiona features
    waypoint_properties = []

    for waypoint in root.findall(".//gpx:wpt", namespace):
        properties = {}

        # Extract basic GPX properties
        for el in waypoint:
            tag = el.tag.split("}")[-1]
            if tag in {"name", "desc", "type", "time", "ele"} and el.text:
                properties[tag] = el.text.strip()
            elif tag == "link":
                href = el.get("href")
                if href:
                    properties["link"] = href

        # Extract OsmAnd extensions from within the extensions element
        extensions_el = waypoint.find(".//gpx:extensions", namespace)
        if extensions_el is not None:
            for ext_el in extensions_el.findall(".//osmand:*", namespace):
                tag = ext_el.tag.split("}")[-1]
                if ext_el.text:
                    properties[f"osmand:{tag}"] = ext_el.text.strip()

        waypoint_properties.append(properties)

    # Use Fiona for reliable geometry parsing via GDAL/OGR
    layers = fiona.listlayers(path)
    waypoint_index = 0

    for layer in layers:
        with fiona.open(path, layer=layer, driver="GPX") as collection:
            for feature in collection:
                # Get Fiona's basic properties and normalize names
                fiona_props = (
                    dict(feature["properties"]) if feature["properties"] else {}
                )

                # Normalize Fiona property names for consistency
                normalized_fiona_props = {}
                for key, value in fiona_props.items():
                    if value not in (None, "", "None"):
                        # Skip OsmAnd extensions that Fiona might have captured
                        if not key.startswith("osmand"):
                            normalized_fiona_props[key.lower()] = value

                # Merge properties (XML takes precedence for comprehensive data)
                if waypoint_index < len(waypoint_properties):
                    final_properties = {
                        **normalized_fiona_props,
                        **waypoint_properties[waypoint_index],
                    }
                else:
                    final_properties = normalized_fiona_props

                # Handle Locus Map link fields - combine all link-related fields
                # NOTE: This is lossy - we lose individual field names (link_1_href, link_2_href, etc.)
                # and merge into a single comma-separated string.
                link_fields = [
                    k
                    for k in final_properties.keys()
                    if "link" in k.lower() and k != "link"
                ]
                links = []
                for field in sorted(link_fields):  # Sort for consistent output
                    if final_properties[field]:
                        links.append(str(final_properties[field]))
                        del final_properties[field]

                if links:
                    # Preserve existing link if present, otherwise create new one
                    existing_link = final_properties.get("link", "")
                    if existing_link:
                        # Avoid duplication by checking if the link is already in the list
                        existing_links = [
                            link.strip() for link in existing_link.split(",")
                        ]
                        new_links = [
                            link.strip()
                            for link in links
                            if link.strip() not in existing_links
                        ]
                        if new_links:
                            final_properties["link"] = (
                                f"{existing_link}, {', '.join(new_links)}"
                            )
                    else:
                        final_properties["link"] = ", ".join(links)

                # Clean up empty properties
                final_properties = {
                    k: v
                    for k, v in final_properties.items()
                    if v not in (None, "", "None")
                }

                # Generate unique ID for the feature
                feature_id = final_properties.get(
                    "name", f"waypoint_{waypoint_index + 1}"
                )

                features.append(
                    {
                        "type": "Feature",
                        "id": feature_id,
                        "geometry": dict(feature["geometry"]),
                        "properties": final_properties,
                    }
                )
                waypoint_index += 1

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

            # Generate unique ID for the feature
            feature_id = final_properties.get("name", f"placemark_{i + 1}")

            features.append(
                {
                    "type": "Feature",
                    "id": feature_id,
                    "geometry": dict(feature["geometry"]),
                    "properties": final_properties,
                }
            )

    if not features:
        raise ValueError("No valid features found in input file")

    return {"type": "FeatureCollection", "features": features}


@handle_file_errors
def smart_xml_to_geojson(path: Path):
    """
    Converts a SMART patrol XML file to GeoJSON format.

    This function parses SMART Conservation Software patrol XML files and extracts
    observations with full hierarchical context (patrol -> leg -> day -> waypoint -> observation).
    Each observation includes spatial data (waypoint coordinates) and is returned as a
    GeoJSON Feature with Point geometry.

    Returns
    -------
    dict
        GeoJSON FeatureCollection containing observation features with properties:
        - Patrol-level fields (patrol_id, patrol_type, etc.)
        - Leg-level fields (leg_id, leg_members, etc.)
        - Day-level fields (day_date, day_start_time, etc.)
        - Waypoint-level fields (waypoint_id, waypoint_x, waypoint_y, etc.)
        - Observation-level fields (category, attributes, etc.)
    """
    # Import here to avoid forcing SMART dependencies (lxml, psycopg2) on all users of data_conversion.py
    from f.connectors.smart.smart_patrols import parse_smart_patrol_xml

    return parse_smart_patrol_xml(path)


def slugify(value: Any, allow_unicode: bool = False) -> str:
    """A safe slugify utility.

    - Converts to str, normalizes unicode, optionally keeps unicode.
    - Returns an ASCII-ish slug of the input suitable for file names.
    - Returns 'unnamed' for empty inputs.

    Source: https://github.com/django/django/blob/main/django/utils/text.py#L453
    """
    value = "" if value is None else str(value)
    if not value:
        return "unnamed"

    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("ascii")
        )

    value = value.lower()
    # keep alphanumerics, underscores, spaces and hyphens
    value = re.sub(r"[^\w\s-]", "", value)
    value = re.sub(r"[-\s]+", "-", value).strip("-_ ")
    return value or "unnamed"
