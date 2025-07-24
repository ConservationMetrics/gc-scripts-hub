import csv
import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path

import fiona
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    In that case a better funcitonal API might be a suite of `read_*()` functions that return some intermediate representation, and a suite of `to_*()` fns that takes that intermediate representation.

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
    Reads a CSV file and returns its content as a list of lists.
    The first row is treated as the header.
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
    Reads an Excel file and returns its content as a list of lists.
    The first row is treated as the header.
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
    Reads a GeoJSON file and validates its structure.
    Returns the parsed GeoJSON as a dictionary.
    Raises ValueError if the file is not a valid FeatureCollection
    or if it contains no features.

    Returns
    -------
    dict
        The parsed GeoJSON data as a dictionary.
    """
    # Always use manual parsing for better validation and error messages
    # Fiona is too permissive with invalid GeoJSON files
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
    Reads a JSON file and returns its content as a list of lists.
    The first row is treated as the header.
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

    # Collect all unique field names across all records
    # and create a header row
    # Also ensure all values are strings and strip whitespace
    # to avoid issues with CSV formatting
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
