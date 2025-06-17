import csv
import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path

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


def normalize_data(file_path: str, file_format: str):
    """
    Normalizes a structured file into either CSV (list of lists) or GeoJSON (dict).

    Right now, we are assuming that geospatial data (e.g. GPX, KML) will only
    be normalized into GeoJSON, and not CSV. And that tabular data (e.g. Excel,
    JSON) will only be normalized into CSV, and not GeoJSON.

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
        Normalized data as CSV (list of lists) or GeoJSON (dict).
    """
    path = Path(file_path)
    logger.debug(f"Normalizing {file_path} with format {file_format}")

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
    Converts a GPX file to GeoJSON format.
    Parses waypoints and track segments, returning a FeatureCollection.

    Returns
    -------
    dict
        The converted GPX data as a GeoJSON dictionary.
    """
    tree = ET.parse(path)
    root = tree.getroot()
    namespace = {"default": root.tag.split("}")[0].strip("{")}

    features = []

    def parse_point(el):
        lat = el.get("lat")
        lon = el.get("lon")
        props = {}
        links = []

        for child in el:
            tag = child.tag.split("}")[-1]
            if tag == "link":
                href = child.attrib.get("href")
                if href:
                    links.append(href)
                else:
                    # Fallback: nested <text> tag for Locus variants
                    text_el = child.find("./default:text", namespace)
                    if text_el is not None and text_el.text:
                        links.append(text_el.text.strip())
                        logger.warning("Fallback to nested <text> tag for link")
            else:
                props[tag] = child.text

        if links:
            props["link"] = ", ".join(links)

        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            raise ValueError("Invalid lat/lon in GPX waypoint")

        return {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        }

    # Waypoints
    for wpt in root.findall(".//default:wpt", namespace):
        features.append(parse_point(wpt))

    # Track segments as LineStrings
    for trk in root.findall(".//default:trk", namespace):
        track_name_el = trk.find("default:name", namespace)
        track_desc_el = trk.find("default:desc", namespace)
        props = {}
        if track_name_el is not None:
            props["name"] = track_name_el.text
        if track_desc_el is not None:
            props["description"] = track_desc_el.text

        for trkseg in trk.findall("default:trkseg", namespace):
            coords = []
            for trkpt in trkseg.findall("default:trkpt", namespace):
                try:
                    lat = float(trkpt.get("lat"))
                    lon = float(trkpt.get("lon"))
                except (TypeError, ValueError):
                    raise ValueError("Invalid lat/lon in GPX track point")
                coords.append([lon, lat])
            if coords:
                features.append(
                    {
                        "type": "Feature",
                        "geometry": {"type": "LineString", "coordinates": coords},
                        "properties": props,
                    }
                )

    return {"type": "FeatureCollection", "features": features}


@handle_file_errors
def kml_to_geojson(path: Path):
    """
    Converts a KML file to GeoJSON format.
    Parses Placemarks, extracting metadata and geometry.
    Returns a FeatureCollection.

    Returns
    -------
    dict
        The converted KML data as a GeoJSON dictionary.
    """
    tree = ET.parse(path)
    root = tree.getroot()
    namespace = {
        "kml": "http://www.opengis.net/kml/2.2",
        "gx": "http://www.google.com/kml/ext/2.2",
    }

    features = []
    for placemark in root.findall(".//kml:Placemark", namespace):
        props = {}

        # Dynamically pull all direct metadata fields under Placemark (not nested elements)
        for el in placemark:
            tag = el.tag.split("}")[-1]
            if el.text and not list(el):  # Skip children like ExtendedData
                props[tag] = el.text.strip()

        # Add <ExtendedData><Data name="..."><value>...</value></Data>
        for data_el in placemark.findall(".//kml:ExtendedData/kml:Data", namespace):
            key = data_el.attrib.get("name")
            val = data_el.findtext("kml:value", default="", namespaces=namespace)
            if key:
                props[key] = val.strip()

        # Add any <attachment> tags regardless of namespace (e.g. for Locus Map)
        attachments = [
            el.text.strip().split("/")[-1]
            for el in placemark.findall(".//{*}attachment")
            if el.text
        ]
        if attachments:
            props["attachments"] = ", ".join(attachments)
            logger.debug("Attachments found and processed")

        geometry = None

        # Polygon
        coords_el = placemark.find(
            ".//kml:Polygon/kml:outerBoundaryIs/kml:LinearRing/kml:coordinates",
            namespace,
        )
        if coords_el is not None:
            coords = [
                list(map(float, c.split(",")[:2]))
                for c in coords_el.text.strip().split()
                if c.strip()
            ]
            if coords:
                geometry = {"type": "Polygon", "coordinates": [coords]}

        # LineString
        if geometry is None:
            coords_el = placemark.find(".//kml:LineString/kml:coordinates", namespace)
            if coords_el is not None:
                coords = [
                    list(map(float, c.split(",")[:2]))
                    for c in coords_el.text.strip().split()
                    if c.strip()
                ]
                if coords:
                    geometry = {"type": "LineString", "coordinates": coords}

        # Point
        if geometry is None:
            coords_el = placemark.find(".//kml:Point/kml:coordinates", namespace)
            if coords_el is not None:
                coords = coords_el.text.strip().split(",")
                if len(coords) >= 2:
                    lon, lat = map(float, coords[:2])
                    geometry = {"type": "Point", "coordinates": [lon, lat]}

        if geometry is None:
            raise ValueError("Placemark is missing <Point>, <LineString>, or <Polygon>")

        features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": props,
            }
        )

    return {"type": "FeatureCollection", "features": features}
