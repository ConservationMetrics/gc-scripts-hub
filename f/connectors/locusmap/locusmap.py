# requirements:
# lxml
# psycopg2-binary

import csv
import json
import logging
import shutil
import uuid
from pathlib import Path

from lxml import etree

from f.common_logic.db_operations import postgresql
from f.common_logic.save_disk import save_data_to_file
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    db: postgresql,
    db_table_name: str,
    locusmap_export_path: str,
    attachment_root: str = "/persistent-storage/datalake/",
):
    storage_path = Path(attachment_root) / db_table_name

    if Path(locusmap_export_path).suffix.lower() in [".zip", ".kmz"]:
        locusmap_data_path, locusmap_attachments_path = extract_locusmap_archive(
            locusmap_export_path
        )
    else:
        locusmap_data_path = Path(locusmap_export_path)
        if locusmap_data_path.suffix.lower() not in [".kml", ".gpx", ".csv"]:
            raise ValueError(
                "Unsupported file format. Only CSV, GPX, and KML are supported."
            )
        locusmap_attachments_path = None

    # TODO: transform to GeoJSON
    geojson = transform_locusmap_data(locusmap_data_path)

    if locusmap_attachments_path:
        copy_locusmap_attachments(
            locusmap_attachments_path, db_table_name, attachment_root
        )

    save_locusmap_outputs(
        geojson,
        locusmap_data_path,
        storage_path,
        db_table_name,
    )

    rel_geojson_path = Path(db_table_name) / f"{db_table_name}.geojson"

    save_geojson_to_postgres(
        db,
        db_table_name,
        rel_geojson_path,
        attachment_root,
        False,
    )

    delete_locusmap_export_files(
        locusmap_export_path,
        locusmap_data_path,
        locusmap_attachments_path,
    )


def extract_locusmap_archive(archive_path):
    """
    Extracts a Locus Map ZIP or KMZ archive, returning the KML/GPX/CSV file path
    and the attachments directory (if applicable).

    Parameters
    ----------
    archive_path : str
        The path to the ZIP or KMZ archive.

    Returns
    -------
    tuple
        A tuple containing the paths to the extracted spatial data file and attachments directory.
    """
    archive_path = Path(archive_path)
    extract_to = archive_path.parent / archive_path.stem

    # Handle KMZ by temporarily renaming it to a ZIP
    temp_archive_path = archive_path
    if archive_path.suffix.lower() == ".kmz":
        temp_archive_path = archive_path.with_suffix(".zip")
        shutil.copyfile(archive_path, temp_archive_path)

    try:
        shutil.unpack_archive(temp_archive_path, extract_to)
        logger.info(f"Extracted archive: {archive_path}")
    except shutil.ReadError as e:
        raise ValueError(f"Unable to extract archive: {e}")
    finally:
        # Clean up temporary zip if it was a KMZ
        if temp_archive_path != archive_path:
            temp_archive_path.unlink()

    # Find the main spatial data file
    extracted_files = list(extract_to.glob("*.*"))
    for file in extracted_files:
        if file.suffix.lower() in [".kml", ".gpx", ".csv"]:
            locusmap_data_path = file
            break
    else:
        raise ValueError(
            "Unsupported file format. Only CSV, GPX, and KML are supported in the archive."
        )

    locusmap_attachments_path = None
    for folder in extract_to.iterdir():
        if folder.is_dir() and (
            # LocusMap exports attachments in an '-attachments' suffixed folder when zipped
            # in a ZIP archive, or as a 'files' folder when zipped in a KMZ archive
            folder.name.endswith("-attachments") or folder.name == "files"
        ):
            locusmap_attachments_path = folder
            break

    return locusmap_data_path, locusmap_attachments_path


def _make_geojson_feature(properties, lon, lat):
    """Creates a GeoJSON Feature."""
    return {
        "type": "Feature",
        "id": str(
            uuid.uuid5(uuid.NAMESPACE_OID, json.dumps(properties, sort_keys=True))
        ),
        "properties": properties,
        "geometry": {
            "type": "Point",
            "coordinates": [float(lon), float(lat)],
        },
    }


def _transform_csv(csv_path):
    """Transforms CSV data into a list of dictionaries."""
    features = []
    with open(csv_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            props = {
                k: (
                    ", ".join(
                        v.split("|")[-1].split("/")[-1] for v in row[k].split("|")
                    )
                    if k == "attachments"
                    else v
                )
                for k, v in row.items()
            }
            if "lat" in props and "lon" in props:
                lat, lon = props.pop("lat"), props.pop("lon")
                feature = _make_geojson_feature(props, lon, lat)
                features.append(feature)
    return features


def _transform_gpx(gpx_path):
    """Transforms GPX data into a list of dictionaries."""
    features = []
    tree = etree.parse(gpx_path)
    namespace = {"default": "http://www.topografix.com/GPX/1/1"}

    for wpt in tree.xpath("//default:wpt", namespaces=namespace):
        attachments = [
            link.attrib["href"].split("/")[-1]
            for link in wpt.xpath("./default:link", namespaces=namespace)
        ]
        props = {
            "name": wpt.xpath("./default:name/text()", namespaces=namespace)[0]
            if wpt.xpath("./default:name/text()", namespaces=namespace)
            else None,
            "description": wpt.xpath("./default:desc/text()", namespaces=namespace)[0]
            if wpt.xpath("./default:desc/text()", namespaces=namespace)
            else None,
            "attachments": ", ".join(attachments),
            "timestamp": wpt.xpath("./default:time/text()", namespaces=namespace)[0]
            if wpt.xpath("./default:time/text()", namespaces=namespace)
            else None,
        }
        feature = _make_geojson_feature(props, wpt.attrib["lon"], wpt.attrib["lat"])
        features.append(feature)
    return features


def _transform_kml(kml_path):
    """Transforms KML data into a list of dictionaries."""
    features = []
    tree = etree.parse(kml_path)
    root = tree.getroot()
    namespace = {
        "kml": "http://www.opengis.net/kml/2.2",
        "lc": "http://www.locusmap.eu",
    }

    for placemark in root.findall(".//kml:Placemark", namespace):
        name = placemark.find("kml:name", namespace).text
        description = (
            placemark.find("kml:description", namespace).text
            if placemark.find("kml:description", namespace) is not None
            else ""
        )
        attachments = [
            attachment.text.split("/")[-1]
            for attachment in placemark.findall(
                "kml:ExtendedData/lc:attachment", namespace
            )
        ]
        point = placemark.find("kml:Point/kml:coordinates", namespace)
        if point is not None:
            lon, lat = point.text.strip().split(",")[:2]
            timestamp = (
                placemark.find("kml:TimeStamp/kml:when", namespace).text
                if placemark.find("kml:TimeStamp/kml:when", namespace) is not None
                else None
            )
            props = {
                "name": name,
                "description": description,
                "attachments": ", ".join(attachments),
                "timestamp": timestamp,
            }
            feature = _make_geojson_feature(props, lon, lat)
            features.append(feature)
    return features


def transform_locusmap_data(locusmap_data_path):
    """
    Transforms Locus Map spatial data from a file into a list of dictionaries.

    Parameters
    ----------
    locusmap_data_path : str
        The path to the file containing LocusMap spatial data (CSV, GPX, or KML).

    Returns
    -------
    list
        A list of dictionaries, where each dictionary represents a transformed LocusMap feature.

    Notes
    -----
    Each helper function reads the file and performs the following transformations for each feature
        - Converts the 'attachments' field from a string to a list of strings.
        - Creates 'g__coordinates' and 'g__type' fields from the 'lat' and 'lon' fields.
        - Generates a UUID for each feature based on its dictionary contents and assigns it to the '_id' field.

    The transformed data are returned as a list of dictionaries.

    TODO: Support track data (which will be a LineString type).
    """
    file_extension = locusmap_data_path.suffix[1:].lower()

    if file_extension == "csv":
        features = _transform_csv(locusmap_data_path)
    elif file_extension == "gpx":
        features = _transform_gpx(locusmap_data_path)
    elif file_extension == "kml":
        features = _transform_kml(locusmap_data_path)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

    logger.info(f"Processed {len(features)} features from LocusMap.")
    return {
        "type": "FeatureCollection",
        "features": features,
    }


def copy_locusmap_attachments(
    locusmap_attachments_path, db_table_name, attachment_root
):
    """
    Copies Locus Map attachment files from the original export directory to a specified root directory.

    Parameters
    ----------
    locusmap_attachments_path : str
        The path to the directory containing Locus Map attachment files.
    db_table_name : str
        The name of the database table where the spatial data will be stored.
    attachment_root : str
        The root directory where the attachment files will be copied.
    """
    attachment_dest_path = Path(attachment_root) / db_table_name
    attachment_dest_path.mkdir(parents=True, exist_ok=True)

    for src_path in Path(locusmap_attachments_path).glob("*"):
        dest_path = attachment_dest_path / src_path.name
        if not dest_path.exists():
            shutil.copy2(src_path, dest_path)
        else:
            logger.warning(f"File {dest_path} already exists, skipping copy.")

    logger.info(f"Copied Locus Map attachments to {attachment_dest_path}.")


def save_locusmap_outputs(
    geojson,
    original_data_path,
    storage_path,
    db_table_name,
):
    """
    Saves both the GeoJSON and original Locus Map file (CSV, GPX, or KML) to the storage path.

    Parameters
    ----------
    geojson : dict
        The GeoJSON FeatureCollection to save.
    original_data_path : Path
        The path to the original Locus Map spatial file.
    storage_path : Path
        The directory where files will be saved.
    db_table_name : str
        The name used for saved filenames.
    """
    storage_path.mkdir(parents=True, exist_ok=True)

    save_data_to_file(
        geojson,
        db_table_name,
        storage_path,
        file_type="geojson",
    )

    shutil.copy2(
        original_data_path,
        storage_path / f"{db_table_name}{original_data_path.suffix.lower()}",
    )
    logger.info(
        f"Saved original data to {storage_path / f'{db_table_name}{original_data_path.suffix.lower()}'}"
    )


def delete_locusmap_export_files(
    locusmap_export_path,
    locusmap_data_path=None,
    locusmap_attachments_path=None,
):
    """
    Clean up the Locus Map export files and attachments directory after processing.

    Parameters
    ----------
    locusmap_path : str or Path
        The path to the Locus Map export file (CSV, ZIP, etc.).
    locusmap_data_path : str or Path, optional
        The path to the spatial data file extracted from the ZIP file, if applicable.
    locusmap_attachments_path : str or Path, optional
        The path to the directory containing Locus Map attachment files.
    """
    paths_to_delete = []

    # Always delete extracted attachments if they exist
    if locusmap_attachments_path:
        paths_to_delete.append(Path(locusmap_attachments_path))

    # Delete extracted spatial data if it was extracted from a ZIP
    if locusmap_data_path and locusmap_data_path != Path(locusmap_export_path):
        paths_to_delete.append(Path(locusmap_data_path))

    # Delete the orginal export file
    paths_to_delete.append(Path(locusmap_export_path))

    for path in paths_to_delete:
        try:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Failed to delete {path}: {e}")
        else:
            logger.info(f"Deleted {path}")
