# requirements:
# psycopg2-binary

import hashlib
import json
import logging
import uuid
from pathlib import Path

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    db: postgresql,
    db_table_name: str,
    geojson_path: str,
    attachment_root: str = "/persistent-storage/datalake/",
    delete_geojson_file: bool = False,
):
    geojson_path = Path(attachment_root) / Path(geojson_path)
    transformed_geojson_data = transform_geojson_data(geojson_path)

    db_writer = StructuredDBWriter(
        conninfo(db),
        db_table_name,
        use_mapping_table=False,
        reverse_properties_separated_by=None,
    )
    db_writer.handle_output(transformed_geojson_data)

    if delete_geojson_file:
        delete_geojson_file(geojson_path)


def _generate_deterministic_id(feature):
    """
    Generate a deterministic UUID based on MD5 hash of feature content
    and UUID namespace for deterministic generation.

    Parameters
    ----------
    feature : dict
        GeoJSON feature object.

    Returns
    -------
    str
        Deterministic UUID string based on feature content.
    """
    content_for_hash = {
        "type": feature["type"],
        "geometry": feature["geometry"],
        "properties": feature.get("properties", {}),
    }

    content_json = json.dumps(content_for_hash, sort_keys=True, separators=(",", ":"))

    content_hash = hashlib.md5(content_json.encode("utf-8")).hexdigest()

    deterministic_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, content_hash)

    return str(deterministic_uuid)


def transform_geojson_data(geojson_path):
    """
    Transforms GeoJSON data from a file into a list of dictionaries suitable for database insertion.

    Parameters
    ----------
    geojson_path : str or Path
        The file path to the GeoJSON file.

    Returns
    -------
    list
        A list of dictionaries where each dictionary represents a GeoJSON feature with keys:
        '_id' for the feature's unique identifier (generated if not present in source),
        'g__type' for the geometry type,
        'g__coordinates' for the geometry coordinates,
        and any additional properties from the feature.
    """
    with open(geojson_path, "r") as f:
        geojson_data = json.load(f)

    transformed_geojson_data = []
    for i, feature in enumerate(geojson_data["features"]):
        feature_id = feature.get("id")

        if feature_id is None:
            feature_id = _generate_deterministic_id(feature)
            logger.info(f"Generated deterministic ID for feature {i}: {feature_id}")

        transformed_feature = {
            "_id": feature_id,
            "g__type": feature["geometry"]["type"],
            "g__coordinates": feature["geometry"]["coordinates"],
            **feature.get("properties", {}),
        }
        transformed_geojson_data.append(transformed_feature)
    return transformed_geojson_data


def delete_geojson_file(
    geojson_path: str,
):
    """
    Deletes the GeoJSON file after processing.

    Parameters
    ----------
    geojson_path : str
        The path to the GeoJSON file to delete.
    """
    try:
        geojson_path.unlink()
        logger.info(f"Deleted GeoJSON file: {geojson_path}")
    except FileNotFoundError:
        logger.warning(f"GeoJSON file not found: {geojson_path}")
    except Exception as e:
        logger.error(f"Error deleting GeoJSON file: {e}")
        raise
