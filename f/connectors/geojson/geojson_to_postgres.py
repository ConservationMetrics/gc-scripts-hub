# requirements:
# psycopg2-binary

import json
import logging
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


def transform_geojson_data(geojson_path):
    """
    Transforms GeoJSON data from a file into a list of dictionaries suitable for database insertion.

    Args:
        geojson_path (str or Path): The file path to the GeoJSON file.

    Returns:
        list: A list of dictionaries where each dictionary represents a GeoJSON feature with keys:
              '_id' for the feature's unique identifier,
              'g__type' for the geometry type,
              'g__coordinates' for the geometry coordinates,
              and any additional properties from the feature.
    """
    with open(geojson_path, "r") as f:
        geojson_data = json.load(f)

    transformed_geojson_data = []
    for feature in geojson_data["features"]:
        transformed_feature = {
            "_id": feature[
                "id"
            ],  # Assuming that the GeoJSON feature has unique "id" field that can be used as the primary key
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
