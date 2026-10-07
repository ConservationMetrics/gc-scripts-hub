# requirements:
# psycopg[binary]

import json
import logging
import uuid
from pathlib import Path
from typing import Literal

from f.common_logic.db_operations import (
    DynSelect_existing_db_table_name,
    StructuredDBWriter,
    conninfo,
    existing_db_table_name as list_dataset_tables,
    postgresql,
    resolve_db_table_name,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def existing_db_table_name(db: postgresql | None = None, **_):
    """Windmill dynamic select. Defined here so the `db` resource is resolved."""
    return list_dataset_tables(db)


def main(
    db: postgresql,
    db_table_name: str | None = None,
    geojson_path: str | None = None,
    attachment_root: str = "/persistent-storage/datalake/",
    delete_geojson_file: bool = False,
    reverse_properties_separated_by: str | None = None,
    sep_policy: str = "remove",
    destination_action: Literal["create_new_dataset", "use_existing_dataset"]
    | None = None,
    existing_db_table_name: DynSelect_existing_db_table_name | None = None,
):
    if destination_action is not None:
        db_table_name = resolve_db_table_name(
            db, destination_action, db_table_name, existing_db_table_name
        )
    geojson_path = Path(attachment_root) / Path(geojson_path)
    transformed_geojson_data = transform_geojson_data(geojson_path)

    db_writer = StructuredDBWriter(
        conninfo(db),
        db_table_name,
        use_mapping_table=False,
        reverse_properties_separated_by=reverse_properties_separated_by,
        sep_policy=sep_policy,
    )
    db_writer.handle_output(transformed_geojson_data)

    if delete_geojson_file:
        delete_geojson_file(geojson_path)


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
        '_id' for the feature's unique identifier (random UUID if not present in source),
        'g__type' for the geometry type,
        'g__coordinates' for the geometry coordinates,
        and any additional properties from the feature.
    """
    with open(geojson_path, "r") as f:
        geojson_data = json.load(f)

    transformed_geojson_data = []
    for i, feature in enumerate(geojson_data["features"]):
        # TODO: consider using a more deterministic ID generation method,
        # once we are ready to tackle the challenge of appending data
        # to existing tables with existing IDs
        feature_id = feature.get("id", str(uuid.uuid4()))
        geometry = feature.get("geometry")

        transformed_feature = {
            "_id": feature_id,
            "g__type": geometry["type"] if geometry else None,
            "g__coordinates": geometry["coordinates"] if geometry else None,
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
