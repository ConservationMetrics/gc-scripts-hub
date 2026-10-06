import json
import logging

from f.common_logic.db_operations import (
    USE_EXISTING_DATASET,
    DynSelect_existing_db_table_name,
    conninfo,
    existing_db_table_name as list_dataset_tables,
    fetch_data_from_postgres,
    postgresql,
    resolve_db_table_name,
)
from f.common_logic.file_operations import save_data_to_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def existing_db_table_name(db: postgresql | None = None):
    """Windmill dynamic select. Only `db` is an argument, so the picker does not wait on the other fields."""
    return list_dataset_tables(db)


def main(
    db: postgresql,
    existing_db_table_name: DynSelect_existing_db_table_name,
    storage_path: str = "/persistent-storage/datalake/export",
):
    db_table_name = resolve_db_table_name(
        db, USE_EXISTING_DATASET, selected_table=existing_db_table_name
    )
    data = fetch_data_from_postgres(conninfo(db), db_table_name)

    feature_collection = format_data_as_geojson(data)

    save_data_to_file(
        feature_collection, db_table_name, storage_path, file_type="geojson"
    )


def format_data_as_geojson(data):
    """
    Converts data from a PostgreSQL table into a GeoJSON FeatureCollection.

    Parameters
    ----------
        data (tuple): A tuple containing columns and rows fetched from the database.

    Returns
    -------
        dict: A GeoJSON FeatureCollection with features extracted from the data.
    """

    columns, rows = data
    features = []
    for row in rows:
        properties = {}
        geometry = {}
        feature_id = None

        # The expected schema here is that geometry columns are prefixed with "g__"
        # If an "_id" column is present, it is used as the feature id
        # All other columns are treated as properties
        for col, value in zip(columns, row):
            if col == "_id":
                feature_id = value
            elif col == "g__coordinates":
                if value:
                    geometry["coordinates"] = json.loads(value)
                else:
                    geometry["coordinates"] = None
            elif col == "g__type":
                geometry["type"] = value
            else:
                properties[col] = value

        feature = {
            "type": "Feature",
            "id": feature_id,
            "properties": properties,
            "geometry": geometry,
        }
        features.append(feature)

    feature_collection = {
        "type": "FeatureCollection",
        "features": features,
    }

    logger.info(f"GeoJSON created with {len(features)} features")

    return feature_collection
