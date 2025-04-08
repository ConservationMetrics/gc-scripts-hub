# requirements:
# psycopg2-binary

import json
import logging
from pathlib import Path

from psycopg2 import Error, connect, sql

from f.common_logic.db_connection import conninfo, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    db: postgresql,
    db_table_name: str,
    storage_path: str = "/persistent-storage/datalake/export",
):
    data = fetch_data_from_postgres(conninfo(db), db_table_name)

    feature_collection = format_data_as_geojson(data)

    save_file(feature_collection, db_table_name, storage_path)


def fetch_data_from_postgres(db_connection_string, table_name: str):
    """
    Fetches all data from a specified PostgreSQL table.

    Parameters
    ----------
        db_connection_string (str): The connection string for the PostgreSQL database.
        table_name (str): The name of the table to fetch data from.

    Returns
    -------
        tuple: A tuple containing a list of column names and a list of rows fetched from the table.
    """

    try:
        conn = connect(db_connection_string)
        cursor = conn.cursor()
        cursor.execute(
            sql.SQL("SELECT * FROM {table_name}").format(
                table_name=sql.Identifier(table_name)
            )
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    except Error as e:
        logger.error(f"Error fetching data from {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(f"Data fetched from {table_name}")
    return columns, rows


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


def save_file(data, db_table_name: str, storage_path: str):
    """
    Saves the provided data as a GeoJSON file in the specified storage path.

    Parameters
    ----------
        data (dict): The data to be saved, formatted as a GeoJSON FeatureCollection.
        db_table_name (str): The name of the database table, used to name the output file.
        storage_path (str): The directory path where the GeoJSON file will be saved.
    """

    storage_path = Path(storage_path)
    storage_path.mkdir(parents=True, exist_ok=True)
    geojson_path = storage_path / f"{db_table_name}.geojson"
    with geojson_path.open("w") as f:
        json.dump(data, f)
    logger.info(f"GeoJSON file saved to {geojson_path}")
