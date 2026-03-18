# requirements:
# psycopg[binary]
# requests~=2.32

import logging
from pathlib import Path

import requests

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file
from f.common_logic.identifier_utils import normalize_identifier
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

BASE_URL = "https://api.earthindex.ai"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    api_key: str,
    project_id: str,
    db: postgresql,
    db_table_prefix: str = "earthindex",
    attachment_root: str = "/persistent-storage/datalake",
):
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {api_key}"})

    project = fetch_project(session, project_id)
    project_title = project["title"]
    sanitized_title = normalize_identifier(project_title)

    storage_path = Path(attachment_root) / db_table_prefix / sanitized_title
    save_data_to_file(project, "project", storage_path, file_type="json")
    logger.info(f"Saved project details for '{project_title}'.")

    layers = project.get("layers", [])
    if not layers:
        logger.info("No layers found. Skipping data processing.")
        return

    # NOTE: Currently Earth Index returns exactly 1 layer per project.
    # See README for more details.
    layer_id = layers[0]["id"]
    points_geojson = fetch_layer_points(session, project_id, layer_id)

    geojson = format_features_as_geojson(
        points_geojson, project_id, project_title, layer_id
    )

    if not geojson["features"]:
        logger.info("No features found. Nothing to save.")
        return

    table_name = f"{db_table_prefix}_{sanitized_title}"

    save_data_to_file(geojson, table_name, storage_path, file_type="geojson")

    rel_geojson_path = Path(db_table_prefix) / sanitized_title / f"{table_name}.geojson"
    save_geojson_to_postgres(db, table_name, rel_geojson_path, attachment_root, False)

    logger.info(f"Saved {len(geojson['features'])} feature(s) to table '{table_name}'.")


def fetch_project(session, project_id):
    url = f"{BASE_URL}/v1/projects/{project_id}"
    logger.info(f"Fetching project details for {project_id}...")
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def fetch_layer_points(session, project_id, layer_id):
    url = f"{BASE_URL}/v1/projects/{project_id}/layers/{layer_id}/points"
    logger.info(f"Fetching points for layer {layer_id}...")
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def format_features_as_geojson(points_geojson, project_id, project_title, layer_id):
    """Enrich Earth Index point features with project metadata and wrap as a
    GeoJSON FeatureCollection.

    Parameters
    ----------
    points_geojson : dict
        Raw response from the ``/layers/<layer_id>/points`` endpoint.
    project_id : str
        The Earth Index project identifier.
    project_title : str
        Human-readable project title.
    layer_id : str
        The layer the features belong to.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection with enriched properties.
    """
    features = points_geojson.get("features", [])

    for feature in features:
        feature["properties"]["layer_id"] = layer_id
        feature["properties"]["data_source"] = "Earth Index"
        feature["properties"]["project_id"] = project_id
        feature["properties"]["project_title"] = project_title

    logger.info(f"Formatted {len(features)} feature(s) from layer {layer_id}.")

    return {"type": "FeatureCollection", "features": features}
