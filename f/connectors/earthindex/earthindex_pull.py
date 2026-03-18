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

    layers = fetch_layers(session, project_id)

    if not layers:
        logger.info("No layers found. Skipping data processing.")
        return

    logger.info(f"Found {len(layers)} layer(s) for project '{project_title}'.")

    all_features = []
    for layer in layers:
        layer_id = layer["id"]
        points_geojson = fetch_layer_points(session, project_id, layer_id)
        features = points_geojson.get("features", [])

        for feature in features:
            feature["properties"]["layer_id"] = layer_id
            feature["properties"]["data_source"] = "Earth Index"
            feature["properties"]["project_id"] = project_id
            feature["properties"]["project_title"] = project_title

        all_features.extend(features)
        logger.info(f"Fetched {len(features)} feature(s) from layer {layer_id}.")

    if not all_features:
        logger.info("No features found across layers. Nothing to save.")
        return

    geojson = {"type": "FeatureCollection", "features": all_features}
    table_name = f"{db_table_prefix}_{sanitized_title}"

    save_data_to_file(geojson, table_name, storage_path, file_type="geojson")

    rel_geojson_path = (
        Path(db_table_prefix) / sanitized_title / f"{table_name}.geojson"
    )
    save_geojson_to_postgres(db, table_name, rel_geojson_path, attachment_root, False)

    logger.info(f"Saved {len(all_features)} feature(s) to table '{table_name}'.")


def fetch_project(session, project_id):
    url = f"{BASE_URL}/v1/projects/{project_id}"
    logger.info(f"Fetching project details for {project_id}...")
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def fetch_layers(session, project_id):
    url = f"{BASE_URL}/v1/projects/{project_id}/layers"
    logger.info(f"Fetching layers for project {project_id}...")
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def fetch_layer_points(session, project_id, layer_id):
    url = f"{BASE_URL}/v1/projects/{project_id}/layers/{layer_id}/points"
    logger.info(f"Fetching points for layer {layer_id}...")
    response = session.get(url)
    response.raise_for_status()
    return response.json()
