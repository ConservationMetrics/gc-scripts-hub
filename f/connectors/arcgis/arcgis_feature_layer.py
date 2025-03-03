# requirements:
# psycopg2-binary
# requests~=2.32

import logging

import requests

from f.common_logic.db_connection import postgresql

# type names that refer to Windmill Resources
c_arcgis_account = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    arcgis_account: c_arcgis_account,
    feature_layer_url: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    arcgis_token = get_arcgis_token(arcgis_account)

    features = get_features_from_arcgis(feature_layer_url, arcgis_token)

    # logging.info(f"Wrote response content to database table [{db_table_name}]")


def get_arcgis_token(arcgis_account: c_arcgis_account):
    arcgis_username = arcgis_account["username"]
    arcgis_password = arcgis_account["password"]

    token_response = requests.post(
        "https://www.arcgis.com/sharing/rest/generateToken",
        data={
            "username": arcgis_username,
            "password": arcgis_password,
            "client": "requestip",
            "f": "json",
        },
    )

    arcgis_token = token_response.json().get("token")

    return arcgis_token


def get_features_from_arcgis(feature_layer_url: str, arcgis_token: str):
    response = requests.get(
        feature_layer_url,
        params={
            "where": "1=1",  # get all features
            "outFields": "*",  # get all fields
            "returnGeometry": "true",
            "f": "geojson",
            "token": arcgis_token,
        },
    )

    if (
        response.status_code != 200 or "error" in response.json()
    ):  # ArcGIS sometimes returns 200 with an error message e.g. if a token is invalid
        error_message = response.json().get("error", {}).get("message", "Unknown error")
        raise ValueError(f"Error fetching features: {error_message}")

    logger.info(
        f"{len(response.json().get('features', []))} features fetched from the ArcGIS feature layer"
    )
    return response.json()
