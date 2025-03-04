# requirements:
# psycopg2-binary
# requests~=2.32

import json
import logging
import os
from pathlib import Path

import requests

from f.common_logic.db_connection import postgresql
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

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
    storage_path = Path(attachment_root) / db_table_name

    arcgis_token = get_arcgis_token(arcgis_account)

    features = get_features_from_arcgis(feature_layer_url, arcgis_token)

    features_with_attachments = download_feature_attachments(
        features, feature_layer_url, arcgis_token, storage_path
    )

    features_with_global_ids = set_global_id(features_with_attachments)

    save_geojson_file_to_disk(features_with_global_ids, storage_path)

    # At this point, the ArcGIS data is GeoJSON-compliant, and we don't need anything
    # from the REST API anymore. The data can therefore be handled further using the
    # existing GeoJSON connector.
    save_geojson_to_postgres(
        db,
        db_table_name,
        str(storage_path / "data.geojson"),
        storage_path,
        False,
    )


def get_arcgis_token(arcgis_account: c_arcgis_account):
    """
    Generate an ArcGIS token using the provided account credentials.

    Parameters
    ----------
    arcgis_account : dict
        A dictionary containing the ArcGIS account credentials with keys "username" and "password".

    Returns
    -------
    str
        The generated ArcGIS token.
    """
    arcgis_username = arcgis_account["username"]
    arcgis_password = arcgis_account["password"]

    token_response = requests.post(
        "https://www.arcgis.com/sharing/rest/generateToken",
        data={
            "username": arcgis_username,
            "password": arcgis_password,
            "client": "referer",
            "referer": os.environ.get("WM_BASE_URL"),
            "f": "json",
        },
    )

    arcgis_token = token_response.json().get("token")

    return arcgis_token


def get_features_from_arcgis(feature_layer_url: str, arcgis_token: str):
    """
    Fetch features from an ArcGIS feature layer using the provided token.

    Parameters
    ----------
    feature_layer_url : str
        The URL of the ArcGIS feature layer.
    arcgis_token : str
        The ArcGIS token for authentication.

    Returns
    -------
    list
        A list of features retrieved from the ArcGIS feature layer.
    """
    response = requests.get(
        f"{feature_layer_url}/0/query",
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
        try:
            error_message = (
                response.json().get("error", {}).get("message", "Unknown error")
            )
        except (KeyError, ValueError):
            error_message = "Unknown error"
        raise ValueError(f"Error fetching features: {error_message}")

    features = response.json().get("features", [])

    logger.info(f"{len(features)} features fetched from the ArcGIS feature layer")
    return features


def download_feature_attachments(
    features: list, feature_layer_url: str, arcgis_token: str, storage_path: str
):
    """
    Download attachments for each feature and save them to the specified directory.

    Parameters
    ----------
    features : list
        A list of features for which attachments need to be downloaded.
    feature_layer_url : str
        The URL of the ArcGIS feature layer.
    arcgis_token : str
        The ArcGIS token for authentication.
    storage_path : str
        The directory where attachments should be saved.

    Returns
    -------
    list
        The list of features with updated properties including attachment information.
    """
    total_downloaded_attachments = 0
    skipped_attachments = 0

    for feature in features:
        object_id = feature["properties"]["objectid"]

        attachments_response = requests.get(
            f"{feature_layer_url}/0/{object_id}/attachments",
            params={"f": "json", "token": arcgis_token},
        )

        attachments_response.raise_for_status()

        attachments = attachments_response.json().get("attachmentInfos", [])

        if not attachments:
            logger.info(f"No attachments found for object_id {object_id}")
            continue

        for attachment in attachments:
            attachment_id = attachment["id"]
            attachment_name = attachment["name"]
            attachment_content_type = attachment["contentType"]
            attachment_keywords = attachment["keywords"]

            feature["properties"][f"{attachment_keywords}_filename"] = attachment_name
            feature["properties"][f"{attachment_keywords}_content_type"] = (
                attachment_content_type
            )

            attachment_path = Path(storage_path) / "attachments" / attachment_name

            if attachment_path.exists():
                logger.debug(
                    f"File already exists, skipping download: {attachment_path}"
                )
                skipped_attachments += 1
                continue

            attachment_response = requests.get(
                f"{feature_layer_url}/0/{object_id}/attachments/{attachment_id}",
                params={"f": "json", "token": arcgis_token},
            )

            attachment_response.raise_for_status()

            attachment_data = attachment_response.content

            attachment_path.parent.mkdir(parents=True, exist_ok=True)

            with open(attachment_path, "wb") as f:
                f.write(attachment_data)

            logger.info(
                f"Downloaded attachment {attachment_name} (content type: {attachment_content_type})"
            )

            total_downloaded_attachments += 1

    logger.info(f"Total downloaded attachments: {total_downloaded_attachments}")
    logger.info(f"Total skipped attachments: {skipped_attachments}")
    return features


def set_global_id(features: list):
    """
    Set the feature ID of each feature to its global ID (which is a uuid).
    ArcGIS uses global IDs to uniquely identify features, but the
    feature ID is set to the object ID by default (which is an integer
    incremented by 1 for each feature). UUIDs are more reliable for
    uniquely identifying features, and using them instead is consistent
    with how we store other data in the data warehouse.
    https://support.esri.com/en-us/gis-dictionary/globalid

    Parameters
    ----------
    features : list
        A list of features to update.

    Returns
    -------
    list
        The list of features with updated feature IDs.
    """
    for feature in features:
        feature["id"] = feature["properties"]["globalid"]

    return features


def save_geojson_file_to_disk(
    features: list,
    storage_path: str,
):
    """
    Save the GeoJSON file to disk.

    Parameters
    ----------
    features : list
        A list of features to save.
    storage_path : str
        The directory where the GeoJSON file should be saved.
    """
    geojson = {"type": "FeatureCollection", "features": features}

    geojson_filename = Path(storage_path) / "data.geojson"

    geojson_filename.parent.mkdir(parents=True, exist_ok=True)

    with open(geojson_filename, "w") as f:
        json.dump(geojson, f)

    logger.info(f"GeoJSON file saved to: {geojson_filename}")
