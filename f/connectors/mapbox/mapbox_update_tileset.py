# requirements:
# requests~=2.32

import logging
from pathlib import Path

import requests

from f.common_logic.geo_utils import geojson_to_line_delimited

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _replace_tileset_source(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
    source_path: Path,
):
    """
    Replace a Mapbox tileset source with new GeoJSON data.

    Converts the file to line-delimited GeoJSON and uploads it via the
    Mapbox Tiling Service `PUT /tilesets/v1/sources/{username}/{source_id}` endpoint.
    """
    ld_file_path = geojson_to_line_delimited(source_path)

    logger.info(
        "Replacing tileset source '%s' for user '%s' with file: %s",
        tileset_id,
        mapbox_username,
        ld_file_path,
    )

    url = (
        "https://api.mapbox.com/tilesets/v1/sources/"
        f"{mapbox_username}/{tileset_id}"
        f"?access_token={mapbox_secret_access_token}"
    )

    try:
        with ld_file_path.open("rb") as f:
            files = {"file": (ld_file_path.name, f, "application/json")}
            response = requests.put(url, files=files)

        response.raise_for_status()

        logger.info("Tileset source replaced successfully.")
        return response.json()
    finally:
        ld_file_path.unlink()
        logger.debug("Deleted temporary line-delimited GeoJSON file: %s", ld_file_path)


def _publish_tileset(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
):
    """
    Publish a Mapbox tileset, triggering MTS to rebuild tiles from the current source.

    Calls `POST /tilesets/v1/{tileset_id}/publish`.  Returns the API
    response which includes a `jobId` for monitoring progress.
    """
    url = (
        f"https://api.mapbox.com/tilesets/v1/{mapbox_username}.{tileset_id}/publish"
        f"?access_token={mapbox_secret_access_token}"
    )

    logger.info("Publishing tileset '%s'.", tileset_id)
    response = requests.post(url)

    response.raise_for_status()

    logger.info("Tileset published successfully.")
    return response.json()


def main(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
    file_location: str,
    attachment_root: str = "/persistent-storage/datalake/",
):
    """
    Replace a Mapbox tileset source and publish the tileset.

    Parameters
    ----------
    mapbox_username : str
        The Mapbox username of the account that owns the tileset source.
    mapbox_secret_access_token : str
        A Mapbox secret access token with the `tilesets:write`` scope.
        Must start with `sk.ey`.
    tileset_id : str
        The tileset ID to replace and publish.
    file_location : str
        Relative path (from *attachment_root*) to the GeoJSON file to upload.
    attachment_root : str, optional
        Base directory where the source file is stored. Defaults to
        `"/persistent-storage/datalake/"`.

    Returns
    -------
    dict
        Combined result with keys `source` (replace response) and
        `publish` (publish response containing `jobId`).
    """
    if not mapbox_secret_access_token.startswith("sk.ey"):
        raise ValueError("mapbox_secret_access_token must start with 'sk.ey'")

    source_path = Path(attachment_root) / file_location
    if not source_path.is_file():
        raise FileNotFoundError(f"Source file not found at: {source_path}")

    source_result = _replace_tileset_source(
        mapbox_username, mapbox_secret_access_token, tileset_id, source_path
    )
    publish_result = _publish_tileset(
        mapbox_username, mapbox_secret_access_token, tileset_id
    )

    return {"source": source_result, "publish": publish_result}
