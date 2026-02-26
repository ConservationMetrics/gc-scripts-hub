import logging
from pathlib import Path

import requests

from f.common_logic.geo_utils import geojson_to_line_delimited
from f.connectors.mapbox.mapbox_update_tileset import _publish_tileset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _create_tileset_source(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
    source_path: Path,
):
    """
    Create a Mapbox tileset source from a GeoJSON file.

    Converts the file to line-delimited GeoJSON and uploads it via the
    Mapbox Tiling Service
    `POST /tilesets/v1/sources/{username}/{id}` endpoint.
    """
    ld_file_path = geojson_to_line_delimited(source_path)

    logger.info(
        "Creating tileset source '%s' for user '%s' from file: %s",
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
            response = requests.post(url, files=files)

        response.raise_for_status()

        logger.info("Tileset source created successfully.")
        return response.json()
    finally:
        ld_file_path.unlink()
        logger.debug("Deleted temporary line-delimited GeoJSON file: %s", ld_file_path)


def _build_recipe(
    mapbox_username: str,
    tileset_id: str,
    layer_id: str,
    max_zoom: int,
):
    """
    Build a Mapbox tileset recipe.

    Creates a single-layer recipe with zoom levels 0 to max_zoom.
    This works well for most vector datasets, but may need to be extended to
    accept dynamic parameters (e.g. per-layer zoom ranges, feature filters,
    or tile-size overrides) if a one-size-fits-all recipe proves insufficient.

    See Mapbox recipe specification for more details: https://docs.mapbox.com/help/troubleshooting/tileset-recipe-reference/#zoom-level-configuration
    """
    return {
        "version": 1,
        "layers": {
            layer_id: {
                "source": f"mapbox://tileset-source/{mapbox_username}/{tileset_id}",
                "minzoom": 0,
                "maxzoom": max_zoom,  # max: 16
            }
        },
    }


def _create_tileset(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
    max_zoom: int,
):
    """
    Create a Mapbox tileset with a simple recipe that points at the tileset source.

    Calls `POST /tilesets/v1/{username}.{tileset_id}` with a recipe that uses the
    tileset source `mapbox://tileset-source/{username}/{tileset_id}` and
    zoom levels 0 to max_zoom.
    """
    url = (
        f"https://api.mapbox.com/tilesets/v1/{mapbox_username}.{tileset_id}"
        f"?access_token={mapbox_secret_access_token}"
    )

    layer_id = tileset_id.replace("-", "_")
    recipe = _build_recipe(mapbox_username, tileset_id, layer_id, max_zoom)

    payload = {"recipe": recipe, "name": tileset_id}

    logger.info("Creating tileset '%s.%s'.", mapbox_username, tileset_id)
    response = requests.post(url, json=payload)

    response.raise_for_status()

    logger.info("Tileset created successfully.")
    return response.json()


def main(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
    file_location: str,
    attachment_root: str = "/persistent-storage/datalake/",
    max_zoom: int = 11,
):
    """
    Create a Mapbox tileset from a GeoJSON file.

    This script:

    1. Creates a tileset source from the provided GeoJSON.
    2. Creates a tileset with a simple recipe that references that source.
    3. Publishes the tileset to start processing.

    Parameters
    ----------
    mapbox_username : str
        The Mapbox username of the account that will own the tileset.
    mapbox_secret_access_token : str
        A Mapbox secret access token with the `tilesets:write` scope.
        Must start with `sk.ey`.
    tileset_id : str
        The tileset ID to create (for example, `hello-world`).
    file_location : str
        Relative path (from *attachment_root*) to the GeoJSON file to upload.
    attachment_root : str, optional
        Base directory where the source file is stored. Defaults to
        `"/persistent-storage/datalake/"`.
    max_zoom : int, optional
        Maximum zoom level for the tileset. Defaults to 16. Valid range is 0-22.

    Returns
    -------
    dict
        Combined result with keys:

        - `source`: create tileset source response
        - `tileset`: create tileset response
        - `publish`: publish response containing `jobId`
    """
    source_path = Path(attachment_root) / file_location
    if not source_path.is_file():
        raise FileNotFoundError(f"Source file not found at: {source_path}")

    source_result = _create_tileset_source(
        mapbox_username,
        mapbox_secret_access_token,
        tileset_id,
        source_path,
    )
    tileset_result = _create_tileset(
        mapbox_username,
        mapbox_secret_access_token,
        tileset_id,
        max_zoom,
    )
    publish_result = _publish_tileset(
        mapbox_username,
        mapbox_secret_access_token,
        tileset_id,
    )

    return {
        "source": source_result,
        "tileset": tileset_result,
        "publish": publish_result,
    }
