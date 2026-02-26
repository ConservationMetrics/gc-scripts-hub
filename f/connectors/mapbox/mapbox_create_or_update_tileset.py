# requirements:
# requests~=2.32

import logging
from pathlib import Path

import requests

from f.common_logic.geo_utils import geojson_to_line_delimited
from f.common_logic.identifier_utils import validate_identifier

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
    file_location: str,
    attachment_root: str = "/persistent-storage/datalake/",
    max_zoom: int = 11,
) -> dict:
    """
    Create or update a Mapbox tileset from a GeoJSON file.

    Flow:
    - GET /tilesets/v1/{tileset_id} (where `{tileset_id}` is the full id, e.g. `username.hello-world`)
      - 404: create source → create tileset → publish
      - 200: replace source → publish
    """
    _assert_secret_access_token(mapbox_secret_access_token)
    validate_identifier(tileset_id, type="mapbox_tileset_id")

    source_path = Path(attachment_root) / file_location
    if not source_path.is_file():
        raise FileNotFoundError(f"Source file not found at: {source_path}")

    tileset_exists = _tileset_exists(
        mapbox_username, mapbox_secret_access_token, tileset_id
    )

    # Pathway 1: Update existing tileset
    if tileset_exists:
        source_result = _replace_tileset_source(
            mapbox_username, mapbox_secret_access_token, tileset_id, source_path
        )
        publish_result = _publish_tileset(
            mapbox_username, mapbox_secret_access_token, tileset_id
        )
        return {"action": "update", "source": source_result, "publish": publish_result}

    # Pathway 2: Create new tileset
    source_result = _create_tileset_source(
        mapbox_username, mapbox_secret_access_token, tileset_id, source_path
    )
    tileset_result = _create_tileset(
        mapbox_username, mapbox_secret_access_token, tileset_id, max_zoom
    )
    publish_result = _publish_tileset(
        mapbox_username, mapbox_secret_access_token, tileset_id
    )
    return {
        "action": "create",
        "source": source_result,
        "tileset": tileset_result,
        "publish": publish_result,
    }


def _assert_secret_access_token(mapbox_secret_access_token: str) -> None:
    """Validate that the Mapbox access token is a secret token.

    Parameters
    ----------
    mapbox_secret_access_token : str
        The Mapbox secret access token to validate.

    Raises
    ------
    ValueError
        If the token does not start with 'sk.ey'.
    """
    if not mapbox_secret_access_token.startswith("sk.ey"):
        raise ValueError("mapbox_secret_access_token must start with 'sk.ey'")


def _tileset_full_id(mapbox_username: str, tileset_id: str) -> str:
    """Build the full Mapbox tileset identifier.

    The Mapbox API uses ``username.tileset_id`` as the full identifier,
    e.g. ``myuser.hello-world``.

    Parameters
    ----------
    mapbox_username : str
        The Mapbox account username.
    tileset_id : str
        The short tileset identifier.

    Returns
    -------
    str
        The full tileset identifier in the form ``username.tileset_id``.
    """
    return f"{mapbox_username}.{tileset_id}"


def _tileset_exists(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
) -> bool:
    """Check whether a Mapbox tileset already exists.

    Parameters
    ----------
    mapbox_username : str
        The Mapbox account username.
    mapbox_secret_access_token : str
        The Mapbox secret access token.
    tileset_id : str
        The short tileset identifier.

    Returns
    -------
    bool
        True if the tileset exists (HTTP 200), False if not found (HTTP 404).
    """
    tileset_full_id = _tileset_full_id(mapbox_username, tileset_id)
    url = f"https://api.mapbox.com/tilesets/v1/{tileset_full_id}?access_token={mapbox_secret_access_token}"

    response = requests.get(url)

    if response.status_code == 404:
        logger.info("Tileset '%s' does not exist.", tileset_full_id)
        return False
    if response.status_code == 200:
        logger.info("Tileset '%s' exists.", tileset_full_id)
        return True

    response.raise_for_status()
    raise RuntimeError("Unreachable: raise_for_status() should have raised")


def _create_tileset_source(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
    source_path: Path,
) -> dict:
    """Create a new tileset source by uploading a GeoJSON file to the Mapbox Tiling Service.

    Converts the source GeoJSON to line-delimited format before uploading.
    The temporary line-delimited file is deleted after the upload regardless of success.

    Parameters
    ----------
    mapbox_username : str
        The Mapbox account username.
    mapbox_secret_access_token : str
        The Mapbox secret access token.
    tileset_id : str
        The short tileset identifier.
    source_path : Path
        Path to the GeoJSON source file.

    Returns
    -------
    dict
        The JSON response from the Mapbox API.
    """
    ld_file_path = geojson_to_line_delimited(source_path)

    logger.info(
        "Creating tileset source '%s' for user '%s' from file: %s",
        tileset_id,
        mapbox_username,
        ld_file_path,
    )

    try:
        with ld_file_path.open("rb") as f:
            files = {"file": (ld_file_path.name, f, "application/json")}
            response = requests.post(
                f"https://api.mapbox.com/tilesets/v1/sources/{mapbox_username}/{tileset_id}?access_token={mapbox_secret_access_token}",
                files=files,
            )
        response.raise_for_status()
        logger.info("Tileset source created successfully.")
        return response.json()
    finally:
        ld_file_path.unlink(missing_ok=True)
        logger.debug("Deleted temporary line-delimited GeoJSON file: %s", ld_file_path)


def _replace_tileset_source(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
    source_path: Path,
) -> dict:
    """Replace an existing tileset source by uploading a new GeoJSON file.

    Converts the source GeoJSON to line-delimited format before uploading.
    The temporary line-delimited file is deleted after the upload regardless of success.

    Parameters
    ----------
    mapbox_username : str
        The Mapbox account username.
    mapbox_secret_access_token : str
        The Mapbox secret access token.
    tileset_id : str
        The short tileset identifier.
    source_path : Path
        Path to the GeoJSON source file.

    Returns
    -------
    dict
        The JSON response from the Mapbox API.
    """
    ld_file_path = geojson_to_line_delimited(source_path)

    logger.info(
        "Replacing tileset source '%s' for user '%s' with file: %s",
        tileset_id,
        mapbox_username,
        ld_file_path,
    )

    try:
        with ld_file_path.open("rb") as f:
            files = {"file": (ld_file_path.name, f, "application/json")}
            response = requests.put(
                f"https://api.mapbox.com/tilesets/v1/sources/{mapbox_username}/{tileset_id}?access_token={mapbox_secret_access_token}",
                files=files,
            )
        response.raise_for_status()
        logger.info("Tileset source replaced successfully.")
        return response.json()
    except requests.HTTPError as e:
        if e.response.status_code == 409:
            raise RuntimeError(
                f"Tileset source '{tileset_id}' is still processing and cannot be updated at this time. "
                "Please wait for the current processing to complete before retrying."
            ) from e
        raise
    finally:
        ld_file_path.unlink(missing_ok=True)
        logger.debug("Deleted temporary line-delimited GeoJSON file: %s", ld_file_path)


def _build_tileset_recipe(
    mapbox_username: str,
    tileset_id: str,
    layer_id: str,
    max_zoom: int,
) -> dict:
    """Build a tileset recipe dictionary.

    Parameters
    ----------
    mapbox_username : str
        The Mapbox account username.
    tileset_id : str
        The short tileset identifier.
    layer_id : str
        The layer identifier within the recipe.
    max_zoom : int
        The maximum zoom level for the tileset (max 16).

    Returns
    -------
    dict
        A MTS recipe dictionary with version, source reference, and zoom range.
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
) -> dict:
    """Create a new Mapbox tileset with a recipe and publish-ready configuration.

    Parameters
    ----------
    mapbox_username : str
        The Mapbox account username.
    mapbox_secret_access_token : str
        The Mapbox secret access token.
    tileset_id : str
        The short tileset identifier.
    max_zoom : int
        The maximum zoom level for the tileset (max 16).

    Returns
    -------
    dict
        The JSON response from the Mapbox API.
    """
    tileset_full_id = _tileset_full_id(mapbox_username, tileset_id)
    layer_id = tileset_id.replace("-", "_")
    payload = {
        "recipe": _build_tileset_recipe(
            mapbox_username, tileset_id, layer_id, max_zoom
        ),
        "name": tileset_id,
    }

    logger.info("Creating tileset '%s'.", tileset_full_id)
    response = requests.post(
        f"https://api.mapbox.com/tilesets/v1/{tileset_full_id}?access_token={mapbox_secret_access_token}",
        json=payload,
    )
    response.raise_for_status()
    logger.info("Tileset created successfully.")
    return response.json()


def _publish_tileset(
    mapbox_username: str,
    mapbox_secret_access_token: str,
    tileset_id: str,
) -> dict:
    """Publish a Mapbox tileset, triggering tile processing.

    Parameters
    ----------
    mapbox_username : str
        The Mapbox account username.
    mapbox_secret_access_token : str
        The Mapbox secret access token.
    tileset_id : str
        The short tileset identifier.

    Returns
    -------
    dict
        The JSON response from the Mapbox API.
    """
    tileset_full_id = _tileset_full_id(mapbox_username, tileset_id)

    logger.info("Publishing tileset '%s'.", tileset_full_id)
    response = requests.post(
        f"https://api.mapbox.com/tilesets/v1/{tileset_full_id}/publish?access_token={mapbox_secret_access_token}"
    )
    response.raise_for_status()
    logger.info("Tileset published successfully.")
    return response.json()
