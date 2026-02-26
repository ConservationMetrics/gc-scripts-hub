# requirements:
# requests~=2.32

from pathlib import Path
from typing import Any, Dict

import requests


def main(
    mapbox_username: str,
    mapbox_access_token: str,
    dataset_id: str,
    file_location: str,
    attachment_root: str = "/persistent-storage/datalake/",
) -> Dict[str, Any]:
    """
    Replace a Mapbox tileset source with new data using the Mapbox Tiling Service.

    Parameters
    ----------
    mapbox_username : str
        The Mapbox username of the account that owns the tileset source.
    mapbox_access_token : str
        A Mapbox access token with the `tilesets:write` scope. Must start with `pk.ey`.
    dataset_id : str
        The ID for the tileset source to be replaced.
    file_location : str
        Relative path (from `attachment_root`) to the GeoJSON file to upload.
        The file contents must be line-delimited GeoJSON, as required by MTS.
    attachment_root : str, optional
        Base directory where the source file is stored. Defaults to
        "/persistent-storage/datalake/".

    Returns
    -------
    dict
        The JSON response from the Mapbox API, containing (at minimum):
        - file_size: int
        - files: int
        - id: str
        - source_size: int
    """
    if not mapbox_access_token.startswith("pk.ey"):
        raise ValueError(
            "mapbox_access_token must start with 'pk.ey' and be a public token "
            "with the `tilesets:write` scope."
        )

    source_path = Path(attachment_root) / file_location
    if not source_path.is_file():
        raise FileNotFoundError(f"Source file not found at: {source_path}")

    url = (
        "https://api.mapbox.com/tilesets/v1/sources/"
        f"{mapbox_username}/{dataset_id}"
        f"?access_token={mapbox_access_token}"
    )

    with source_path.open("rb") as f:
        files = {"file": (source_path.name, f, "application/json")}
        response = requests.put(url, files=files)

    response.raise_for_status()
    return response.json()


