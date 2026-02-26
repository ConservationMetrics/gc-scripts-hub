BASE_URL = "https://api.mapbox.com"


def mapbox_tileset_source_url(
    username: str,
    dataset_id: str,
    access_token: str,
) -> str:
    """
    Build the Replace a tileset source URL for a given user, dataset, and token.
    """
    return (
        f"{BASE_URL}/tilesets/v1/sources/"
        f"{username}/{dataset_id}"
        f"?access_token={access_token}"
    )


def mapbox_tileset_source_replace_response(
    username: str,
    dataset_id: str,
    file_size: int = 10592,
    files: int = 1,
    source_size: int = 10592,
) -> dict:
    """
    Example response object for Replace a tileset source, mirroring Mapbox docs.
    """
    return {
        "file_size": file_size,
        "files": files,
        "id": f"mapbox://tileset-source/{username}/{dataset_id}",
        "source_size": source_size,
    }
