BASE_URL = "https://api.mapbox.com"


def mapbox_tileset_get_url(tileset_full_id: str, access_token: str) -> str:
    """Build the Get a tileset URL."""
    return f"{BASE_URL}/tilesets/v1/{tileset_full_id}?access_token={access_token}"


def mapbox_tileset_get_response(tileset_full_id: str) -> dict:
    """Minimal example response object for Get a tileset (content not used by tests)."""
    return {"id": tileset_full_id}


def mapbox_tileset_source_url(
    username: str,
    tileset_id: str,
    access_token: str,
) -> str:
    """
    Build the Replace a tileset source URL for a given user, tileset, and token.
    """
    return (
        f"{BASE_URL}/tilesets/v1/sources/"
        f"{username}/{tileset_id}"
        f"?access_token={access_token}"
    )


def mapbox_tileset_source_replace_response(
    username: str,
    tileset_id: str,
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
        "id": f"mapbox://tileset-source/{username}/{tileset_id}",
        "source_size": source_size,
    }


def mapbox_publish_url(
    username: str,
    tileset_id: str,
    access_token: str,
) -> str:
    """Build the Publish a tileset URL."""
    return (
        f"{BASE_URL}/tilesets/v1/{username}.{tileset_id}/publish"
        f"?access_token={access_token}"
    )


def mapbox_publish_response(
    username: str,
    tileset_id: str,
) -> dict:
    """Example response object for Publish a tileset."""
    return {
        "jobId": "test-job-id-123",
        "message": f"Processing {username}.{tileset_id}",
    }


def mapbox_tileset_source_create_url(
    username: str,
    tileset_id: str,
    access_token: str,
) -> str:
    """
    Build the Create a tileset source URL for a given user, tileset, and token.
    """
    return (
        f"{BASE_URL}/tilesets/v1/sources/"
        f"{username}/{tileset_id}"
        f"?access_token={access_token}"
    )


def mapbox_tileset_source_create_response(
    username: str,
    tileset_id: str,
    file_size: int = 219,
    files: int = 1,
    source_size: int = 219,
) -> dict:
    """
    Example response object for Create a tileset source, mirroring Mapbox docs.
    """
    return {
        "id": f"mapbox://tileset-source/{username}/{tileset_id}",
        "files": files,
        "source_size": source_size,
        "file_size": file_size,
    }


def mapbox_create_tileset_url(
    username: str,
    tileset_id: str,
    access_token: str,
) -> str:
    """Build the Create a tileset URL."""
    return f"{BASE_URL}/tilesets/v1/{username}.{tileset_id}?access_token={access_token}"


def mapbox_create_tileset_response(
    username: str,
    tileset_id: str,
) -> dict:
    """Example response object for Create a tileset."""
    return {
        "message": (
            f"Successfully created empty tileset {username}.{tileset_id}. "
            "Publish your tileset to begin processing your data into tiles."
        ),
    }
