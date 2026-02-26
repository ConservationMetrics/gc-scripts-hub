from pathlib import Path

import pytest

from f.connectors.mapbox.mapbox_create_or_update_tileset import (
    main,
)
from f.connectors.mapbox.tests.assets import server_responses

ASSETS_DIR = Path(__file__).parent / "assets"


def test_invalid_access_token_raises():
    with pytest.raises(ValueError, match="sk.ey"):
        main(
            mapbox_username="test-user",
            mapbox_secret_access_token="pk.not_a_secret_token",
            tileset_id="hello-world",
            file_location="sample.geojson",
            attachment_root="/does/not/matter",
        )


def test_missing_file_raises(tmp_path):
    attachment_root = tmp_path / "datalake"
    attachment_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(FileNotFoundError):
        main(
            mapbox_username="test-user",
            mapbox_secret_access_token="sk.ey_valid_token",
            tileset_id="hello-world",
            file_location="does_not_exist.geojson",
            attachment_root=str(attachment_root),
        )


def _stage_geojson(tmp_path: Path, asset_name: str) -> tuple[str, str]:
    """Copy a test asset GeoJSON into a temporary attachment_root."""
    attachment_root = tmp_path / "datalake"
    attachment_root.mkdir(parents=True, exist_ok=True)

    src = ASSETS_DIR / asset_name
    dest = attachment_root / asset_name
    dest.write_bytes(src.read_bytes())

    return str(attachment_root), asset_name


def _assert_create_source_tileset_and_publish(
    mocked_responses, result, mapbox_tileset_create_source
):
    """Verify the standard four-call sequence (GET exists, POST create source, POST create tileset, POST publish)."""
    source = mapbox_tileset_create_source

    expected_source = server_responses.mapbox_tileset_source_create_response(
        source.username,
        source.tileset_id,
    )
    expected_tileset = server_responses.mapbox_create_tileset_response(
        source.username,
        source.tileset_id,
    )
    expected_publish = server_responses.mapbox_publish_response(
        source.username,
        source.tileset_id,
    )
    assert result == {
        "action": "create",
        "source": expected_source,
        "tileset": expected_tileset,
        "publish": expected_publish,
    }

    get_call, create_source_call, create_tileset_call, publish_call = (
        mocked_responses.calls[-4:]
    )
    assert get_call.request.method == "GET"
    assert get_call.request.url == server_responses.mapbox_tileset_get_url(
        f"{source.username}.{source.tileset_id}",
        source.access_token,
    )
    assert create_source_call.request.method == "POST"
    assert create_source_call.request.url == (
        server_responses.mapbox_tileset_source_create_url(
            source.username,
            source.tileset_id,
            source.access_token,
        )
    )
    assert create_tileset_call.request.method == "POST"
    assert create_tileset_call.request.url == (
        server_responses.mapbox_create_tileset_url(
            source.username,
            source.tileset_id,
            source.access_token,
        )
    )
    assert publish_call.request.method == "POST"
    assert publish_call.request.url == server_responses.mapbox_publish_url(
        source.username,
        source.tileset_id,
        source.access_token,
    )

    return create_source_call, create_tileset_call


def _assert_replace_and_publish(mocked_responses, result, mapbox_tileset_source):
    """Verify the standard three-call sequence (GET exists, PUT replace, POST publish)."""
    source = mapbox_tileset_source

    expected_source = server_responses.mapbox_tileset_source_replace_response(
        source.username, source.tileset_id
    )
    expected_publish = server_responses.mapbox_publish_response(
        source.username, source.tileset_id
    )
    assert result == {
        "action": "update",
        "source": expected_source,
        "publish": expected_publish,
    }

    get_call, replace_call, publish_call = mocked_responses.calls[-3:]
    assert get_call.request.method == "GET"
    assert get_call.request.url == server_responses.mapbox_tileset_get_url(
        f"{source.username}.{source.tileset_id}",
        source.access_token,
    )
    assert replace_call.request.method == "PUT"
    assert replace_call.request.url == server_responses.mapbox_tileset_source_url(
        source.username,
        source.tileset_id,
        source.access_token,
    )
    assert publish_call.request.method == "POST"
    assert publish_call.request.url == server_responses.mapbox_publish_url(
        source.username,
        source.tileset_id,
        source.access_token,
    )

    return replace_call


def test_create_tileset_from_geojson(
    tmp_path, mocked_responses, mapbox_tileset_create_source
):
    attachment_root, file_location = _stage_geojson(tmp_path, "initial.geojson")

    result = main(
        mapbox_username=mapbox_tileset_create_source.username,
        mapbox_secret_access_token=mapbox_tileset_create_source.access_token,
        tileset_id=mapbox_tileset_create_source.tileset_id,
        file_location=file_location,
        attachment_root=attachment_root,
    )

    create_source_call, create_tileset_call = _assert_create_source_tileset_and_publish(
        mocked_responses, result, mapbox_tileset_create_source
    )

    # Verify the source was created with the correct GeoJSON content
    assert b"Northern Virginia" in create_source_call.request.body

    # Verify the tileset was created with the correct recipe
    request_json = create_tileset_call.request.body
    import json

    payload = json.loads(request_json)
    assert payload["name"] == mapbox_tileset_create_source.tileset_id
    assert "recipe" in payload
    assert payload["recipe"]["version"] == 1
    assert "layers" in payload["recipe"]
    layer_id = mapbox_tileset_create_source.tileset_id.replace("-", "_")
    assert layer_id in payload["recipe"]["layers"]
    assert (
        payload["recipe"]["layers"][layer_id]["source"]
        == f"mapbox://tileset-source/{mapbox_tileset_create_source.username}/{mapbox_tileset_create_source.tileset_id}"
    )
    assert payload["recipe"]["layers"][layer_id]["minzoom"] == 0
    assert payload["recipe"]["layers"][layer_id]["maxzoom"] == 11


def test_update_replaces_with_new_data(
    tmp_path, mocked_responses, mapbox_tileset_source
):
    attachment_root, file_location = _stage_geojson(tmp_path, "update.geojson")

    result = main(
        mapbox_username=mapbox_tileset_source.username,
        mapbox_secret_access_token=mapbox_tileset_source.access_token,
        tileset_id=mapbox_tileset_source.tileset_id,
        file_location=file_location,
        attachment_root=attachment_root,
    )

    replace_call = _assert_replace_and_publish(
        mocked_responses, result, mapbox_tileset_source
    )
    assert b"Southern Colorado" in replace_call.request.body
    assert b"Northern Virginia" not in replace_call.request.body


def test_replace_tileset_source_409_conflict(tmp_path, mocked_responses):
    """Test that a 409 Conflict error is caught and re-raised as RuntimeError."""
    attachment_root, file_location = _stage_geojson(tmp_path, "update.geojson")

    username = "test-user"
    tileset_id = "hello-world"
    access_token = "sk.ey_test_secret_token"

    # Register Get tileset endpoint (tileset exists)
    tileset_full_id = f"{username}.{tileset_id}"
    get_url = server_responses.mapbox_tileset_get_url(tileset_full_id, access_token)
    get_body = server_responses.mapbox_tileset_get_response(tileset_full_id)
    mocked_responses.get(get_url, json=get_body, status=200)

    # Register PUT response with 409 Conflict (this is what we're testing)
    replace_url = server_responses.mapbox_tileset_source_url(
        username, tileset_id, access_token
    )
    mocked_responses.put(replace_url, status=409)

    with pytest.raises(
        RuntimeError,
        match=(
            f"Tileset source '{tileset_id}' is still processing "
            "and cannot be updated at this time"
        ),
    ):
        main(
            mapbox_username=username,
            mapbox_secret_access_token=access_token,
            tileset_id=tileset_id,
            file_location=file_location,
            attachment_root=attachment_root,
        )
