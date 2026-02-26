from pathlib import Path

import pytest

from f.connectors.mapbox.mapbox_create_tileset import main
from f.connectors.mapbox.tests.assets import server_responses

ASSETS_DIR = Path(__file__).parent / "assets"


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
    """Verify the standard three-call sequence (POST create source, POST create tileset, POST publish)."""
    expected_source = server_responses.mapbox_tileset_source_create_response(
        mapbox_tileset_create_source.username,
        mapbox_tileset_create_source.tileset_id,
    )
    expected_tileset = server_responses.mapbox_create_tileset_response(
        mapbox_tileset_create_source.username,
        mapbox_tileset_create_source.tileset_id,
    )
    expected_publish = server_responses.mapbox_publish_response(
        mapbox_tileset_create_source.username,
        mapbox_tileset_create_source.tileset_id,
    )
    assert result == {
        "source": expected_source,
        "tileset": expected_tileset,
        "publish": expected_publish,
    }

    create_source_call, create_tileset_call, publish_call = mocked_responses.calls[-3:]
    assert create_source_call.request.method == "POST"
    assert create_source_call.request.url == (
        server_responses.mapbox_tileset_source_create_url(
            mapbox_tileset_create_source.username,
            mapbox_tileset_create_source.tileset_id,
            mapbox_tileset_create_source.access_token,
        )
    )
    assert create_tileset_call.request.method == "POST"
    assert create_tileset_call.request.url == (
        server_responses.mapbox_create_tileset_url(
            mapbox_tileset_create_source.username,
            mapbox_tileset_create_source.tileset_id,
            mapbox_tileset_create_source.access_token,
        )
    )
    assert publish_call.request.method == "POST"
    assert publish_call.request.url == server_responses.mapbox_publish_url(
        mapbox_tileset_create_source.username,
        mapbox_tileset_create_source.tileset_id,
        mapbox_tileset_create_source.access_token,
    )

    return create_source_call, create_tileset_call


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
    assert payload["recipe"]["layers"][layer_id]["maxzoom"] == 22


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

