from pathlib import Path

import pytest

from f.connectors.mapbox.mapbox_replace_tileset_source import main
from f.connectors.mapbox.tests.assets import server_responses


def _copy_sample_geojson(tmp_path: Path) -> tuple[str, str]:
    """Copy the bundled sample GeoJSON into a temporary attachment_root."""
    attachment_root = tmp_path / "datalake"
    attachment_root.mkdir(parents=True, exist_ok=True)

    sample_src = Path(__file__).parent / "assets" / "sample.geojson"
    target_path = attachment_root / "sample.geojson"
    target_path.write_bytes(sample_src.read_bytes())

    # Return (attachment_root, relative file_location)
    return str(attachment_root), "sample.geojson"


def test_replace_and_publish_success(tmp_path, mocked_responses, mapbox_tileset_source):
    attachment_root, file_location = _copy_sample_geojson(tmp_path)

    result = main(
        mapbox_username=mapbox_tileset_source.username,
        mapbox_secret_access_token=mapbox_tileset_source.access_token,
        tileset_id=mapbox_tileset_source.tileset_id,
        file_location=file_location,
        attachment_root=attachment_root,
    )

    # Verify combined return structure
    expected_source = server_responses.mapbox_tileset_source_replace_response(
        mapbox_tileset_source.username, mapbox_tileset_source.tileset_id
    )
    expected_publish = server_responses.mapbox_publish_response(
        mapbox_tileset_source.username, mapbox_tileset_source.tileset_id
    )
    assert result == {"source": expected_source, "publish": expected_publish}

    # Two calls: PUT (replace source), then POST (publish)
    assert len(mocked_responses.calls) == 2

    replace_call, publish_call = mocked_responses.calls
    assert replace_call.request.method == "PUT"
    assert replace_call.request.url == server_responses.mapbox_tileset_source_url(
        mapbox_tileset_source.username,
        mapbox_tileset_source.tileset_id,
        mapbox_tileset_source.access_token,
    )
    assert publish_call.request.method == "POST"
    assert publish_call.request.url == server_responses.mapbox_publish_url(
        mapbox_tileset_source.username,
        mapbox_tileset_source.tileset_id,
        mapbox_tileset_source.access_token,
    )

    # Uploaded body should contain line-delimited GeoJSON feature content
    assert b"Northern Virginia" in replace_call.request.body


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
