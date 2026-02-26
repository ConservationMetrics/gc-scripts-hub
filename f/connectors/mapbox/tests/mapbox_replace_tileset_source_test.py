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


def test_replace_tileset_source_success(
    tmp_path, mocked_responses, mapbox_tileset_source
):
    attachment_root, file_location = _copy_sample_geojson(tmp_path)

    expected_response = server_responses.mapbox_tileset_source_replace_response(
        mapbox_tileset_source.username,
        mapbox_tileset_source.dataset_id,
    )
    expected_url = server_responses.mapbox_tileset_source_url(
        mapbox_tileset_source.username,
        mapbox_tileset_source.dataset_id,
        mapbox_tileset_source.access_token,
    )

    result = main(
        mapbox_username=mapbox_tileset_source.username,
        mapbox_access_token=mapbox_tileset_source.access_token,
        dataset_id=mapbox_tileset_source.dataset_id,
        file_location=file_location,
        attachment_root=attachment_root,
    )

    assert result == expected_response
    assert len(mocked_responses.calls) == 1
    call = mocked_responses.calls[0]
    assert call.request.method == "PUT"
    assert call.request.url == expected_url
    # Sanity-check that the uploaded body contains part of the sample GeoJSON content
    assert b"Northern Virginia" in call.request.body


def test_invalid_access_token_raises():
    with pytest.raises(ValueError):
        main(
            mapbox_username="test-user",
            mapbox_access_token="sk.invalid",
            dataset_id="hello-world",
            file_location="sample.geojson",
            attachment_root="/does/not/matter",
        )


def test_missing_file_raises(tmp_path):
    attachment_root = tmp_path / "datalake"
    attachment_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(FileNotFoundError):
        main(
            mapbox_username="test-user",
            mapbox_access_token="pk.ey_valid",
            dataset_id="hello-world",
            file_location="does_not_exist.geojson",
            attachment_root=str(attachment_root),
        )
