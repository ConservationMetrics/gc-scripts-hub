import json
from pathlib import Path
from unittest.mock import patch

import mercantile
import pytest

from f.connectors.mapbox.mapbox_download_vector_tiles import (
    _reconstruct_features,
    main,
    mapbox,
)
from f.connectors.mapbox.tests.assets import server_responses

ACCESS_TOKEN = "pk.ey_test_public_token"
TILESET = "mapbox.mapbox-streets-v8"
# Tiny bbox near the equator that resolves to a single tile at zoom 10.
BBOX = "-77.05,-1.05,-77.04,-1.04"
ZOOM = "10"


def _tiles():
    return list(
        mercantile.tiles(*[float(p) for p in BBOX.split(",")], zooms=[int(ZOOM)])
    )


def _creds() -> mapbox:
    return mapbox(username="unused", access_token=ACCESS_TOKEN)


def _register_tile(
    mocked_responses,
    tile: mercantile.Tile,
    *,
    status: int = 200,
    body: bytes = b"fake-pbf",
):
    url = server_responses.mapbox_vector_tile_url(
        TILESET, tile.z, tile.x, tile.y, ACCESS_TOKEN
    )
    mocked_responses.get(
        url,
        body=body,
        status=status,
        content_type="application/vnd.mapbox-vector-tile",
    )


def test_dry_run_returns_plan_without_http(tmp_path, mocked_responses):
    result = main(
        mapbox=_creds(),
        bbox=BBOX,
        tileset=TILESET,
        zoom=ZOOM,
        attachment_root=str(tmp_path),
        dry_run=True,
    )

    assert result["dry_run"] is True
    assert result["tile_count"] == len(_tiles())
    assert result["to_fetch_count"] == result["tile_count"]
    assert result["cached_count"] == 0
    assert result["tileset"] == TILESET
    assert len(mocked_responses.calls) == 0


def test_invalid_bbox_raises(tmp_path):
    with pytest.raises(ValueError, match="expected four comma-separated"):
        main(
            mapbox=_creds(),
            bbox="not-a-bbox",
            tileset=TILESET,
            attachment_root=str(tmp_path),
            dry_run=True,
        )


def test_empty_tileset_raises(tmp_path):
    with pytest.raises(ValueError, match="tileset is required"):
        main(
            mapbox=_creds(),
            bbox=BBOX,
            tileset="  ",
            attachment_root=str(tmp_path),
        )


def test_zoom_out_of_range_raises(tmp_path):
    with pytest.raises(ValueError, match="between 0 and 14"):
        main(
            mapbox=_creds(),
            bbox=BBOX,
            tileset=TILESET,
            zoom="15",
            attachment_root=str(tmp_path),
            dry_run=True,
        )


def test_download_and_process_writes_geojson(tmp_path, mocked_responses):
    tiles = _tiles()
    assert len(tiles) == 1
    tile = tiles[0]
    _register_tile(mocked_responses, tile)

    fake_features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-77.045, -1.045]},
            "properties": {"name": "test"},
        }
    ]

    with patch(
        "f.connectors.mapbox.mapbox_download_vector_tiles.vt_bytes_to_geojson",
        return_value={"type": "FeatureCollection", "features": fake_features},
    ):
        result = main(
            mapbox=_creds(),
            bbox=BBOX,
            tileset=TILESET,
            zoom=ZOOM,
            output_filename="ecuador_tiles",
            attachment_root=str(tmp_path),
            delete_tiles=False,
        )

    assert result["dry_run"] is False
    assert result["tiles_downloaded"] == 1
    assert result["tiles_failed"] == 0
    assert result["feature_count"] == 1
    assert result["tiles_deleted"] is False
    assert result["output_path"].endswith("ecuador_tiles.geojson")

    cache_path = tmp_path / "ecuador_tiles" / "tiles" / str(tile.z) / str(tile.x) / f"{tile.y}.pbf"
    assert cache_path.is_file()
    assert cache_path.read_bytes() == b"fake-pbf"

    geojson_path = Path(result["output_path"])
    assert geojson_path.is_file()
    saved = json.loads(geojson_path.read_text())
    assert saved["features"][0]["properties"]["name"] == "test"
    assert len(mocked_responses.calls) == 1


def test_delete_tiles_removes_cache_after_success(tmp_path, mocked_responses):
    tile = _tiles()[0]
    _register_tile(mocked_responses, tile)

    with patch(
        "f.connectors.mapbox.mapbox_download_vector_tiles.vt_bytes_to_geojson",
        return_value={
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                }
            ],
        },
    ):
        result = main(
            mapbox=_creds(),
            bbox=BBOX,
            tileset=TILESET,
            zoom=ZOOM,
            output_filename="cleanup",
            attachment_root=str(tmp_path),
            delete_tiles=True,
        )

    assert result["tiles_deleted"] is True
    assert Path(result["output_path"]).is_file()
    assert not (tmp_path / "cleanup" / "tiles").exists()
    assert not (tmp_path / "cleanup").exists()


def test_blank_tile_cached_as_empty_marker(tmp_path, mocked_responses):
    tile = _tiles()[0]
    _register_tile(mocked_responses, tile, status=204, body=b"")

    result = main(
        mapbox=_creds(),
        bbox=BBOX,
        tileset=TILESET,
        zoom=ZOOM,
        output_filename="blank",
        attachment_root=str(tmp_path),
        delete_tiles=False,
    )

    assert result["tiles_downloaded"] == 1
    assert result["feature_count"] == 0
    cache_path = tmp_path / "blank" / "tiles" / str(tile.z) / str(tile.x) / f"{tile.y}.pbf"
    assert cache_path.is_file()
    assert cache_path.stat().st_size == 0


def test_missing_tile_404_cached_as_empty_marker(tmp_path, mocked_responses):
    tile = _tiles()[0]
    _register_tile(mocked_responses, tile, status=404, body=b"Tile not found")

    result = main(
        mapbox=_creds(),
        bbox=BBOX,
        tileset=TILESET,
        zoom=ZOOM,
        output_filename="missing",
        attachment_root=str(tmp_path),
        delete_tiles=False,
    )

    assert result["tiles_downloaded"] == 1
    assert result["tiles_failed"] == 0
    assert result["feature_count"] == 0
    cache_path = (
        tmp_path / "missing" / "tiles" / str(tile.z) / str(tile.x) / f"{tile.y}.pbf"
    )
    assert cache_path.is_file()
    assert cache_path.stat().st_size == 0


def test_cache_skip_does_not_re_request(tmp_path, mocked_responses):
    tile = _tiles()[0]
    cache_path = (
        tmp_path / "cached" / "tiles" / str(tile.z) / str(tile.x) / f"{tile.y}.pbf"
    )
    cache_path.parent.mkdir(parents=True)
    cache_path.write_bytes(b"already-cached")

    with patch(
        "f.connectors.mapbox.mapbox_download_vector_tiles.vt_bytes_to_geojson",
        return_value={
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                }
            ],
        },
    ):
        result = main(
            mapbox=_creds(),
            bbox=BBOX,
            tileset=TILESET,
            zoom=ZOOM,
            output_filename="cached",
            attachment_root=str(tmp_path),
            delete_tiles=False,
        )

    assert result["tiles_downloaded"] == 0
    assert result["tiles_cached_skipped"] == 1
    assert result["to_fetch_count"] == 0
    assert result["tiles_deleted"] is False
    assert len(mocked_responses.calls) == 0
    assert cache_path.is_file()


def test_tile_failure_raises(tmp_path, mocked_responses):
    tile = _tiles()[0]
    _register_tile(mocked_responses, tile, status=500, body=b"error")

    with pytest.raises(RuntimeError, match="Failed to download 1"):
        main(
            mapbox=_creds(),
            bbox=BBOX,
            tileset=TILESET,
            zoom=ZOOM,
            output_filename="fail",
            attachment_root=str(tmp_path),
        )

    cache_path = tmp_path / "fail" / "tiles" / str(tile.z) / str(tile.x) / f"{tile.y}.pbf"
    assert not cache_path.exists()


def test_reconstruct_features_merges_by_column():
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[0, 0], [1, 0]],
            },
            "properties": {"id": "road-1", "name": "Main"},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[1, 0], [2, 0]],
            },
            "properties": {"id": "road-1", "name": "Main"},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [5, 5],
            },
            "properties": {"name": "no-id"},
        },
    ]

    reconstructed = _reconstruct_features(features, "id")
    assert len(reconstructed) == 2
    by_id = [
        f for f in reconstructed if (f.get("properties") or {}).get("id") == "road-1"
    ]
    assert len(by_id) == 1
    assert by_id[0]["geometry"]["type"] in {"LineString", "MultiLineString"}
    passthrough = [
        f for f in reconstructed if "id" not in (f.get("properties") or {})
    ]
    assert len(passthrough) == 1
