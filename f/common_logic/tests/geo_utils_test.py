import json

import pytest

from f.common_logic.geo_utils import (
    bounding_box_to_wkt,
    geojson_point,
    geojson_to_line_delimited,
    is_valid_longitude_latitude,
    parse_lon_lat,
)

# --- geojson_to_line_delimited ---


def test_geojson_to_line_delimited_feature_collection(tmp_path):
    source = tmp_path / "features.geojson"
    feature_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-59.0, 5.0]},
                "properties": {"id": 1},
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-58.0, 6.0]},
                "properties": {"id": 2},
            },
        ],
    }
    source.write_text(json.dumps(feature_collection), encoding="utf-8")

    ld_path = geojson_to_line_delimited(source_path=source)

    assert ld_path.is_file()
    assert str(ld_path).endswith(".geojson.ld")

    lines = ld_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2

    loaded_features = [json.loads(line) for line in lines]
    assert loaded_features == feature_collection["features"]


def test_geojson_to_line_delimited_single_object(tmp_path):
    source = tmp_path / "single.geojson"
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-59.0, 5.0]},
        "properties": {"name": "test"},
    }
    source.write_text(json.dumps(feature), encoding="utf-8")

    ld_path = geojson_to_line_delimited(source_path=source)

    lines = ld_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == feature


def test_bounding_box_to_wkt_accepts_list_and_json():
    expected = "POLYGON((-55.03 3.23,-54.12 3.23,-54.12 3.67,-55.03 3.67,-55.03 3.23))"
    assert bounding_box_to_wkt(bounding_box=[[-55.03, 3.23], [-54.12, 3.67]]) == expected
    assert bounding_box_to_wkt(bounding_box=json.dumps([[-55.03, 3.23], [-54.12, 3.67]])) == expected


def test_bounding_box_to_wkt_rejects_invalid_bounds():
    invalid = [
        "not json",
        [[0, 0]],
        [[0, 0], [0, 1]],
        [[1, 0], [0, 1]],
        [[0, 0], [1, 0]],
        [[0, 0], [1, 91]],
        [[True, 0], [1, 1]],
        [[float("nan"), 0], [1, 1]],
        [[170, 0], [-170, 1]],
    ]
    for bounds in invalid:
        try:
            bounding_box_to_wkt(bounding_box=bounds)
        except ValueError:
            continue
        raise AssertionError(f"Expected invalid bounds to fail: {bounds}")


def test_bounding_box_to_wkt_area_limit_uses_geodesic_area():
    assert bounding_box_to_wkt(bounding_box=[[0, 0], [0.89, 0.89]], max_area_km2=10_000)
    try:
        bounding_box_to_wkt(bounding_box=[[0, 0], [1, 1]], max_area_km2=10_000)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected area above 10,000 km2 to fail")
    bounding_box_to_wkt(bounding_box=[[0, 70], [1, 71]], max_area_km2=10_000)
    assert bounding_box_to_wkt(bounding_box=[[0, 0], [1, 1]], max_area_km2=12_350)


def test_bounding_box_to_wkt_preserves_close_coordinate_precision():
    wkt = bounding_box_to_wkt(bounding_box=[[-122.12346, 45.0], [-122.12344, 45.00002]])
    assert "-122.12346 45.0,-122.12344 45.0" in wkt
    assert "-122.12344 45.00002,-122.12346 45.00002" in wkt


def test_is_valid_longitude_latitude_rejects_non_finite_and_out_of_range_values():
    assert is_valid_longitude_latitude(longitude=-180, latitude=-90)
    assert is_valid_longitude_latitude(longitude=180, latitude=90)
    assert not is_valid_longitude_latitude(longitude=float("nan"), latitude=0)
    assert not is_valid_longitude_latitude(longitude=0, latitude=float("inf"))
    assert not is_valid_longitude_latitude(longitude=-180.1, latitude=0)
    assert not is_valid_longitude_latitude(longitude=0, latitude=90.1)


@pytest.mark.parametrize(
    "lon_lat, match",
    [
        ("181, 0", "longitude must be between -180 and 180"),
        ("-180.0001, 0", "longitude must be between -180 and 180"),
        ("0, 90.1", "latitude must be between -90 and 90"),
        ("0, -91", "latitude must be between -90 and 90"),
        ("79.4551", 'lon_lat must be "lon, lat"'),
        ("79.4551, 43.5, 1", 'lon_lat must be "lon, lat"'),
        ("west, 43.5", 'lon_lat must be "lon, lat"'),
        ("", 'lon_lat must be "lon, lat"'),
        (None, 'lon_lat must be "lon, lat"'),
    ],
)
def test_invalid_lon_lat(lon_lat, match):
    with pytest.raises(ValueError, match=match):
        parse_lon_lat(lon_lat)


@pytest.mark.parametrize(
    "lon_lat, expected",
    [
        ("-180, -90", (-180.0, -90.0)),
        ("180, 90", (180.0, 90.0)),
        ("79.455132,43.530936", (79.455132, 43.530936)),
        ("  -74.5262526, 41.2833266  ", (-74.5262526, 41.2833266)),
    ],
)
def test_parse_lon_lat(lon_lat, expected):
    assert parse_lon_lat(lon_lat) == expected


def test_geojson_point():
    assert geojson_point(longitude=79.455132, latitude=43.530936) == {
        "type": "Point",
        "coordinates": [79.455132, 43.530936],
    }
    assert geojson_point(longitude=0, latitude=0)["coordinates"] == [0, 0]
    assert geojson_point(longitude=None, latitude=1) is None
    assert geojson_point(longitude=1, latitude=None) is None
