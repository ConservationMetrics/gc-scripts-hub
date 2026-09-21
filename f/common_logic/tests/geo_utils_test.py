import json

from f.common_logic.geo_utils import (
    bounding_box_to_wkt,
    geojson_to_line_delimited,
    is_valid_longitude_latitude,
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

    ld_path = geojson_to_line_delimited(source)

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

    ld_path = geojson_to_line_delimited(source)

    lines = ld_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == feature


def test_bounding_box_to_wkt_accepts_list_and_json():
    expected = "POLYGON((-55.03 3.23,-54.12 3.23,-54.12 3.67,-55.03 3.67,-55.03 3.23))"
    assert bounding_box_to_wkt([[-55.03, 3.23], [-54.12, 3.67]]) == expected
    assert bounding_box_to_wkt(json.dumps([[-55.03, 3.23], [-54.12, 3.67]])) == expected


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
            bounding_box_to_wkt(bounds)
        except ValueError:
            continue
        raise AssertionError(f"Expected invalid bounds to fail: {bounds}")


def test_bounding_box_to_wkt_area_limit_uses_geodesic_area():
    assert bounding_box_to_wkt([[0, 0], [0.89, 0.89]], max_area_km2=10_000)
    try:
        bounding_box_to_wkt([[0, 0], [1, 1]], max_area_km2=10_000)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected area above 10,000 km2 to fail")
    bounding_box_to_wkt([[0, 70], [1, 71]], max_area_km2=10_000)
    assert bounding_box_to_wkt([[0, 0], [1, 1]], max_area_km2=12_350)


def test_bounding_box_to_wkt_preserves_close_coordinate_precision():
    wkt = bounding_box_to_wkt([[-122.12346, 45.0], [-122.12344, 45.00002]])
    assert "-122.12346 45.0,-122.12344 45.0" in wkt
    assert "-122.12344 45.00002,-122.12346 45.00002" in wkt


def test_is_valid_longitude_latitude_rejects_non_finite_and_out_of_range_values():
    assert is_valid_longitude_latitude(-180, -90)
    assert is_valid_longitude_latitude(180, 90)
    assert not is_valid_longitude_latitude(float("nan"), 0)
    assert not is_valid_longitude_latitude(0, float("inf"))
    assert not is_valid_longitude_latitude(-180.1, 0)
    assert not is_valid_longitude_latitude(0, 90.1)
