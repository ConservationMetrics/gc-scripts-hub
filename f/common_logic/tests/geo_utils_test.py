import json

import pytest

from f.common_logic.geo_utils import (
    coords_to_geojson_geometry,
    geojson_to_line_delimited,
    infer_geometry_type,
)

# --- infer_geometry_type ---


def test_infer_point():
    assert infer_geometry_type([-59.0, 5.0]) == "Point"
    assert infer_geometry_type([-59, 5]) == "Point"


def test_infer_point_with_elevation():
    assert infer_geometry_type([-59.0, 5.0, 100.0]) == "Point"


def test_infer_linestring():
    assert infer_geometry_type([[-59.0, 5.0], [-58.0, 6.0]]) == "LineString"


def test_infer_polygon():
    coords = [[[-59, 5], [-58, 5], [-58, 6], [-59, 5]]]
    assert infer_geometry_type(coords) == "Polygon"


def test_infer_multipolygon():
    coords = [[[[-59, 5], [-58, 5], [-58, 6], [-59, 5]]]]
    assert infer_geometry_type(coords) == "MultiPolygon"


def test_unsupported_nesting_depth():
    coords = [[[[[-59, 5], [-58, 5]]]]]
    with pytest.raises(ValueError, match="unsupported nesting depth"):
        infer_geometry_type(coords)


def test_empty_list():
    """Empty list has no numeric first element, but depth stays 0."""
    assert infer_geometry_type([]) == "Point"


def test_tuple_coordinates():
    assert infer_geometry_type((-59.0, 5.0)) == "Point"
    assert infer_geometry_type(((-59.0, 5.0), (-58.0, 6.0))) == "LineString"


# --- coords_to_geojson_geometry ---


def test_coords_to_geojson_point():
    result = coords_to_geojson_geometry("[-59.0, 5.0]")
    assert result["type"] == "Point"
    assert result["coordinates"] == (-59.0, 5.0)


def test_coords_to_geojson_linestring():
    result = coords_to_geojson_geometry("[[-59.0, 5.0], [-58.0, 6.0]]")
    assert result["type"] == "LineString"
    assert len(result["coordinates"]) == 2


def test_coords_to_geojson_polygon():
    raw = "[[[-59, 5], [-58, 5], [-58, 6], [-59, 6], [-59, 5]]]"
    result = coords_to_geojson_geometry(raw)
    assert result["type"] == "Polygon"
    assert len(result["coordinates"]) == 1  # one ring


def test_coords_to_geojson_multipolygon():
    raw = (
        "[[[[-59, 5], [-58, 5], [-58, 6], [-59, 6], [-59, 5]]],"
        " [[[-70, 30], [-69, 30], [-69, 31], [-70, 31], [-70, 30]]]]"
    )
    result = coords_to_geojson_geometry(raw)
    assert result["type"] == "MultiPolygon"
    assert len(result["coordinates"]) == 2


def test_coords_to_geojson_invalid_json():
    with pytest.raises(ValueError, match="not valid JSON"):
        coords_to_geojson_geometry("not json")


def test_coords_to_geojson_invalid_geometry():
    # Self-intersecting polygon (bowtie)
    raw = "[[[-1, -1], [1, 1], [1, -1], [-1, 1], [-1, -1]]]"
    with pytest.raises(ValueError, match="Invalid Polygon geometry"):
        coords_to_geojson_geometry(raw)


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
