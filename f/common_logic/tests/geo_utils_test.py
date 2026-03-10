import json

from f.common_logic.geo_utils import (
    geojson_to_line_delimited,
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
