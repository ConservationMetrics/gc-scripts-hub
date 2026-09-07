import json
import logging
from pathlib import Path

import pytest

from f.connectors.gazettes.gazettes_filter_by_boundary import main, load_boundary
from f.connectors.gazettes.gazettes_ggmc_parse import text_to_featurecollection
from f.connectors.gazettes.tests.assets.site_data import (
    ASSETS,
    DOC_URL,
    read_asset,
)

logger = logging.getLogger(__name__)


def _feature(ring, props=None):
    geometry = {"type": "Polygon", "coordinates": [ring]} if ring else None
    return {
        "type": "Feature",
        "id": "test-feature",
        "properties": props or {},
        "geometry": geometry,
    }


# covers boundary: lon -60.2..-59.8, lat 6.3..6.9
# fully inside
POLY_INSIDE = [[-60.0, 6.5], [-59.95, 6.5], [-59.95, 6.6], [-60.0, 6.6], [-60.0, 6.5]]
# straddles the west edge of the covers boundary (intersects, not within, centroid inside)
POLY_STRADDLE = [[-60.3, 6.5], [-59.9, 6.5], [-59.9, 6.6], [-60.3, 6.6], [-60.3, 6.5]]
# fully outside
POLY_OUTSIDE = [[-58.6, 3.0], [-58.4, 3.0], [-58.4, 3.2], [-58.6, 3.2], [-58.6, 3.0]]


def _write_fc(tmp_path, features, name="input.geojson"):
    fc = {"type": "FeatureCollection", "features": features}
    path = tmp_path / name
    path.write_text(json.dumps(fc))
    return path


def test_filter_intersects(tmp_path):
    fc_path = _write_fc(
        tmp_path,
        [
            _feature(POLY_INSIDE),
            _feature(POLY_STRADDLE),
            _feature(POLY_OUTSIDE),
        ],
    )
    result = main(
        str(fc_path), str(ASSETS / "boundary_covers.geojson"), keep="intersects"
    )
    assert result["kept"] == 2
    assert result["dropped"] == 1
    out = json.loads(Path(result["geojson_path"]).read_text())
    assert len(out["features"]) == 2
    assert result["geojson_path"].endswith("input__filtered.geojson")


def test_filter_within_is_stricter(tmp_path):
    fc_path = _write_fc(
        tmp_path,
        [_feature(POLY_INSIDE), _feature(POLY_STRADDLE), _feature(POLY_OUTSIDE)],
    )
    result = main(str(fc_path), str(ASSETS / "boundary_covers.geojson"), keep="within")
    assert result["kept"] == 1
    assert result["dropped"] == 2


def test_filter_centroid(tmp_path):
    # polygon huge but centroid inside covers boundary
    centroid_ring = [[-61.0, 5.0], [-59.0, 5.0], [-59.0, 8.0], [-61.0, 8.0], [-61.0, 5.0]]
    fc_path = _write_fc(
        tmp_path,
        [_feature(centroid_ring), _feature(POLY_OUTSIDE)],
    )
    result = main(
        str(fc_path), str(ASSETS / "boundary_covers.geojson"), keep="centroid"
    )
    assert result["kept"] == 1
    assert result["dropped"] == 1


def test_filter_null_geometry_toggle(tmp_path):
    fc_path = _write_fc(
        tmp_path,
        [_feature(POLY_INSIDE), _feature(None)],
    )
    kept_null = main(
        str(fc_path), str(ASSETS / "boundary_covers.geojson"), keep_null_geometry=True
    )
    assert kept_null["kept"] == 2
    assert kept_null["null_geometry"] == 1

    dropped_null = main(
        str(fc_path),
        str(ASSETS / "boundary_covers.geojson"),
        keep_null_geometry=False,
        output_path=str(fc_path.parent / "out.geojson"),
    )
    assert dropped_null["kept"] == 1
    assert dropped_null["dropped"] == 1
    assert Path(dropped_null["geojson_path"]).name == "out.geojson"


def test_filter_rejects_invalid_keep(tmp_path):
    fc_path = _write_fc(tmp_path, [_feature(POLY_INSIDE)])
    with pytest.raises(ValueError):
        main(str(fc_path), str(ASSETS / "boundary_covers.geojson"), keep="nope")


def test_load_boundary_multi_feature_union(tmp_path):
    geom = load_boundary(str(ASSETS / "boundary_multipolygon.geojson"))
    from shapely.geometry import shape

    inside_a = shape({"type": "Point", "coordinates": [-60.0, 6.4]})
    inside_b = shape({"type": "Point", "coordinates": [-60.0, 6.8]})
    between = shape({"type": "Point", "coordinates": [-60.0, 6.6]})
    assert geom.contains(inside_a)
    assert geom.contains(inside_b)
    assert not geom.contains(between)


def test_load_boundary_bare_geometry(tmp_path):
    bare = {"type": "Polygon", "coordinates": [POLY_INSIDE]}
    path = tmp_path / "bare.geojson"
    path.write_text(json.dumps(bare))
    geom = load_boundary(str(path))
    assert geom.area > 0


def test_filter_end_to_end_on_parsed_gazette(tmp_path):
    """Parsed clean-gazette features survive the covers boundary, drop with far one."""
    parsed = text_to_featurecollection(read_asset("mines_notice_clean.txt"), DOC_URL)
    fc_path = _write_fc(tmp_path, parsed["features"])

    near = main(str(fc_path), str(ASSETS / "boundary_covers.geojson"))
    assert near["kept"] == 2  # both notices sit inside the AOI

    far = main(
        str(fc_path),
        str(ASSETS / "boundary_far.geojson"),
        output_path=str(tmp_path / "far.geojson"),
    )
    assert far["kept"] == 0
    assert far["dropped"] == 2
