import pytest

from f.common_logic.geom_utils import infer_geometry_type


def test_infer_point():
    assert infer_geometry_type([-59.0, 5.0]) == "Point"


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
    coords = [[[[[-59, 5], [-58, 5]]]]]  # depth 4
    with pytest.raises(ValueError, match="unsupported nesting depth"):
        infer_geometry_type(coords)


def test_empty_list():
    """Empty list has no numeric first element, but depth stays 0."""
    assert infer_geometry_type([]) == "Point"


def test_integer_coordinates():
    assert infer_geometry_type([-59, 5]) == "Point"


def test_tuple_coordinates():
    assert infer_geometry_type((-59.0, 5.0)) == "Point"
    assert infer_geometry_type(((-59.0, 5.0), (-58.0, 6.0))) == "LineString"

