import json

from shapely.geometry import mapping as shapely_mapping
from shapely.geometry import shape as shapely_shape


def infer_geometry_type(coordinates) -> str:
    """
    Infer GeoJSON geometry type from coordinate nesting depth.

    Depth 0 ([lon, lat])           → Point
    Depth 1 ([[lon, lat], ...])    → LineString
    Depth 2 ([[[lon, lat], ...]])  → Polygon
    Depth 3 ([[[[lon, lat], ...]]]) → MultiPolygon

    Note: MultiPoint and MultiLineString share nesting depth with LineString
    and Polygon respectively. We default to the more common single-geometry
    types. If disambiguation is needed in the future, an explicit geometry
    type column can be added.
    """

    DEPTH_TO_GEOM_TYPE = {
        0: "Point",
        1: "LineString",
        2: "Polygon",
        3: "MultiPolygon",
    }

    depth = 0
    level = coordinates
    while (
        isinstance(level, (list, tuple))
        and level
        and not isinstance(level[0], (int, float))
    ):
        depth += 1
        level = level[0]
    geom_type = DEPTH_TO_GEOM_TYPE.get(depth)
    if geom_type is None:
        raise ValueError(
            f"Cannot infer geometry type: unsupported nesting depth {depth}"
        )
    return geom_type


def coords_to_geojson_geometry(raw: str) -> dict:
    """
    Parse a JSON coordinate string into a validated GeoJSON geometry dict.

    Accepts coordinate arrays at any supported nesting depth (Point through
    MultiPolygon), infers the geometry type, validates with Shapely, and
    returns the normalized GeoJSON geometry.

    Parameters
    ----------
    raw : str
        JSON string of coordinates, e.g. '[-74.0, 40.7]' or
        '[[[-74, 40], [-73, 40], [-73, 41], [-74, 40]]]'.

    Returns
    -------
    dict
        GeoJSON geometry dict with 'type' and 'coordinates' keys.

    Raises
    ------
    ValueError
        If JSON is invalid, geometry type cannot be inferred, or
        Shapely rejects the geometry.
    """
    try:
        coordinates = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError(f"Coordinate value is not valid JSON: {raw!r}")

    geom_type = infer_geometry_type(coordinates)

    try:
        geom = shapely_shape({"type": geom_type, "coordinates": coordinates})
    except Exception as e:
        raise ValueError(f"Invalid {geom_type} geometry — {e}")

    if not geom.is_valid:
        raise ValueError(f"Invalid {geom_type} geometry — {geom.is_valid}")

    return dict(shapely_mapping(geom))
