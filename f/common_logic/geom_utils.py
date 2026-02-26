# requirements:
# shapely


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

    _DEPTH_TO_GEOM_TYPE = {
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
    geom_type = _DEPTH_TO_GEOM_TYPE.get(depth)
    if geom_type is None:
        raise ValueError(
            f"Cannot infer geometry type: unsupported nesting depth {depth}"
        )
    return geom_type

