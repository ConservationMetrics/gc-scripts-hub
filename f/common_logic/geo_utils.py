import json
import logging
import math
import tempfile
from pathlib import Path

from pyproj import Geod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
_WGS84_GEOD = Geod(ellps="WGS84")


def is_valid_longitude_latitude(longitude: float, latitude: float) -> bool:
    """Return whether longitude and latitude are finite WGS84 coordinates."""
    return (
        math.isfinite(longitude)
        and math.isfinite(latitude)
        and -180 <= longitude <= 180
        and -90 <= latitude <= 90
    )


def bounding_box_to_wkt(
    bounding_box: list | str, max_area_km2: float | None = None
) -> str:
    """Validate two longitude/latitude corners and return a counter-clockwise WKT polygon.

    Parameters
    ----------
    bounding_box : list or str
        ``[[west, south], [east, north]]`` or a JSON string with that shape.
    max_area_km2 : float, optional
        Maximum permitted approximate geographic area in square kilometres.

    Returns
    -------
    str
        A closed, counter-clockwise longitude/latitude WKT polygon.
    """
    if isinstance(bounding_box, str):
        try:
            bounding_box = json.loads(bounding_box)
        except json.JSONDecodeError as exc:
            raise ValueError("bounding_box must be valid JSON.") from exc
    if not isinstance(bounding_box, list) or len(bounding_box) != 2:
        raise ValueError("bounding_box must be [[west, south], [east, north]].")

    corners = []
    for corner in bounding_box:
        if not isinstance(corner, list) or len(corner) != 2:
            raise ValueError(
                "bounding_box must contain two [longitude, latitude] corners."
            )
        if any(
            isinstance(value, bool) or not isinstance(value, (int, float))
            for value in corner
        ):
            raise ValueError("bounding_box coordinates must be finite numeric values.")
        longitude, latitude = (float(value) for value in corner)
        if not is_valid_longitude_latitude(longitude, latitude):
            raise ValueError(
                "bounding_box coordinates must be finite and within WGS84 bounds."
            )
        corners.append((longitude, latitude))

    (west, south), (east, north) = corners
    if west >= east:
        raise ValueError(
            "bounding_box west must be less than east; antimeridian bounds are unsupported."
        )
    if south >= north:
        raise ValueError("bounding_box south must be less than north.")

    area_m2, _ = _WGS84_GEOD.polygon_area_perimeter(
        [west, east, east, west, west],
        [south, south, north, north, south],
    )
    area_km2 = abs(area_m2) / 1_000_000
    if max_area_km2 is not None and area_km2 > max_area_km2:
        raise ValueError(
            f"bounding_box area ({area_km2:.0f} km2) exceeds {max_area_km2:g} km2."
        )

    # This ring is counter-clockwise in longitude/latitude coordinates; GBIF treats
    # clockwise polygons as their complementary area.
    return (
        f"POLYGON(({west} {south},{east} {south},{east} {north},"
        f"{west} {north},{west} {south}))"
    )


def geojson_to_line_delimited(source_path: Path) -> Path:
    """
    Convert a standard GeoJSON file into line-delimited GeoJSON (one feature per line).
    """
    logger.info("Converting GeoJSON file %s to line-delimited format.", source_path)

    with source_path.open(encoding="utf-8") as src:
        data = json.load(src)

    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        features = data.get("features", [])
    else:
        # Fallback: treat the whole object as a single feature/geometry line
        features = [data]

    with tempfile.NamedTemporaryFile(
        "w",
        suffix=".geojson.ld",
        encoding="utf-8",
        delete=False,
    ) as tmp:
        ld_path = Path(tmp.name)
        for feature in features:
            json.dump(feature, tmp, ensure_ascii=False, separators=(",", ":"))
            tmp.write("\n")

    logger.debug(
        "Finished writing %d line-delimited GeoJSON feature(s) to temporary file %s.",
        len(features),
        ld_path,
    )

    return ld_path
