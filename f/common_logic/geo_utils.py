import json
import logging
import math
import re
import tempfile
from pathlib import Path

from pyproj import Geod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
_WGS84_GEOD = Geod(ellps="WGS84")
_LON_LAT_RE = re.compile(r"^-?[0-9]+(?:\.[0-9]+)?\s*,\s*-?[0-9]+(?:\.[0-9]+)?$")
_BBOX_RANGES = {
    "swlat": (-90, 90),
    "nelat": (-90, 90),
    "swlng": (-180, 180),
    "nelng": (-180, 180),
}


def is_valid_longitude_latitude(longitude: float, latitude: float) -> bool:
    """Return whether longitude and latitude are finite WGS84 coordinates."""
    return (
        math.isfinite(longitude)
        and math.isfinite(latitude)
        and -180 <= longitude <= 180
        and -90 <= latitude <= 90
    )


def parse_lon_lat(lon_lat: str | None) -> tuple[float, float]:
    """Parse ``"lon, lat"`` and reject values outside geographic bounds.

    Parameters
    ----------
    lon_lat : str or None
        Longitude and latitude separated by a comma, for example
        ``"-74.5263, 41.2833"``.

    Returns
    -------
    tuple of float
        ``(longitude, latitude)``.

    Raises
    ------
    ValueError
        If the value is not two numbers, longitude is outside -180 to 180,
        or latitude is outside -90 to 90.
    """
    text = lon_lat.strip() if isinstance(lon_lat, str) else ""
    if not _LON_LAT_RE.fullmatch(text):
        raise ValueError('lon_lat must be "lon, lat" with numeric values only.')
    lon_text, lat_text = (part.strip() for part in text.split(",", 1))
    longitude = float(lon_text)
    latitude = float(lat_text)
    if not is_valid_longitude_latitude(longitude, latitude):
        if not -180 <= longitude <= 180:
            raise ValueError("longitude must be between -180 and 180.")
        raise ValueError("latitude must be between -90 and 90.")
    return longitude, latitude


def geojson_point(longitude: float | None, latitude: float | None) -> dict | None:
    """Return a GeoJSON Point, or None when either coordinate is missing.

    Parameters
    ----------
    longitude : float or None
        WGS84 longitude. Stored first in GeoJSON coordinate order.
    latitude : float or None
        WGS84 latitude.

    Returns
    -------
    dict or None
        ``{"type": "Point", "coordinates": [longitude, latitude]}``, or None
        when either value is None. ``0`` is a real coordinate and is kept.
    """
    if longitude is None or latitude is None:
        return None
    return {"type": "Point", "coordinates": [longitude, latitude]}


def parse_bounding_box(bounding_box: str | list | None) -> dict[str, float] | None:
    """Parse ``[[west, south], [east, north]]`` into southwest and northeast corners.

    Parameters
    ----------
    bounding_box : str, list, or None
        Two ``[longitude, latitude]`` corners, or a JSON string of that shape.
        ``None``, ``""``, and ``[]`` mean no bounding box.

    Returns
    -------
    dict or None
        ``swlng``, ``swlat``, ``nelng``, and ``nelat``. Corners are ordered
        with ``min`` and ``max``, so either corner may be given first.
        ``None`` when no bounding box was given.

    Raises
    ------
    ValueError
        If the value is not two numeric corners, a coordinate is outside
        WGS84 bounds, or the two corners do not span both longitude and
        latitude.
    """
    if bounding_box is None or bounding_box == "" or bounding_box == []:
        return None
    if isinstance(bounding_box, str):
        bounding_box = bounding_box.strip()
        if not bounding_box:
            return None
        try:
            bounding_box = json.loads(bounding_box)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "bounding_box must be a JSON list: [[west, south], [east, north]]."
            ) from exc
    if not isinstance(bounding_box, (list, tuple)):
        raise ValueError("bounding_box must be [[west, south], [east, north]].")
    if len(bounding_box) == 0:
        return None
    if len(bounding_box) != 2:
        raise ValueError(
            "bounding_box must contain exactly two [longitude, latitude] corners."
        )

    corners: list[tuple[float, float]] = []
    for point in bounding_box:
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            raise ValueError(
                "bounding_box must contain exactly two [longitude, latitude] corners."
            )
        try:
            corners.append((float(point[0]), float(point[1])))
        except (TypeError, ValueError) as exc:
            raise ValueError("bounding_box coordinates must be numeric.") from exc

    (lng_a, lat_a), (lng_b, lat_b) = corners
    parsed = {
        "swlng": min(lng_a, lng_b),
        "swlat": min(lat_a, lat_b),
        "nelng": max(lng_a, lng_b),
        "nelat": max(lat_a, lat_b),
    }
    for key, (low, high) in _BBOX_RANGES.items():
        if not low <= parsed[key] <= high:
            raise ValueError(f"bounding_box {key} must be between {low} and {high}.")
    if parsed["swlat"] >= parsed["nelat"]:
        raise ValueError("bounding_box swlat must be less than nelat.")
    if parsed["swlng"] >= parsed["nelng"]:
        raise ValueError("bounding_box swlng must be less than nelng.")
    return parsed


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
