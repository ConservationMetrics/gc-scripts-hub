# requirements:
# shapely

import json
import logging
from pathlib import Path

from shapely import prepare
from shapely.geometry import shape
from shapely.ops import unary_union

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VALID_KEEP_MODES = ("intersects", "within", "centroid")


def main(
    geojson_path: str,
    boundary_path: str,
    keep: str = "intersects",
    keep_null_geometry: bool = True,
    output_path: str = None,
):
    """Filter a GeoJSON FeatureCollection down to features within a boundary.

    Loads a boundary geometry from a local GeoJSON file (Feature,
    FeatureCollection, or bare geometry; multiple features are unioned) and
    keeps only features whose geometry passes the chosen spatial predicate.
    Detections outside the area of interest are dropped before they reach the
    database, explorer, or notifications.

    Parameters
    ----------
    geojson_path : str
        Path to the input FeatureCollection file.
    boundary_path : str
        Path to a GeoJSON file defining the area of interest boundary.
    keep : str, optional
        Spatial predicate: 'intersects' (any overlap, default), 'within'
        (fully inside), or 'centroid' (centroid inside).
    keep_null_geometry : bool, optional
        Whether features without geometry (non-spatial notices) pass through.
        Default: True.
    output_path : str, optional
        Where to write the filtered FeatureCollection. Default: input path
        with an `__filtered` suffix.

    Returns
    -------
    dict
        {"geojson_path": str, "kept": int, "dropped": int, "null_geometry": int}
    """
    keep = keep or "intersects"
    if keep_null_geometry is None:
        keep_null_geometry = True
    if keep not in VALID_KEEP_MODES:
        raise ValueError(f"keep must be one of {VALID_KEEP_MODES}, got '{keep}'.")

    with open(geojson_path, "r") as f:
        fc = json.load(f)
    boundary = load_boundary(boundary_path)

    kept, dropped, null_count = [], 0, 0
    prepare(boundary)

    for feature in fc.get("features", []):
        geometry = feature.get("geometry")
        if geometry is None or (
            isinstance(geometry, dict) and geometry.get("type") is None
        ):
            null_count += 1
            if keep_null_geometry:
                kept.append(feature)
            else:
                dropped += 1
            continue
        geom = shape(geometry)
        if _passes(geom, boundary, keep):
            kept.append(feature)
        else:
            dropped += 1

    fc["features"] = kept
    out = Path(output_path) if output_path else Path(geojson_path).with_name(
        Path(geojson_path).stem + "__filtered.geojson"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(fc, f)

    logger.info(
        f"Boundary filter ({keep}): kept {len(kept)}, dropped {dropped} "
        f"({null_count} without geometry) -> {out}"
    )
    return {
        "geojson_path": str(out),
        "kept": len(kept),
        "dropped": dropped,
        "null_geometry": null_count,
    }


def load_boundary(boundary_path: str):
    """Load a shapely geometry from a GeoJSON file of any top-level type.

    Parameters
    ----------
    boundary_path : str
        Path to the boundary GeoJSON file.

    Returns
    -------
    shapely.geometry.base.BaseGeometry
        The (possibly unioned) boundary geometry.
    """
    with open(boundary_path, "r") as f:
        data = json.load(f)

    geoms = []
    if data.get("type") == "FeatureCollection":
        for feature in data.get("features", []):
            if feature.get("geometry"):
                geoms.append(shape(feature["geometry"]))
    elif data.get("type") == "Feature":
        if data.get("geometry"):
            geoms.append(shape(data["geometry"]))
    else:
        geoms.append(shape(data))

    if not geoms:
        raise ValueError(f"No geometries found in boundary file {boundary_path}.")
    if len(geoms) == 1:
        return geoms[0]
    return unary_union(geoms)


def _passes(geom, boundary, keep: str) -> bool:
    """Apply the chosen spatial predicate between a feature and the boundary.

    Parameters
    ----------
    geom : shapely.geometry.base.BaseGeometry
        Feature geometry.
    boundary : shapely.geometry.base.BaseGeometry
        Boundary geometry (may be prepared for repeated predicates).
    keep : str
        One of 'intersects', 'within', 'centroid'.

    Returns
    -------
    bool
        True when the feature should be kept.
    """
    if keep == "intersects":
        return boundary.intersects(geom)
    if keep == "within":
        return geom.within(boundary)
    # centroid
    return boundary.contains(geom.centroid)
