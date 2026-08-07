# requirements:
# requests~=2.32
# mercantile
# vt2geojson
# shapely

import logging
import shutil
from pathlib import Path
from typing import TypedDict

import mercantile
import requests
from vt2geojson.tools import vt_bytes_to_geojson

from f.common_logic.file_operations import save_data_to_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_S = 30


# https://hub.windmill.dev/resource_types/340/mapbox_credentials
class mapbox(TypedDict):
    username: str
    access_token: str


def main(
    mapbox: mapbox,
    bbox: str,
    tileset: str,
    zoom: str = "12",
    reconstruct_column: str = "",
    output_filename: str = "my_filename",
    attachment_root: str = "/persistent-storage/datalake/mapbox_vector_tiles",
    delete_tiles: bool = True,
    dry_run: bool = False,
) -> dict:
    """Download Mapbox vector tiles and convert them to a GeoJSON file.

    Parameters
    ----------
    mapbox : mapbox
        Mapbox credentials resource. Only ``access_token`` is used (public or
        secret token). ``username`` is ignored.
    bbox : str
        Bounding box as ``minlon,minlat,maxlon,maxlat`` in WGS84 degrees.
    tileset : str
        Mapbox tileset id (e.g. ``mapbox.mapbox-streets-v8``).
    zoom : str, optional
        Zoom level for the tile grid as a string ``"0"``-``"14"`` (default
        ``"12"``). Validated by the Windmill script schema pattern.
    reconstruct_column : str, optional
        Feature property used to reconstruct features clipped across tile
        boundaries. Empty string disables reconstruction.
    output_filename : str, optional
        Base name for the GeoJSON file and tile cache directory.
    attachment_root : str, optional
        Datalake directory for output.
    delete_tiles : bool, optional
        If True (default), delete the tile cache after a successful GeoJSON
        write so only the GeoJSON remains.
    dry_run : bool, optional
        If True, return the download plan without fetching or processing tiles.

    Returns
    -------
    dict
        Dry-run plan or full-run result with download and feature counts.
    """
    access_token = mapbox["access_token"]
    if not access_token:
        raise ValueError("mapbox.access_token is required")
    if not tileset.strip():
        raise ValueError("tileset is required")

    zoom_level = _parse_zoom(zoom)
    parsed_bbox = _parse_bbox(bbox)

    tiles_dir = Path(attachment_root) / output_filename / "tiles"
    tiles = list(mercantile.tiles(*parsed_bbox, zooms=[zoom_level]))
    to_fetch = [t for t in tiles if not _tile_path(tiles_dir, t).is_file()]
    cached_count = len(tiles) - len(to_fetch)

    plan = {
        "dry_run": dry_run,
        "zoom": zoom_level,
        "bbox": parsed_bbox,
        "tileset": tileset,
        "tile_count": len(tiles),
        "cached_count": cached_count,
        "to_fetch_count": len(to_fetch),
    }
    logger.info(
        "Bounding box at zoom %s: %s tiles (%s cached, %s to download).",
        zoom_level,
        len(tiles),
        cached_count,
        len(to_fetch),
    )

    if dry_run:
        return plan

    downloaded, failed = _download_tiles(
        tileset=tileset,
        access_token=access_token,
        tiles_dir=tiles_dir,
        to_fetch=to_fetch,
    )
    if failed:
        raise RuntimeError(
            f"Failed to download {failed} of {len(to_fetch)} tiles. "
            "Re-run to retry failed tiles; cached tiles are skipped."
        )

    features = _process_tiles(tiles_dir, zoom_level, reconstruct_column or None)
    collection = {"type": "FeatureCollection", "features": features}
    save_data_to_file(
        collection,
        output_filename,
        attachment_root,
        file_type="geojson",
    )
    output_path = str(Path(attachment_root).resolve() / f"{output_filename}.geojson")

    tiles_deleted = False
    if delete_tiles:
        logger.info(
            "Deleting tile cache at %s after successful GeoJSON write.",
            tiles_dir,
        )
        shutil.rmtree(tiles_dir)
        parent = tiles_dir.parent
        if parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
        tiles_deleted = True
    else:
        logger.info(
            "Keeping tile cache at %s on the datalake.",
            tiles_dir,
        )

    return {
        **plan,
        "tiles_downloaded": downloaded,
        "tiles_failed": failed,
        "tiles_cached_skipped": cached_count,
        "feature_count": len(features),
        "output_path": output_path,
        "tiles_deleted": tiles_deleted,
    }


def _parse_zoom(raw: str | int) -> int:
    """Parse and validate a zoom level in the range 0-14.

    Parameters
    ----------
    raw : str or int
        Zoom level from the Windmill form (string) or tests (int).

    Returns
    -------
    int
        Zoom level between 0 and 14 inclusive.

    Raises
    ------
    ValueError
        If the value is not an integer in 0-14.
    """
    try:
        zoom = int(raw)
    except (TypeError, ValueError) as e:
        raise ValueError(f"zoom must be an integer 0-14, got {raw!r}") from e
    if not 0 <= zoom <= 14:
        raise ValueError(f"zoom must be between 0 and 14, got {zoom}")
    return zoom


def _parse_bbox(raw: str) -> tuple[float, float, float, float]:
    """Parse a ``minlon,minlat,maxlon,maxlat`` string into four floats.

    Parameters
    ----------
    raw : str
        Comma-separated bounding box string.

    Returns
    -------
    tuple of float
        ``(minlon, minlat, maxlon, maxlat)``.

    Raises
    ------
    ValueError
        If the string is malformed or the box is invalid.
    """
    try:
        parts = [float(p) for p in raw.split(",")]
    except ValueError as e:
        raise ValueError(
            f"expected four comma-separated numbers, got {raw!r}"
        ) from e
    if len(parts) != 4:
        raise ValueError(
            f"expected 4 values (minlon,minlat,maxlon,maxlat), got {len(parts)}: {raw!r}"
        )
    minlon, minlat, maxlon, maxlat = parts
    if not -180 <= minlon < maxlon <= 180:
        raise ValueError(f"invalid longitude range: {minlon} >= {maxlon}")
    if not -90 <= minlat < maxlat <= 90:
        raise ValueError(f"invalid latitude range: {minlat} >= {maxlat}")
    return minlon, minlat, maxlon, maxlat


def _tile_path(tiles_dir: Path, tile: mercantile.Tile) -> Path:
    """Return the cache path for a tile."""
    return tiles_dir / str(tile.z) / str(tile.x) / f"{tile.y}.pbf"


def _is_missing_tile(response: requests.Response) -> bool:
    """Return True for Mapbox ``404 Tile not found`` (sparse / outside coverage).

    A 404 whose body says the tileset itself does not exist is treated as a real
    error so a bad tileset id fails the run instead of caching empty markers.
    """
    if response.status_code != 404:
        return False
    detail = (response.text or "").lower()
    return "does not exist" not in detail


def _download_tiles(
    tileset: str,
    access_token: str,
    tiles_dir: Path,
    to_fetch: list[mercantile.Tile],
) -> tuple[int, int]:
    """Fetch uncached tiles into ``tiles_dir``.

    Parameters
    ----------
    tileset : str
        Mapbox tileset id.
    access_token : str
        Mapbox access token.
    tiles_dir : Path
        Root of the local tile cache.
    to_fetch : list of mercantile.Tile
        Tiles that are not yet cached.

    Returns
    -------
    tuple of int
        ``(downloaded_count, failed_count)``.
    """
    if not to_fetch:
        logger.info("Nothing to download: every tile is already cached locally.")
        return 0, 0

    downloaded = 0
    failed = 0

    with requests.Session() as session:
        for i, tile in enumerate(to_fetch, 1):
            url = (
                f"https://api.mapbox.com/v4/{tileset}/"
                f"{tile.z}/{tile.x}/{tile.y}.vector.pbf"
            )
            path = _tile_path(tiles_dir, tile)
            try:
                response = session.get(
                    url,
                    params={"access_token": access_token},
                    timeout=_REQUEST_TIMEOUT_S,
                )
                if response.status_code == 200:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(response.content)
                    downloaded += 1
                    logger.info(
                        "[%s/%s] Tile %s/%s: downloaded (%s bytes)",
                        i,
                        len(to_fetch),
                        tile.x,
                        tile.y,
                        len(response.content),
                    )
                elif response.status_code == 204 or _is_missing_tile(response):
                    # 204: tile exists but is empty. 404 "Tile not found": sparse
                    # coverage / outside the tileset extent. Cache an empty marker
                    # so the cell is never re-requested.
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(b"")
                    downloaded += 1
                    logger.info(
                        "[%s/%s] Tile %s/%s: empty (HTTP %s, cached marker)",
                        i,
                        len(to_fetch),
                        tile.x,
                        tile.y,
                        response.status_code,
                    )
                else:
                    failed += 1
                    detail = (response.text or "").strip()
                    logger.error(
                        "[%s/%s] Tile %s/%s: failed with status %s%s",
                        i,
                        len(to_fetch),
                        tile.x,
                        tile.y,
                        response.status_code,
                        f": {detail}" if detail else "",
                    )
            except requests.RequestException as e:
                failed += 1
                logger.error(
                    "[%s/%s] Tile %s/%s: failed: %s",
                    i,
                    len(to_fetch),
                    tile.x,
                    tile.y,
                    e,
                )

    logger.info(
        "Download pass complete: %s downloaded, %s failed.",
        downloaded,
        failed,
    )
    return downloaded, failed


def _process_tiles(
    tiles_dir: Path,
    zoom: int,
    reconstruct_column: str | None,
) -> list[dict]:
    """Convert cached PBF tiles at ``zoom`` into GeoJSON features.

    Parameters
    ----------
    tiles_dir : Path
        Root of the local tile cache.
    zoom : int
        Zoom level directory to read.
    reconstruct_column : str or None
        Optional property used to reconstruct features clipped across tiles.

    Returns
    -------
    list of dict
        GeoJSON Feature dictionaries.

    Raises
    ------
    FileNotFoundError
        If no cached tiles exist for the requested zoom.
    """
    zoom_dir = tiles_dir / str(zoom)
    if not zoom_dir.is_dir():
        raise FileNotFoundError(
            f"No cached tiles found at {zoom_dir}. Download must run first."
        )

    all_features: list[dict] = []
    tiles_read = 0
    for x_dir in sorted(p for p in zoom_dir.iterdir() if p.is_dir()):
        for path in sorted(x_dir.glob("*.pbf")):
            if path.stat().st_size == 0:
                continue
            try:
                x = int(x_dir.name)
                y = int(path.stem)
            except ValueError:
                logger.warning("Skipping unexpected cache file %s", path)
                continue
            collection = vt_bytes_to_geojson(path.read_bytes(), x, y, zoom)
            all_features.extend(collection.get("features", []))
            tiles_read += 1

    logger.info(
        "Read %s cached tiles -> %s feature fragments.",
        tiles_read,
        len(all_features),
    )

    if reconstruct_column:
        reconstructed = _reconstruct_features(all_features, reconstruct_column)
        logger.info(
            "Reconstructed on column %r: %s fragments -> %s features.",
            reconstruct_column,
            len(all_features),
            len(reconstructed),
        )
        return reconstructed

    return all_features


def _reconstruct_features(features: list[dict], column: str) -> list[dict]:
    """Reconstruct features clipped across tile boundaries.

    Vector tiles clip geometries at tile edges, so one real-world feature can
    appear as a fragment in several tiles. Grouping by ``column`` and unioning
    geometries reconstructs the original feature. Features without the column
    are passed through unchanged.

    Parameters
    ----------
    features : list of dict
        GeoJSON Feature dictionaries.
    column : str
        Property name used to group fragments.

    Returns
    -------
    list of dict
        Reconstructed GeoJSON Feature dictionaries.
    """
    from shapely.geometry import mapping, shape
    from shapely.ops import linemerge, unary_union

    groups: dict = {}
    passthrough: list[dict] = []
    for feature in features:
        key = (feature.get("properties") or {}).get(column)
        if key is None:
            passthrough.append(feature)
        else:
            groups.setdefault(key, []).append(feature)

    reconstructed: list[dict] = []
    for group in groups.values():
        if len(group) == 1:
            reconstructed.append(group[0])
            continue
        merged = unary_union([shape(f["geometry"]) for f in group])
        if merged.geom_type == "MultiLineString":
            # union splits lines at shared nodes; merge fragments that connect
            # end-to-end back into single LineStrings
            merged = linemerge(merged)
        reconstructed.append(
            {
                "type": "Feature",
                "geometry": mapping(merged),
                "properties": dict(group[0].get("properties") or {}),
            }
        )
    return passthrough + reconstructed
