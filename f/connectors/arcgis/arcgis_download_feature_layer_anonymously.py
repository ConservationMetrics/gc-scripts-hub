# requirements:
# filetype~=1.2
# fiona~=1.10
# openpyxl~=3.1
# pandas~=2.2
# pyproj~=3.7
# psycopg2-binary
# requests~=2.32

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests
from pyproj import Transformer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from f.common_logic.data_conversion import slugify
from f.common_logic.file_operations import save_data_to_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    subdomain: str,
    service_id: str,
    feature_id: str,
    layer_index_list: List[int],
    download_attachments: bool = False,
    output_format: str = "geojson",
    folder_name: str = "arcgis",
    attachment_root: str = "/persistent-storage/datalake",
) -> List[Path]:
    storage_path = Path(attachment_root) / slugify(folder_name)

    results: List[Path] = []

    session = make_session()

    for li in layer_index_list:
        path = fetch_layer_data(
            subdomain=subdomain,
            service_id=service_id,
            feature_id=feature_id,
            layer_index=li,
            storage_path=storage_path,
            download_attachments=download_attachments,
            output_format=output_format,
            session=session,
        )
        results.append(path)

    logger.info(f"Finished fetching all layers, {len(results)} fetched.")

    return results


DEFAULT_RETRY = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
)


def make_session(retry: Retry = DEFAULT_RETRY, timeout: int = 30) -> requests.Session:
    """
    Create a requests. Session with default retry and timeout settings.

    Parameters
    ----------
    retry : Retry
        Retry configuration for HTTP requests.

    timeout : int
        Default timeout (in seconds) for all requests.

    Returns
    -------
    requests.Session
        Configured session instance.
    """
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    # store a default timeout on the session for convenience
    session.request = _wrap_timeout(session.request, timeout)
    return session


def _wrap_timeout(request_func, timeout):
    """
    Call a function with a timeout and standardized error handling.

    This helper is mainly intended for wrapping network calls (e.g. `requests.get`),
    ensuring that all requests enforce a timeout and log errors consistently.

    Parameters
    ----------
    func : Function
        The callable to execute (must accept a `timeout` keyword argument).
    *args :
        Positional arguments to pass to the callable.
    timeout : int
        Maximum time in seconds before the call times out (default: 30).
    **kwargs :
        Keyword arguments to pass to the callable.

    Returns
    -------
    Function
        The result of the wrapped function if successful.

    Raises
    ------
    requests.Timeout
        If the function call exceeds the given timeout.
    requests.RequestException
        If any other request-related error occurs.

    Side effects
    ------------
    Logs timeout and request errors with the function name for easier debugging.
    """

    def wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return request_func(method, url, **kwargs)

    return wrapped


def get_layer_metadata(
    session: requests.Session, subdomain: str, service_id: str, feature_id: str
) -> Dict[str, Any]:
    """
    Fetch the metadata of a feature service layer and return its name.

    This function queries the ArcGIS REST service endpoint at `full_url` to
    retrieve metadata in JSON format, then extracts the name of the layer
    matching the given `layer_index`.

    Args:
        full_url: Base URL of the ArcGIS FeatureServer (without query params).
        layer_index: The integer index of the target layer within the service.
        http: A requests.Session (with retry strategy already configured).

    Returns:
        The layer name as a slugified string if found, otherwise None.

    Raises:
        requests.RequestException: If the metadata request fails.
        json.JSONDecodeError: If the response is not valid JSON.

    Notes:
        - The returned layer name is normalized with `slugify` to be filesystem-safe.
        - If the layer index is not found, the function logs a warning and returns None.
    """
    base = f"https://{subdomain}.arcgis.com/{service_id}/arcgis/rest/services/{feature_id}/FeatureServer"
    url = f"{base}?f=pjson"
    logger.debug("Fetching layer metadata from %s", url)
    resp = session.get(url)
    try:
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Failed fetching metadata for %s: %s", feature_id, exc)
        raise
    try:
        return resp.json()
    except json.JSONDecodeError as exc:
        logger.error("Metadata JSON decode error for %s: %s", feature_id, exc)
        raise


def fetch_features(
    session: requests.Session,
    base_feature_url: str,
    layer_index: Optional[int],
    batch_size: int = 2000,
    where_clause: str = "1=1",
) -> List[Dict[str, Any]]:
    """Fetch all features for a layer with pagination.

    Returns list of attribute dicts with geometry preserved.
    """
    query_url = (
        f"{base_feature_url}/{layer_index}/query"
        if layer_index is not None
        else f"{base_feature_url}/query"
    )
    params = {
        "where": where_clause,
        "outFields": "*",
        "f": "json",
        "returnGeometry": "true",
        "resultOffset": 0,
        "resultRecordCount": batch_size,
    }

    all_records: List[Dict[str, Any]] = []

    while True:
        logger.debug("Querying %s with offset %s", query_url, params["resultOffset"])

        resp = session.get(query_url, params=params)

        try:
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Failed to fetch features: %s", exc)
            raise

        data = resp.json()

        if "features" not in data:
            if "error" in data:
                logger.error("ArcGIS returned error: %s", data["error"].get("message"))
                raise RuntimeError(data["error"].get("message"))
            logger.info("No features key present in response; stopping.")
            break

        features = data["features"]
        if not features:
            break

        for feat in features:
            # Keep attributes and geometry grouped together
            attrs = dict(feat.get("attributes", {}))
            geom = feat.get("geometry")
            if geom is not None:
                attrs["__geometry"] = geom
            all_records.append(attrs)

        params["resultOffset"] += params["resultRecordCount"]

    return all_records


def transform_record_geometry(
    record: Dict[str, Any], transformer: Transformer
) -> Dict[str, Any]:
    """
    Given a single record with possible __geometry, add WGS84 lon/lat coordinates for geojson output.

    Parameters
    ----------

    record: Dict
        a single record with possible __geometry
    transformer: Transformer
        should convert from source CRS to EPSG:4326.

    Returns
    -------

    Dict
        The same record that was passed as a parameter, mutated
    """
    geom = record.get("__geometry")
    if not geom:
        return record

    # Points
    if "x" in geom and "y" in geom:
        lon, lat = transformer.transform(geom["x"], geom["y"])
        record["__geojson_geometry"] = {"type": "Point", "coordinates": [lon, lat]}
        return record

    # Polylines
    if "paths" in geom:
        coords = []
        for path in geom["paths"]:
            coords.append([list(transformer.transform(x, y)) for x, y in path])
        # Flatten single-path to LineString, else MultiLineString
        if len(coords) == 1:
            record["__geojson_geometry"] = {
                "type": "LineString",
                "coordinates": coords[0],
            }
        else:
            record["__geojson_geometry"] = {
                "type": "MultiLineString",
                "coordinates": coords,
            }
        return record

    # Polygons (rings)
    if "rings" in geom:
        coords = []
        for ring in geom["rings"]:
            coords.append([list(transformer.transform(x, y)) for x, y in ring])
        # GeoJSON polygon expects list of linear rings
        record["__geojson_geometry"] = {"type": "Polygon", "coordinates": coords}
        return record

    # Unknown geometry
    logger.warning("Unknown geometry type for record: %s", geom)
    return record


def build_geojson(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    features = []
    for record in records:
        props = {k: v for k, v in record.items() if not k.startswith("__")}
        geom = record.get("__geojson_geometry")
        feature = {"type": "Feature", "properties": props, "geometry": geom}
        features.append(feature)
    return {"type": "FeatureCollection", "features": features}


def save_output_geojson(
    geojson: Dict[str, Any], filename: Path, storage_path: Optional[Path]
) -> None:
    filename.parent.mkdir(parents=True, exist_ok=True)
    # Use provided helper for saving to configured storage, plus local dump
    # save_data_to_file adds the extension, so pass filename.stem not filename.name
    save_data_to_file(geojson, filename.stem, storage_path, file_type="geojson")


def download_attachments_for_feature(
    session: requests.Session,
    base_feature_url: str,
    object_id: int,
    attachments_dir: Path,
) -> None:
    attachments_dir.mkdir(parents=True, exist_ok=True)
    info_url = f"{base_feature_url}/{object_id}/attachments"

    resp = session.get(info_url, params={"f": "json"})
    resp.raise_for_status()

    info = resp.json()

    for attachment in info.get("attachmentInfos", []):
        aid = attachment.get("id")
        name = attachment.get("name") or f"att_{aid}"
        # Safe filename
        safe_name = slugify(name)
        file_path = attachments_dir / f"{object_id}_{aid}_{safe_name}"
        if file_path.exists():
            logger.info("Attachment %s already exists; skipping", file_path)
            continue
        att_url = f"{info_url}/{aid}"
        att_resp = session.get(att_url, stream=True)
        att_resp.raise_for_status()
        with open(file_path, "wb") as fh:
            for chunk in att_resp.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
        logger.info("Downloaded attachment %s", file_path)


def fetch_layer_data(
    subdomain: str,
    service_id: str,
    feature_id: str,
    layer_index: int,
    storage_path: Path,
    download_attachments: bool = False,
    output_format: str = "geojson",
    transformer: Transformer | None = None,
    session: Optional[requests.Session] = None,
) -> Path:
    """High-level function to fetch a single layer and save it.

    Returns path to the saved local file.
    """
    if session is None:
        session = make_session()

    if transformer is None:
        # default assume input is WebMercator (EPSG:3857) and convert to EPSG:4326
        transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

    metadata = get_layer_metadata(session, subdomain, service_id, feature_id)

    # find layer name
    layers = metadata.get("layers", [])
    layer_obj = next((ly for ly in layers if ly.get("id") == layer_index), None)
    if layer_obj is None:
        raise ValueError(f"Layer index {layer_index} not found in service {feature_id}")

    layer_name = slugify(layer_obj.get("name", f"layer_{layer_index}"))

    filename = storage_path / f"{layer_name}.{output_format}"
    relative_output = Path(storage_path.name) / f"{layer_name}.{output_format}"

    if filename.exists():
        logger.info("File %s already exists. Skipping download.", filename)
        return relative_output

    base_feature_url = f"https://{subdomain}.arcgis.com/{service_id}/arcgis/rest/services/{feature_id}/FeatureServer"

    records = fetch_features(session, base_feature_url, layer_index)

    # transform geometries
    for rec in records:
        transform_record_geometry(rec, transformer)

    if output_format == "csv":
        # drop internal geometry helpers before saving
        df = pd.DataFrame(records)
        filename.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(filename, index=False)
    else:
        geojson = build_geojson(records)
        save_output_geojson(geojson, filename, storage_path)

    if download_attachments:
        attachments_root = storage_path / f"{service_id}_attachments"
        layer_url = f"{base_feature_url}/{layer_index}"
        for rec in records:
            objid = rec.get("OBJECTID") or rec.get("objectid") or rec.get("ObjectID")
            if objid is not None:
                try:
                    download_attachments_for_feature(
                        session,
                        layer_url,
                        int(objid),
                        attachments_root / str(objid),
                    )
                except Exception as exc:
                    logger.exception(
                        "Failed to download attachments for object %s: %s", objid, exc
                    )

    logger.info("Saved layer %s to %s", layer_name, filename)
    return relative_output
