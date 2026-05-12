import json
import logging
import re
from pathlib import Path

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Repeat-parent ``fieldValues`` child-row references use the same opaque ids as
# ``records[*].uid``: 32 hex digits, no separators, in ``data/0.json`` backups
# (repo fixture and larger on-disk samples). ``_CT_ROW_ID_UUID`` matches dashed
# RFC-4122 UUID text—the alternate spelling of that 128-bit value—so id lists are
# still detected if input carries that serialization.
_CT_ROW_ID_HEX32 = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)
_CT_ROW_ID_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def main(
    cybertracker_observations_path: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    """
    Parse CyberTracker (CT) JSON and save observations to database as GeoJSON.

    Parameters
    ----------
    cybertracker_observations_path : str
        The path (in attachment root) to the CT JSON file to import.
    db : postgresql
        Database connection configuration.
    db_table_name : str
        The name of the database table where observations will be stored.
    attachment_root : str
        Root directory for persistent storage.
    """
    # Construct full path to JSON file
    json_path = Path(attachment_root) / cybertracker_observations_path

    if not json_path.exists():
        raise FileNotFoundError(f"CyberTracker JSON file not found: {json_path}")

    logger.info(f"Reading CyberTracker JSON from {json_path}")

    # Parse JSON and extract observations as GeoJSON
    observations_geojson = parse_cybertracker_json(json_path)

    num_observations = len(observations_geojson.get("features", []))
    logger.info(f"Extracted {num_observations} observations from CyberTracker JSON")

    # Create project directory in datalake
    project_dir = Path(attachment_root) / db_table_name
    project_dir.mkdir(parents=True, exist_ok=True)

    # Copy raw JSON content to datalake with utf-8 encoding
    json_save_path = project_dir / json_path.name
    json_save_path.write_text(
        json_path.read_text(encoding="utf-8-sig"), encoding="utf-8"
    )
    logger.info(f"Saved raw JSON file to: {json_save_path}")

    # Apply transformation to add data_source
    observations_geojson = transform_cybertracker_data(observations_geojson)

    # Save GeoJSON file to datalake
    if observations_geojson.get("features"):
        save_data_to_file(
            observations_geojson,
            db_table_name,
            project_dir,
            file_type="geojson",
        )
        logger.info(
            f"Saved GeoJSON file to: {project_dir / f'{db_table_name}.geojson'}"
        )

        # Write to database using geojson_to_postgres
        geojson_rel_path = Path(db_table_name) / f"{db_table_name}.geojson"
        save_geojson_to_postgres(
            db=db,
            db_table_name=db_table_name,
            geojson_path=str(geojson_rel_path),
            attachment_root=attachment_root,
            delete_geojson_file=False,
        )
        logger.info(
            f"CyberTracker observations successfully written to database table: [{db_table_name}]"
        )
    else:
        logger.warning("No observations found in CyberTracker JSON")


def transform_cybertracker_data(data: dict, dataset_name: str = None) -> dict:
    """
    Apply CT-specific transformations to observations data.

    This transformation adds the data_source field to identify the data as coming
    from CT.

    Parameters
    ----------
    data : dict
        GeoJSON FeatureCollection with CT observations.
    dataset_name : str, optional
        Human-readable name of the dataset (currently unused, included for
        consistency with other transformation functions).

    Returns
    -------
    dict
        Transformed GeoJSON with data_source field added to all feature properties.
    """
    if "features" in data:
        for feature in data["features"]:
            if "properties" not in feature:
                feature["properties"] = {}
            feature["properties"]["data_source"] = "CyberTracker"
    return data


def _looks_like_uid(value: object) -> bool:
    """True if ``value`` looks like an opaque CT child row id, not user text.

    Parent repeat fields store lists of these ids so the app can resolve child rows
    (e.g. the row that holds the real photo filename). We skip persisting those lists
    only when every element matches this heuristic. False negatives (missing a
    real id shape) leave noisy uid lists in ``properties``. After ASCII ``strip()``,
    we accept 32 contiguous hex digits (the form in ``data/0.json`` backups) or dashed
    RFC-4122 UUID text for the same 128-bit value; both patterns stay intentionally tight.
    """
    if not isinstance(value, str):
        return False
    s = value.strip()
    if not s:
        return False
    return bool(_CT_ROW_ID_HEX32.match(s) or _CT_ROW_ID_UUID.match(s))


def _normalize_field_key(raw_key: str) -> str:
    """Derive a flat property key from CT ``fieldValues`` keys.

    Form field ids are dynamic: typically ``s<digits>_<your_field_name>`` or
    ``repeat_s<digits>_<your_field_name>`` (and slash-separated child keys).

    We strip the timestamp prefix and ``repeat_`` wrapper only — the suffix is kept
    verbatim as the output property name (e.g. ``additional_note``, ``photo_of_site``).

    Keys beginning ``cto_`` are CT Online envelope fields (device id, session bounds,
    captured location snapshot, etc.). We prefix the output name with ``_``
    (e.g. ``cto_location`` → ``_location``) so they read as metadata alongside
    CT Classic-style keys such as plain ``location``.
    """
    key = raw_key.split("/", 1)[-1]
    if key.startswith("repeat_"):
        key = key.removeprefix("repeat_")

    # CT Online: session / capture metadata uses the ``cto_`` namespace.
    if key.startswith("cto_"):
        return "_" + key.removeprefix("cto_")

    if key.startswith("s") and "_" in key:
        prefix, rest = key.split("_", 1)
        if prefix[1:].isdigit():
            return rest

    return key


def _normalize_field_value(raw_key: str, value):
    """Normalize a CT ``fieldValues`` entry without collapsing list shape.

    Multi-value fields (attachments, repeats) stay as lists even when only one
    element is present so the downstream column type is stable across rows —
    otherwise a survey that captured one photo serializes as TEXT ``"a.jpg"``
    while a survey that captured two serializes as TEXT ``'["a.jpg","b.jpg"]'``
    in the same column, breaking any consumer that tries to parse it.

    Attachment dicts (``{"filename": ...}``) are surfaced as the bare filename,
    whether they appear as the top-level value or inside a list.
    """
    if isinstance(value, dict) and "filename" in value:
        return value.get("filename")

    if isinstance(value, list):
        # Parent repeat fields contain child uids; prefer the child rows which
        # include the actual filenames under a ".../..." key.
        if "/" not in raw_key and value and all(_looks_like_uid(v) for v in value):
            return None
        return [
            v["filename"] if isinstance(v, dict) and "filename" in v else v
            for v in value
        ]

    return value


def _point_geometry_from_xy_dict(value: dict) -> dict | None:
    """Return GeoJSON Point geometry if ``value`` has numeric lon/lat ``x``/``y``."""
    if not isinstance(value, dict):
        return None
    x, y = value.get("x"), value.get("y")
    try:
        if x is None or y is None:
            return None
        lon = float(x)
        lat = float(y)
    except (TypeError, ValueError):
        return None
    return {"type": "Point", "coordinates": [lon, lat]}


def parse_cybertracker_json(json_path: Path) -> dict:
    """Parse CyberTracker backup JSON (``data/0.json``) into a GeoJSON FeatureCollection.

    Each top-level element in the JSON array is a session. We flatten all
    ``records[*].fieldValues`` for that session into one feature's ``properties``.

    Property keys come from the form schema: we strip CT's ``s<timestamp>_``
    / ``repeat_`` prefixes (and map CT Online ``cto_*`` keys → ``_*``),
    but do not rename arbitrary field slugs — those are whatever the mobile
    form defines.

    Sessions composed of GPS tracks are skipped (not imported here). See
    README.md for more details.

    A session is emitted only when we can build a non-null Point geometry from
    a location dict: CT Online surveys use ``cto_location`` (normalized to
    ``_location``); CT Classic surveys often use a plain ``location`` key.
    The same ``x`` / ``y`` semantics in both cases.

    Parameters
    ----------
    json_path : Path
        Path to the CyberTracker JSON file.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection with observation features (Point geometry only).
    """
    payload = json.loads(json_path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, list):
        raise ValueError("CyberTracker JSON must be a list of records")

    features = []
    skipped_no_geometry = 0

    for session in payload:
        db = session.get("db") or {}
        feature_id = db.get("uid") or session.get("uid")
        if not feature_id:
            continue

        props = {"_id": feature_id}
        geometry = None

        for record in session.get("records") or []:
            field_values = record.get("fieldValues") or {}
            for raw_key, raw_value in field_values.items():
                key = _normalize_field_key(raw_key)
                val = _normalize_field_value(raw_key, raw_value)
                if val is None:
                    continue

                props[key] = val

                # Online: ``_location`` from ``cto_location``; Classic: ``location``.
                if key in ("_location", "location") and isinstance(val, dict):
                    geom = _point_geometry_from_xy_dict(val)
                    if geom is not None:
                        geometry = geom

        if geometry is None:
            skipped_no_geometry += 1
            continue

        features.append(
            {
                "type": "Feature",
                "id": feature_id,
                "geometry": geometry,
                "properties": props,
            }
        )

    if skipped_no_geometry:
        logger.info(
            "Skipped %d CyberTracker session(s) with no point coordinates "
            "(e.g. track-only / KMZ rows — not imported as observations)",
            skipped_no_geometry,
        )

    return {"type": "FeatureCollection", "features": features}
