# requirements:
# psycopg[binary]
# shapely

import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg import sql
from shapely.geometry import Polygon, shape

from f.common_logic.db_operations import conninfo, postgresql
from f.common_logic.file_operations import get_safe_file_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NOTICE_MARKER = "GUYANA GEOLOGY AND MINES COMMISSION"
NOTICE_KEYWORDS = ("NOTICE OF INTENTION", "TRANSFER")

# Guyana bounding box used as a geographic guardrail.
GUYANA_BBOX = (-62.0, 1.0, -56.0, 9.0)  # min_lon, min_lat, max_lon, max_lat

class SchemaDriftError(Exception):
    """Raised when document structure, coordinates, or spatial geometries fail validation."""

    pass


def main(
    texts: list,
    db: postgresql,
    db_table_name: str,
    destination_path: str = "/persistent-storage/datalake/gazettes",
):
    """Parse GGMC gazette notice texts into GeoJSON features with the deterministic parser.

    For each extracted text document, notices are parsed with the deterministic
    regex parser. If any notice block fails, the failure is logged and recorded
    on the documents state table, and that document is skipped so the run
    continues with the successfully parsed ones (reprocess failures later with
    include_errored=true after extending the parser). The merged
    FeatureCollection is written to the data lake for downstream boundary
    filtering and database loading.

    Parameters
    ----------
    texts : list of dict
        Documents with extracted text, as returned by gazettes_pdf_to_text
        ([{"_id", "url", "text_path"}, ...]).
    db : postgresql
        A dictionary containing database connection parameters.
    db_table_name : str
        Detections table name; state table `<db_table_name>__documents` receives
        parse results.
    destination_path : str, optional
        Data lake directory where the merged FeatureCollection is written.

    Returns
    -------
    dict
        {
            "db_table_name": str,
            "geojson_path": str or None,
            "gazettes_statistics": dict or None,  # alerts_twilio-compatible
            "documents": [{"_id", "url", "parse_status", "features", "notes"}, ...],
            "failed": [{"url", "diagnosis"}, ...],
        }
        If any document has notice blocks the deterministic parser could not
        handle, it is logged, marked as failed on the state table, and skipped;
        the run continues with the successfully parsed documents.
    """
    destination_path = destination_path or "/persistent-storage/datalake/gazettes"

    state_table = f"{db_table_name.lower()[:54]}__documents"

    all_features = []
    documents = []
    failed = []
    warnings = []

    for item in texts or []:
        doc_url = item.get("url") or item.get("text_path")
        text = Path(item["text_path"]).read_text(encoding="utf-8")

        result = text_to_featurecollection(text, doc_url)
        features = result["features"]
        failed_blocks = result["failed_blocks"]
        parse_status = "parsed"
        notes = {}

        if failed_blocks:
            # No automatic recovery: log loudly and record the parser errors
            # as the diagnosis on the state row; this document is skipped and
            # the run continues with the successfully parsed ones.
            logger.error(
                f"PARSE FAILURE {doc_url}: deterministic parser failed on "
                f"{len(failed_blocks)}/{result['notice_blocks']} notice block(s): "
                + " | ".join(fb["error"] for fb in failed_blocks)
            )
            notes["regex_failures"] = failed_blocks
            parse_status = "failed"
            failed.append({"url": doc_url, "diagnosis": result["schema_errors"]})

        if parse_status != "failed" and not features:
            if result["notice_blocks"] == 0:
                # Legally spatial-free document (e.g. Orders gazette): not an error.
                parse_status = "no_notices"
            else:
                parse_status = "failed"
                failed.append({"url": doc_url, "diagnosis": result["schema_errors"]})

        all_features.extend(features)
        documents.append(
            {
                "_id": item.get("_id"),
                "url": doc_url,
                "parse_status": parse_status,
                "features": len(features),
                "notes": notes or None,
            }
        )
        _mark_state(conninfo(db), state_table, item.get("_id"), parse_status, notes)

    geojson_path = None
    gazettes_statistics = None
    if all_features:
        feature_collection = {
            "type": "FeatureCollection",
            "properties": {
                "total_notices": sum(
                    d["features"] for d in documents if d["parse_status"] != "failed"
                ),
                "parsed_notices": len(all_features),
                "warnings": warnings,
            },
            "features": all_features,
        }
        geojson_path = write_feature_collection(
            feature_collection, db_table_name, destination_path
        )
        gazettes_statistics = build_alerts_statistics(all_features)

    logger.info(
        f"Parsed {len(documents)} document(s): "
        f"{sum(1 for d in documents if d['parse_status'] in ('parsed', 'no_notices'))} ok, "
        f"{len(failed)} failed."
    )

    if failed:
        for entry in failed:
            logger.error(
                f"SKIPPED DOCUMENT {entry['url']} - diagnosis: {entry['diagnosis']}"
            )
        logger.warning(
            f"Continuing with the successfully parsed documents; "
            f"{len(failed)} of {len(documents)} document(s) failed and were "
            f"marked on state table [{state_table}] for review (reprocess them "
            f"with include_errored=true after fixing the parser)."
        )

    return {
        "db_table_name": db_table_name,
        "geojson_path": geojson_path,
        "gazettes_statistics": gazettes_statistics,
        "documents": documents,
        "failed": failed,
    }


# ==========================================
# 1. TEXT CLEANING & COORDINATE SANITIZATION
# ==========================================


def parse_dms(dms_str: str) -> float:
    """Parses Degrees Minutes Seconds (DMS) string variants into decimal degrees.

    Handles GGMC typos like missing direction letters, tick placement typos,
    and odd quotes.
    """
    if not dms_str:
        return 0.0

    # Sanitize known GGMC OCR/typo quirks
    s = (
        dms_str.replace("º", "°")
        .replace("’", "'")
        .replace("”", '"')
        .replace("''", '"')
        .strip()
    )

    # Append 'N' if latitude direction letter is missing
    if not re.search(r"[NWSEnwse]$", s):
        s += " N"

    s = re.sub(r"([0-9\'\"\.]+)\s*([NWSEnwse])", r"\1 \2", s)
    s = re.sub(r"(\d+)\'\.(\d+)\"", r"\1.\2'", s)  # Fixes typos like 51'.086" -> 51.086'

    is_west = "W" in s.upper()
    is_south = "S" in s.upper()

    clean = re.sub(r"[NWSEnwse]", "", s).strip()
    parts = clean.split("°")
    deg = float(parts[0]) if parts[0] else 0.0

    minute, sec = 0.0, 0.0
    if len(parts) > 1 and parts[1].strip():
        rest = parts[1].strip()
        if "'" in rest:
            min_sec = rest.split("'")
            minute = float(min_sec[0]) if min_sec[0] else 0.0
            if len(min_sec) > 1 and min_sec[1].strip():
                sec_str = re.sub(r"[^\d\.]", "", min_sec[1])
                sec = float(sec_str) if sec_str else 0.0
        else:
            sec_str = re.sub(r"[^\d\.]", "", rest)
            minute = float(sec_str) if sec_str else 0.0

    dd = deg + (minute / 60.0) + (sec / 3600.0)
    return round(-dd if (is_west or is_south) else dd, 6)


def clean_pdf_text(raw_text: str) -> str:
    """Strips recurring Official Gazette header artifacts, footers, and page break text."""
    cleaned = re.sub(
        r"The Official Gazette\s*\n\(EXTRAORDINARY\)\s*\nOF GUYANA.*?\n\d{4}",
        "",
        raw_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return re.sub(r"\n\d+\s*\n(?:GS\d+:[^\n]+\n)?", "\n", cleaned)


# ==========================================
# 2. DEADLINE & METADATA EXTRACTORS
# ==========================================


def extract_objection_deadline(notice_text: str, notice_date_str: str) -> dict:
    """Extracts the statutory objection period and calculates the ISO deadline date."""
    days_match = re.search(
        r"within\s+([a-zA-Z0-9\(\)\s\-]+?)\s*days\s+(?:from|after)",
        notice_text,
        re.IGNORECASE,
    )

    objection_days = None
    if days_match:
        raw_days = days_match.group(1)
        num_match = re.search(r"\((\d+)\)", raw_days)
        if num_match:
            objection_days = int(num_match.group(1))
        elif "fourteen" in raw_days.lower():
            objection_days = 14
        elif (
            "twenty-one" in raw_days.lower() or "twenty one" in raw_days.lower()
        ):
            objection_days = 21

    deadline_date_iso = None
    if objection_days and notice_date_str != "UNKNOWN_DATE":
        try:
            pub_date = datetime.strptime(notice_date_str, "%B %d, %Y")
            deadline_date = pub_date + timedelta(days=objection_days)
            deadline_date_iso = deadline_date.strftime("%Y-%m-%d")
        except ValueError:
            deadline_date_iso = None

    return {
        "objection_period_days": objection_days,
        "objection_deadline_date": deadline_date_iso,
        "objection_clause_found": bool(days_match),
    }


# ==========================================
# 3. NOTICE PARSER ENGINE (deterministic / regex path)
# ==========================================


def _feature_id(doc_url: str, file_reference: str, block_index: int) -> str:
    """Deterministic feature id so re-runs upsert instead of duplicating."""
    return str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_url}#{file_reference}#{block_index}")
    )


def split_notice_blocks(text: str) -> list:
    """Split cleaned gazette text into candidate notice blocks.

    Parameters
    ----------
    text : str
        Cleaned document text.

    Returns
    -------
    list of str
        Blocks that start with the agency marker and mention notice keywords.
    """
    return [
        b.strip()
        for b in re.split(f"(?={NOTICE_MARKER})", text)
        if any(k in b.upper() for k in NOTICE_KEYWORDS) and NOTICE_MARKER in b
    ]


def parse_single_notice(
    notice_text: str, doc_url: str = "", block_index: int = 0
) -> dict:
    """Parses a single GGMC notice block into a validated GeoJSON Feature dict.

    Parameters
    ----------
    notice_text : str
        Text of one notice block.
    doc_url : str, optional
        Source document URL recorded on the feature and used for its id.
    block_index : int, optional
        Index of the block within the document (part of the deterministic id).

    Returns
    -------
    dict
        GeoJSON Feature with a deterministic `id`.

    Raises
    ------
    SchemaDriftError
        On under-vertexed, invalid, or out-of-Guyana geometries.
    """
    # 1. File Reference
    ref_match = re.search(
        r"(GS\d+:\s*[\w\-\/]+|MP\s+No\.\s*[\w\-\/]+|Mahdia\s+Block\s+No\.\s*\d+)",
        notice_text,
        re.IGNORECASE,
    )
    file_ref = ref_match.group(1).strip() if ref_match else "UNKNOWN_REF"

    # 2. Notice Date
    date_match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
        notice_text,
    )
    notice_date = date_match.group(0) if date_match else "UNKNOWN_DATE"

    # 3. Notice Type & Permit Category
    type_match = re.search(
        r"(NOTICE OF INTENTION TO [A-Z\s]+|TRANSFER|AMENDMENT)", notice_text
    )
    notice_type = (
        type_match.group(0).strip().title() if type_match else "General Notice"
    )

    if "PROSPECTING" in notice_text.upper():
        permit_category = "Prospecting"
    elif "MINING" in notice_text.upper():
        permit_category = "Mining"
    elif "RELINQUISH" in notice_text.upper():
        permit_category = "Relinquishment"
    elif "TRANSFER" in notice_text.upper():
        permit_category = "Transfer"
    else:
        permit_category = "Other"

    # 4. Applicant & Transferee Metadata
    applicant_match = re.search(
        r"(?:Grant to|favor of|that)\s+([A-Z\s]{3,35})\s+whose address is\s+([^,]+,[^,]+,[^,]+, Guyana)",
        notice_text,
        re.IGNORECASE,
    )
    applicant_name = applicant_match.group(1).strip() if applicant_match else None
    applicant_address = applicant_match.group(2).strip() if applicant_match else None

    transfer_match = re.search(
        r"Transfer to and in favor of\s+([A-Z\s]{3,35})\s+whose address is\s+([^,]+,[^,]+,[^,]+, Guyana)",
        notice_text,
        re.IGNORECASE,
    )
    transfer_to_name = transfer_match.group(1).strip() if transfer_match else None
    transfer_to_address = transfer_match.group(2).strip() if transfer_match else None

    # 5. Spatial & Location Context
    district_match = re.search(
        r"located in the\s+([A-Za-z0-9\s]+District No\.\s*\d+)",
        notice_text,
        re.IGNORECASE,
    )
    mining_district = district_match.group(1).strip() if district_match else "Unknown"

    map_match = re.search(r"Topographic Map\s+([0-9A-Z]+)", notice_text, re.IGNORECASE)
    topo_map = map_match.group(1).strip() if map_match else "Unknown"

    area_match = re.search(
        r"enclosing an area of approximately\s+([\d\.]+)\s+acres",
        notice_text,
        re.IGNORECASE,
    )
    enclosed_area = area_match.group(1).strip() if area_match else "Unknown"

    # 6. Objection Deadline
    objection_info = extract_objection_deadline(notice_text, notice_date)

    # 7. Reference Point X
    ref_x_match = re.search(
        r"reference point 'X' located at\s+([^with]+)with geographical coordinates of longitude\s*([0-9º°\'\"\.\sW]+)\s*and\s*latitude\s*([0-9º°\'\"\.\sN]+)",
        notice_text,
        re.IGNORECASE,
    )
    ref_x_info = {}
    if ref_x_match:
        ref_x_info = {
            "description": ref_x_match.group(1).strip(),
            "longitude_dms": ref_x_match.group(2).strip(),
            "latitude_dms": ref_x_match.group(3).strip(),
            "longitude_dd": parse_dms(ref_x_match.group(2)),
            "latitude_dd": parse_dms(ref_x_match.group(3)),
        }

    # 8. Extract Boundary Vertices
    vertex_pattern = re.compile(
        r"Point\s+([A-Z]{1,2})[\s,]+(?:located\s+at\s+)?(?:geographical\s+)?coordinates\s+of\s+longitude\s*([0-9º°\'\"\.\sW]+?)\s*and\s*latitude\s*([0-9º°\'\"\.\sN]+?)(?=[,\s]+thence|[,\s]+thus|\.|$)",
        re.IGNORECASE | re.DOTALL,
    )
    raw_vertices = vertex_pattern.findall(notice_text)

    if len(raw_vertices) < 3:
        raise SchemaDriftError(
            f"Notice {file_ref} yielded under 3 boundary vertices "
            f"(found {len(raw_vertices)})."
        )

    coords_dd = []
    vertices_metadata = []
    for label, lon_dms, lat_dms in raw_vertices:
        lon_dd = parse_dms(lon_dms)
        lat_dd = parse_dms(lat_dms)
        coords_dd.append((lon_dd, lat_dd))
        vertices_metadata.append(
            {
                "point": label.upper(),
                "longitude_dms": lon_dms.strip(),
                "latitude_dms": lat_dms.strip(),
                "longitude_dd": lon_dd,
                "latitude_dd": lat_dd,
            }
        )

    # 9. Spatial Geometry Validation (Shapely)
    try:
        poly = Polygon(coords_dd)
    except Exception as e:
        raise SchemaDriftError(
            f"Shapely polygon creation failed for {file_ref}: {str(e)}"
        )

    if not poly.is_valid:
        raise SchemaDriftError(
            f"Notice {file_ref} produced an invalid or self-intersecting polygon."
        )

    min_x, min_y, max_x, max_y = poly.bounds
    if not (
        GUYANA_BBOX[0] <= min_x
        and max_x <= GUYANA_BBOX[2]
        and GUYANA_BBOX[1] <= min_y
        and max_y <= GUYANA_BBOX[3]
    ):
        raise SchemaDriftError(
            f"Notice {file_ref} polygon bounds {poly.bounds} fall outside Guyana boundaries."
        )

    # 10. Construct GeoJSON Feature (plain dict, deterministic id)
    return {
        "type": "Feature",
        "id": _feature_id(doc_url, file_ref, block_index),
        "geometry": {
            "type": "Polygon",
            "coordinates": [list(poly.exterior.coords)],
        },
        "properties": {
            "source_document": doc_url,
            "file_reference": file_ref,
            "notice_date": notice_date,
            "notice_type": notice_type,
            "permit_category": permit_category,
            "applicant_name": applicant_name,
            "applicant_address": applicant_address,
            "transfer_to_name": transfer_to_name,
            "transfer_to_address": transfer_to_address,
            "mining_district": mining_district,
            "topographic_map": topo_map,
            "enclosed_area_acres": enclosed_area,
            "objection_period_days": objection_info["objection_period_days"],
            "objection_deadline_date": objection_info["objection_deadline_date"],
            "objection_clause_found": objection_info["objection_clause_found"],
            "reference_point_x": ref_x_info,
            "boundary_vertices": vertices_metadata,
        },
    }


def text_to_featurecollection(text: str, doc_url: str = "") -> dict:
    """Parse gazette document text into features via the deterministic parser.

    Parameters
    ----------
    text : str
        Raw (or OCR) document text.
    doc_url : str, optional
        Source document URL.

    Returns
    -------
    dict
        {
            "features": [GeoJSON Feature dict, ...],
            "notice_blocks": int,
            "failed_blocks": [{"block_index", "file_reference_guess", "error"}, ...],
            "schema_errors": [str, ...],
        }
    """
    cleaned_text = clean_pdf_text(text)
    notice_blocks = split_notice_blocks(cleaned_text)

    features = []
    failed_blocks = []
    for idx, block in enumerate(notice_blocks, 1):
        try:
            features.append(parse_single_notice(block, doc_url, idx))
        except SchemaDriftError as e:
            failed_blocks.append(
                {
                    "block_index": idx,
                    "file_reference_guess": block[:80].replace("\n", " "),
                    "error": str(e),
                }
            )

    return {
        "features": features,
        "notice_blocks": len(notice_blocks),
        "failed_blocks": failed_blocks,
        "schema_errors": [fb["error"] for fb in failed_blocks],
    }


# ==========================================
# 4. OUTPUT HELPERS
# ==========================================


def write_feature_collection(
    feature_collection: dict, db_table_name: str, destination_path: str
) -> str:
    """Write the merged FeatureCollection to the data lake and return its path.

    Parameters
    ----------
    feature_collection : dict
        Merged FeatureCollection.
    db_table_name : str
        Used for the output file name.
    destination_path : str
        Data lake directory.

    Returns
    -------
    str
        Absolute path of the written file.
    """
    filename = f"{db_table_name}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    destination = Path(destination_path)
    destination.mkdir(parents=True, exist_ok=True)
    file_path = get_safe_file_path(str(destination), filename, "geojson")
    with open(file_path, "w") as f:
        json.dump(feature_collection, f)
    logger.info(f"FeatureCollection written to {file_path}")
    return str(file_path)


def build_alerts_statistics(features: list) -> dict:
    """Summarize parsed features into the alerts_twilio statistics shape.

    Parameters
    ----------
    features : list of dict
        Parsed GeoJSON features.

    Returns
    -------
    dict
        {"total_alerts": str, "date": str (DD/MM/YYYY), "description_alerts": str}
        or None if no dated features exist.
    """
    latest = None
    for feature in features:
        props = feature.get("properties") or {}
        raw_date = props.get("notice_date")
        if not raw_date or raw_date == "UNKNOWN_DATE":
            continue
        try:
            parsed = datetime.strptime(raw_date, "%B %d, %Y")
        except ValueError:
            continue
        if latest is None or parsed > latest:
            latest = parsed

    if latest is None:
        return None

    categories = sorted(
        {
            (f.get("properties") or {}).get("permit_category")
            for f in features
            if (f.get("properties") or {}).get("permit_category")
        }
    )
    return {
        "total_alerts": str(len(features)),
        "date": f"{latest.day}/{latest.month}/{latest.year}",
        "description_alerts": ", ".join(c.replace("_", " ") for c in categories),
    }


def _mark_state(
    db_connection_string: str,
    state_table: str,
    doc_id: str,
    parse_status: str,
    notes: dict = None,
):
    """Record the parse outcome (and diagnostics) on the document state row."""
    if not doc_id:
        return
    with psycopg.connect(db_connection_string, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    """
                    UPDATE {table}
                    SET status = %s,
                        parse_status = %s,
                        parse_notes = %s,
                        error = %s,
                        last_processed = now()
                    WHERE _id = %s
                    """
                ).format(table=sql.Identifier(state_table)),
                (
                    "processed" if parse_status != "failed" else "error",
                    parse_status,
                    json.dumps(notes) if notes else None,
                    None if parse_status != "failed" else "Unresolvable notice blocks; see parse_notes",
                    doc_id,
                ),
            )
