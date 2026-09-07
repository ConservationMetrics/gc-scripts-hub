import json
import logging
import uuid
from pathlib import Path

import psycopg
import pytest

import f.connectors.gazettes.gazettes_ggmc_parse as parse_module
from f.connectors.gazettes.gazettes_ggmc_parse import (
    build_alerts_statistics,
    main,
    parse_dms,
    text_to_featurecollection,
)
from f.connectors.gazettes.gazettes_scrape import create_documents_state_table
from f.connectors.gazettes.tests.assets.site_data import DOC_URL, read_asset

logger = logging.getLogger(__name__)


def _texts_entry(tmp_path, db, asset_name, url=DOC_URL):
    """Create a state row + text file, returning the texts entry gazettes_ggmc_parse expects."""
    with psycopg.connect(autocommit=True, **db) as conn:
        with conn.cursor() as cur:
            create_documents_state_table(cur, "ggmc_notices__documents")
            doc_id = str(uuid.uuid5(uuid.NAMESPACE_URL, url))
            cur.execute(
                "INSERT INTO ggmc_notices__documents (_id, url, status) "
                "VALUES (%s, %s, 'text_ready') ON CONFLICT DO NOTHING",
                (doc_id, url),
            )
    text_file = tmp_path / (Path(asset_name).stem + ".txt")
    text_file.write_text(read_asset(asset_name), encoding="utf-8")
    return {"_id": doc_id, "url": url, "text_path": str(text_file)}


def _get_state_row(db, url):
    with psycopg.connect(autocommit=True, **db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, parse_status, parse_notes, error "
                "FROM ggmc_notices__documents WHERE url = %s",
                (url,),
            )
            return cur.fetchone()


# ---------- deterministic parser unit tests ----------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("6° 27' 0.00\"N", 6.45),
        ("59° 57' 0.00\"W", -59.95),
        ("59º57'0.00\"W", -59.95),  # unicode degree sign
        ("6º51'.086\"N", 6 + 51.086 / 60),  # misplaced minute tick
        ("6°27'", 6.45),  # missing direction -> inferred North
        ("5°48'58.68\"", round(5 + 48 / 60 + 58.68 / 3600, 6)),
    ],
)
def test_parse_dms_quirks(raw, expected):
    assert abs(parse_dms(raw) - expected) < 1e-6


def test_text_to_featurecollection_clean_fixture():
    result = text_to_featurecollection(read_asset("mines_notice_clean.txt"), DOC_URL)
    assert result["notice_blocks"] == 2
    assert len(result["features"]) == 2
    assert result["failed_blocks"] == []

    first = result["features"][0]
    props = first["properties"]
    assert props["file_reference"] == "GS1: 3746/15/2024"
    assert props["applicant_name"] == "JOHN PAUL"
    assert props["permit_category"] == "Prospecting"
    assert props["mining_district"] == "Mazaruni Mining District No. 3"
    assert props["topographic_map"] == "32NW"
    assert props["objection_period_days"] == 21
    assert props["objection_deadline_date"] == "2026-02-05"

    ring = first["geometry"]["coordinates"][0]
    assert ring[0] == ring[-1]  # polygon closed
    assert len(ring) >= 4


def test_text_to_featurecollection_ocr_typos_fixture():
    result = text_to_featurecollection(
        read_asset("mines_notice_ocr_typos.txt"), DOC_URL
    )
    assert len(result["features"]) == 1
    assert result["failed_blocks"] == []


def test_text_to_featurecollection_lt3_vertices_fails():
    result = text_to_featurecollection(
        read_asset("mines_notice_lt3_vertices.txt"), DOC_URL
    )
    assert result["notice_blocks"] == 1
    assert len(result["features"]) == 0
    assert "under 3 boundary vertices" in result["failed_blocks"][0]["error"]


def test_text_to_featurecollection_orders_has_no_blocks():
    result = text_to_featurecollection(read_asset("orders_no_notices.txt"), DOC_URL)
    assert result["notice_blocks"] == 0
    assert result["features"] == []


def test_feature_ids_are_deterministic():
    text = read_asset("mines_notice_clean.txt")
    r1 = text_to_featurecollection(text, DOC_URL)
    r2 = text_to_featurecollection(text, "https://elsewhere.test/doc.pdf")
    ids1 = [f["id"] for f in r1["features"]]
    assert ids1 == [f["id"] for f in text_to_featurecollection(text, DOC_URL)["features"]]
    # source url is part of the key
    assert set(ids1).isdisjoint({f["id"] for f in r2["features"]})


def test_out_of_bounds_polygon_raises_schema_drift():
    fake = read_asset("mines_notice_clean.txt").replace("59°", "11°").replace("60°", "11°")
    result = text_to_featurecollection(fake, DOC_URL)
    assert result["features"] == []
    assert len(result["failed_blocks"]) == 2
    assert "outside Guyana" in result["failed_blocks"][0]["error"]


def test_build_alerts_statistics_shape():
    result = text_to_featurecollection(read_asset("mines_notice_clean.txt"), DOC_URL)
    stats = build_alerts_statistics(result["features"])
    # alerts_twilio-compatible keys
    assert set(stats) == {"total_alerts", "date", "description_alerts"}
    assert stats["total_alerts"] == "2"
    assert stats["date"] == "15/1/2026"
    assert "Prospecting" in stats["description_alerts"]


def test_build_alerts_statistics_without_dates():
    assert (
        build_alerts_statistics([{"properties": {"notice_date": "UNKNOWN_DATE"}}])
        is None
    )


# ---------- failure alerting (no automatic recovery) ----------


def test_failed_document_logs_alerts_and_fails_run(pg_database, tmp_path):
    entry = _texts_entry(tmp_path, pg_database, "mines_notice_lt3_vertices.txt")
    with pytest.raises(RuntimeError) as excinfo:
        main(
            [entry],
            pg_database,
            "ggmc_notices",
            destination_path=str(tmp_path / "out"),
        )
    assert "could not be parsed" in str(excinfo.value)
    assert entry["url"] in str(excinfo.value)

    status, parse_status, notes, error = _get_state_row(pg_database, entry["url"])
    assert status == "error"
    assert parse_status == "failed"
    assert "under 3 boundary vertices" in json.loads(notes)["regex_failures"][0]["error"]
    assert error == "Unresolvable notice blocks; see parse_notes"


def test_partially_broken_run_still_records_good_documents(pg_database, tmp_path):
    good = _texts_entry(
        tmp_path, pg_database, "mines_notice_clean.txt",
        url="https://ggmc.test/ggmc_web/wp-content/uploads/2026/07/good.pdf",
    )
    broken = _texts_entry(
        tmp_path, pg_database, "mines_notice_lt3_vertices.txt",
        url="https://ggmc.test/ggmc_web/wp-content/uploads/2026/07/broken.pdf",
    )
    with pytest.raises(RuntimeError):
        main(
            [good, broken],
            pg_database,
            "ggmc_notices",
            destination_path=str(tmp_path / "out"),
        )

    status, parse_status, _, _ = _get_state_row(pg_database, good["url"])
    assert (status, parse_status) == ("processed", "parsed")
    status, parse_status, _, _ = _get_state_row(pg_database, broken["url"])
    assert (status, parse_status) == ("error", "failed")
    # the merged file was written before the run was failed
    assert tmp_path.joinpath("out").exists()


def test_llm_fallback_machinery_removed():
    assert not hasattr(parse_module, "llm_extract")
    assert not hasattr(parse_module, "validate_llm_features")
    assert not hasattr(parse_module, "LLM_SYSTEM_PROMPT")


def test_clean_document_does_not_touch_ocr_or_llm(pg_database, tmp_path):
    entry = _texts_entry(tmp_path, pg_database, "mines_notice_clean.txt")
    result = main(
        [entry],
        pg_database,
        "ggmc_notices",
        destination_path=str(tmp_path / "out"),
    )
    assert result["documents"][0]["parse_status"] == "parsed"
    assert result["gazettes_statistics"]["total_alerts"] == "2"


def test_orders_document_is_not_an_error(pg_database, tmp_path):
    entry = _texts_entry(tmp_path, pg_database, "orders_no_notices.txt")
    result = main(
        [entry],
        pg_database,
        "ggmc_notices",
        destination_path=str(tmp_path / "out"),
    )
    assert result["documents"][0]["parse_status"] == "no_notices"
    assert result["geojson_path"] is None
    assert result["gazettes_statistics"] is None
    assert result["failed"] == []

    status, parse_status, _, _ = _get_state_row(pg_database, entry["url"])
    assert status == "processed"
    assert parse_status == "no_notices"
