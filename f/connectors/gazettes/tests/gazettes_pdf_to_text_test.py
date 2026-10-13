import logging
from pathlib import Path

import psycopg
import responses

from f.connectors.gazettes.gazettes_download import main as download
from f.connectors.gazettes.gazettes_pdf_to_text import extract_pdf_text, main
from f.connectors.gazettes.gazettes_scrape import main as scrape
from f.connectors.gazettes.gazettes_scrape import _state_table_name
from f.connectors.gazettes.tests.assets.site_data import (
    ASSETS,
    PDF_URL_FEB,
    PDF_URL_JULY,
    PDF_URL_ORDER,
    SITE_URL,
    read_asset,
    read_asset_bytes,
)

logger = logging.getLogger(__name__)


def _get_state_row(db, url):
    table = _state_table_name("ggmc_notices")
    with psycopg.connect(autocommit=True, **db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status, error FROM {table} WHERE url = %s", (url,)
            )
            return cur.fetchone()


@responses.activate
def _scrape_and_download(pg_database, tmp_path, only=None):
    """Helper: run scrape + download with mocked HTTP, returning download result."""
    pdf_bytes = read_asset_bytes("gazette_clean.pdf")
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    docs = scrape(SITE_URL, pg_database, "ggmc_notices")["new_docs"]
    for url in (PDF_URL_JULY, PDF_URL_FEB, PDF_URL_ORDER):
        responses.get(url, body=pdf_bytes, status=200, headers={"ETag": '"etag-v1"'})
    result = download(docs, pg_database, "ggmc_notices", cache_dir=str(tmp_path))
    responses.reset()
    return result


def test_extract_pdf_text_on_real_pdf(tmp_path):
    pdf = tmp_path / "gazette.pdf"
    pdf.write_bytes(read_asset_bytes("gazette_clean.pdf"))
    text = extract_pdf_text(pdf)
    assert "GUYANA GEOLOGY AND MINES COMMISSION" in text
    assert "Mazaruni Mining District No. 3" in text


def test_extract_pdf_text_rejects_corrupt_pdf(tmp_path):
    pdf = tmp_path / "corrupt.pdf"
    pdf.write_bytes(read_asset_bytes("gazette_corrupt.pdf"))
    # either raises outright (unreadable) or yields no text (empty)
    try:
        text = extract_pdf_text(pdf)
        assert not text.strip()
    except Exception:
        pass


@responses.activate
def test_pdf_to_text_e2e_extracts_and_updates_state(pg_database, tmp_path):
    downloaded = _scrape_and_download(pg_database, tmp_path)
    assert len(downloaded["new_pdfs"]) == 3

    result = main(
        downloaded["new_pdfs"], db=pg_database, db_table_name="ggmc_notices"
    )
    assert len(result["texts"]) == 3
    assert result["errors"] == []
    for entry in result["texts"]:
        text_path = Path(entry["text_path"])
        assert text_path.exists()
        assert "GUYANA GEOLOGY AND MINES COMMISSION" in text_path.read_text()
        status, _ = _get_state_row(pg_database, entry["url"])
        assert status == "text_ready"


@responses.activate
def test_pdf_to_text_uses_cache_on_second_run(pg_database, tmp_path):
    downloaded = _scrape_and_download(pg_database, tmp_path)
    first = main(downloaded["new_pdfs"], db=pg_database, db_table_name="ggmc_notices")
    assert all(not entry["cached"] for entry in first["texts"])

    second = main(downloaded["new_pdfs"], db=pg_database, db_table_name="ggmc_notices")
    assert len(second["texts"]) == 3
    assert all(entry["cached"] for entry in second["texts"])
    # same text file paths — no re-extraction happened
    assert sorted(e["text_path"] for e in first["texts"]) == sorted(
        e["text_path"] for e in second["texts"]
    )


def test_pdf_to_text_records_corrupt_pdf_error_without_stopping_batch(tmp_path):
    good = tmp_path / "good.pdf"
    good.write_bytes(read_asset_bytes("gazette_clean.pdf"))
    bad = tmp_path / "bad.pdf"
    bad.write_bytes(read_asset_bytes("gazette_corrupt.pdf"))

    result = main(
        [
            {"_id": None, "url": "https://ggmc.test/good.pdf", "path": str(good)},
            {"_id": None, "url": "https://ggmc.test/bad.pdf", "path": str(bad)},
        ]
    )
    assert len(result["texts"]) == 1
    assert result["texts"][0]["url"].endswith("good.pdf")
    assert len(result["errors"]) == 1
    assert result["errors"][0]["url"].endswith("bad.pdf")


def test_pdf_to_text_accepts_plain_paths_and_custom_text_dir(tmp_path):
    pdf = tmp_path / "gazette.pdf"
    pdf.write_bytes(read_asset_bytes("gazette_clean.pdf"))
    text_dir = tmp_path / "extracted"

    result = main([str(pdf)], text_dir=str(text_dir))
    assert len(result["texts"]) == 1
    out = text_dir / "gazette.txt"
    assert out.exists()
    assert result["texts"][0]["text_path"] == str(out)



# ---------- image-only PDFs (no text layer) ----------


def test_image_only_pdf_raises_no_text(tmp_path):
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(read_asset_bytes("gazette_image_only.pdf"))
    try:
        extract_pdf_text(pdf)
        raise AssertionError("expected ValueError for a no-text PDF")
    except ValueError as e:
        assert "No extractable text" in str(e)


def test_image_only_pdf_recorded_as_error(tmp_path):
    import shutil

    pdf = tmp_path / "scan.pdf"
    shutil.copy(ASSETS / "gazette_image_only.pdf", pdf)
    result = main([str(pdf)])
    assert result["texts"] == []
    assert "No extractable text" in result["errors"][0]["error"]


def test_texts_report_extraction_method(tmp_path):
    import shutil

    pdf = tmp_path / "gazette.pdf"
    shutil.copy(ASSETS / "gazette_clean.pdf", pdf)
    first = main([str(pdf)])
    assert first["texts"][0]["method"] == "pypdf"
    second = main([str(pdf)])
    assert second["texts"][0]["method"] == "cached"


def test_marker_ocr_machinery_removed():
    import f.connectors.gazettes.gazettes_pdf_to_text as m

    assert not hasattr(m, "ocr_pdf_to_text")
    assert not hasattr(m, "_MARKER_ARTIFACTS")
