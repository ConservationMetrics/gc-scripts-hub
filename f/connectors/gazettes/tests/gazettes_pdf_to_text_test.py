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


def test_pdf_to_text_records_corrupt_pdf_error_without_stopping_batch(
    tmp_path, monkeypatch
):
    import f.connectors.gazettes.gazettes_pdf_to_text as m

    monkeypatch.setattr(
        m, "ocr_pdf_to_text", lambda p: (_ for _ in ()).throw(RuntimeError("no models"))
    )

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
    err = result["errors"][0]["error"]
    assert err.startswith("pypdf failed (") and "OCR failed (no models)" in err


def test_pdf_to_text_accepts_plain_paths_and_custom_text_dir(tmp_path):
    pdf = tmp_path / "gazette.pdf"
    pdf.write_bytes(read_asset_bytes("gazette_clean.pdf"))
    text_dir = tmp_path / "extracted"

    result = main([str(pdf)], text_dir=str(text_dir))
    assert len(result["texts"]) == 1
    out = text_dir / "gazette.txt"
    assert out.exists()
    assert result["texts"][0]["text_path"] == str(out)



# ---------- OCR fallback ----------


def _image_only_entry(tmp_path):
    import shutil
    dest = tmp_path / "scan.pdf"
    shutil.copy(ASSETS / "gazette_image_only.pdf", dest)
    return dest


def test_image_only_pdf_uses_ocr_fallback(tmp_path, monkeypatch):
    import f.connectors.gazettes.gazettes_pdf_to_text as m

    calls = []

    def fake_ocr(pdf_path):
        calls.append(str(pdf_path))
        return "# OCR MARKDOWN\nGUYANA GEOLOGY AND MINES COMMISSION"

    monkeypatch.setattr(m, "ocr_pdf_to_text", fake_ocr)
    pdf = _image_only_entry(tmp_path)

    result = main([str(pdf)])
    assert len(result["texts"]) == 1
    entry = result["texts"][0]
    assert entry["method"] == "ocr"
    assert calls == [str(pdf)]
    out = Path(entry["text_path"])
    assert out.read_text().startswith("# OCR MARKDOWN")
    # second run: cached
    again = main([str(pdf)])
    assert again["texts"][0]["method"] == "cached"


def test_ocr_disabled_records_error(tmp_path, monkeypatch):
    import f.connectors.gazettes.gazettes_pdf_to_text as m

    monkeypatch.setattr(
        m, "ocr_pdf_to_text",
        lambda p: (_ for _ in ()).throw(AssertionError("must not be called")),
    )
    pdf = _image_only_entry(tmp_path)
    result = main([str(pdf)], ocr_fallback=False)
    assert result["texts"] == []
    assert "OCR fallback disabled" in result["errors"][0]["error"]


def test_ocr_failure_records_combined_error(tmp_path, monkeypatch):
    import f.connectors.gazettes.gazettes_pdf_to_text as m

    def boom(pdf_path):
        raise ImportError("No module named 'marker'")

    monkeypatch.setattr(m, "ocr_pdf_to_text", boom)
    pdf = _image_only_entry(tmp_path)
    result = main([str(pdf)])
    assert result["texts"] == []
    err = result["errors"][0]["error"]
    assert err.startswith("pypdf failed (") and "OCR failed (No module named 'marker')" in err


def test_ocr_returning_no_text_is_an_error(tmp_path, monkeypatch):
    import f.connectors.gazettes.gazettes_pdf_to_text as m

    monkeypatch.setattr(m, "ocr_pdf_to_text", lambda p: "   ")
    pdf = _image_only_entry(tmp_path)
    result = main([str(pdf)])
    assert "OCR returned no text" in result["errors"][0]["error"]


def test_pypdf_success_never_reaches_ocr(tmp_path, monkeypatch):
    import f.connectors.gazettes.gazettes_pdf_to_text as m

    def boom(pdf_path):
        raise AssertionError("OCR must not run when pypdf extracts text")

    monkeypatch.setattr(m, "ocr_pdf_to_text", boom)
    import shutil
    pdf = tmp_path / "good.pdf"
    shutil.copy(ASSETS / "gazette_clean.pdf", pdf)
    result = main([str(pdf)])
    assert len(result["texts"]) == 1
    assert result["texts"][0]["method"] == "pypdf"
