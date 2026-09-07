import logging
from pathlib import Path

import psycopg
import pytest
import responses

from f.connectors.gazettes.gazettes_download import main, _url_filename
from f.connectors.gazettes.gazettes_scrape import main as scrape, _state_table_name
from f.connectors.gazettes.tests.assets.site_data import (
    PDF_URL_FEB,
    PDF_URL_JULY,
    PDF_URL_ORDER,
    SITE_URL,
    read_asset,
    read_asset_bytes,
)

logger = logging.getLogger(__name__)

ETAG = '"etag-v1"'


def _get_state_row(db, url):
    table = _state_table_name("ggmc_notices")
    with psycopg.connect(autocommit=True, **db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status, local_path, etag, content_length, error FROM {table} WHERE url = %s",
                (url,),
            )
            return cur.fetchone()


def test_url_filename_is_stable_and_unique():
    name1 = _url_filename(PDF_URL_JULY)
    name2 = _url_filename(PDF_URL_JULY)
    assert name1 == name2
    assert name1.endswith(".pdf")
    assert name1.startswith("Mines-4th-July-2026__")
    assert name1 != _url_filename(PDF_URL_FEB)
    # same basename at different paths must not collide
    other = "https://ggmc.test/ggmc_web/wp-content/uploads/2025/12/Mines-4th-July-2026.pdf"
    assert _url_filename(other) != name1


@responses.activate
def test_download_e2e_and_state_update(pg_database, tmp_path):
    pdf_bytes = read_asset_bytes("gazette_clean.pdf")
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    docs = scrape(SITE_URL, pg_database, "ggmc_notices")["new_docs"]

    for url in (PDF_URL_JULY, PDF_URL_FEB, PDF_URL_ORDER):
        responses.get(url, body=pdf_bytes, status=200, headers={"ETag": ETAG})

    result = main(docs, pg_database, "ggmc_notices", cache_dir=str(tmp_path))

    assert len(result["new_pdfs"]) == 3
    assert result["errors"] == []
    assert result["cached"] == 0
    for entry in result["new_pdfs"]:
        assert (tmp_path / entry["path"].split("/")[-1]).exists()
        status, local_path, etag, size, _ = _get_state_row(pg_database, entry["url"])
        assert status == "downloaded"
        assert local_path == entry["path"]
        assert etag == ETAG
        assert size == len(pdf_bytes)


@responses.activate
def test_download_skips_cached_files(pg_database, tmp_path):
    pdf_bytes = read_asset_bytes("gazette_clean.pdf")
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    docs = scrape(SITE_URL, pg_database, "ggmc_notices")["new_docs"]
    for url in (PDF_URL_JULY, PDF_URL_FEB, PDF_URL_ORDER):
        responses.get(url, body=pdf_bytes, status=200, headers={"ETag": ETAG})
    main(docs, pg_database, "ggmc_notices", cache_dir=str(tmp_path))

    # simulate a retry where scrape re-returned docs but files are cached and current
    responses.head(PDF_URL_JULY, status=200, headers={"ETag": ETAG})
    result = main(
        [d for d in docs if d["url"] == PDF_URL_JULY],
        pg_database,
        "ggmc_notices",
        cache_dir=str(tmp_path),
    )
    assert result["new_pdfs"] == []
    assert result["cached"] == 1


@responses.activate
def test_download_detects_same_url_content_change(pg_database, tmp_path):
    old_bytes = read_asset_bytes("gazette_clean.pdf")
    new_bytes = b"%PDF-1.4\n" + b"x" * 5000 + b"\n%%EOF\n"
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    docs = scrape(SITE_URL, pg_database, "ggmc_notices")["new_docs"]
    july = [d for d in docs if d["url"] == PDF_URL_JULY]

    responses.get(PDF_URL_JULY, body=old_bytes, status=200, headers={"ETag": ETAG})
    main(july, pg_database, "ggmc_notices", cache_dir=str(tmp_path))

    # remote file replaced under same URL: HEAD now returns a different etag
    responses.reset()
    responses.head(PDF_URL_JULY, status=200, headers={"ETag": '"etag-v2"'})
    responses.get(PDF_URL_JULY, body=new_bytes, status=200, headers={"ETag": '"etag-v2"'})
    result = main(july, pg_database, "ggmc_notices", cache_dir=str(tmp_path))

    assert len(result["new_pdfs"]) == 1
    assert tmp_path.joinpath(Path(result["new_pdfs"][0]["path"]).name).read_bytes() == new_bytes
    status, _, etag, size, _ = _get_state_row(pg_database, PDF_URL_JULY)
    assert status == "downloaded"
    assert etag == '"etag-v2"'
    assert size == len(new_bytes)


@responses.activate
def test_download_records_http_error_in_state(pg_database, tmp_path):
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    docs = scrape(SITE_URL, pg_database, "ggmc_notices")["new_docs"]

    responses.get(PDF_URL_JULY, body=b"missing", status=404)
    responses.get(PDF_URL_FEB, body=b"missing", status=404)
    responses.get(PDF_URL_ORDER, body=b"missing", status=404)

    result = main(docs, pg_database, "ggmc_notices", cache_dir=str(tmp_path))
    assert result["new_pdfs"] == []
    assert len(result["errors"]) == 3
    status, _, _, _, error = _get_state_row(pg_database, PDF_URL_JULY)
    assert status == "error"
    assert "404" in error


@responses.activate
def test_download_rejects_non_pdf_content(pg_database, tmp_path):
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    docs = scrape(SITE_URL, pg_database, "ggmc_notices")["new_docs"]
    july = [d for d in docs if d["url"] == PDF_URL_JULY]

    responses.get(PDF_URL_JULY, body=b"<html>not a pdf</html>", status=200)
    result = main(july, pg_database, "ggmc_notices", cache_dir=str(tmp_path))

    assert result["new_pdfs"] == []
    assert "not a PDF" in result["errors"][0]["error"]
    # no partial file left behind
    assert list(tmp_path.glob("*.pdf")) == []


@responses.activate
def test_download_passes_cached_errored_docs_downstream(pg_database, tmp_path):
    """include_errored retries must reach the parser even though the file is cached."""
    pdf_bytes = read_asset_bytes("gazette_clean.pdf")
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    docs = scrape(SITE_URL, pg_database, "ggmc_notices")["new_docs"]
    july = [d for d in docs if d["url"] == PDF_URL_JULY]
    responses.get(PDF_URL_JULY, body=pdf_bytes, status=200, headers={"ETag": ETAG})
    main(july, pg_database, "ggmc_notices", cache_dir=str(tmp_path))

    # mark as error (e.g. parser failure), HEAD says cached copy is current
    table = _state_table_name("ggmc_notices")
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE {table} SET status = 'error' WHERE url = %s", (PDF_URL_JULY,))
    responses.head(PDF_URL_JULY, status=200, headers={"ETag": ETAG})

    result = main(july, pg_database, "ggmc_notices", cache_dir=str(tmp_path))
    assert len(result["new_pdfs"]) == 1
    assert result["new_pdfs"][0]["changed"] is False

