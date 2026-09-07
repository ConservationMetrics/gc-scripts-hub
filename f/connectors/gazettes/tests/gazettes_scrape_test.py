import logging
import psycopg
import pytest
import responses

from f.connectors.gazettes.gazettes_scrape import main, _extract_pdf_links, _state_table_name
from f.connectors.gazettes.tests.assets.site_data import (
    PDF_BASE,
    PDF_URL_FEB,
    PDF_URL_JULY,
    PDF_URL_ORDER,
    SITE_URL,
    read_asset,
)

logger = logging.getLogger(__name__)


def _table_exists(db, table):
    with psycopg.connect(autocommit=True, **db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
                (table,),
            )
            return cur.fetchone()[0]


def test_extract_pdf_links_absolute_and_relative():
    html = read_asset("gazettes_page_sample.html")
    docs = _extract_pdf_links(html, SITE_URL, "a[href$='.pdf']", None)
    urls = {d["url"] for d in docs}
    # the three real PDF links, deduplicated
    assert urls == {PDF_URL_JULY, PDF_URL_FEB, PDF_URL_ORDER}
    # contact-us and docx links are excluded by the selector
    assert all(u.endswith(".pdf") for u in urls)
    # link_text falls back to anchor text
    july = next(d for d in docs if d["url"] == PDF_URL_JULY)
    assert july["link_text"] == "Mines 4th July, 2026"


def test_extract_pdf_links_relative_resolution():
    html = read_asset("gazettes_page_relative.html")
    docs = _extract_pdf_links(html, SITE_URL, "a[href$='.pdf']", None)
    urls = {d["url"] for d in docs}
    assert "https://ggmc.test/ggmc_web/wp-content/uploads/2025/12/Mines-27th-September-2025.pdf" in urls


def test_extract_pdf_links_pattern_filter():
    html = read_asset("gazettes_page_sample.html")
    docs = _extract_pdf_links(html, SITE_URL, "a[href$='.pdf']", r"/2026/07/Mines-4th")
    assert [d["url"] for d in docs] == [PDF_URL_JULY]


def test_extract_pdf_links_no_pdfs():
    html = read_asset("gazettes_page_no_pdfs.html")
    assert _extract_pdf_links(html, SITE_URL, "a[href$='.pdf']", None) == []


def test_extract_pdf_links_dedupes_repeated_href():
    html = (
        "<html><body>"
        f"<a href='{PDF_URL_JULY}'>one</a>"
        f"<a href='{PDF_URL_JULY}'>duplicate</a>"
        "</body></html>"
    )
    docs = _extract_pdf_links(html, SITE_URL, "a[href$='.pdf']", None)
    assert len(docs) == 1


def test_extract_pdf_links_skips_non_http_scheme():
    html = "<html><body><a href='javascript:void(0)' class='x'>js</a></body></html>"
    assert _extract_pdf_links(html, SITE_URL, "a.x", None) == []


@responses.activate
def test_scrape_e2e_new_docs_and_state_table(pg_database):
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    result = main(SITE_URL, pg_database, "ggmc_notices")
    state = _state_table_name("ggmc_notices")

    assert _table_exists(pg_database, state)
    assert len(result["new_docs"]) == 3
    assert result["stats"] == {"found": 3, "new": 3, "errored": 0}
    assert all(d["status"] == "new" for d in result["new_docs"])

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT status, COUNT(*) FROM {state} GROUP BY status")
            assert cur.fetchall() == [("new", 3)]


@responses.activate
def test_scrape_is_incremental_second_run_returns_no_new(pg_database):
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    first = main(SITE_URL, pg_database, "ggmc_notices")
    second = main(SITE_URL, pg_database, "ggmc_notices")
    assert len(first["new_docs"]) == 3
    assert second["new_docs"] == []

    # but the docs stay recorded (last_seen refreshed)
    state = _state_table_name("ggmc_notices")
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {state}")
            assert cur.fetchone()[0] == 3


@responses.activate
def test_scrape_detects_a_new_update_on_the_site(pg_database):
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    main(SITE_URL, pg_database, "ggmc_notices")

    base_page = read_asset("gazettes_page_sample.html")
    new_page = base_page.replace(
        "</div>\n</body>",
        f"<p><a href='{PDF_BASE}/Mines-20th-December-2025.pdf'>Mines 20th Dec 2025</a></p></div>\n</body>",
    )
    assert new_page != base_page  # inserted inside the #content area
    responses.reset()
    responses.get(SITE_URL, body=new_page, status=200)
    second = main(SITE_URL, pg_database, "ggmc_notices")
    assert len(second["new_docs"]) == 1
    assert second["new_docs"][0]["url"].endswith("Mines-20th-December-2025.pdf")


@responses.activate
def test_scrape_include_errored_returns_errored_docs(pg_database):
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    main(SITE_URL, pg_database, "ggmc_notices")

    state = _state_table_name("ggmc_notices")
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE {state} SET status = 'error' WHERE url = %s", (PDF_URL_JULY,))

    again = main(SITE_URL, pg_database, "ggmc_notices", include_errored=False)
    assert again["new_docs"] == []

    retried = main(SITE_URL, pg_database, "ggmc_notices", include_errored=True)
    assert [d["url"] for d in retried["new_docs"]] == [PDF_URL_JULY]
    assert retried["new_docs"][0]["status"] == "error"


@responses.activate
def test_scrape_raises_on_http_error(pg_database):
    responses.get(SITE_URL, body="boom", status=500)
    with pytest.raises(Exception) as excinfo:
        main(SITE_URL, pg_database, "ggmc_notices")
    assert "500" in str(excinfo.value)


@responses.activate
def test_scrape_custom_selector_and_null_defaults(pg_database):
    html = "<html><body><div class='doc'><a href='report.pdf'>r</a></div></body></html>"
    responses.get(SITE_URL, body=html, status=200)
    result = main(
        SITE_URL,
        pg_database,
        "custom_notices",
        pdf_selector="div.doc a",
        link_pattern=None,
        user_agent=None,
        request_timeout=None,
    )
    assert result["new_docs"][0]["url"] == "https://ggmc.test/gazettes/report.pdf"
