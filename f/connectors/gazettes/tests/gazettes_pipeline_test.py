"""End-to-end pipeline test: scrape -> download -> to_text -> parse -> filter -> DB.

Proves the scripts chain together (as the Windmill flow does) with a real
PostgreSQL and mocked HTTP, and that the result is explorer-compatible via
the repo's postgres_to_geojson export script.
"""

import json
import logging
from pathlib import Path

import psycopg
import responses

from f.connectors.gazettes.gazettes_download import main as download
from f.connectors.gazettes.gazettes_filter_by_boundary import main as filter_boundary
from f.connectors.gazettes.gazettes_ggmc_parse import main as ggmc_parse
from f.connectors.gazettes.gazettes_pdf_to_text import main as pdf_to_text
from f.connectors.gazettes.gazettes_scrape import main as scrape
from f.connectors.gazettes.tests.assets.site_data import (
    ASSETS,
    PDF_URL_FEB,
    PDF_URL_JULY,
    PDF_URL_ORDER,
    SITE_URL,
    read_asset,
    read_asset_bytes,
)
from f.connectors.geojson.geojson_to_postgres import main as geojson_to_postgres
from f.export.postgres_to_file.postgres_to_geojson import format_data_as_geojson

logger = logging.getLogger(__name__)


@responses.activate
def test_full_pipeline_to_database(pg_database, tmp_path):
    pdf_bytes = read_asset_bytes("gazette_clean.pdf")

    # 1. scrape
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    for url in (PDF_URL_JULY, PDF_URL_FEB, PDF_URL_ORDER):
        responses.get(url, body=pdf_bytes, status=200, headers={"ETag": '"etag-v1"'})

    scraped = scrape(SITE_URL, pg_database, "ggmc_notices")
    assert len(scraped["new_docs"]) == 3

    # 2. download into cache
    cache_dir = tmp_path / "pdf_cache"
    downloaded = download(
        scraped["new_docs"], pg_database, "ggmc_notices", cache_dir=str(cache_dir)
    )
    assert len(downloaded["new_pdfs"]) == 3

    # 3. pdf -> text
    extracted = pdf_to_text(
        downloaded["new_pdfs"], db=pg_database, db_table_name="ggmc_notices"
    )
    assert len(extracted["texts"]) == 3

    # 4. text -> geojson (regex only; no LLM needed for these fixtures)
    parsed = ggmc_parse(
        extracted["texts"],
        pg_database,
        "ggmc_notices",
        destination_path=str(tmp_path / "datalake"),
    )
    assert parsed["gazettes_statistics"]["total_alerts"] == "6"  # 2 notices x 3 docs
    assert Path(parsed["geojson_path"]).exists()

    # 5. boundary filter
    filtered = filter_boundary(
        parsed["geojson_path"], str(ASSETS / "boundary_covers.geojson")
    )
    assert filtered["kept"] == 6

    # 6. database load (existing repo connector)
    geojson_to_postgres(
        pg_database, "ggmc_notices", filtered["geojson_path"], attachment_root="/"
    )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ggmc_notices")
            assert cur.fetchone()[0] == 6

            # explorer-compatible geometry columns
            cur.execute(
                "SELECT _id, g__type, g__coordinates, source_document, file_reference "
                "FROM ggmc_notices ORDER BY file_reference"
            )
            rows = cur.fetchall()
            assert all(r[1] == "Polygon" for r in rows)
            assert {r[4] for r in rows} == {
                "GS1: 3746/15/2024",
                "GS2: 3746/16/2025",
            }
            json.loads(rows[0][2])  # coordinates are valid JSON text

    # 7. second scheduled run finds nothing new (incremental tracking)
    responses.reset()
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    second_scrape = scrape(SITE_URL, pg_database, "ggmc_notices")
    assert second_scrape["new_docs"] == []


@responses.activate
def test_pipeline_export_back_to_geojson_for_explorer(pg_database, tmp_path):
    """The loaded table must round-trip through the repo's export script."""
    from f.export.postgres_to_file.postgres_to_geojson import main as to_geojson

    pdf_bytes = read_asset_bytes("gazette_clean.pdf")
    responses.get(SITE_URL, body=read_asset("gazettes_page_sample.html"), status=200)
    for url in (PDF_URL_JULY, PDF_URL_FEB, PDF_URL_ORDER):
        responses.get(url, body=pdf_bytes, status=200, headers={"ETag": '"etag-v1"'})

    scraped = scrape(SITE_URL, pg_database, "ggmc_notices")
    downloaded = download(
        scraped["new_docs"],
        pg_database,
        "ggmc_notices",
        cache_dir=str(tmp_path / "pdf_cache"),
    )
    extracted = pdf_to_text(
        downloaded["new_pdfs"], db=pg_database, db_table_name="ggmc_notices"
    )
    parsed = ggmc_parse(
        extracted["texts"],
        pg_database,
        "ggmc_notices",
        destination_path=str(tmp_path / "datalake"),
    )
    geojson_to_postgres(
        pg_database, "ggmc_notices", parsed["geojson_path"], attachment_root="/"
    )

    # read back like the export script does and rebuild a FeatureCollection
    from f.common_logic.db_operations import conninfo, fetch_data_from_postgres

    data = fetch_data_from_postgres(conninfo(pg_database), "ggmc_notices")
    fc = format_data_as_geojson(data)
    assert len(fc["features"]) == 6
    geom = fc["features"][0]["geometry"]
    assert geom["type"] == "Polygon"
    assert geom["coordinates"][0][0] == geom["coordinates"][0][-1]  # closed ring
    assert fc["features"][0]["properties"]["file_reference"].startswith("GS")
