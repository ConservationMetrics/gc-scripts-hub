import json
import logging
import re
from datetime import datetime
from urllib.parse import parse_qs, unquote, urlparse

import pytest
import responses
from psycopg import connect, sql

from f.connectors.gbif import gbif_pull_statistics
from f.connectors.gbif.tests.assets import server_responses

_SEARCH_URL = re.compile(r"https://api\.gbif\.org/v1/occurrence/search")
_REGISTRY_URL = re.compile(r"https://api\.gbif\.org/v1/(dataset|organization|species)/")


def test_statistics_writes_excerpt_and_replaces_stale_rows(
    mocked_responses, pg_database, statistics_snapshot, caplog
):
    _register_snapshot(mocked_responses, statistics_snapshot)
    bounds = json.dumps(statistics_snapshot["bounds"])
    with caplog.at_level(logging.INFO, logger="f.connectors.gbif.gbif_pull_statistics"):
        _run_excerpt_import(mocked_responses, pg_database, statistics_snapshot, bounds)
    assert "Starting GBIF statistics for gbif_occurrences." in caplog.text
    assert "Requesting GBIF speciesKey facet at offset 0." in caplog.text
    assert "Resolving 22 GBIF registry names." in caplog.text
    assert "Resolved 1 of 22 GBIF registry names." in caplog.text
    assert "Replacing GBIF statistics table gbif_occurrences." in caplog.text
    assert "Finished GBIF statistics for gbif_occurrences." in caplog.text


def _run_excerpt_import(mocked_responses, pg_database, statistics_snapshot, bounds):
    with (
        connect(**pg_database, autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        result = gbif_pull_statistics.main(bounds, pg_database, "GBIF_Occurrences")
        assert result == {
            "occurrence_count": 856,
            "dataset": 6,
            "publisher": 5,
            "year": 8,
            "species": 11,
            "basis_of_record": 3,
        }
        _assert_snapshot_tables(cursor, "gbif_occurrences", statistics_snapshot)
        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'gbif_occurrences'
            ORDER BY ordinal_position
            """
        )
        assert cursor.fetchall() == [
            ("facet", "text"),
            ("key", "text"),
            ("label", "text"),
            ("count", "bigint"),
        ]
        _assert_import_timestamp(cursor, "statistics_imported_at")
        cursor.execute(
            "INSERT INTO gbif_occurrences (facet, key, label, count) "
            "VALUES ('dataset', 'stale', 'Stale dataset', 1)"
        )
        calls_after_first_run = len(mocked_responses.calls)
        gbif_pull_statistics.main(bounds, pg_database, "gbif_occurrences")
        cursor.execute("SELECT count(*) FROM gbif_occurrences WHERE key = 'stale'")
        assert cursor.fetchone()[0] == 0
        _assert_import_timestamp(cursor, "statistics_imported_at")
        _assert_snapshot_tables(cursor, "gbif_occurrences", statistics_snapshot)

    search_calls = [
        call
        for call in mocked_responses.calls[:calls_after_first_run]
        if "/occurrence/search" in call.request.url
    ]
    registry_calls = calls_after_first_run - len(search_calls)
    assert len(search_calls) == 5
    assert registry_calls == 6 + 5 + 11
    assert len(mocked_responses.calls) == calls_after_first_run * 2
    assert all(
        call.request.headers["User-Agent"].startswith("GuardianConnector")
        for call in mocked_responses.calls
    )


def test_statistics_pages_through_facet_results(
    mocked_responses, pg_database, statistics_snapshot, monkeypatch
):
    monkeypatch.setattr(gbif_pull_statistics, "_FACET_LIMIT", 2)
    _register_snapshot(mocked_responses, statistics_snapshot)
    gbif_pull_statistics.main(
        statistics_snapshot["bounds"], pg_database, "gbif_occurrences"
    )
    offsets = [
        int(parse_qs(urlparse(call.request.url).query)["facetOffset"][0])
        for call in mocked_responses.calls
        if parse_qs(urlparse(call.request.url).query).get("facet") == ["datasetKey"]
    ]
    # Six dataset values at page size 2, plus the empty page that ends the scan.
    assert offsets == [0, 2, 4, 6]
    with connect(**pg_database) as connection, connection.cursor() as cursor:
        _assert_snapshot_tables(cursor, "gbif_occurrences", statistics_snapshot)


def test_statistics_keeps_keys_when_registry_lookup_fails(
    mocked_responses, pg_database, caplog
):
    dataset_key = server_responses.DATASET_KEY
    publisher_key = server_responses.PUBLISHING_ORG_KEY
    snapshot = {
        "occurrence_count": 4,
        "geometry": (
            "POLYGON((-55.03 3.23,-54.12 3.23,-54.12 3.67,-55.03 3.67,-55.03 3.23))"
        ),
        "facets": {
            "datasetKey": [{"name": dataset_key, "count": 4}],
            "publishingOrg": [{"name": publisher_key, "count": 4}],
            "year": [{"name": "2020", "count": 3}, {"name": "nope", "count": 1}],
            "speciesKey": [{"name": "2474363", "count": 4}],
            "basisOfRecord": [{"name": "HUMAN_OBSERVATION", "count": 4}],
        },
        "names": {
            "dataset": {},
            "organization": {publisher_key: "  Cornell Lab of Ornithology  "},
            "species": {},
        },
    }
    _register_snapshot(mocked_responses, snapshot, fail_kinds={"dataset", "species"})
    with caplog.at_level(
        logging.WARNING, logger="f.connectors.gbif.gbif_pull_statistics"
    ):
        gbif_pull_statistics.main(
            [[-55.03, 3.23], [-54.12, 3.67]], pg_database, "gbif_occurrences"
        )
    with connect(**pg_database) as connection, connection.cursor() as cursor:
        assert _fetch_facet(cursor, "gbif_occurrences", "dataset") == [
            (dataset_key, None, 4)
        ]
        assert _fetch_facet(cursor, "gbif_occurrences", "publisher") == [
            (publisher_key, "Cornell Lab of Ornithology", 4)
        ]
        assert _fetch_facet(cursor, "gbif_occurrences", "species") == [
            ("2474363", None, 4)
        ]
        assert _fetch_facet(
            cursor, "gbif_occurrences", "year", "key DESC"
        ) == [("2020", None, 3)]
    assert "Skipping GBIF year facet value 'nope'" in caplog.text
    assert "dataset statistics left 1 of 1 names unresolved" in caplog.text
    assert "species statistics left 1 of 1 names unresolved" in caplog.text


def test_statistics_stops_when_a_facet_page_repeats(
    mocked_responses, pg_database, monkeypatch
):
    monkeypatch.setattr(gbif_pull_statistics, "_FACET_LIMIT", 1)
    calls = {}

    def search(request):
        facet = parse_qs(urlparse(request.url).query)["facet"][0]
        calls[facet] = calls.get(facet, 0) + 1
        if calls[facet] > 3:
            raise RuntimeError(f"{facet} paging did not stop")
        counts = [{"name": "dataset-1", "count": 4}] if facet == "datasetKey" else []
        return (200, {}, json.dumps({"count": 4, "facets": [{"counts": counts}]}))

    def registry(_request):
        return (200, {}, json.dumps({"title": "Stuck dataset"}))

    mocked_responses.add_callback(responses.GET, _SEARCH_URL, callback=search)
    mocked_responses.add_callback(responses.GET, _REGISTRY_URL, callback=registry)
    gbif_pull_statistics.main(
        [[-55.03, 3.23], [-54.12, 3.67]], pg_database, "gbif_occurrences"
    )
    assert calls["datasetKey"] == 2
    assert calls["speciesKey"] == 1
    with connect(**pg_database) as connection, connection.cursor() as cursor:
        assert _fetch_facet(cursor, "gbif_occurrences", "dataset") == [
            ("dataset-1", "Stuck dataset", 4)
        ]


def test_statistics_creates_an_empty_table_for_an_empty_area(
    mocked_responses, pg_database, statistics_snapshot
):
    empty = {
        **statistics_snapshot,
        "occurrence_count": 0,
        "facets": {facet: [] for facet in statistics_snapshot["facets"]},
    }
    _register_snapshot(mocked_responses, empty)
    table_name = "A" * gbif_pull_statistics._MAX_TABLE_NAME_LENGTH
    result = gbif_pull_statistics.main(
        empty["bounds"], pg_database, table_name
    )
    assert result["occurrence_count"] == 0
    assert result["species"] == 0
    statistics = table_name.lower()
    assert len(statistics) == 63
    with connect(**pg_database) as connection, connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT facet, label, count FROM {}").format(
                sql.Identifier(statistics)
            )
        )
        assert cursor.fetchall() == [("statistics_imported_at", None, None)]


def test_statistics_rejects_oversized_bounds_before_request(
    mocked_responses, pg_database
):
    with pytest.raises(ValueError):
        gbif_pull_statistics.main(
            [[0, 0], [2, 2]], pg_database, "gbif_occurrences"
        )
    assert not mocked_responses.calls


@pytest.mark.parametrize("name", ["a" * 64, "bad/name", "", "bad\\name"])
def test_statistics_rejects_invalid_table_names(
    mocked_responses, pg_database, name
):
    with pytest.raises(ValueError, match="db_table_name"):
        gbif_pull_statistics.main([[-55.03, 3.23], [-54.12, 3.67]], pg_database, name)
    assert not mocked_responses.calls


def _register_snapshot(mocked, snapshot, fail_kinds=()):
    def search(request):
        params = parse_qs(urlparse(request.url).query)
        assert params["geometry"] == [snapshot["geometry"]]
        assert params["limit"] == ["0"]
        facet = params["facet"][0]
        offset = int(params["facetOffset"][0])
        limit = int(params["facetLimit"][0])
        counts = snapshot["facets"][facet][offset : offset + limit]
        body = {
            "count": snapshot["occurrence_count"],
            "facets": [{"field": facet, "counts": counts}],
        }
        return (200, {}, json.dumps(body))

    def registry(request):
        kind, key = urlparse(request.url).path.rstrip("/").split("/")[-2:]
        key = unquote(key)
        if kind in fail_kinds:
            return (503, {}, "")
        field = "scientificName" if kind == "species" else "title"
        return (200, {}, json.dumps({field: snapshot["names"][kind][key]}))

    mocked.add_callback(
        responses.GET, _SEARCH_URL, callback=search, content_type="application/json"
    )
    needs_names = any(
        snapshot["facets"][facet]
        for facet in ("datasetKey", "publishingOrg", "speciesKey")
    )
    if needs_names:
        mocked.add_callback(
            responses.GET,
            _REGISTRY_URL,
            callback=registry,
            content_type="application/json",
        )


def test_statistics_keeps_distinct_keys_that_share_a_label(
    mocked_responses, pg_database
):
    snapshot = {
        "occurrence_count": 6,
        "geometry": (
            "POLYGON((-55.03 3.23,-54.12 3.23,-54.12 3.67,-55.03 3.67,-55.03 3.23))"
        ),
        "facets": {
            "datasetKey": [
                {"name": "dataset-a", "count": 4},
                {"name": "dataset-b", "count": 2},
            ],
            "publishingOrg": [
                {"name": "org-a", "count": 4},
                {"name": "org-b", "count": 2},
            ],
            "year": [],
            "speciesKey": [
                {"name": "1", "count": 3},
                {"name": "2", "count": 3},
            ],
            "basisOfRecord": [],
        },
        "names": {
            "dataset": {"dataset-a": "Shared title", "dataset-b": "Shared title"},
            "organization": {
                "org-a": "Shared publisher",
                "org-b": "Shared publisher",
            },
            "species": {"1": "Panthera onca", "2": "Panthera onca"},
        },
    }
    _register_snapshot(mocked_responses, snapshot)
    gbif_pull_statistics.main(
        [[-55.03, 3.23], [-54.12, 3.67]], pg_database, "gbif_occurrences"
    )
    with connect(**pg_database) as connection, connection.cursor() as cursor:
        assert _fetch_facet(cursor, "gbif_occurrences", "dataset") == [
            ("dataset-a", "Shared title", 4),
            ("dataset-b", "Shared title", 2),
        ]
        assert _fetch_facet(cursor, "gbif_occurrences", "publisher") == [
            ("org-a", "Shared publisher", 4),
            ("org-b", "Shared publisher", 2),
        ]
        assert _fetch_facet(cursor, "gbif_occurrences", "species") == [
            ("1", "Panthera onca", 3),
            ("2", "Panthera onca", 3),
        ]


def _assert_snapshot_tables(cursor, table_name, snapshot):
    names = snapshot["names"]
    statistics = table_name
    expected = {
        "dataset": _keyed_rows(snapshot["facets"]["datasetKey"], names["dataset"]),
        "publisher": _keyed_rows(
            snapshot["facets"]["publishingOrg"], names["organization"]
        ),
        "species": _keyed_rows(snapshot["facets"]["speciesKey"], names["species"]),
        "basis_of_record": _keyed_rows(snapshot["facets"]["basisOfRecord"]),
        "year": sorted(
            (
                (entry["name"], None, entry["count"])
                for entry in snapshot["facets"]["year"]
            ),
            key=lambda row: -int(row[0]),
        ),
    }
    fetched = {
        facet: _fetch_facet(
            cursor,
            statistics,
            facet,
            "key DESC" if facet == "year" else "count DESC, label, key",
        )
        for facet in expected
    }
    assert fetched == expected
    assert fetched["year"][0] == ("2026", None, 195)
    assert [label for _key, label, count in fetched["species"] if count == 3] == [
        "Hemiodus huraulti (Géry, 1964)",
        "Heteroscada reckia Hübner, 1806",
    ]


def _keyed_rows(entries, names=None):
    rows = [
        (
            entry["name"],
            None if names is None else names.get(entry["name"]),
            entry["count"],
        )
        for entry in entries
    ]
    rows.sort(key=lambda row: (-row[2], row[1] or "", row[0]))
    return rows


def _fetch_facet(cursor, table_name, facet, order="count DESC, label, key"):
    cursor.execute(
        sql.SQL(
            "SELECT key, label, count FROM {} WHERE facet = %s ORDER BY {}"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(order),
        ),
        (facet,),
    )
    return cursor.fetchall()


def _assert_import_timestamp(cursor, facet):
    cursor.execute(
        "SELECT key, label, count FROM gbif_occurrences WHERE facet = %s",
        (facet,),
    )
    rows = cursor.fetchall()
    assert len(rows) == 1
    key, label, count = rows[0]
    assert label is None and count is None
    assert datetime.fromisoformat(key).tzinfo is not None
