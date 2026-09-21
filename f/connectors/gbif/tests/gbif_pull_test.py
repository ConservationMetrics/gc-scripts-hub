import csv
import logging
import zipfile
from datetime import datetime, timedelta, timezone

import pytest
import responses
from psycopg import connect, sql

from f.connectors.gbif import gbif_check_download, gbif_pull, gbif_submit_download
from f.connectors.gbif.tests.assets import server_responses


def test_submit_uses_basic_auth_and_last_interpreted_predicate(
    mocked_responses, monkeypatch
):
    monkeypatch.setattr(
        gbif_submit_download, "calculate_cutoff_date", lambda _: (2026, 7)
    )
    mocked_responses.add(
        responses.POST,
        "https://api.gbif.org/v1/occurrence/download/request",
        body=server_responses.DOWNLOAD_KEY,
    )
    result = gbif_submit_download.main(
        {"username": "account", "password": "secret"},
        [[-55.03, 3.23], [-54.12, 3.67]],
        2,
    )
    request = mocked_responses.calls[0].request
    assert request.headers["Authorization"].startswith("Basic ")
    assert b'"key": "LAST_INTERPRETED"' in request.body
    assert b'"value": "2026-07-01"' in request.body
    assert result["download_key"] == server_responses.DOWNLOAD_KEY


def test_submit_rejects_oversized_bounds_before_request(mocked_responses):
    with pytest.raises(ValueError):
        gbif_submit_download.main(
            {"username": "u", "password": "p"}, [[0, 0], [1.2, 1.2]]
        )
    assert not mocked_responses.calls


@pytest.mark.parametrize("status", ["PREPARING", "RUNNING", "SUSPENDED", "SUCCEEDED"])
def test_check_known_pending_and_success_statuses(mocked_responses, status):
    mocked_responses.add(
        responses.GET,
        f"https://api.gbif.org/v1/occurrence/download/{server_responses.DOWNLOAD_KEY}",
        json=server_responses.metadata(status),
    )
    result = gbif_check_download.main(
        server_responses.DOWNLOAD_KEY,
        (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
    )
    assert result["succeeded"] is (status == "SUCCEEDED")


@pytest.mark.parametrize(
    "status", ["CANCELLED", "KILLED", "FAILED", "FILE_ERASED", "UNKNOWN"]
)
def test_check_terminal_or_unknown_status_fails(mocked_responses, status):
    mocked_responses.add(
        responses.GET,
        f"https://api.gbif.org/v1/occurrence/download/{server_responses.DOWNLOAD_KEY}",
        json=server_responses.metadata(status),
    )
    with pytest.raises(RuntimeError):
        gbif_check_download.main(
            server_responses.DOWNLOAD_KEY,
            (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        )


def test_check_deadline_fails_without_http(mocked_responses):
    with pytest.raises(TimeoutError):
        gbif_check_download.main(
            server_responses.DOWNLOAD_KEY, "2000-01-01T00:00:00+00:00"
        )
    assert not mocked_responses.calls


@pytest.mark.parametrize("status", [429, 500])
def test_check_marks_transient_http_failures_retryable(mocked_responses, status):
    mocked_responses.add(
        responses.GET,
        f"https://api.gbif.org/v1/occurrence/download/{server_responses.DOWNLOAD_KEY}",
        status=status,
    )
    with pytest.raises(RuntimeError, match="retryable"):
        gbif_check_download.main(
            server_responses.DOWNLOAD_KEY,
            (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        )


def test_real_archive_conversion_has_852_rows_and_preserves_geometry(
    archive_bytes, tmp_path
):
    archive = tmp_path / "fixture.zip"
    archive.write_bytes(archive_bytes)
    csv_path, count = gbif_pull._convert_archive(archive, tmp_path / "converted.csv")
    assert count == 852
    rows = list(csv.DictReader(csv_path.open(encoding="utf-8")))
    assert len(rows) == 852
    assert all(row["gbif_id"] == row["_id"] and row["_id"] for row in rows)
    assert any(row["g__type"] == "Point" for row in rows)


def test_pull_imports_and_upserts_real_archive(
    mocked_responses, archive_bytes, pg_database, tmp_path, caplog
):
    metadata_url = (
        f"https://api.gbif.org/v1/occurrence/download/{server_responses.DOWNLOAD_KEY}"
    )
    mocked_responses.add(responses.GET, metadata_url, json=server_responses.metadata())
    mocked_responses.add(
        responses.GET, server_responses.ARCHIVE_URL, body=archive_bytes
    )
    with caplog.at_level(logging.INFO, logger="f.connectors.gbif.gbif_pull"):
        result = gbif_pull.main(
            server_responses.DOWNLOAD_KEY,
            pg_database,
            "gbif_occurrences",
            str(tmp_path),
        )
    assert result["record_count"] == 852
    destination = tmp_path / "gbif_occurrences"
    assert f"GBIF archive saved to {destination / f'{server_responses.DOWNLOAD_KEY}.zip'}" in caplog.text
    assert f"GBIF occurrence CSV saved to {destination / f'{server_responses.DOWNLOAD_KEY}.csv'} (852 records)" in caplog.text
    assert f"GBIF provenance metadata saved to {destination / f'{server_responses.DOWNLOAD_KEY}.json'}" in caplog.text
    with connect(**pg_database) as connection, connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT count(*) FROM {} ").format(
                sql.Identifier("gbif_occurrences")
            )
        )
        assert cursor.fetchone()[0] == 852
    mocked_responses.add(responses.GET, metadata_url, json=server_responses.metadata())
    mocked_responses.add(
        responses.GET, server_responses.ARCHIVE_URL, body=archive_bytes
    )
    gbif_pull.main(
        server_responses.DOWNLOAD_KEY, pg_database, "gbif_occurrences", str(tmp_path)
    )
    with connect(**pg_database) as connection, connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT count(*) FROM {} ").format(
                sql.Identifier("gbif_occurrences")
            )
        )
        assert cursor.fetchone()[0] == 852


def test_convert_rejects_missing_and_duplicate_ids(tmp_path):
    for name, data in [
        ("missing", "gbifID\tdecimalLongitude\n\t1\n"),
        ("duplicate", "gbifID\tdecimalLongitude\n1\t1\n1\t2\n"),
    ]:
        archive = tmp_path / f"{name}.zip"
        with zipfile.ZipFile(archive, "w") as zipped:
            zipped.writestr("occurrences.csv", data)
        with pytest.raises(ValueError):
            gbif_pull._convert_archive(archive, tmp_path / f"{name}.csv")


def test_convert_preserves_invalid_coordinates_without_geometry(tmp_path):
    archive = tmp_path / "coordinates.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr(
            "occurrences.csv",
            "gbifID\tdecimalLongitude\tdecimalLatitude\tspecies\n1\tbad\t2\tTest\n",
        )
    csv_path, count = gbif_pull._convert_archive(archive, tmp_path / "coordinates.csv")
    assert count == 1
    row = next(csv.DictReader(csv_path.open(encoding="utf-8")))
    assert row["decimal_longitude"] == "bad"
    assert row["species"] == "Test"
    assert row["g__type"] == ""
    assert row["g__coordinates"] == ""


@pytest.mark.parametrize(
    ("longitude", "latitude"),
    [("181", "0"), ("0", "-91"), ("nan", "0"), ("0", "inf")],
)
def test_coordinates_rejects_invalid_wgs84_values(longitude, latitude):
    assert gbif_pull._coordinates(longitude, latitude) is None


def test_convert_treats_literal_quotes_as_tsv_data(tmp_path):
    archive = tmp_path / "literal-quotes.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr(
            "occurrences.csv",
            'gbifID\tverbatimScientificName\n1\t"unmatched quote\n2\tsecond record\n',
        )
    csv_path, count = gbif_pull._convert_archive(archive, tmp_path / "quotes.csv")
    assert count == 2
    rows = list(csv.DictReader(csv_path.open(encoding="utf-8")))
    assert [row["gbif_id"] for row in rows] == ["1", "2"]
    assert rows[0]["verbatim_scientific_name"] == '"unmatched quote'


def test_convert_rejects_headers_that_collide_after_snake_case_conversion(tmp_path):
    archive = tmp_path / "colliding-headers.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("occurrences.csv", "gbifID\tgbif_id\n1\t1\n")
    with pytest.raises(ValueError, match="conflict"):
        gbif_pull._convert_archive(archive, tmp_path / "colliding-headers.csv")


@pytest.mark.parametrize(
    ("members", "message"),
    [([], "exactly one"), (["a.csv", "b.csv"], "exactly one")],
)
def test_convert_rejects_missing_multiple_or_header_only_members(
    tmp_path, members, message
):
    archive = tmp_path / "invalid.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        for member in members:
            zipped.writestr(member, "gbifID\tdecimalLongitude\n")
    with pytest.raises(ValueError, match=message):
        gbif_pull._convert_archive(archive, tmp_path / "invalid.csv")


def test_pull_retains_empty_download_without_creating_a_table(
    mocked_responses, pg_database, tmp_path
):
    archive = tmp_path / "empty.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("occurrences.csv", "gbifID\tdecimalLongitude\n")
    metadata_url = (
        f"https://api.gbif.org/v1/occurrence/download/{server_responses.DOWNLOAD_KEY}"
    )
    mocked_responses.add(responses.GET, metadata_url, json=server_responses.metadata())
    mocked_responses.add(
        responses.GET, server_responses.ARCHIVE_URL, body=archive.read_bytes()
    )
    result = gbif_pull.main(
        server_responses.DOWNLOAD_KEY, pg_database, "empty_gbif", str(tmp_path)
    )
    assert result["record_count"] == 0
    assert (tmp_path / "empty_gbif" / f"{server_responses.DOWNLOAD_KEY}.json").is_file()
    with connect(**pg_database) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass('empty_gbif')")
        assert cursor.fetchone()[0] is None
