import base64

import psycopg
import pytest

from f.common_logic.dataset_importer_poc import (
    ImportValidationError,
    apply_import,
    check_dataset_name,
    list_datasets,
    preview_import,
    stage_import,
)


def upload(name, text):
    return {"name": name, "data": base64.b64encode(text.encode()).decode()}


def table_rows(dsn, table):
    with psycopg.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(f'SELECT * FROM "public"."{table}" ORDER BY _id')
        columns = [column.name for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def test_create_csv_stages_previews_and_applies(mock_db_connection):
    staged = stage_import(
        mock_db_connection,
        upload("birds.csv", "species,count\nheron,2\nibis,1\n"),
        "create",
        "Bird Observations",
    )

    assert staged["fields"] == ["species", "count"]
    assert check_dataset_name(mock_db_connection, "Bird Observations") == {
        "table_name": "bird_observations",
        "available": True,
    }
    preview = preview_import(mock_db_connection, staged["import_id"])
    assert {
        key: preview[key]
        for key in (
            "deleted",
            "updated",
            "added",
            "columns_added",
            "unchanged",
            "final_count",
        )
    } == {
        "deleted": 0,
        "updated": 0,
        "added": 2,
        "columns_added": 2,
        "unchanged": 0,
        "final_count": 2,
    }

    assert apply_import(mock_db_connection, staged["import_id"])["success"] is True
    assert list_datasets(mock_db_connection) == ["bird_observations"]
    assert {
        tuple(sorted({key: row[key] for key in ("species", "count")}.items()))
        for row in table_rows(mock_db_connection, "bird_observations")
    } == {
        (("count", "2"), ("species", "heron")),
        (("count", "1"), ("species", "ibis")),
    }
    with pytest.raises(ImportValidationError, match="already been applied"):
        preview_import(mock_db_connection, staged["import_id"])


def test_merge_preserves_omitted_values_and_imported_null_clears_them(
    mock_db_connection,
):
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE "public"."observations" (_id TEXT PRIMARY KEY, code TEXT, note TEXT)'
        )
        cursor.execute(
            'INSERT INTO "public"."observations" VALUES (%s, %s, %s)',
            ("one", "A", "old"),
        )
        cursor.execute(
            'INSERT INTO "public"."observations" VALUES (%s, %s, %s)',
            ("two", "B", "retain"),
        )

    staged = stage_import(
        mock_db_connection,
        upload("update.csv", "code,note\nA,\nC,new\n"),
        "merge",
        "observations",
    )
    preview = preview_import(mock_db_connection, staged["import_id"], ["code"])
    assert preview["added"] == 1
    assert preview["updated"] == 1
    assert preview["unchanged"] == 1
    apply_import(mock_db_connection, staged["import_id"])
    rows = table_rows(mock_db_connection, "observations")
    assert {(row["code"], row["note"]) for row in rows} == {
        ("A", None),
        ("B", "retain"),
        ("C", "new"),
    }


def test_sync_uses_null_safe_identity_and_deletes_only_absent_records(
    mock_db_connection,
):
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE "public"."observations" (_id TEXT PRIMARY KEY, code TEXT, note TEXT)'
        )
        cursor.execute(
            'INSERT INTO "public"."observations" VALUES (%s, %s, %s)',
            ("one", None, "keep"),
        )
        cursor.execute(
            'INSERT INTO "public"."observations" VALUES (%s, %s, %s)',
            ("two", "B", "delete"),
        )

    staged = stage_import(
        mock_db_connection,
        upload("update.csv", "code,note\n,changed\n"),
        "sync",
        "observations",
    )
    preview = preview_import(mock_db_connection, staged["import_id"], ["code"])
    assert preview["deleted"] == 1
    assert preview["updated"] == 1
    apply_import(mock_db_connection, staged["import_id"])
    assert [
        (row["code"], row["note"])
        for row in table_rows(mock_db_connection, "observations")
    ] == [(None, "changed")]


def test_duplicate_identity_is_rejected_before_writes(mock_db_connection):
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE "public"."observations" (_id TEXT PRIMARY KEY, code TEXT)'
        )
    staged = stage_import(
        mock_db_connection,
        upload("duplicate.csv", "code\nA\nA\n"),
        "merge",
        "observations",
    )
    with pytest.raises(ImportValidationError, match="Duplicate record identities"):
        preview_import(mock_db_connection, staged["import_id"], ["code"])
    assert table_rows(mock_db_connection, "observations") == []


def test_geojson_preserves_geometry_and_nested_properties(mock_db_connection):
    staged = stage_import(
        mock_db_connection,
        upload(
            "birds.geojson",
            '{"type":"FeatureCollection","features":[{"type":"Feature","id":"feature-123","properties":{"name":"heron","tags":["wetland"]},"geometry":{"type":"Point","coordinates":[1,2]}}]}',
        ),
        "create",
        "birds",
    )
    assert staged["fields"] == ["name", "tags", "id", "g__type", "g__coordinates"]
    preview_import(mock_db_connection, staged["import_id"])
    apply_import(mock_db_connection, staged["import_id"])
    row = table_rows(mock_db_connection, "birds")[0]
    assert row["g__type"] == "Point"
    assert row["g__coordinates"] == "[1,2]"
    assert row["id"] == "feature-123"
    assert row["tags"] == '["wetland"]'


def test_confirmation_rejects_a_target_changed_after_review(mock_db_connection):
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE "public"."observations" (_id TEXT PRIMARY KEY, code TEXT)'
        )
        cursor.execute(
            'INSERT INTO "public"."observations" VALUES (%s, %s)', ("one", "A")
        )
    staged = stage_import(
        mock_db_connection,
        upload("update.csv", "code\nB\n"),
        "merge",
        "observations",
    )
    preview_import(mock_db_connection, staged["import_id"], ["code"])
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'INSERT INTO "public"."observations" VALUES (%s, %s)', ("two", "C")
        )
    with pytest.raises(ImportValidationError, match="changed after review"):
        apply_import(mock_db_connection, staged["import_id"])
    assert {
        (row["_id"], row["code"])
        for row in table_rows(mock_db_connection, "observations")
    } == {
        ("one", "A"),
        ("two", "C"),
    }
