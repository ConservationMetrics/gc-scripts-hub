import base64
import io
import zipfile
from pathlib import Path

import fiona
import openpyxl
import psycopg
import pytest

import f.common_logic.dataset_importer_poc as importer
from f.common_logic.dataset_importer_poc import (
    ImportValidationError,
    apply_import,
    check_dataset_name,
    cleanup_expired_imports,
    list_datasets,
    preview_import,
    stage_import,
)


def upload(name, text):
    return {"name": name, "data": base64.b64encode(text.encode()).decode()}


def upload_bytes(name, contents):
    return {"name": name, "data": base64.b64encode(contents).decode()}


@pytest.fixture(autouse=True)
def importer_datalake(tmp_path, monkeypatch):
    datalake = tmp_path / "datalake"
    monkeypatch.setenv("DATASET_IMPORTER_DATALAKE_ROOT", str(datalake))
    return datalake


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
        "available": False,
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
    assert staged["fields"] == [
        "name",
        "tags",
        "feature.id",
        "g__type",
        "g__coordinates",
    ]
    preview_import(mock_db_connection, staged["import_id"])
    apply_import(mock_db_connection, staged["import_id"])
    row = table_rows(mock_db_connection, "birds")[0]
    assert row["g__type"] == "Point"
    assert row["g__coordinates"] == "[1,2]"
    assert row["feature_id"] == "feature-123"
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


def test_only_compatible_datasets_are_listed_and_targetable(mock_db_connection):
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE "public"."observations" (_id TEXT PRIMARY KEY, code TEXT)'
        )
        cursor.execute('CREATE TABLE "public"."application_state" (name TEXT)')
        cursor.execute(
            'CREATE TABLE "public"."observations__metadata" (_id TEXT PRIMARY KEY)'
        )

    assert list_datasets(mock_db_connection) == ["observations"]
    with pytest.raises(ImportValidationError, match="not a compatible"):
        stage_import(
            mock_db_connection,
            upload("rows.csv", "name\nunsafe\n"),
            "append",
            "application_state",
        )


def test_create_persists_column_mapping_for_later_imports(mock_db_connection):
    staged = stage_import(
        mock_db_connection,
        upload("birds.csv", "Bird Name,Count\nHeron,2\n"),
        "create",
        "Bird Observations",
    )
    preview_import(mock_db_connection, staged["import_id"])
    apply_import(mock_db_connection, staged["import_id"])

    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'SELECT original_column, sql_column FROM "public"."bird_observations__columns" ORDER BY original_column'
        )
        assert cursor.fetchall() == [("Bird Name", "Bird_Name"), ("Count", "Count")]

    appended = stage_import(
        mock_db_connection,
        upload("more.csv", "Bird Name,Count\nIbis,1\n"),
        "append",
        "bird_observations",
    )
    preview_import(mock_db_connection, appended["import_id"])
    apply_import(mock_db_connection, appended["import_id"])
    assert {
        (row["Bird_Name"], row["Count"])
        for row in table_rows(mock_db_connection, "bird_observations")
    } == {
        ("Heron", "2"),
        ("Ibis", "1"),
    }


def test_existing_column_mapping_is_used_for_identity(mock_db_connection):
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE "public"."observations" (_id TEXT PRIMARY KEY, bird_name TEXT, note TEXT)'
        )
        cursor.execute(
            'CREATE TABLE "public"."observations__columns" (original_column TEXT, sql_column TEXT)'
        )
        cursor.execute(
            'INSERT INTO "public"."observations__columns" VALUES (%s, %s)',
            ("Bird Name", "bird_name"),
        )
        cursor.execute(
            'INSERT INTO "public"."observations" VALUES (%s, %s, %s)',
            ("one", "Heron", "old"),
        )

    staged = stage_import(
        mock_db_connection,
        upload("birds.csv", "Bird Name,note\nHeron,new\n"),
        "merge",
        "observations",
    )
    preview = preview_import(
        mock_db_connection, staged["import_id"], ["Bird Name"]
    )
    assert preview["updated"] == 1
    apply_import(mock_db_connection, staged["import_id"])
    assert table_rows(mock_db_connection, "observations")[0]["note"] == "new"


@pytest.mark.parametrize("dataset_name", ["", "   ", "---"])
def test_dataset_name_requires_letters_or_numbers(mock_db_connection, dataset_name):
    with pytest.raises(ImportValidationError, match="dataset name"):
        check_dataset_name(mock_db_connection, dataset_name)


def test_stage_rejects_invalid_and_reserved_create_names(mock_db_connection):
    for name in ("---", "observations__columns"):
        with pytest.raises(ImportValidationError, match="dataset name|reserved suffix"):
            stage_import(
                mock_db_connection,
                upload("rows.csv", "name\nHeron\n"),
                "create",
                name,
            )


def test_dataset_name_is_unavailable_when_any_relation_uses_it(mock_db_connection):
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute('CREATE VIEW "public"."observations" AS SELECT 1 AS value')

    assert check_dataset_name(mock_db_connection, "observations")["available"] is False


def test_persisted_mapping_is_not_overwritten_by_fallback(mock_db_connection):
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE "public"."observations" (_id TEXT PRIMARY KEY, code TEXT, code_raw TEXT)'
        )
        cursor.execute(
            'CREATE TABLE "public"."observations__columns" (original_column TEXT, sql_column TEXT)'
        )
        cursor.execute(
            'INSERT INTO "public"."observations__columns" VALUES (%s, %s)',
            ("code", "code_raw"),
        )

    staged = stage_import(
        mock_db_connection,
        upload("rows.csv", "code\nA\n"),
        "append",
        "observations",
    )
    preview_import(mock_db_connection, staged["import_id"])
    apply_import(mock_db_connection, staged["import_id"])
    row = table_rows(mock_db_connection, "observations")[0]
    assert row["code"] is None
    assert row["code_raw"] == "A"


def test_expired_sessions_are_rejected_and_cleaned_up(mock_db_connection):
    staged = stage_import(
        mock_db_connection,
        upload("rows.csv", "name\nHeron\n"),
        "create",
        "observations",
    )
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            "UPDATE dataset_importer_poc.import_sessions SET expires_at = now() - INTERVAL '1 second' WHERE import_id = %s",
            (staged["import_id"],),
        )

    with pytest.raises(ImportValidationError, match="expired"):
        preview_import(mock_db_connection, staged["import_id"])
    assert cleanup_expired_imports(mock_db_connection) == 0


def test_mapping_change_after_staging_rejects_apply(mock_db_connection):
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE "public"."observations" (_id TEXT PRIMARY KEY, code TEXT, alternate TEXT)'
        )

    staged = stage_import(
        mock_db_connection,
        upload("rows.csv", "code\nA\n"),
        "append",
        "observations",
    )
    preview_import(mock_db_connection, staged["import_id"])
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE "public"."observations__columns" (original_column TEXT, sql_column TEXT)'
        )
        cursor.execute(
            'INSERT INTO "public"."observations__columns" VALUES (%s, %s)',
            ("code", "alternate"),
        )

    with pytest.raises(ImportValidationError, match="mapping changed"):
        apply_import(mock_db_connection, staged["import_id"])


def test_create_rejects_an_orphaned_mapping_table(mock_db_connection):
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE "public"."observations__columns" (original_column TEXT, sql_column TEXT)'
        )

    assert check_dataset_name(mock_db_connection, "observations")["available"] is False
    with pytest.raises(ImportValidationError, match="mapping|conflicts"):
        stage_import(
            mock_db_connection,
            upload("rows.csv", "name\nHeron\n"),
            "create",
            "observations",
        )


def test_staged_create_reserves_its_mapping_table_name(mock_db_connection):
    shared_prefix = "a" * 54
    first_name = f"{shared_prefix}_first"
    second_name = f"{shared_prefix}_second"
    stage_import(
        mock_db_connection,
        upload("rows.csv", "name\nHeron\n"),
        "create",
        first_name,
    )

    with pytest.raises(ImportValidationError, match="conflicts"):
        stage_import(
            mock_db_connection,
            upload("rows.csv", "name\nIbis\n"),
            "create",
            second_name,
        )


def test_existing_long_dataset_reserves_its_mapping_name(mock_db_connection):
    shared_prefix = "a" * 54
    existing_name = f"{shared_prefix}_existing"
    new_name = f"{shared_prefix}_new"
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            psycopg.sql.SQL("CREATE TABLE {} (_id TEXT PRIMARY KEY)").format(
                psycopg.sql.Identifier(existing_name)
            )
        )

    assert check_dataset_name(mock_db_connection, new_name)["available"] is False


def test_existing_staged_create_is_backfilled_into_registry(mock_db_connection):
    staged = stage_import(
        mock_db_connection,
        upload("rows.csv", "name\nHeron\n"),
        "create",
        "observations",
    )
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            "DELETE FROM dataset_importer_poc.dataset_registry WHERE target_table = 'observations'"
        )

    preview_import(mock_db_connection, staged["import_id"])
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            "SELECT status, source_import_id FROM dataset_importer_poc.dataset_registry WHERE target_table = 'observations'"
        )
        status, source_import_id = cursor.fetchone()
        assert status == "reserved"
        assert str(source_import_id) == staged["import_id"]


@pytest.mark.parametrize(
    ("relative_path", "expected_format"),
    [
        ("locusmap_favorites.gpx", "gpx"),
        ("locusmap_favorites.kml", "kml"),
        ("smart_patrol_sample.xml", "smart"),
        ("my_shapefile_data.zip", "shapefile"),
    ],
)
def test_supported_formats_are_staged(
    mock_db_connection, relative_path, expected_format
):
    path = Path(__file__).parents[1] / "assets" / relative_path
    staged = stage_import(
        mock_db_connection,
        upload_bytes(path.name, path.read_bytes()),
        "create",
        f"dataset_{expected_format}",
    )
    assert staged["source_format"] == expected_format
    assert staged["record_count"] > 0


def test_single_layer_geopackage_is_staged(mock_db_connection, tmp_path):
    path = tmp_path / "points.gpkg"
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer="points",
        schema={"geometry": "Point", "properties": {"name": "str"}},
        crs="EPSG:4326",
    ) as collection:
        collection.write(
            {
                "geometry": {"type": "Point", "coordinates": (1, 2)},
                "properties": {"name": "Heron"},
            }
        )
    staged = stage_import(
        mock_db_connection,
        upload_bytes(path.name, path.read_bytes()),
        "create",
        "geopackage_rows",
    )
    assert staged["source_format"] == "geopackage"
    assert staged["record_count"] == 1


def test_multi_layer_geopackage_is_rejected(mock_db_connection):
    path = Path(__file__).parents[1] / "assets" / "datasets_bees.gpkg"
    with pytest.raises(ImportValidationError, match="exactly one spatial layer"):
        stage_import(
            mock_db_connection,
            upload_bytes(path.name, path.read_bytes()),
            "create",
            "geopackage_rows",
        )


def test_single_sheet_xlsx_is_staged(mock_db_connection, tmp_path):
    path = tmp_path / "birds.xlsx"
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(["name", "count"])
    sheet.append(["Heron", 2])
    workbook.save(path)
    staged = stage_import(
        mock_db_connection,
        upload_bytes(path.name, path.read_bytes()),
        "create",
        "xlsx_rows",
    )
    assert staged["source_format"] == "xlsx"
    assert staged["record_count"] == 1


def test_json_and_cybertracker_are_detected_by_content(mock_db_connection):
    regular = stage_import(
        mock_db_connection,
        upload("rows.json", '[{"name":"Heron","tags":["wetland"]}]'),
        "create",
        "json_rows",
    )
    assert regular["source_format"] == "json"
    cybertracker = (
        Path(__file__).parents[3]
        / "connectors"
        / "cybertracker"
        / "tests"
        / "assets"
        / "0.json"
    )
    detected = stage_import(
        mock_db_connection,
        upload_bytes(cybertracker.name, cybertracker.read_bytes()),
        "create",
        "cybertracker_rows",
    )
    assert detected["source_format"] == "cybertracker"
    assert detected["record_count"] > 0


def test_zip_rejects_unsupported_members_without_staging(mock_db_connection):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("rows.csv", "name\nHeron\n")
        archive.writestr("notes.txt", "unsupported")

    with pytest.raises(ImportValidationError, match="unsupported"):
        stage_import(
            mock_db_connection,
            upload_bytes("mixed.zip", buffer.getvalue()),
            "create",
            "observations",
        )
    assert check_dataset_name(mock_db_connection, "observations")["available"] is True


def test_zip_rejects_unsafe_paths(mock_db_connection):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("../rows.csv", "name\nHeron\n")

    with pytest.raises(ImportValidationError, match="unsafe"):
        stage_import(
            mock_db_connection,
            upload_bytes("unsafe.zip", buffer.getvalue()),
            "create",
            "observations",
        )


def test_multi_file_zip_appends_rows_in_archive_order(mock_db_connection):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("first.csv", "name\nHeron\n")
        archive.writestr("second.json", '[{"name":"Ibis"}]')

    staged = stage_import(
        mock_db_connection,
        upload_bytes("birds.zip", buffer.getvalue()),
        "create",
        "observations",
    )
    assert staged["source_format"] == "zip"
    assert staged["record_count"] == 2


def test_successful_import_archives_exact_source(
    mock_db_connection, importer_datalake
):
    source = b"name\nHeron\n"
    staged = stage_import(
        mock_db_connection,
        upload_bytes("birds.csv", source),
        "create",
        "observations",
    )
    preview_import(mock_db_connection, staged["import_id"])
    result = apply_import(mock_db_connection, staged["import_id"])

    archived = Path(result["archive_path"])
    assert archived.is_relative_to(importer_datalake)
    assert archived.read_bytes() == source
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            "SELECT status, source_data FROM dataset_importer_poc.import_sessions WHERE import_id = %s",
            (staged["import_id"],),
        )
        assert cursor.fetchone() == ("archived", None)


def test_archive_failure_retains_source_for_retry(
    mock_db_connection, importer_datalake, tmp_path, monkeypatch
):
    blocked_root = tmp_path / "blocked"
    blocked_root.write_text("not a directory")
    monkeypatch.setenv("DATASET_IMPORTER_DATALAKE_ROOT", str(blocked_root))
    staged = stage_import(
        mock_db_connection,
        upload("birds.csv", "name\nHeron\n"),
        "create",
        "observations",
    )
    preview_import(mock_db_connection, staged["import_id"])
    with pytest.raises(OSError):
        apply_import(mock_db_connection, staged["import_id"])

    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute(
            "UPDATE dataset_importer_poc.import_sessions SET expires_at = now() - INTERVAL '1 second' WHERE import_id = %s",
            (staged["import_id"],),
        )
    assert cleanup_expired_imports(mock_db_connection) == 0

    monkeypatch.setenv("DATASET_IMPORTER_DATALAKE_ROOT", str(importer_datalake))
    result = apply_import(mock_db_connection, staged["import_id"])
    assert Path(result["archive_path"]).read_bytes() == b"name\nHeron\n"


def test_geojson_reserved_properties_are_rejected(mock_db_connection):
    source = """{
        "type":"FeatureCollection",
        "features":[{
            "type":"Feature",
            "properties":{"g__type":"user value"},
            "geometry":{"type":"Point","coordinates":[1,2]}
        }]
    }"""
    with pytest.raises(ImportValidationError, match="reserved fields"):
        stage_import(
            mock_db_connection,
            upload("points.geojson", source),
            "create",
            "observations",
        )


def test_source_size_is_checked_before_base64_decode(
    mock_db_connection, monkeypatch
):
    payload = upload_bytes("rows.csv", b"four")
    monkeypatch.setattr(importer, "MAX_SOURCE_BYTES", 3)

    def fail_decode(*args, **kwargs):
        raise AssertionError("oversized payload was decoded")

    monkeypatch.setattr(importer.base64, "b64decode", fail_decode)
    with pytest.raises(ImportValidationError, match="25 MiB"):
        stage_import(mock_db_connection, payload, "create", "observations")


def test_zip_expanded_size_limit_is_enforced(mock_db_connection, monkeypatch):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("rows.csv", "name\nHeron\n")
    monkeypatch.setattr(importer, "MAX_EXPANDED_BYTES", 3)
    with pytest.raises(ImportValidationError, match="100 MiB"):
        stage_import(
            mock_db_connection,
            upload_bytes("rows.zip", buffer.getvalue()),
            "create",
            "observations",
        )


def test_final_column_limit_includes_internal_id(mock_db_connection):
    headers = ",".join(f"column_{index}" for index in range(150))
    values = ",".join("value" for _ in range(150))
    with pytest.raises(ImportValidationError, match="at most 150 columns"):
        stage_import(
            mock_db_connection,
            upload("wide.csv", f"{headers}\n{values}\n"),
            "create",
            "wide_dataset",
        )
