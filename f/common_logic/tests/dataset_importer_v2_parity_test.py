"""Compare V1's conversion/connector writes with V2's stored warehouse values."""

import base64
import csv
import json
import zipfile
from collections import Counter
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from f.common_logic import dataset_importer_v2 as importer
from f.common_logic.data_conversion import convert_data, detect_structured_data_type
from f.connectors.csv.csv_to_postgres import main as save_csv
from f.connectors.geojson.geojson_to_postgres import main as save_geojson

ASSETS = Path(__file__).parent / "assets"
FIXTURES = [
    ASSETS / name
    for name in (
        "CoMapeo_Springfield Camera Trap Project_Obsvns_2026_07_23.geojson",
        "datasets_bees.gpkg",
        "garmin_sample.gpx",
        "gc_alerts.kml",
        "googleearth_sample.kml",
        "gpx_with_duplicate_names.gpx",
        "kobotoolbox_submissions.csv",
        "kobotoolbox_submissions.xlsx",
        "locusmap_favorites.gpx",
        "locusmap_favorites.kml",
        "locusmap_tracks.gpx",
        "locusmap_tracks.kml",
        "mapeo_observations.geojson",
        "my_shapefile_data.zip",
        "osm_overpass.geojson",
        "osm_overpass.gpx",
        "osm_overpass.kml",
        "osmand_notes.gpx",
        "osmand_poi.gpx",
        "smart_patrol_sample.xml",
    )
] + [ASSETS.parents[2] / "connectors/cybertracker/tests/assets/0.json"]


@pytest.fixture(autouse=True)
def datalake(tmp_path, monkeypatch):
    monkeypatch.setenv("DATASET_IMPORTER_DATALAKE_ROOT", str(tmp_path / "datalake"))


def write_v1(db, path, tmp_path):
    # Reproduce the V1 app's conversion and save steps without optional
    # source-specific transformations or its persistent-storage UI plumbing.
    paths = [str(path)]
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            archive.extractall(tmp_path / "source")
        paths = [str(p) for p in (tmp_path / "source").rglob("*") if p.is_file()]
    converted, output_format = convert_data(paths, detect_structured_data_type(paths))
    parsed = tmp_path / f"parsed.{output_format}"
    if output_format == "csv":
        with parsed.open("w", newline="", encoding="utf-8") as stream:
            csv.writer(stream).writerows(converted)
        save = save_csv
    else:
        parsed.write_text(json.dumps(converted), encoding="utf-8")
        save = save_geojson
    save(
        db,
        "legacy",
        parsed.name,
        attachment_root=str(tmp_path),
        reverse_properties_separated_by="/",
        sep_policy="underscore",
    )


def stage(db, path, goal="create", target="observations"):
    return importer.stage_import(
        db,
        {"name": path.name, "data": base64.b64encode(path.read_bytes()).decode()},
        goal,
        target,
    )


def table_rows(db, table):
    with psycopg.connect(db) as conn, conn.cursor() as cursor:
        cursor.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))
        columns = [column.name for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def row_values(rows, columns):
    return Counter(tuple(row[column] for column in sorted(columns)) for row in rows)


def assert_stored_parity(db, nullable_columns=()):
    legacy = table_rows(db, "legacy")
    current = table_rows(db, "observations")
    assert len(legacy) == len(current) > 0
    columns = set(legacy[0]) - {"_id"}
    assert columns <= current[0].keys()
    # Compare raw TEXT, not parsed JSON or numerically normalized values.
    assert row_values(legacy, columns) == row_values(current, columns)
    extra_columns = current[0].keys() - columns - {"_id", "source_id", "feature_id"}
    assert extra_columns <= set(nullable_columns)
    for column in extra_columns:
        assert all(row[column] is None for row in current)


@pytest.mark.parametrize("path", FIXTURES, ids=lambda path: path.name)
def test_fixture_database_parity(mock_db_dict, mock_db_connection, tmp_path, path):
    write_v1(mock_db_dict, path, tmp_path)
    staged = stage(mock_db_connection, path)
    review = importer.preview_import(mock_db_connection, staged["import_id"])
    importer.apply_import(mock_db_connection, staged["import_id"], review["preview_id"])
    # V2 intentionally retains these all-NULL GeoPackage attributes; V1 omits them.
    nullable_columns = (
        {
            "fix_status_descr",
            "horizontal_accuracy",
            "nr_used_satellites",
            "quality",
            "source",
            "x",
            "y",
            "z",
        }
        if path.name == "datasets_bees.gpkg"
        else ()
    )
    assert_stored_parity(mock_db_connection, nullable_columns)


@pytest.fixture
def scalar_geojson(tmp_path):
    path = tmp_path / "scalars.geojson"
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "id": str(index),
                        "geometry": {"type": "Point", "coordinates": [index, 2]},
                        "properties": {
                            "whole": float(index),
                            "fraction": 3.02,
                            "small": 1e-8,
                            "large": 1e20,
                            "negative_zero": -0.0,
                            "integer": 42,
                            "boolean": True,
                            "null": None,
                            "empty": "",
                            "text": " 2.0 ",
                            "json_text": '["a","b"]',
                            "details": {"label": "Fotografía", "values": [index, 2.0]},
                        },
                    }
                    for index in (1, 2)
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_scalar_database_parity(
    mock_db_dict, mock_db_connection, tmp_path, scalar_geojson
):
    write_v1(mock_db_dict, scalar_geojson, tmp_path)
    staged = stage(mock_db_connection, scalar_geojson)
    review = importer.preview_import(mock_db_connection, staged["import_id"])
    importer.apply_import(mock_db_connection, staged["import_id"], review["preview_id"])
    assert_stored_parity(mock_db_connection)


@pytest.mark.parametrize("goal", ["merge", "sync"])
@pytest.mark.parametrize(
    "source,identity",
    [
        ("locusmap_favorites.gpx", ["g__coordinates"]),
        ("mapeo_observations.geojson", ["g__coordinates"]),
        ("scalars", ["whole"]),
        ("scalars", ["details"]),
    ],
)
def test_v1_rows_match_v2_without_replacement(
    mock_db_dict, mock_db_connection, tmp_path, scalar_geojson, goal, source, identity
):
    path = scalar_geojson if source == "scalars" else ASSETS / source
    write_v1(mock_db_dict, path, tmp_path)
    before = table_rows(mock_db_connection, "legacy")
    staged = stage(mock_db_connection, path, goal, "legacy")
    review = importer.preview_import(mock_db_connection, staged["import_id"], identity)
    assert review["added"] == review["deleted"] == 0
    assert review["final_count"] == len(before)
    # The new feature_id column is a legitimate update, not a serialization change.
    assert review["columns_added"] == 1
    assert review["updated"] == len(before)
    importer.apply_import(mock_db_connection, staged["import_id"], review["preview_id"])
    after = table_rows(mock_db_connection, "legacy")
    assert row_values(before, before[0].keys()) == row_values(after, before[0].keys())
