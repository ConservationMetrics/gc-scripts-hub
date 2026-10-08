"""Validate V2 support for legacy app exports supported by V1.

This was added as an audit to avoid regressions with V2 but is worth keeping long term
It covers Kobo, Locus Map, OsmAnd, SMART, CyberTracker, Mapeo,
and OSM exports. Tests check that important fields survive upload parsing and,
for selected files, staging, preview, and database storage.

Expected values are fixed from existing fixtures and conversion tests, rather
than calculated with the converter.
"""

import base64
import io
import json
from pathlib import Path

import openpyxl
import psycopg
import pytest

from f.common_logic import dataset_importer_v2 as importer

ASSETS = Path(__file__).parent / "assets"
CYBERTRACKER = ASSETS.parents[2] / "connectors/cybertracker/tests/assets/0.json"


def parse(path, filename=None):
    return importer._parse_source(filename or path.name, path.read_bytes())


@pytest.mark.parametrize("suffix", ["csv", "xlsx"])
def test_kobo_metadata_and_missing_cells(suffix):
    source_format, rows = parse(ASSETS / f"kobotoolbox_submissions.{suffix}")
    assert source_format == suffix
    assert len(rows) == 3
    row = rows[0]
    assert row["What community are you from?"] == "Arlington"
    assert (
        row["How do you describe the current state of the local ecosystem?"]
        == "Flourishing"
    )
    assert (
        row["Which traditional plants and animals are important to your community?"]
        == "bamboo, wild boar"
    )
    assert row["_id"] == "254135872"
    assert row["_index"] == "1"
    assert row["_submission_time"] == "2023-07-19 18:33:06"
    assert row["Enter the community name:"] is None


def test_excel_integer_decimal_text_and_missing_cells():
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(["id", "count", "score", "note"])
    sheet.append([1, 42, 3.14, "ok"])
    sheet.append([2, 0, 1.5, "  padded  "])
    sheet.append(["3", 10.0, None, "5.0"])
    contents = io.BytesIO()
    workbook.save(contents)
    source_format, rows = importer._parse_source("numbers.xlsx", contents.getvalue())
    assert source_format == "xlsx"
    assert rows == [
        {"id": "1", "count": "42", "score": "3.14", "note": "ok"},
        {"id": "2", "count": "0", "score": "1.5", "note": "padded"},
        {"id": "3", "count": "10", "score": None, "note": "5.0"},
    ]


def test_csv_explicit_decimal_text():
    _, rows = importer._parse_source(
        "numbers.csv", b"id,count,score\n1,42,3.14\n2,42.0,1.50\n"
    )
    assert rows == [
        {"id": "1", "count": "42", "score": "3.14"},
        {"id": "2", "count": "42.0", "score": "1.50"},
    ]


def test_kobo_multiple_sheets_are_rejected():
    with pytest.raises(
        importer.ImportValidationError, match="only single-sheet files are supported"
    ):
        parse(ASSETS / "kobotoolbox_submissions_multiple_sheets.xlsx")


@pytest.mark.parametrize(
    "suffix,description,attachment",
    [
        ("gpx", "desc", "link"),
        ("kml", "description", "attachments"),
    ],
)
def test_locus_descriptions_and_consolidated_attachments(
    suffix, description, attachment
):
    source_format, rows = parse(ASSETS / f"locusmap_favorites.{suffix}")
    assert source_format == suffix
    assert len(rows) == 2
    tree, rock = rows
    assert tree[description] == "My favorite tree"
    assert rock[description] == "My favorite rock"
    filenames = {
        "p_2025-01-09_15-_20250109_154929.jpg",
        "p_2025-01-09_15-_20250109_154918.m4a",
    }
    links = set(rock[attachment].split(", "))
    assert links == (
        {f"./Favorites-attachments/{name}" for name in filenames}
        if suffix == "gpx"
        else filenames
    )
    assert rock["g__type"] == "Point"
    assert json.loads(rock["g__coordinates"]) == [-73.297812, 40.948215]
    assert "Name" not in tree and "Description" not in tree


def test_kml_extended_data():
    # The Locus fixture only has custom attachment elements, not standard Data.
    contents = (
        (ASSETS / "locusmap_favorites.kml")
        .read_bytes()
        .replace(
            b'<ExtendedData xmlns:lc="http://www.locusmap.eu">',
            b'<ExtendedData xmlns:lc="http://www.locusmap.eu"><Data name="survey:status"><value>reviewed</value></Data>',
            1,
        )
    )
    _, rows = importer._parse_source("extended.kml", contents)
    assert rows[0]["survey:status"] == "reviewed"
    assert rows[0]["attachments"] == "p_2025-01-09_15-_20250109_151051.jpg"


def test_osmand_namespaced_extensions_and_photo_notes():
    source_format, rows = parse(ASSETS / "osmand_poi.gpx")
    assert source_format == "gpx"
    cara = next(row for row in rows if row["name"] == "Cara Lodge")
    assert cara["osmand:address"] == "Quamina Street, Alberttown"
    assert cara["osmand:amenity_origin"] == "Amenity:Cara Lodge: tourism:hotel"
    assert cara["osmand:osm_tag_wikidata"] == "Q111880937"
    assert cara["osmand:visited_date"] == "2025-05-20T16:14:43Z"
    _, notes = parse(ASSETS / "osmand_notes.gpx")
    assert len(notes) == 10
    note = next(row for row in notes if row["name"] == "YHpvuyI9--.1.jpg")
    assert note["type"] == "photonote"
    assert note["desc"] == "Fotografía"
    assert note["link"] == "YHpvuyI9--.1.jpg"
    assert note["time"] == "2025-05-03T21:53:38Z"


def test_smart_content_detection_and_observations():
    source_format, rows = parse(ASSETS / "smart_patrol_sample.xml", "export.xml")
    assert source_format == "smart"
    assert len(rows) == 3
    jaguar = next(row for row in rows if row.get("species") == "jaguar")
    assert jaguar["patrol_id"] == "patrol-001"
    assert jaguar["patrol_team"] == "Alpha Team"
    assert jaguar["leg_id"] == "leg-001"
    assert jaguar["waypoint_id"] == "wp-001"
    assert jaguar["category"] == "wildlife-mammal"
    assert jaguar["count"] == 2.0
    assert jaguar["healthy"] is True
    assert json.loads(jaguar["g__coordinates"]) == [-58.5, 5.5]
    assert sum(row["waypoint_id"] == "wp-002" for row in rows) == 2
    logging = next(row for row in rows if row["category"] == "threat-logging")
    assert logging["trees_cut"] == 5.0
    assert logging["severity"] == "medium"


def test_cybertracker_content_detection_and_observation_properties():
    source_format, rows = parse(CYBERTRACKER, "export.json")
    assert source_format == "cybertracker"
    assert len(rows) == 3
    row = next(
        row for row in rows if row["feature.id"] == "8bc8d8c38801469bb09a0e82d4724eb2"
    )
    assert row["_id"] == row["feature.id"]
    assert row["_username"] == "field_team_alpha"
    assert row["number_of_species"] == 7
    assert row["audio_recording"] == "e234a9b27a4a494a9800a918b1d034ce.wav"
    assert json.loads(row["photo_of_site"]) == ["PHOTO_20260430_114040.jpg"]
    assert json.loads(row["_location"])["x"] == -77
    assert json.loads(row["g__coordinates"]) == [-77.0, 38.0]


@pytest.mark.parametrize("filename", ["export.geojson", "export.json"])
def test_mapeo_custom_geojson_route(filename):
    source_format, rows = parse(ASSETS / "mapeo_observations.geojson", filename)
    assert source_format == "geojson"
    assert len(rows) == 3
    row = rows[0]
    assert row["feature.id"] == "a1b2c3d4e5f67890"
    assert row["notes"] == "Gathering spot for bird watchers"
    assert json.loads(row["$photos"]) == ["53a3841fb6028ba608a085d36b1115d9.jpg"]
    assert json.loads(row["g__coordinates"]) == [-73.968285, 40.785091]


def test_osm_ids_and_punctuated_properties():
    _, rows = parse(ASSETS / "osm_overpass.geojson")
    row = next(row for row in rows if row.get("name") == "Bus Station Lijn 5 bus")
    assert row["@id"] == "node/1660255196"
    assert row["name:en"] == "Line 5 bus station"
    assert row["name:nl"] == "Lijn 5 bus station"
    assert row["amenity"] == "bus_station"


@pytest.mark.parametrize(
    "path,identity,expected",
    [
        (
            ASSETS / "kobotoolbox_submissions.xlsx",
            ("source_id", "254135872"),
            {"_index": "1", "Enter_the_community_name": None},
        ),
        (
            ASSETS / "smart_patrol_sample.xml",
            ("species", "jaguar"),
            {
                "patrol_id": "patrol-001",
                "waypoint_id": "wp-001",
                "count": "2",
                "healthy": "true",
                "g__coordinates": "[-58.5, 5.5]",
            },
        ),
        (
            CYBERTRACKER,
            ("source_id", "8bc8d8c38801469bb09a0e82d4724eb2"),
            {
                "feature_id": "8bc8d8c38801469bb09a0e82d4724eb2",
                "number_of_species": "7",
                "photo_of_site": '["PHOTO_20260430_114040.jpg"]',
            },
        ),
        (
            ASSETS / "mapeo_observations.geojson",
            ("feature_id", "a1b2c3d4e5f67890"),
            {
                "__photos": '["53a3841fb6028ba608a085d36b1115d9.jpg"]',
                "g__type": "Point",
                "g__coordinates": "[-73.968285, 40.785091]",
            },
        ),
        (
            ASSETS / "osmand_poi.gpx",
            ("name", "Cara Lodge"),
            {
                "osmandaddress": "Quamina Street, Alberttown",
                "osmandosm_tag_wikidata": "Q111880937",
            },
        ),
    ],
)
def test_app_exports_survive_apply(
    mock_db_connection, tmp_path, monkeypatch, path, identity, expected
):
    monkeypatch.setenv("DATASET_IMPORTER_DATALAKE_ROOT", str(tmp_path / "datalake"))
    staged = importer.stage_import(
        mock_db_connection,
        {
            "name": path.name,
            "data": base64.b64encode(path.read_bytes()).decode(),
        },
        "create",
        "app_observations",
    )
    preview = importer.preview_import(mock_db_connection, staged["import_id"])
    assert (
        importer.apply_import(
            mock_db_connection, staged["import_id"], preview["preview_id"]
        )["success"]
        is True
    )
    with psycopg.connect(mock_db_connection) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT * FROM public.app_observations")
        columns = [column.name for column in cursor.description]
        rows = [dict(zip(columns, values)) for values in cursor.fetchall()]
    key, value = identity
    row = next(row for row in rows if row[key] == value)
    assert {key: row[key] for key in expected} == expected
    assert row["_id"] != value
    if path == CYBERTRACKER:
        location = json.loads(row["_location"])
        assert location["x"] == -77
        assert location["y"] == 38
        assert location["d"] is None
