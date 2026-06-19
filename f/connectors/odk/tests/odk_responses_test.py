import csv
from pathlib import Path

import psycopg

from f.connectors.odk.odk_responses import main, transform_odk_form_data


def test_script_e2e(odkserver, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "odk_responses"

    main(
        odkserver.config,
        odkserver.form_id,
        pg_database,
        table_name,
        asset_storage,
    )

    # Attachments are saved to disk
    assert (asset_storage / table_name / "attachments" / "1739327186781.m4a").exists()

    # CSV artifact is also saved to disk, mirroring the GeoJSON-to-Postgres flow
    csv_file = asset_storage / table_name / f"{table_name}.csv"
    assert csv_file.exists()
    with csv_file.open(newline="", encoding="utf-8") as f:
        csv_rows = list(csv.DictReader(f))
    assert len(csv_rows) == 3
    assert "_id" in csv_rows[0]
    assert {r["_id"] for r in csv_rows} >= {
        "uuid:cb7955d8-dc7c-480f-9699-20555a155c92"
    }
    # Coordinates are JSON-encoded in the CSV cell so they round-trip into DB
    geo_row = next(
        r
        for r in csv_rows
        if r["_id"] == "uuid:cb7955d8-dc7c-480f-9699-20555a155c92"
    )
    assert geo_row["g__coordinates"] == "[-77.9867385, 40.322394]"

    # Survey responses are written to a SQL Table
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM odk_responses")
            assert cursor.fetchone()[0] == 3

            # Check that the coordinates of a fixture entry are stored as a Point
            cursor.execute(
                "SELECT g__type, g__coordinates FROM odk_responses WHERE _id = 'uuid:cb7955d8-dc7c-480f-9699-20555a155c92'"
            )
            assert cursor.fetchone() == ("Point", "[-77.9867385, 40.322394]")

            # Check that a __columns table is created
            cursor.execute(
                "SELECT * FROM information_schema.tables WHERE table_name = 'odk_responses__columns'"
            )
            assert cursor.fetchone() is not None


def test_transform_odk_form_data_from_csv():
    csv_path = Path(__file__).parent / "assets" / "submissions.csv"
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        data = [dict(row) for row in reader]

    form_name = "Test Form"
    result = transform_odk_form_data(data, form_name)

    for submission in result:
        assert submission["dataset_name"] == form_name
        assert submission["data_source"] == "ODK"
        assert "g__type" in submission
        assert submission["g__type"] == "Point"
        assert "g__coordinates" in submission

    # Check specific coordinates from the CSV data
    # First submission: 40.786261, -73.964718 -> [-73.964718, 40.786261]
    assert result[0]["g__coordinates"] == [-73.964718, 40.786261]

    # Second submission: 38.769845, -77.207058 -> [-77.207058, 38.769845]
    assert result[1]["g__coordinates"] == [-77.207058, 38.769845]
