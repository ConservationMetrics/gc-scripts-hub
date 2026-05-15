import psycopg

from f.connectors.epicollect.epicollect_pull import (
    main,
    transform_epicollect_entries,
)
from f.connectors.epicollect.tests.assets.server_responses import (
    AUDIO_FILENAME,
    FORM_NAME,
    PHOTO_FILENAME,
    PRIMARY_UUID,
    VIDEO_FILENAME,
)


def test_script_e2e(epicollect_server, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "ec5_responses"

    main(
        epicollect_server.project_slug,
        epicollect_server.client_id,
        epicollect_server.client_secret,
        pg_database,
        table_name,
        asset_storage,
    )

    # Project logo saved to disk (logo_url is non-empty in fixture)
    assert (asset_storage / table_name / "logo.jpg").exists()

    # Media attachments for entry 1 saved to disk
    attachments = asset_storage / table_name / "attachments"
    assert (attachments / PHOTO_FILENAME).exists()
    assert (attachments / AUDIO_FILENAME).exists()
    assert (attachments / VIDEO_FILENAME).exists()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            # Three entries returned by the single-page fixture
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == 3

            # Geometry: lat=38.760781, lon=-77.197741 → GeoJSON [lon, lat]
            cur.execute(
                f"SELECT g__type, g__coordinates FROM {table_name} "
                f"WHERE _id = %s",
                (PRIMARY_UUID,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "Point"
            assert row[1] == "[-77.197741, 38.760781]"

            # dataset_name comes from the first form's name in project metadata
            cur.execute(
                f"SELECT dataset_name FROM {table_name} WHERE _id = %s",
                (PRIMARY_UUID,),
            )
            assert cur.fetchone()[0] == FORM_NAME

            # data_source is always "EpiCollect5"
            cur.execute(
                f"SELECT data_source FROM {table_name} WHERE _id = %s",
                (PRIMARY_UUID,),
            )
            assert cur.fetchone()[0] == "EpiCollect5"

            # Entry with no GPS fix has no geometry fields
            cur.execute(
                f"SELECT g__type FROM {table_name} "
                f"WHERE _id = 'b1234567-0000-0000-0000-000000000001'"
            )
            assert cur.fetchone()[0] is None

            # Unicode title stored correctly
            cur.execute(
                f"SELECT title FROM {table_name} "
                f"WHERE _id = 'c2345678-0000-0000-0000-000000000002'"
            )
            assert cur.fetchone()[0] == "Bosque tropical húmedo"

            # Columns mapping table was created
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_name = '{table_name}__columns'"
            )
            assert cur.fetchone()[0] == 1


def test_pagination(epicollect_server_paginated, pg_database, tmp_path):
    """Two pages of entries (2 per page, 4 total) are all fetched and stored."""
    asset_storage = tmp_path / "datalake"
    table_name = "ec5_paginated"

    main(
        epicollect_server_paginated.project_slug,
        epicollect_server_paginated.client_id,
        epicollect_server_paginated.client_secret,
        pg_database,
        table_name,
        asset_storage,
    )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cur.fetchone()[0] == 4

            # Entries from both pages present
            cur.execute(f"SELECT _id FROM {table_name} ORDER BY _id")
            ids = {row[0] for row in cur.fetchall()}
            assert PRIMARY_UUID in ids
            assert "b1234567-0000-0000-0000-000000000001" in ids
            assert "c2345678-0000-0000-0000-000000000002" in ids
            assert "d3456789-0000-0000-0000-000000000003" in ids

            # GPS coordinates on page-2 entry
            cur.execute(
                f"SELECT g__type, g__coordinates FROM {table_name} "
                f"WHERE _id = 'c2345678-0000-0000-0000-000000000002'"
            )
            row = cur.fetchone()
            assert row[0] == "Point"
            assert row[1] == "[45.67, 1.23]"


def test_transform_no_location():
    """Entries missing a GPS fix produce no geometry fields."""
    entries = [
        {
            "ec5_uuid": "abc-001",
            "created_at": "2026-05-15T00:00:00.000Z",
            "4_Record_location": {
                "latitude": "",
                "longitude": "",
                "accuracy": "",
                "UTM_Northing": "",
                "UTM_Easting": "",
                "UTM_Zone": "",
            },
        }
    ]
    result = transform_epicollect_entries(entries, form_name="Test Form")

    assert result[0]["_id"] == "abc-001"
    assert result[0]["data_source"] == "EpiCollect5"
    assert result[0]["dataset_name"] == "Test Form"
    assert "g__type" not in result[0]
    assert "g__coordinates" not in result[0]


def test_transform_with_location():
    """Valid location dict produces correct GeoJSON-ordered coordinates."""
    entries = [
        {
            "ec5_uuid": "abc-002",
            "created_at": "2026-05-15T00:00:00.000Z",
            "survey_location": {
                "latitude": 38.760781,
                "longitude": -77.197741,
                "accuracy": 30,
            },
        }
    ]
    result = transform_epicollect_entries(entries)

    assert result[0]["g__type"] == "Point"
    assert result[0]["g__coordinates"] == [-77.197741, 38.760781]


def test_transform_ec5_uuid_renamed():
    """ec5_uuid is renamed to _id."""
    entries = [{"ec5_uuid": "test-uuid-123", "title": "Test"}]
    result = transform_epicollect_entries(entries)

    assert result[0]["_id"] == "test-uuid-123"
    assert "ec5_uuid" not in result[0]
