import csv
from pathlib import Path

import psycopg

from f.connectors.kobotoolbox.kobotoolbox_responses import (
    flatten_kobotoolbox_submission,
    main,
    transform_kobotoolbox_form_data,
)


def test_script_e2e(koboserver, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "kobo_responses"

    main(
        koboserver.account,
        koboserver.form_id,
        pg_database,
        table_name,
        asset_storage,
    )

    # Attachments are saved to disk
    assert (asset_storage / table_name / "attachments" / "1637241249813.jpg").exists()

    # Metadata is saved to disk
    assert (asset_storage / table_name / f"{table_name}_metadata.json").exists()
    with open(asset_storage / table_name / f"{table_name}_metadata.json") as f:
        metadata = f.read()
    assert all(
        key in metadata for key in ["name", "uid", "owner__username", "data", "content"]
    )

    # CSV artifact is also saved to disk
    csv_file = asset_storage / table_name / f"{table_name}.csv"
    assert csv_file.exists()

    # Survey responses are written to a SQL Table
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cursor.fetchone()[0] == 3

            # Check that the coordinates of a fixture entry are stored as a Point,
            # and that the coordinates are reversed (longitude, latitude).
            cursor.execute(
                f"SELECT g__type, g__coordinates FROM {table_name} WHERE _id = '124961136'"
            )
            assert cursor.fetchone() == ("Point", "[-122.0109429, 36.97012]")

            # Check that meta/instanceID was sanitized to instanceID__meta
            cursor.execute(
                f"SELECT \"instanceID__meta\" FROM {table_name} WHERE _id = '124961136'"
            )
            assert cursor.fetchone() == ("uuid:e58da38d-3eee-4bd7-8512-4a97ea8fbb01",)

            # Check that the mapping column was created
            cursor.execute(
                f"SELECT COUNT(*) FROM {table_name}__columns WHERE original_column = 'meta/instanceID' AND sql_column = 'instanceID__meta'"
            )
            assert cursor.fetchone()[0] == 1

    # Form labels are written to a SQL Table
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            # (4 survey + 4 choices) × 3 languages = 24 rows
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}__labels")
            assert cursor.fetchone()[0] == 24

            # Verify specific translations for survey items
            cursor.execute(
                f"""
                SELECT label FROM {table_name}__labels 
                WHERE name = 'Record_your_current_location' AND language = 'en'
                """
            )
            assert cursor.fetchone()[0] == "Record your current location"

            cursor.execute(
                f"""
                SELECT label FROM {table_name}__labels 
                WHERE name = 'Record_your_current_location' AND language = 'es'
                """
            )
            assert cursor.fetchone()[0] == "Registre la ubicación actual"

            cursor.execute(
                f"""
                SELECT label FROM {table_name}__labels 
                WHERE name = 'Record_your_current_location' AND language = 'pt'
                """
            )
            assert cursor.fetchone()[0] == "Registre a localização atual"

            cursor.execute(
                f"""
                SELECT label FROM {table_name}__labels 
                WHERE name = 'Estimate_height_of_your_tree_in_meters' AND language = 'en'
                """
            )
            assert (
                cursor.fetchone()[0] == "Estimate the height of your tree (in meters)"
            )

            # Verify specific translations for choice items
            cursor.execute(
                f"""
                SELECT label FROM {table_name}__labels 
                WHERE name = 'shade' AND language = 'es'
                """
            )
            assert cursor.fetchone()[0] == "Sombra"

            cursor.execute(
                f"""
                SELECT label FROM {table_name}__labels 
                WHERE name = 'wildlife_habitat' AND language = 'pt'
                """
            )
            assert cursor.fetchone()[0] == "Habitat da vida selvagem"

            # Check that the type is set for survey / choice items
            cursor.execute(
                f"""
                SELECT DISTINCT type FROM {table_name}__labels 
                WHERE name = 'Record_your_current_location'
                """
            )
            assert cursor.fetchone()[0] == "survey"

            cursor.execute(
                f"""
                SELECT DISTINCT type FROM {table_name}__labels 
                WHERE name = 'shade'
                """
            )
            assert cursor.fetchone()[0] == "choices"


def test_script_e2e__no_translations(koboserver_no_translations, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "kobo_no_translations"

    main(
        koboserver_no_translations.account,
        koboserver_no_translations.form_id,
        pg_database,
        table_name,
        asset_storage,
    )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            # Confirm that for the labels table, there is only a labels column, no language suffix
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}__labels")
            assert cursor.fetchone()[0] == 8
            cursor.execute(
                f"SELECT label FROM {table_name}__labels WHERE name = 'Record_your_current_location'"
            )
            assert cursor.fetchone() == ("Record your current location",)


def test_transform_kobotoolbox_form_data_from_csv():
    csv_path = Path(__file__).parent / "assets" / "kobotoolbox_submissions.csv"
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        data = [dict(row) for row in reader]

    form_name = "Test Form"
    result = transform_kobotoolbox_form_data(data, form_name)

    for submission in result:
        assert submission["dataset_name"] == form_name
        assert submission["data_source"] == "KoboToolbox"
        assert "g__type" in submission
        assert submission["g__type"] == "Point"
        assert "g__coordinates" in submission

    assert result[0]["g__coordinates"] == [-55.963451, 2.164341]


def test_pagination(koboserver_with_pagination, pg_database, tmp_path):
    """Test that pagination correctly fetches all records across multiple pages."""
    asset_storage = tmp_path / "datalake"
    table_name = "kobo_pagination_test"

    main(
        koboserver_with_pagination.account,
        koboserver_with_pagination.form_id,
        pg_database,
        table_name,
        asset_storage,
    )

    # Verify all submissions were fetched and stored
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            # All 3 submissions from fixture should be present
            assert cursor.fetchone()[0] == 3

            # Verify that specific records from different "pages" are present
            cursor.execute(f"SELECT _id FROM {table_name} ORDER BY _id")
            ids = [row[0] for row in cursor.fetchall()]
            assert "124961136" in ids
            assert "125283733" in ids
            assert "125340283" in ids


def test_flatten_kobotoolbox_submission__repeat_group():
    submission = {
        "household_members": [
            {
                "household_members/group_fixture_member_1/group_fixture_member_1_name": "Person One",
                "household_members/group_fixture_member_1/group_fixture_member_1_age": "25",
            },
            {
                "household_members/group_fixture_member_1/group_fixture_member_1_name": "Person Two",
                "household_members/group_fixture_member_1/group_fixture_member_1_age": "30",
            },
        ],
    }
    result = flatten_kobotoolbox_submission(submission)

    assert "household_members" not in result
    assert result["household_members/1/group_fixture_member_1_name"] == "Person One"
    assert result["household_members/1/group_fixture_member_1_age"] == "25"
    assert result["household_members/2/group_fixture_member_1_name"] == "Person Two"
    assert result["household_members/2/group_fixture_member_1_age"] == "30"


def test_flatten_kobotoolbox_submission__field_list_dict():
    submission = {
        "dwelling_counts": {
            "dwelling_counts/group_fixture_house/group_fixture_house_adults": "2",
            "dwelling_counts/group_fixture_house/group_fixture_house_children": "1",
        },
    }
    result = flatten_kobotoolbox_submission(submission)

    assert "dwelling_counts" not in result
    assert result["dwelling_counts/1/group_fixture_house_adults"] == "2"
    assert result["dwelling_counts/1/group_fixture_house_children"] == "1"


def test_flatten_kobotoolbox_submission__preserves_system_fields():
    submission = {
        "_id": 1,
        "_geolocation": [10.0, 20.0],
        "_attachments": [
            {"download_url": "http://example.org/file.jpg", "filename": "file.jpg"}
        ],
        "_validation_status": {},
        "_tags": [],
        "summary_counts/adults": "2",
        "household_members": [],
    }
    result = flatten_kobotoolbox_submission(submission)

    assert result["_id"] == 1
    assert result["_geolocation"] == [10.0, 20.0]
    assert result["_attachments"] == submission["_attachments"]
    assert result["_validation_status"] == {}
    assert result["_tags"] == []
    assert result["summary_counts/adults"] == "2"
    assert result["household_members"] == []


def test_flatten_kobotoolbox_submission__deeply_nested_repeat():
    """A repeat group nested inside another repeat group must flatten all the way down.

    Kobo emits the inner repeat as a slash-keyed ``list[dict]`` under each outer
    row, so the flattener has to recurse instead of leaving the inner list as a
    JSON blob under ``first_group/{i}/second_group``.
    """
    submission = {
        "first_group": [
            {
                "first_group/second_group": [
                    {
                        "first_group/second_group/group_er3uf83_row/group_er3uf83_row_column": "John",
                        "first_group/second_group/group_er3uf83_row/group_er3uf83_row_second_column": "Doe",
                    },
                    {
                        "first_group/second_group/group_er3uf83_row/group_er3uf83_row_column": "Jane",
                        "first_group/second_group/group_er3uf83_row/group_er3uf83_row_second_column": "Doe",
                    },
                ]
            },
            {
                "first_group/second_group": [
                    {
                        "first_group/second_group/group_er3uf83_row/group_er3uf83_row_column": "Foo",
                        "first_group/second_group/group_er3uf83_row/group_er3uf83_row_second_column": "Bar",
                    },
                ]
            },
        ],
    }
    result = flatten_kobotoolbox_submission(submission)

    assert "first_group" not in result
    # Nothing should remain as a nested container once fully flattened.
    assert not any(isinstance(v, (list, dict)) for v in result.values())
    assert result["first_group/1/second_group/1/group_er3uf83_row_column"] == "John"
    assert result["first_group/1/second_group/2/group_er3uf83_row_column"] == "Jane"
    assert (
        result["first_group/1/second_group/2/group_er3uf83_row_second_column"] == "Doe"
    )
    assert result["first_group/2/second_group/1/group_er3uf83_row_column"] == "Foo"
    assert (
        result["first_group/2/second_group/1/group_er3uf83_row_second_column"] == "Bar"
    )


def test_script_e2e__nested_repeats(koboserver_nested, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "kobo_nested_repeats"

    main(
        koboserver_nested.account,
        koboserver_nested.form_id,
        pg_database,
        table_name,
        asset_storage,
    )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cursor.fetchone()[0] == 3

            cursor.execute(
                f'SELECT "group_fixture_member_1_name__1__household_members" '
                f"FROM {table_name} WHERE _id = '900001'"
            )
            assert cursor.fetchone()[0] == "Person One"

            cursor.execute(
                f'SELECT "group_fixture_house_adults__1__dwelling_counts" '
                f"FROM {table_name} WHERE _id = '900002'"
            )
            assert cursor.fetchone()[0] == "2"

            # A repeat nested inside a repeat is flattened all the way down, so
            # even the innermost leaf lands in its own reversed/underscored column.
            cursor.execute(
                f'SELECT "group_er3uf83_row_column__2__second_group__1__first_group" '
                f"FROM {table_name} WHERE _id = '900003'"
            )
            assert cursor.fetchone()[0] == "Jane"
