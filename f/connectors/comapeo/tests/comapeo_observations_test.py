import psycopg2

from f.connectors.comapeo.comapeo_observations import (
    main,
    transform_comapeo_observations,
)
from f.connectors.comapeo.tests.assets.server_responses import SAMPLE_OBSERVATIONS


def test_transform_comapeo_observations():
    """Test the transformation function with sample data."""
    project_name = "Forest Expedition"
    project_id = "forest_expedition"

    result = transform_comapeo_observations(
        SAMPLE_OBSERVATIONS, project_name, project_id
    )

    assert len(result) == len(SAMPLE_OBSERVATIONS)

    feature1 = result[0]
    assert feature1["type"] == "Feature"
    assert feature1["id"] == "doc_id_1"
    assert feature1["geometry"]["type"] == "Point"
    assert feature1["geometry"]["coordinates"] == [151.2093, -33.8688]  # [lon, lat]

    properties1 = feature1["properties"]
    assert properties1["project_name"] == "Forest Expedition"
    assert properties1["project_id"] == "forest_expedition"
    assert properties1["data_source"] == "CoMapeo"
    assert properties1["notes"] == "Rapid"
    # New fields from updated API structure
    assert "version_id" in properties1
    assert "original_version_id" in properties1
    assert "schema_name" in properties1
    assert properties1["schema_name"] == "observation"
    assert "deleted" in properties1
    assert properties1["deleted"] == "False"
    # Flattened metadata fields
    assert "manual_location" in properties1
    assert properties1["manual_location"] == "False"
    assert "position_timestamp" in properties1
    assert properties1["position_timestamp"] == "2024-10-14T20:18:10.658Z"
    assert "altitude" in properties1
    assert properties1["altitude"] == "39.29999923706055"
    assert "altitude_accuracy" in properties1
    assert properties1["altitude_accuracy"] == "0.6382266283035278"
    assert "heading" in properties1
    assert properties1["heading"] == "0"
    assert "speed" in properties1
    assert properties1["speed"] == "0.013057432137429714"
    assert "accuracy" in properties1
    assert properties1["accuracy"] == "3.7899999618530273"
    assert "mocked" in properties1
    assert properties1["mocked"] == "False"
    assert "links" in properties1
    # presetRef should not be present
    assert "preset_ref" not in properties1

    feature2 = result[1]
    assert feature2["type"] == "Feature"
    assert feature2["id"] == "doc_id_2"
    assert feature2["geometry"]["type"] == "Point"
    assert feature2["geometry"]["coordinates"] == [2.3522, 48.8566]  # [lon, lat]

    properties2 = feature2["properties"]
    assert properties2["project_name"] == "Forest Expedition"
    assert properties2["project_id"] == "forest_expedition"
    assert properties2["data_source"] == "CoMapeo"
    assert (
        properties2["animal_type"] == "capybara"
    )  # camelCase animal-type converted to snake_case
    # Note: when processing CoMapeo API data, attachments are transformed to a string (composed of a comma-separated list
    # of attachment filenames) in the `download_project_observations_and_attachments` function, which is called earlier in
    # the script. This is why the attachment field here is the raw attachment URL.
    # In the test data, attachments are still in array format, so they'll be stringified
    assert "attachments" in properties2
    assert isinstance(properties2["attachments"], str)


def test_script_e2e(comapeoserver_observations, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"

    main(
        comapeoserver_observations.comapeo_server,
        comapeoserver_observations.comapeo_project_blocklist,
        pg_database,
        "comapeo",
        asset_storage,
    )

    # Attachments are saved to disk
    assert (
        asset_storage / "comapeo" / "forest_expedition" / "attachments" / "capybara.jpg"
    ).exists()

    with psycopg2.connect(**pg_database) as conn:
        # Survey responses from forest_expedition are written to a SQL Table in expected format
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM comapeo_forest_expedition")
            assert cursor.fetchone()[0] == 3

            cursor.execute("SELECT * FROM comapeo_forest_expedition LIMIT 0")
            columns = [desc[0] for desc in cursor.description]

            # Check that key fields are present
            assert "notes" in columns
            assert "created_at_2" in columns
            assert "project_name" in columns
            assert "project_id" in columns
            assert "data_source" in columns
            assert "g__type" in columns
            assert "g__coordinates" in columns
            # Check that new fields from updated API structure are present
            assert "version_id" in columns
            assert "original_version_id" in columns
            assert "schema_name" in columns
            # Check that flattened metadata fields are present
            assert "manual_location" in columns
            assert "position_timestamp" in columns
            assert "altitude" in columns
            assert "altitude_accuracy" in columns
            assert "heading" in columns
            assert "speed" in columns
            assert "accuracy" in columns
            assert "mocked" in columns
            assert "links" in columns
            # presetRef should not be present
            assert "preset_ref" not in columns

            # Check geometry data
            cursor.execute("SELECT g__type FROM comapeo_forest_expedition LIMIT 1")
            assert cursor.fetchone()[0] == "Point"

            # Check specific coordinate values from the test data
            cursor.execute(
                "SELECT g__coordinates FROM comapeo_forest_expedition WHERE \"docId\" = 'doc_id_1'"
            )
            coords = cursor.fetchone()[0]
            assert (
                coords == "[151.2093, -33.8688]"
            )  # [lon, lat] format in database (GeoJSON)

            cursor.execute(
                "SELECT g__coordinates FROM comapeo_forest_expedition WHERE \"docId\" = 'doc_id_2'"
            )
            coords = cursor.fetchone()[0]
            assert (
                coords == "[2.3522, 48.8566]"
            )  # [lon, lat] format in database (GeoJSON)

            # Check that metadata fields are properly set
            cursor.execute(
                "SELECT project_name, project_id, data_source FROM comapeo_forest_expedition LIMIT 1"
            )
            row = cursor.fetchone()
            assert row[0] == "Forest Expedition"
            assert row[1] == "forest_expedition"
            assert row[2] == "CoMapeo"

            # Check that tags are properly flattened and converted
            cursor.execute(
                "SELECT notes, type, status FROM comapeo_forest_expedition WHERE \"docId\" = 'doc_id_1'"
            )
            row = cursor.fetchone()
            assert row[0] == "Rapid"
            assert row[1] == "water"
            assert row[2] == "active"

            # Check that attachments are properly stored
            cursor.execute(
                "SELECT attachments FROM comapeo_forest_expedition WHERE \"docId\" = 'doc_id_2'"
            )
            attachments = cursor.fetchone()[0]
            assert "capybara.jpg" in attachments

        # comapeo_river_mapping SQL Table does not exist (it's in the blocklist)
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'comapeo_river_mapping'
                )
            """)
            assert not cursor.fetchone()[0]
