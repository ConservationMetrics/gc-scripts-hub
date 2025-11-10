import json

import psycopg2
import requests

from f.connectors.comapeo.comapeo_pull import (
    fetch_preset,
    main,
    transform_comapeo_observations,
    transform_comapeo_tracks,
)
from f.connectors.comapeo.tests.assets import server_responses
from f.connectors.comapeo.tests.assets.server_responses import (
    SAMPLE_OBSERVATIONS,
    SAMPLE_TRACK,
)


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

    # Check that key fields are present
    assert "version_id" in properties1
    assert "original_version_id" in properties1
    assert "schema_name" in properties1
    assert properties1["schema_name"] == "observation"
    assert "deleted" in properties1
    assert properties1["deleted"] == "False"

    # Check that metadata fields are present
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
    # Note: Preset fields (category, terms, color) are not tested here since
    # preset fetching requires server_url/access_token. These are tested in e2e test.

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
    # of attachment filenames) in the `download_project_observations` function, which is called earlier in
    # the script. This is why the attachment field here is the raw attachment URL.
    # In the test data, attachments are still in array format, so they'll be stringified
    assert "attachments" in properties2
    assert isinstance(properties2["attachments"], str)


def test_transform_comapeo_tracks():
    """Test the track transformation function with sample data."""
    project_name = "Forest Expedition"
    project_id = "forest_expedition"

    result = transform_comapeo_tracks(SAMPLE_TRACK, project_name, project_id)

    assert len(result) == len(SAMPLE_TRACK)

    feature1 = result[0]
    assert feature1["type"] == "Feature"
    assert (
        feature1["id"]
        == "8e8d1002ca585382e97d8a7a9ab9ce04d484b2525b6d6e6335340c46ad430d24"
    )
    assert feature1["geometry"]["type"] == "LineString"
    assert len(feature1["geometry"]["coordinates"]) == 12
    assert feature1["geometry"]["coordinates"][0] == [151.2093, -33.8688]  # [lon, lat]
    assert feature1["geometry"]["coordinates"][1] == [151.2094, -33.8689]

    properties1 = feature1["properties"]
    assert properties1["project_name"] == "Forest Expedition"
    assert properties1["project_id"] == "forest_expedition"
    assert properties1["data_source"] == "CoMapeo"
    assert properties1["notes"] == "Cool stream"

    # Check that key fields are present
    assert "version_id" in properties1
    assert "original_version_id" in properties1
    assert "schema_name" in properties1
    assert properties1["schema_name"] == "track"
    assert "deleted" in properties1
    assert properties1["deleted"] == "False"

    # Check that timestamps are present and properly formatted
    assert "timestamps" in properties1
    timestamps = json.loads(properties1["timestamps"])
    assert isinstance(timestamps, list)
    assert len(timestamps) == 12
    assert timestamps[0] == "2024-10-14T20:43:35.919Z"
    assert timestamps[1] == "2024-10-14T20:43:46.658Z"

    # Check that coordinates and timestamps are aligned
    assert len(feature1["geometry"]["coordinates"]) == len(timestamps)

    # presetRef should not be present
    assert "preset_ref" not in properties1


def test_fetch_preset(mocked_responses):
    """Test the preset fetching function."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Test successful preset fetch
    preset_doc_id = "e8438f39d2130f478d72c933a6b30dd564075a57c0a0abcf48fd3dc47b4beb24"
    preset_response = server_responses.comapeo_preset(
        server_url, project_id, preset_doc_id
    )

    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset/{preset_doc_id}",
        json=preset_response,
        status=200,
    )

    result = fetch_preset(server_url, session, project_id, preset_doc_id)

    assert result is not None
    assert result["name"] == "Camp"
    assert result["color"] == "#B209B2"
    assert isinstance(result["terms"], list)
    assert "campsite" in result["terms"]

    # Test preset not found (returns None)
    unknown_preset_id = "unknown_preset_id"
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset/{unknown_preset_id}",
        json={"data": None},
        status=200,
    )
    result = fetch_preset(server_url, session, project_id, unknown_preset_id)
    assert result is None

    # Test HTTP error (returns None)
    error_preset_id = "error_preset_id"
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset/{error_preset_id}",
        status=404,
    )

    result = fetch_preset(server_url, session, project_id, error_preset_id)
    assert result is None

    # Test invalid JSON response (returns None)
    invalid_json_preset_id = "invalid_json_preset_id"
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset/{invalid_json_preset_id}",
        body="not json",
        status=200,
        content_type="text/plain",
    )

    result = fetch_preset(server_url, session, project_id, invalid_json_preset_id)
    assert result is None


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
        asset_storage
        / "comapeo"
        / "forest_expedition"
        / "attachments"
        / "a1b2c3d4e5f6g7h8.jpg"
    ).exists()

    with psycopg2.connect(**pg_database) as conn:
        # Observations from forest_expedition are written to a SQL Table in expected format
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM comapeo_forest_expedition_observations"
            )
            assert cursor.fetchone()[0] == 3

            cursor.execute(
                "SELECT * FROM comapeo_forest_expedition_observations LIMIT 0"
            )
            columns = [desc[0] for desc in cursor.description]

            # Check that key fields are present
            assert "notes" in columns
            assert "created_at_2" in columns
            assert "project_name" in columns
            assert "project_id" in columns
            assert "data_source" in columns
            assert "g__type" in columns
            assert "g__coordinates" in columns
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
            # Preset fields should be present
            assert "category" in columns
            assert "terms" in columns
            assert "color" in columns

            # Check geometry data
            cursor.execute(
                "SELECT g__type FROM comapeo_forest_expedition_observations LIMIT 1"
            )
            assert cursor.fetchone()[0] == "Point"

            # Check specific coordinate values from the test data
            cursor.execute(
                "SELECT g__coordinates FROM comapeo_forest_expedition_observations WHERE \"docId\" = 'doc_id_1'"
            )
            coords = cursor.fetchone()[0]
            assert (
                coords == "[151.2093, -33.8688]"
            )  # [lon, lat] format in database (GeoJSON)

            cursor.execute(
                "SELECT g__coordinates FROM comapeo_forest_expedition_observations WHERE \"docId\" = 'doc_id_2'"
            )
            coords = cursor.fetchone()[0]
            assert (
                coords == "[2.3522, 48.8566]"
            )  # [lon, lat] format in database (GeoJSON)

            # Check that metadata fields are properly set
            cursor.execute(
                "SELECT project_name, project_id, data_source FROM comapeo_forest_expedition_observations LIMIT 1"
            )
            row = cursor.fetchone()
            assert row[0] == "Forest Expedition"
            assert row[1] == "forest_expedition"
            assert row[2] == "CoMapeo"

            # Check that tags are properly flattened and converted
            cursor.execute(
                "SELECT notes, type, status FROM comapeo_forest_expedition_observations WHERE \"docId\" = 'doc_id_1'"
            )
            row = cursor.fetchone()
            assert row[0] == "Rapid"
            assert row[1] == "water"
            assert row[2] == "active"

            # Check that attachments are properly stored
            cursor.execute(
                "SELECT attachments FROM comapeo_forest_expedition_observations WHERE \"docId\" = 'doc_id_2'"
            )
            attachments = cursor.fetchone()[0]
            assert "a1b2c3d4e5f6g7h8.jpg" in attachments

            # Check that preset fields are properly stored
            cursor.execute(
                "SELECT category, terms, color FROM comapeo_forest_expedition_observations WHERE \"docId\" = 'doc_id_1'"
            )
            row = cursor.fetchone()
            assert row[0] == "Camp"  # category from preset name
            assert row[1] == "campsite, camping, hunting"  # terms as comma-separated
            assert row[2] == "#B209B2"  # color

            cursor.execute(
                "SELECT category, terms, color FROM comapeo_forest_expedition_observations WHERE \"docId\" = 'doc_id_2'"
            )
            row = cursor.fetchone()
            assert row[0] == "Water Source"  # category from preset name
            assert row[1] == "water, stream, river, well"  # terms as comma-separated
            assert row[2] == "#00A8FF"  # color

        # Tracks from forest_expedition are written to a SQL Table in expected format
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM comapeo_forest_expedition_tracks")
            assert cursor.fetchone()[0] == 1

            cursor.execute("SELECT * FROM comapeo_forest_expedition_tracks LIMIT 0")
            columns = [desc[0] for desc in cursor.description]

            # Check that key fields are present
            assert "notes" in columns
            assert "project_name" in columns
            assert "project_id" in columns
            assert "data_source" in columns
            assert "g__type" in columns
            assert "g__coordinates" in columns
            assert "timestamps" in columns
            assert "version_id" in columns
            assert "schema_name" in columns

            # Check geometry data
            cursor.execute(
                "SELECT g__type FROM comapeo_forest_expedition_tracks LIMIT 1"
            )
            assert cursor.fetchone()[0] == "LineString"

            # Check specific coordinate values from the test data
            cursor.execute(
                "SELECT g__coordinates FROM comapeo_forest_expedition_tracks WHERE \"docId\" = '8e8d1002ca585382e97d8a7a9ab9ce04d484b2525b6d6e6335340c46ad430d24'"
            )
            coords_str = cursor.fetchone()[0]
            coords = json.loads(coords_str)
            assert isinstance(coords, list)
            assert len(coords) == 12
            assert coords[0] == [151.2093, -33.8688]  # [lon, lat] format
            assert coords[1] == [151.2094, -33.8689]

            # Check timestamps
            cursor.execute(
                "SELECT timestamps FROM comapeo_forest_expedition_tracks WHERE \"docId\" = '8e8d1002ca585382e97d8a7a9ab9ce04d484b2525b6d6e6335340c46ad430d24'"
            )
            timestamps_str = cursor.fetchone()[0]
            timestamps = json.loads(timestamps_str)
            assert isinstance(timestamps, list)
            assert len(timestamps) == 12
            assert timestamps[0] == "2024-10-14T20:43:35.919Z"
            assert timestamps[1] == "2024-10-14T20:43:46.658Z"

            # Check that coordinates and timestamps are aligned
            assert len(coords) == len(timestamps)

            # Check that metadata fields are properly set
            cursor.execute(
                "SELECT project_name, project_id, data_source, notes FROM comapeo_forest_expedition_tracks LIMIT 1"
            )
            row = cursor.fetchone()
            assert row[0] == "Forest Expedition"
            assert row[1] == "forest_expedition"
            assert row[2] == "CoMapeo"
            assert row[3] == "Cool stream"

        # comapeo_river_mapping SQL Table does not exist (it's in the blocklist)
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'comapeo_river_mapping'
                )
            """)
            assert not cursor.fetchone()[0]
