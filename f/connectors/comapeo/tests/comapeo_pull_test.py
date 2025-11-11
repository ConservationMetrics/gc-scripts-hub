import json

import psycopg2
import requests

from f.connectors.comapeo.comapeo_pull import (
    download_file,
    download_project_observations,
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

    result, stats = transform_comapeo_observations(
        SAMPLE_OBSERVATIONS, project_name, project_id
    )

    assert len(result) == len(SAMPLE_OBSERVATIONS)
    assert stats["skipped_icons"] == 0
    assert stats["icon_failed"] == 0

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


def test_transform_comapeo_observations_with_icon_failures(mocked_responses, tmp_path):
    """Test that icon failures are properly counted in stats."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"
    project_name = "Forest Expedition"
    attachment_root = str(tmp_path)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Create observations with presetRefs that will fail to download icons
    observations = [
        {
            "docId": "doc_id_1",
            "lat": -33.8688,
            "lon": 151.2093,
            "presetRef": {
                "docId": "preset_1",
            },
        },
        {
            "docId": "doc_id_2",
            "lat": 48.8566,
            "lon": 2.3522,
            "presetRef": {
                "docId": "preset_2",
            },
        },
    ]

    # Mock preset endpoints to return valid presets with iconRefs
    for i, preset_id in enumerate(["preset_1", "preset_2"], 1):
        preset_response = {
            "data": {
                "name": f"Preset {i}",
                "iconRef": {
                    "docId": f"icon_{i}",
                    "url": f"{server_url}/projects/{project_id}/icon/icon_{i}",
                },
            }
        }
        mocked_responses.get(
            f"{server_url}/projects/{project_id}/preset/{preset_id}",
            json=preset_response,
            status=200,
        )
        # Mock icon endpoints to fail (404)
        mocked_responses.get(
            f"{server_url}/projects/{project_id}/icon/icon_{i}",
            status=404,
        )

    features, stats = transform_comapeo_observations(
        observations,
        project_name,
        project_id,
        server_url,
        session,
        attachment_root,
    )

    assert len(features) == 2
    assert stats["icon_failed"] == 2  # Both icons failed
    assert stats["skipped_icons"] == 0


def test_transform_comapeo_observations_with_skipped_icons(mocked_responses, tmp_path):
    """Test that skipped icons are properly counted in stats."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"
    project_name = "Forest Expedition"
    attachment_root = str(tmp_path)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Create icon directory and pre-existing icon file
    # The icon filename is based on normalize_identifier(preset_name)
    # "TestPreset" normalizes to "test_preset"
    icon_dir = tmp_path / "comapeo" / "forest_expedition" / "icons"
    icon_dir.mkdir(parents=True)
    (icon_dir / "test_preset.png").write_bytes(b"existing icon")

    observations = [
        {
            "docId": "doc_id_1",
            "lat": -33.8688,
            "lon": 151.2093,
            "presetRef": {
                "docId": "preset_1",
            },
        },
    ]

    # Mock preset endpoint
    preset_response = {
        "data": {
            "name": "TestPreset",
            "iconRef": {
                "docId": "icon_1",
                "url": f"{server_url}/projects/{project_id}/icon/icon_1",
            },
        }
    }
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset/preset_1",
        json=preset_response,
        status=200,
    )

    features, stats = transform_comapeo_observations(
        observations,
        project_name,
        project_id,
        server_url,
        session,
        attachment_root,
    )

    assert len(features) == 1
    assert stats["skipped_icons"] == 1  # Icon was skipped (already exists)
    assert stats["icon_failed"] == 0


def test_download_project_observations_with_failures(
    mocked_responses, tmp_path
):
    """Test that attachment failures are properly counted in stats."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"
    project_name = "Forest Expedition"
    attachment_root = str(tmp_path)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Mock observations endpoint with observations that have attachments
    observations_response = {
        "data": [
            {
                "docId": "doc_1",
                "attachments": [
                    {"url": f"{server_url}/attachments/attachment1.jpg"},
                    {"url": f"{server_url}/attachments/attachment2.jpg"},
                ],
            },
            {
                "docId": "doc_2",
                "attachments": [
                    {"url": f"{server_url}/attachments/attachment3.jpg"},
                ],
            },
        ]
    }
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/observation",
        json=observations_response,
        status=200,
    )

    # Mock some attachments to succeed and some to fail
    mocked_responses.get(
        f"{server_url}/attachments/attachment1.jpg",
        body=b"successful attachment",
        content_type="image/jpeg",
        status=200,
    )
    # attachment2.jpg will fail (404)
    mocked_responses.get(
        f"{server_url}/attachments/attachment2.jpg",
        status=404,
    )
    # attachment3.jpg will fail (500)
    mocked_responses.get(
        f"{server_url}/attachments/attachment3.jpg",
        status=500,
    )

    observations, stats = download_project_observations(
        server_url, session, project_id, project_name, attachment_root
    )

    assert len(observations) == 2
    assert stats["attachment_failed"] == 2  # Two attachments failed
    assert stats["skipped_attachments"] == 0


def test_download_project_observations_with_skipped(
    mocked_responses, tmp_path
):
    """Test that skipped attachments are properly counted in stats."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"
    project_name = "Forest Expedition"
    attachment_root = str(tmp_path)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Create attachment directory and pre-existing attachment file
    attachment_dir = tmp_path / "comapeo" / "forest_expedition" / "attachments"
    attachment_dir.mkdir(parents=True)
    (attachment_dir / "attachment1.jpg").write_bytes(b"existing attachment")

    # Mock observations endpoint
    observations_response = {
        "data": [
            {
                "docId": "doc_1",
                "attachments": [
                    {"url": f"{server_url}/attachments/attachment1.jpg"},
                ],
            },
        ]
    }
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/observation",
        json=observations_response,
        status=200,
    )

    observations, stats = download_project_observations(
        server_url, session, project_id, project_name, attachment_root
    )

    assert len(observations) == 1
    assert stats["skipped_attachments"] == 1  # Attachment was skipped (already exists)
    assert stats["attachment_failed"] == 0


def test_fetch_preset(mocked_responses, tmp_path):
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

    result, skipped, failed = fetch_preset(
        server_url, session, project_id, preset_doc_id
    )

    assert result is not None
    assert result["name"] == "Camp"
    assert result["color"] == "#B209B2"
    assert isinstance(result["terms"], list)
    assert "campsite" in result["terms"]
    assert skipped == 0
    assert failed is False

    # Test preset fetch with icon downloading
    icon_dir = tmp_path / "icons"
    existing_icon_stems = set()
    icon_doc_id = preset_response["data"]["iconRef"]["docId"]
    icon_body = b"fake icon data" * 10
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/icon/{icon_doc_id}",
        body=icon_body,
        content_type="image/png",
        headers={"Content-Length": str(len(icon_body))},
    )

    preset_data, skipped, failed = fetch_preset(
        server_url, session, project_id, preset_doc_id, icon_dir, existing_icon_stems
    )

    assert preset_data is not None
    assert skipped == 0
    assert failed is False
    # Verify icon was downloaded
    icon_path = icon_dir / "camp.png"
    assert icon_path.exists()
    assert icon_path.read_bytes() == icon_body

    # Test preset not found (returns None)
    unknown_preset_id = "unknown_preset_id"
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset/{unknown_preset_id}",
        json={"data": None},
        status=200,
    )
    result, skipped, failed = fetch_preset(
        server_url, session, project_id, unknown_preset_id
    )
    assert result is None
    assert skipped == 0
    assert failed is False

    # Test HTTP error (returns None)
    error_preset_id = "error_preset_id"
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset/{error_preset_id}",
        status=404,
    )

    result, skipped, failed = fetch_preset(
        server_url, session, project_id, error_preset_id
    )
    assert result is None
    assert skipped == 0
    assert failed is False

    # Test invalid JSON response (returns None)
    invalid_json_preset_id = "invalid_json_preset_id"
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset/{invalid_json_preset_id}",
        body="not json",
        status=200,
        content_type="text/plain",
    )

    result, skipped, failed = fetch_preset(
        server_url, session, project_id, invalid_json_preset_id
    )
    assert result is None
    assert skipped == 0
    assert failed is False


def test_download_file(mocked_responses, tmp_path):
    """Test the file downloading function."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    icon_dir = tmp_path / "icons"
    icon_dir.mkdir(parents=True)

    # Test successful PNG icon download
    icon_url = f"{server_url}/projects/test_project/icon/test_icon_id"
    icon_body = b"fake png icon data" * 10
    mocked_responses.get(
        icon_url,
        body=icon_body,
        content_type="image/png",
        headers={"Content-Length": str(len(icon_body))},
    )

    existing_icon_stems = set()
    file_name, skipped = download_file(
        icon_url, session, str(icon_dir / "test_icon"), existing_icon_stems
    )

    assert file_name == "test_icon.png"
    assert skipped == 0
    icon_path = icon_dir / "test_icon.png"
    assert icon_path.exists()
    assert icon_path.read_bytes() == icon_body

    # Test icon download when file already exists (skip)
    existing_icon_stems.add("existing_icon")
    existing_icon_path = icon_dir / "existing_icon.png"
    existing_icon_path.write_bytes(b"existing icon data")

    file_name, skipped = download_file(
        icon_url, session, str(icon_dir / "existing_icon"), existing_icon_stems
    )

    assert file_name == "existing_icon.png"
    assert skipped == 1
    # Verify file wasn't overwritten
    assert existing_icon_path.read_bytes() == b"existing icon data"

    # Test HTTP error (404)
    error_icon_url = f"{server_url}/projects/test_project/icon/not_found_icon"
    mocked_responses.get(error_icon_url, status=404)

    file_name, skipped = download_file(
        error_icon_url, session, str(icon_dir / "error_icon"), existing_icon_stems
    )

    assert file_name is None
    assert skipped == 0  # Errors are not skips
    assert not (icon_dir / "error_icon").exists()

    # Test HTTP error (500)
    server_error_url = f"{server_url}/projects/test_project/icon/server_error_icon"
    mocked_responses.get(server_error_url, status=500)

    file_name, skipped = download_file(
        server_error_url,
        session,
        str(icon_dir / "server_error_icon"),
        existing_icon_stems,
    )

    assert file_name is None
    assert skipped == 0  # Errors are not skips

    # Test missing Content-Type header (should save without extension)
    no_content_type_url = (
        f"{server_url}/projects/test_project/icon/no_content_type_icon"
    )
    no_content_type_body = b"icon data without content type"
    mocked_responses.get(
        no_content_type_url,
        body=no_content_type_body,
        headers={
            "Content-Length": str(len(no_content_type_body)),
            "Content-Type": "",
        },
    )

    file_name, skipped = download_file(
        no_content_type_url,
        session,
        str(icon_dir / "no_content_type_icon"),
        existing_icon_stems,
    )

    assert file_name == "no_content_type_icon"
    assert skipped == 0
    no_content_type_path = icon_dir / "no_content_type_icon"
    assert no_content_type_path.exists()
    assert no_content_type_path.read_bytes() == no_content_type_body


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

    # Icons are saved to disk with sanitized preset names
    assert (
        asset_storage / "comapeo" / "forest_expedition" / "icons" / "camp.png"
    ).exists()
    assert (
        asset_storage / "comapeo" / "forest_expedition" / "icons" / "water_source.png"
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
