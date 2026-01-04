import json

import psycopg2
import requests

from f.connectors.comapeo.comapeo_pull import (
    build_preset_mapping,
    download_file,
    download_preset_icons,
    download_project_observations,
    fetch_all_fields,
    fetch_all_presets,
    fetch_preset,
    main,
    transform_comapeo_observations,
    transform_comapeo_tracks,
)
from f.connectors.comapeo.tests.assets import server_responses
from f.connectors.comapeo.tests.assets.server_responses import (
    SAMPLE_FIELDS,
    SAMPLE_OBSERVATIONS,
    SAMPLE_PRESETS,
    SAMPLE_TRACK,
)


def test_transform_comapeo_observations():
    """Test the transformation function with sample data."""
    project_name = "Forest Expedition"
    project_id = "forest_expedition"

    # Build preset mapping from SAMPLE_PRESETS with actual icon filenames
    icon_filenames = {
        "e8438f39d2130f478d72c933a6b30dd564075a57c0a0abcf48fd3dc47b4beb24": "camp.png",
        "1a08db5f19640fcd22016c35e45aa04f07a3f1a8dc1293dff9fd9232fd5b9c10": "water_source.png",
    }
    preset_mapping = build_preset_mapping(SAMPLE_PRESETS, icon_filenames)

    result = transform_comapeo_observations(
        SAMPLE_OBSERVATIONS, project_name, project_id, preset_mapping
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
    # Preset fields should be present (from preset mapping) including .png extension
    assert properties1["category"] == "Camp"
    assert properties1["terms"] == "campsite, camping, hunting"
    assert properties1["color"] == "#B209B2"
    assert properties1["category_icon"] == "camp.png"

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
    # Preset fields for observation 2 including .png extension
    assert properties2["category"] == "Water Source"
    assert properties2["terms"] == "water, stream, river, well"
    assert properties2["color"] == "#00A8FF"
    assert properties2["category_icon"] == "water_source.png"


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


def test_build_preset_mapping():
    """Test the preset mapping builder."""
    # Test without icon filenames
    preset_mapping = build_preset_mapping(SAMPLE_PRESETS)

    # Check that all presets are in the mapping
    assert len(preset_mapping) == len(SAMPLE_PRESETS)

    # Check the "Camp" preset (without icon filenames, should be just sanitized name)
    camp_preset_id = "e8438f39d2130f478d72c933a6b30dd564075a57c0a0abcf48fd3dc47b4beb24"
    assert camp_preset_id in preset_mapping
    camp_data = preset_mapping[camp_preset_id]
    assert camp_data["name"] == "Camp"
    assert camp_data["terms"] == "campsite, camping, hunting"
    assert camp_data["color"] == "#B209B2"
    assert camp_data["icon_filename"] == "camp"

    # Test with icon filenames
    icon_filenames = {
        camp_preset_id: "camp.png",
        "1a08db5f19640fcd22016c35e45aa04f07a3f1a8dc1293dff9fd9232fd5b9c10": "water_source.png",
    }
    preset_mapping_with_icons = build_preset_mapping(SAMPLE_PRESETS, icon_filenames)

    # Check that icon filenames are included
    camp_data_with_icon = preset_mapping_with_icons[camp_preset_id]
    assert camp_data_with_icon["icon_filename"] == "camp.png"

    water_preset_id = "1a08db5f19640fcd22016c35e45aa04f07a3f1a8dc1293dff9fd9232fd5b9c10"
    water_data_with_icon = preset_mapping_with_icons[water_preset_id]
    assert water_data_with_icon["name"] == "Water Source"
    assert water_data_with_icon["terms"] == "water, stream, river, well"
    assert water_data_with_icon["color"] == "#00A8FF"
    assert water_data_with_icon["icon_filename"] == "water_source.png"

    # Check preset with empty terms
    clay_preset_id = "237f202583456ecb4c689c4216e0651132b115307630a7f8d65571406efff3f5"
    assert clay_preset_id in preset_mapping
    clay_data = preset_mapping[clay_preset_id]
    assert clay_data["name"] == "Clay"
    assert clay_data["terms"] == ""
    assert clay_data["color"] == "#073B4C"
    # No icon filename provided, should use sanitized name
    assert clay_data["icon_filename"] == "clay"


def test_fetch_all_presets(mocked_responses):
    """Test fetching all presets for a project."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Mock the batch preset endpoint
    presets_response = server_responses.comapeo_all_presets(server_url, project_id)
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset",
        json=presets_response,
        status=200,
    )

    presets = fetch_all_presets(server_url, session, project_id)

    assert len(presets) == len(SAMPLE_PRESETS)
    assert presets[0]["name"] == "Camp"
    assert presets[1]["name"] == "Water Source"


def test_fetch_all_fields(mocked_responses):
    """Test fetching all fields for a project."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Mock the fields endpoint
    fields_response = server_responses.comapeo_all_fields(server_url, project_id)
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/field",
        json=fields_response,
        status=200,
    )

    fields = fetch_all_fields(server_url, session, project_id)

    assert len(fields) == len(SAMPLE_FIELDS)
    # Check that field data contains expected keys
    assert "tagKey" in fields[0]
    assert "type" in fields[0]
    assert "label" in fields[0]
    assert "helperText" in fields[0]
    # Check specific field values
    assert fields[0]["tagKey"] == "campsite-notes"
    assert fields[0]["type"] == "text"
    assert fields[0]["label"] == "Campsite notes"


def test_download_preset_icons(mocked_responses, tmp_path):
    """Test downloading preset icons."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"
    icon_dir = tmp_path / "icons"

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Get presets with icon URLs
    presets = server_responses.comapeo_all_presets(server_url, project_id)["data"]

    # Mock icon downloads
    for preset in presets[:2]:  # Only mock first 2 icons
        icon_ref = preset.get("iconRef", {})
        icon_url = icon_ref.get("url")
        if icon_url:
            icon_body = b"fake icon data"
            mocked_responses.get(
                icon_url,
                body=icon_body,
                content_type="image/png",
            )

    stats, icon_filenames = download_preset_icons(presets[:2], icon_dir, session)

    assert stats["skipped_icons"] == 0
    assert stats["icon_failed"] == 0

    # Check that icon filenames are returned with extensions
    assert len(icon_filenames) == 2
    camp_preset_id = "e8438f39d2130f478d72c933a6b30dd564075a57c0a0abcf48fd3dc47b4beb24"
    water_preset_id = "1a08db5f19640fcd22016c35e45aa04f07a3f1a8dc1293dff9fd9232fd5b9c10"
    assert icon_filenames[camp_preset_id] == "camp.png"
    assert icon_filenames[water_preset_id] == "water_source.png"

    # Verify icons were downloaded
    assert (icon_dir / "camp.png").exists()
    assert (icon_dir / "water_source.png").exists()

    # Test skipping existing icons
    stats2, icon_filenames2 = download_preset_icons(presets[:2], icon_dir, session)
    assert stats2["skipped_icons"] == 2
    assert stats2["icon_failed"] == 0
    # Should still return icon filenames even when skipped
    assert icon_filenames2[camp_preset_id] == "camp.png"
    assert icon_filenames2[water_preset_id] == "water_source.png"


def test_download_preset_icons_with_failures(mocked_responses, tmp_path):
    """Test that icon download failures are properly counted."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"
    icon_dir = tmp_path / "icons"

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Get presets with icon URLs
    presets = server_responses.comapeo_all_presets(server_url, project_id)["data"]

    # Mock icon downloads to fail
    for preset in presets[:2]:
        icon_ref = preset.get("iconRef", {})
        icon_url = icon_ref.get("url")
        if icon_url:
            mocked_responses.get(icon_url, status=404)

    stats, icon_filenames = download_preset_icons(presets[:2], icon_dir, session)

    assert stats["icon_failed"] == 2
    assert stats["skipped_icons"] == 0
    # Failed downloads are still recorded in icon_filenames with .png extension inferred from URL
    assert len(icon_filenames) == 2
    camp_preset_id = "e8438f39d2130f478d72c933a6b30dd564075a57c0a0abcf48fd3dc47b4beb24"
    water_preset_id = "1a08db5f19640fcd22016c35e45aa04f07a3f1a8dc1293dff9fd9232fd5b9c10"
    assert (
        icon_filenames[camp_preset_id] == "camp.png"
    )  # .png extension inferred from /icon/ in URL
    assert (
        icon_filenames[water_preset_id] == "water_source.png"
    )  # .png extension inferred from /icon/ in URL


def test_download_project_observations_with_failures(mocked_responses, tmp_path):
    """Test that attachment failures are properly counted in stats."""
    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"
    project_name = "Forest Expedition"
    attachment_root = str(tmp_path)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Mock observations endpoint with observations that have attachments
    # Use realistic CoMapeo API URL structure with /photo/ path
    observations_response = {
        "data": [
            {
                "docId": "doc_1",
                "attachments": [
                    {
                        "url": f"{server_url}/projects/{project_id}/attachments/abc123/photo/a1b2c3d4e5f6"
                    },
                    {
                        "url": f"{server_url}/projects/{project_id}/attachments/abc123/photo/f7e8d9c0b1a2"
                    },
                ],
            },
            {
                "docId": "doc_2",
                "attachments": [
                    {
                        "url": f"{server_url}/projects/{project_id}/attachments/abc123/audio/1a2b3c4d5e6f"
                    },
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
        f"{server_url}/projects/{project_id}/attachments/abc123/photo/a1b2c3d4e5f6",
        body=b"successful attachment",
        content_type="image/jpeg",
        status=200,
    )
    # f7e8d9c0b1a2 will fail (404)
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/attachments/abc123/photo/f7e8d9c0b1a2",
        status=404,
    )
    # 1a2b3c4d5e6f will fail (500) - this is an audio file
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/attachments/abc123/audio/1a2b3c4d5e6f",
        status=500,
    )

    observations, stats, failed_observations_info = download_project_observations(
        server_url, session, project_id, project_name, attachment_root
    )

    assert len(observations) == 2
    assert stats["attachment_failed"] == 2  # Two attachments failed
    assert stats["skipped_attachments"] == 0

    # Check that failed attachments are still recorded in the attachments field
    # Successful download gets .jpg extension from Content-Type
    # Failed downloads get extension inferred from URL path (/photo/ -> .jpg, /audio/ -> .m4a)
    assert observations[0]["attachments"] == "a1b2c3d4e5f6.jpg, f7e8d9c0b1a2.jpg"
    assert observations[1]["attachments"] == "1a2b3c4d5e6f.m4a"

    # Check that failed observations info is tracked
    assert len(failed_observations_info) == 2  # Both observations had failures
    assert "doc_1" in failed_observations_info
    assert "doc_2" in failed_observations_info

    # Check error details for doc_1 (has 1 failed attachment out of 2)
    doc_1_info = failed_observations_info["doc_1"]
    assert len(doc_1_info["urls"]) == 1
    assert (
        f"{server_url}/projects/{project_id}/attachments/abc123/photo/f7e8d9c0b1a2"
        in doc_1_info["urls"]
    )

    # Check error details for doc_2 (has 1 failed attachment)
    doc_2_info = failed_observations_info["doc_2"]
    assert len(doc_2_info["urls"]) == 1
    assert (
        f"{server_url}/projects/{project_id}/attachments/abc123/audio/1a2b3c4d5e6f"
        in doc_2_info["urls"]
    )


def test_download_project_observations_with_skipped(mocked_responses, tmp_path):
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

    observations, stats, failed_observations_info = download_project_observations(
        server_url, session, project_id, project_name, attachment_root
    )

    assert len(observations) == 1
    assert stats["skipped_attachments"] == 1  # Attachment was skipped (already exists)
    assert stats["attachment_failed"] == 0
    assert len(failed_observations_info) == 0  # No failures


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
    file_name, skipped, failed = download_file(
        icon_url, session, str(icon_dir / "test_icon"), existing_icon_stems
    )

    assert file_name == "test_icon.png"
    assert skipped == 0
    assert failed == 0
    icon_path = icon_dir / "test_icon.png"
    assert icon_path.exists()
    assert icon_path.read_bytes() == icon_body

    # Test icon download when file already exists (skip)
    existing_icon_stems.add("existing_icon")
    existing_icon_path = icon_dir / "existing_icon.png"
    existing_icon_path.write_bytes(b"existing icon data")

    file_name, skipped, failed = download_file(
        icon_url, session, str(icon_dir / "existing_icon"), existing_icon_stems
    )

    assert file_name == "existing_icon.png"
    assert skipped == 1
    assert failed == 0
    # Verify file wasn't overwritten
    assert existing_icon_path.read_bytes() == b"existing icon data"

    # Test HTTP error (404)
    error_icon_url = f"{server_url}/projects/test_project/icon/not_found_icon"
    mocked_responses.get(error_icon_url, status=404)

    file_name, skipped, failed = download_file(
        error_icon_url, session, str(icon_dir / "error_icon"), existing_icon_stems
    )

    # Filename is still returned even on failure with inferred extension from URL path
    assert file_name == "error_icon.png"  # .png inferred from /icon/ in URL
    assert skipped == 0
    assert failed == 1
    assert not (icon_dir / "error_icon").exists()

    # Test HTTP error (500)
    server_error_url = f"{server_url}/projects/test_project/icon/server_error_icon"
    mocked_responses.get(server_error_url, status=500)

    file_name, skipped, failed = download_file(
        server_error_url,
        session,
        str(icon_dir / "server_error_icon"),
        existing_icon_stems,
    )

    # Filename is still returned even on failure with inferred extension from URL path
    assert file_name == "server_error_icon.png"  # .png inferred from /icon/ in URL
    assert skipped == 0
    assert failed == 1

    # Test missing Content-Type header with /icon/ URL (should infer .png from URL)
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

    file_name, skipped, failed = download_file(
        no_content_type_url,
        session,
        str(icon_dir / "no_content_type_icon"),
        existing_icon_stems,
    )

    # Infers .png from /icon/ in URL when Content-Type is missing
    assert file_name == "no_content_type_icon.png"
    assert skipped == 0
    assert failed == 0
    no_content_type_path = icon_dir / "no_content_type_icon.png"
    assert no_content_type_path.exists()
    assert no_content_type_path.read_bytes() == no_content_type_body

    # Test missing Content-Type header with /photo/ URL (should infer .jpg from URL)
    photo_dir = tmp_path / "attachments"
    photo_no_ct_url = (
        f"{server_url}/projects/test_project/attachments/abc/photo/photo_no_ct"
    )
    photo_no_ct_body = b"photo data without content type"
    mocked_responses.get(
        photo_no_ct_url,
        body=photo_no_ct_body,
        headers={
            "Content-Length": str(len(photo_no_ct_body)),
            "Content-Type": "",
        },
    )

    file_name, skipped, failed = download_file(
        photo_no_ct_url,
        session,
        str(photo_dir / "photo_no_ct"),
        set(),
    )

    # Infers .jpg from /photo/ in URL when Content-Type is missing
    assert file_name == "photo_no_ct.jpg"
    assert skipped == 0
    assert failed == 0
    photo_no_ct_path = photo_dir / "photo_no_ct.jpg"
    assert photo_no_ct_path.exists()
    assert photo_no_ct_path.read_bytes() == photo_no_ct_body

    # Test missing Content-Type header with /audio/ URL (should infer .m4a from URL)
    audio_no_ct_url = (
        f"{server_url}/projects/test_project/attachments/abc/audio/audio_no_ct"
    )
    audio_no_ct_body = b"audio data without content type"
    mocked_responses.get(
        audio_no_ct_url,
        body=audio_no_ct_body,
        headers={
            "Content-Length": str(len(audio_no_ct_body)),
            "Content-Type": "",
        },
    )

    file_name, skipped, failed = download_file(
        audio_no_ct_url,
        session,
        str(photo_dir / "audio_no_ct"),
        set(),
    )

    # Infers .m4a from /audio/ in URL when Content-Type is missing
    assert file_name == "audio_no_ct.m4a"
    assert skipped == 0
    assert failed == 0
    audio_no_ct_path = photo_dir / "audio_no_ct.m4a"
    assert audio_no_ct_path.exists()
    assert audio_no_ct_path.read_bytes() == audio_no_ct_body

    # Test photo URL failure (should infer .jpg extension from URL path)
    photo_dir = tmp_path / "attachments"
    photo_error_url = (
        f"{server_url}/projects/test_project/attachments/abc123/photo/failed_photo"
    )
    mocked_responses.get(photo_error_url, status=404)

    file_name, skipped, failed = download_file(
        photo_error_url,
        session,
        str(photo_dir / "failed_photo"),
        set(),
    )

    assert file_name == "failed_photo.jpg"  # .jpg inferred from /photo/ in URL
    assert skipped == 0
    assert failed == 1

    # Test audio URL failure (should infer .m4a extension from URL path)
    audio_error_url = (
        f"{server_url}/projects/test_project/attachments/abc123/audio/failed_audio"
    )
    mocked_responses.get(audio_error_url, status=404)

    file_name, skipped, failed = download_file(
        audio_error_url,
        session,
        str(photo_dir / "failed_audio"),
        set(),
    )

    assert file_name == "failed_audio.m4a"  # .m4a inferred from /audio/ in URL
    assert skipped == 0
    assert failed == 1

    # Test generic URL failure (no recognizable pattern, no extension)
    generic_error_url = f"{server_url}/some/random/path/generic_file"
    mocked_responses.get(generic_error_url, status=404)

    file_name, skipped, failed = download_file(
        generic_error_url,
        session,
        str(tmp_path / "generic_file"),
        set(),
    )

    assert file_name == "generic_file"  # No extension since URL pattern is unrecognized
    assert skipped == 0
    assert failed == 1


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

    # Presets JSON is saved to disk
    presets_json_path = asset_storage / "comapeo" / "forest_expedition" / "presets.json"
    assert presets_json_path.exists()
    with open(presets_json_path) as f:
        presets_data = json.load(f)
        assert "data" in presets_data
        assert len(presets_data["data"]) > 0

    # Fields JSON is saved to disk
    fields_json_path = asset_storage / "comapeo" / "forest_expedition" / "fields.json"
    assert fields_json_path.exists()
    with open(fields_json_path) as f:
        fields_data = json.load(f)
        assert "data" in fields_data
        assert len(fields_data["data"]) > 0
        # Check that field data contains expected keys
        assert "tagKey" in fields_data["data"][0]
        assert "type" in fields_data["data"][0]
        assert "label" in fields_data["data"][0]

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
            assert "category_icon" in columns

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
                "SELECT category, terms, color, category_icon FROM comapeo_forest_expedition_observations WHERE \"docId\" = 'doc_id_1'"
            )
            row = cursor.fetchone()
            assert row[0] == "Camp"  # category from preset name
            assert row[1] == "campsite, camping, hunting"  # terms as comma-separated
            assert row[2] == "#B209B2"  # color
            assert row[3] == "camp.png"  # category_icon with extension

            cursor.execute(
                "SELECT category, terms, color, category_icon FROM comapeo_forest_expedition_observations WHERE \"docId\" = 'doc_id_2'"
            )
            row = cursor.fetchone()
            assert row[0] == "Water Source"  # category from preset name
            assert row[1] == "water, stream, river, well"  # terms as comma-separated
            assert row[2] == "#00A8FF"  # color
            assert row[3] == "water_source.png"  # category_icon with extension

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


def test_missing_attachments_geojson_created(
    comapeoserver_with_failing_attachments, pg_database, tmp_path
):
    """Test that a missing attachments GeoJSON file is created when attachments fail to download."""
    asset_storage = tmp_path / "datalake"

    # Run the main script - it should raise RuntimeError due to failed attachment
    try:
        main(
            comapeoserver_with_failing_attachments.comapeo_server,
            comapeoserver_with_failing_attachments.comapeo_project_blocklist,
            pg_database,
            "comapeo",
            asset_storage,
        )
        # If we get here, test should fail - we expected a RuntimeError
        assert False, "Expected RuntimeError to be raised"
    except RuntimeError as e:
        # Verify error message contains expected information
        error_msg = str(e)
        assert "1 attachment(s) failed to download" in error_msg
        assert "1 observation(s) affected" in error_msg
        assert "missing_attachments.geojson" in error_msg

    # Check that the missing attachments GeoJSON file was created despite the error
    missing_attachments_path = (
        asset_storage
        / "comapeo"
        / "forest_expedition"
        / "forest_expedition_observations_missing_attachments.geojson"
    )
    assert missing_attachments_path.exists()

    # Read and verify the contents
    with open(missing_attachments_path) as f:
        missing_data = json.load(f)

    assert missing_data["type"] == "FeatureCollection"
    assert len(missing_data["features"]) == 1

    feature = missing_data["features"][0]
    assert feature["id"] == "failing_obs"
    assert "attachment_download_url" in feature["properties"]
    assert "attachment_download_error" in feature["properties"]
    server_url = comapeoserver_with_failing_attachments.comapeo_server["server_url"]
    assert (
        f"{server_url}/projects/forest_expedition/attachments/failing/photo/fail123"
        in feature["properties"]["attachment_download_url"]
    )
    assert "Failed to download" in feature["properties"]["attachment_download_error"]


def test_no_missing_attachments_geojson_when_all_succeed(
    comapeoserver_observations, pg_database, tmp_path
):
    """Test that no missing attachments file is created when all downloads succeed."""
    asset_storage = tmp_path / "datalake"

    # Run the main script (all attachments should succeed based on fixture mocks)
    main(
        comapeoserver_observations.comapeo_server,
        comapeoserver_observations.comapeo_project_blocklist,
        pg_database,
        "comapeo",
        asset_storage,
    )

    # Check that no missing attachments GeoJSON file was created
    missing_attachments_path = (
        asset_storage
        / "comapeo"
        / "forest_expedition"
        / "forest_expedition_observations_missing_attachments.geojson"
    )
    assert not missing_attachments_path.exists()
