import re
from dataclasses import dataclass
from typing import Optional

import pytest
import responses
import testing.postgresql

from f.connectors.comapeo.comapeo_pull import comapeo_server
from f.connectors.comapeo.tests.assets import server_responses


@dataclass
class CoMapeoServer:
    """https://hub.windmill.dev/resource_types/194/comapeo_server"""

    comapeo_server: dict
    comapeo_project_blocklist: Optional[list] = None


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps


@pytest.fixture
def comapeoserver_observations(mocked_responses):
    """A mock CoMapeo Server that you can use to provide projects and their observations"""

    server_url = "http://comapeo.example.org"
    access_token = "MapYourWorldTogether!"
    comapeo_project_blocklist = ["river_mapping"]
    project_id = "forest_expedition"

    mocked_responses.get(
        f"{server_url}/projects",
        json=server_responses.comapeo_projects(server_url),
        status=200,
    )
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/observation",
        json=server_responses.comapeo_project_observations(server_url, project_id),
        status=200,
    )
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/track",
        json=server_responses.comapeo_project_tracks(server_url, project_id),
        status=200,
    )
    # Mock batch preset endpoint
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset",
        json=server_responses.comapeo_all_presets(server_url, project_id),
        status=200,
    )
    # Mock batch field endpoint
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/field",
        json=server_responses.comapeo_all_fields(server_url, project_id),
        status=200,
    )
    # Mock photo attachments
    photo_body = b"fake photo data" * 100  # Make it ~1600 bytes
    mocked_responses.get(
        re.compile(
            rf"{re.escape(server_url)}/projects/{re.escape(project_id)}/attachments/.+/photo/.+"
        ),
        body=photo_body,
        content_type="image/jpeg",
        headers={"Content-Length": str(len(photo_body))},
    )
    # Mock audio attachments
    audio_body = b"fake audio data" * 7  # Make it ~100 bytes to match Content-Length
    mocked_responses.get(
        re.compile(
            rf"{re.escape(server_url)}/projects/{re.escape(project_id)}/attachments/.+/audio/.+"
        ),
        body=audio_body,
        content_type="audio/mpeg",
        headers={"Content-Length": str(len(audio_body))},
    )

    # Mock preset endpoints for all presets in SAMPLE_PRESETS
    # Note: Order matters - specific URLs must be registered before regex patterns
    for preset in server_responses.SAMPLE_PRESETS:
        preset_doc_id = preset["docId"]
        preset_response = server_responses.comapeo_preset(
            server_url, project_id, preset_doc_id
        )
        mocked_responses.get(
            f"{server_url}/projects/{project_id}/preset/{preset_doc_id}",
            json=preset_response,
            status=200,
        )

        # Mock icon endpoints for all presets with iconRef
        if "iconRef" in preset:
            icon_doc_id = preset["iconRef"]["docId"]
            icon_body = b"fake icon data" * 10  # Make it ~150 bytes
            mocked_responses.get(
                f"{server_url}/projects/{project_id}/icon/{icon_doc_id}",
                body=icon_body,
                content_type="image/png",
                headers={"Content-Length": str(len(icon_body))},
            )

    # Mock preset endpoint for any other preset (returns None for presets not in SAMPLE_PRESETS)
    # This regex must come AFTER specific URL matches
    mocked_responses.get(
        re.compile(
            rf"{re.escape(server_url)}/projects/{re.escape(project_id)}/preset/.+"
        ),
        json={"data": None},
        status=200,
    )

    # Mock icon endpoint for any other icon (returns 404 for icons not in SAMPLE_PRESETS)
    mocked_responses.get(
        re.compile(
            rf"{re.escape(server_url)}/projects/{re.escape(project_id)}/icon/.+"
        ),
        status=404,
    )

    server: comapeo_server = dict(server_url=server_url, access_token=access_token)

    return CoMapeoServer(
        comapeo_server=server,
        comapeo_project_blocklist=comapeo_project_blocklist,
    )


@pytest.fixture
def comapeoserver_alerts(mocked_responses):
    """A mock CoMapeo Server that you can use to get and post alerts"""

    server_url = "http://comapeo.example.org"
    project_id = "forest_expedition"
    comapeo_alerts_endpoint = (
        f"{server_url}/projects/{project_id}/remoteDetectionAlerts"
    )
    access_token = "MapYourWorldTogether!"

    mocked_responses.get(
        comapeo_alerts_endpoint,
        json=server_responses.comapeo_alerts(),
        status=200,
    )

    mocked_responses.post(
        comapeo_alerts_endpoint,
        status=201,
    )

    server: comapeo_server = dict(server_url=server_url, access_token=access_token)

    return CoMapeoServer(
        comapeo_server=server,
    )


@pytest.fixture
def comapeoserver_with_failing_attachments(mocked_responses):
    """A mock CoMapeo Server with observations that have failing attachments"""

    server_url = "http://comapeo.example.org"
    access_token = "test_token"
    project_id = "forest_expedition"
    project_name = "Forest Expedition"

    # Mock projects endpoint
    mocked_responses.get(
        f"{server_url}/projects",
        json={"data": [{"projectId": project_id, "name": project_name}]},
        status=200,
    )

    # Mock observations with one that has a failing attachment
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/observation",
        json={
            "data": [
                {
                    "docId": "failing_obs",
                    "lat": -33.8688,
                    "lon": 151.2093,
                    "tags": {"notes": "This one has a failing attachment"},
                    "attachments": [
                        {
                            "url": f"{server_url}/projects/{project_id}/attachments/failing/photo/fail123"
                        }
                    ],
                    "schemaName": "observation",
                    "deleted": False,
                    "createdAt": "2024-10-14T20:18:10.658Z",
                }
            ]
        },
        status=200,
    )

    # Mock tracks endpoint (empty)
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/track",
        json={"data": []},
        status=200,
    )

    # Mock presets endpoint (empty)
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/preset",
        json={"data": []},
        status=200,
    )

    # Mock fields endpoint (empty)
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/field",
        json={"data": []},
        status=200,
    )

    # Mock the failing attachment - specific URL must come before any regex patterns
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/attachments/failing/photo/fail123",
        status=404,
    )

    server: comapeo_server = dict(server_url=server_url, access_token=access_token)

    return CoMapeoServer(
        comapeo_server=server,
        comapeo_project_blocklist=[],
    )


@pytest.fixture
def pg_database():
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
