from dataclasses import dataclass

import pytest
import responses
import testing.postgresql

from f.connectors.localcontexts.tests.assets import server_responses

SERVER_URL = "https://sandbox.localcontextshub.org"


@dataclass
class LocalContextsProject:
    localcontexts_project: dict


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


def _mock_community_verification(rsps, *, status=403):
    rsps.get(
        f"{SERVER_URL}/api/v2/notices/open_to_collaborate",
        json={"detail": "Forbidden"} if status == 403 else [],
        status=status,
    )


@pytest.fixture
def localcontexts_server(mocked_responses):
    """A mock Local Contexts API that you can use to provide labels data"""

    api_key = "my-api-key"
    project_id = "fake0000-0000-0000-0000-000000000001"

    _mock_community_verification(mocked_responses)

    mocked_responses.get(
        f"{SERVER_URL}/api/v2/projects/{project_id}",
        json=server_responses.SAMPLE_PROJECT,
        status=200,
    )

    mocked_responses.get(
        "https://localcontexts.org/wp-content/uploads/2025/04/bc-provenance.png",
        body=b"fake bc-provenance image data",
        content_type="image/png",
        status=200,
    )

    mocked_responses.get(
        "https://localcontexts.org/wp-content/uploads/2025/04/bc-consent-verified.png",
        body=b"fake bc-consent-verified image data",
        content_type="image/png",
        status=200,
    )

    mocked_responses.get(
        "https://storage.googleapis.com/local-context-hub-staging.appspot.com/communities/tklabel-audiofiles/FAKE_AUDIO_ID.mp3?Expires=9999999999&GoogleAccessId=fake-access-id%40example.iam.gserviceaccount.com&Signature=TotallyFakeSignature1234567890",
        body=b"fake audio data",
        content_type="audio/mpeg",
        status=200,
    )

    mocked_responses.get(
        "https://localcontexts.org/wp-content/uploads/2025/04/tk-culturally-sensitive.png",
        body=b"fake tk-culturally-sensitive image data",
        content_type="image/png",
        status=200,
    )

    mocked_responses.get(
        "https://localcontexts.org/wp-content/uploads/2025/04/tk-community-voice.png",
        body=b"fake tk-community-voice image data",
        content_type="image/png",
        status=200,
    )

    mocked_responses.get(
        "https://localcontexts.org/wp-content/uploads/2025/04/tk-attribution.png",
        body=b"fake tk-attribution image data",
        content_type="image/png",
        status=200,
    )

    return LocalContextsProject(
        localcontexts_project=dict(
            server_url=SERVER_URL,
            api_key=api_key,
            project_id=project_id,
        )
    )


@pytest.fixture
def empty_project_server(mocked_responses):
    """A mock Local Contexts API with an empty project (no labels)."""
    api_key = "test-api-key"
    project_id = "empty-project-id"

    _mock_community_verification(mocked_responses)

    mocked_responses.get(
        f"{SERVER_URL}/api/v2/projects/{project_id}",
        json={
            "unique_id": project_id,
            "title": "Empty Project",
            "bc_labels": [],
            "tk_labels": [],
        },
        status=200,
    )

    return LocalContextsProject(
        localcontexts_project=dict(
            server_url=SERVER_URL,
            api_key=api_key,
            project_id=project_id,
        )
    )


@pytest.fixture
def non_community_server(mocked_responses):
    """Mock API where the key belongs to an Institution/Researcher account (200)."""
    _mock_community_verification(mocked_responses, status=200)
    return {"server_url": SERVER_URL, "api_key": "some-key"}


@pytest.fixture
def invalid_key_server(mocked_responses):
    """Mock API where the key is invalid (401)."""
    mocked_responses.get(
        f"{SERVER_URL}/api/v2/notices/open_to_collaborate",
        json={"detail": "Unauthorized"},
        status=401,
    )
    return {"server_url": SERVER_URL, "api_key": "bad-key"}


@pytest.fixture
def pg_database():
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
