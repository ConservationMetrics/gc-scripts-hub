from dataclasses import dataclass

import pytest
import responses
import testing.postgresql

from f.connectors.localcontexts.tests.assets import server_responses


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def localcontexts_server(mocked_responses):
    """A mock Local Contexts API that you can use to provide labels data"""

    api_key = "my-api-key"
    project_id = "fake0000-0000-0000-0000-000000000001"
    server_url = "https://sandbox.localcontextshub.org"

    @dataclass
    class LocalContextsProject:
        localcontexts_project: dict

    # Mock the project endpoint with actual project_id
    mocked_responses.get(
        f"{server_url}/api/v2/projects/{project_id}",
        json=server_responses.SAMPLE_PROJECT,
        status=200,
    )

    # Mock all media attachments from the labels
    # BC Provenance label image
    mocked_responses.get(
        "https://localcontexts.org/wp-content/uploads/2025/04/bc-provenance.png",
        body=b"fake bc-provenance image data",
        content_type="image/png",
        status=200,
    )

    # BC Consent Verified label image
    mocked_responses.get(
        "https://localcontexts.org/wp-content/uploads/2025/04/bc-consent-verified.png",
        body=b"fake bc-consent-verified image data",
        content_type="image/png",
        status=200,
    )

    # BC Consent Verified audio file
    mocked_responses.get(
        "https://storage.googleapis.com/local-context-hub-staging.appspot.com/communities/tklabel-audiofiles/FAKE_AUDIO_ID.mp3?Expires=9999999999&GoogleAccessId=fake-access-id%40example.iam.gserviceaccount.com&Signature=TotallyFakeSignature1234567890",
        body=b"fake audio data",
        content_type="audio/mpeg",
        status=200,
    )

    # TK Culturally Sensitive label image
    mocked_responses.get(
        "https://localcontexts.org/wp-content/uploads/2025/04/tk-culturally-sensitive.png",
        body=b"fake tk-culturally-sensitive image data",
        content_type="image/png",
        status=200,
    )

    # TK Community Voice label image
    mocked_responses.get(
        "https://localcontexts.org/wp-content/uploads/2025/04/tk-community-voice.png",
        body=b"fake tk-community-voice image data",
        content_type="image/png",
        status=200,
    )

    # TK Attribution label image
    mocked_responses.get(
        "https://localcontexts.org/wp-content/uploads/2025/04/tk-attribution.png",
        body=b"fake tk-attribution image data",
        content_type="image/png",
        status=200,
    )

    localcontexts_project_dict = dict(
        server_url=server_url,
        api_key=api_key,
        project_id=project_id,
    )

    return LocalContextsProject(localcontexts_project=localcontexts_project_dict)


@pytest.fixture
def pg_database():
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
