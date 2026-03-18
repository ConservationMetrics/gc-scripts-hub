import pytest
import responses
import testing.postgresql

from f.connectors.earthindex.earthindex_pull import BASE_URL
from f.connectors.earthindex.tests.assets import server_responses


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps


@pytest.fixture
def earthindex_server(mocked_responses):
    """A mock Earth Index API server with one project, one layer, and 7 features."""
    api_key = "test-api-key-12345"
    project_id = server_responses.SAMPLE_PROJECT["id"]
    layer_id = server_responses.SAMPLE_LAYERS[0]["id"]

    mocked_responses.get(
        f"{BASE_URL}/v1/projects/{project_id}",
        json=server_responses.SAMPLE_PROJECT,
        status=200,
    )
    mocked_responses.get(
        f"{BASE_URL}/v1/projects/{project_id}/layers",
        json=server_responses.SAMPLE_LAYERS,
        status=200,
    )
    mocked_responses.get(
        f"{BASE_URL}/v1/projects/{project_id}/layers/{layer_id}/points",
        json=server_responses.SAMPLE_POINTS,
        status=200,
    )

    return {"api_key": api_key, "project_id": project_id}


@pytest.fixture
def earthindex_server_no_layers(mocked_responses):
    """A mock Earth Index API server with a project that has no layers."""
    api_key = "test-api-key-12345"
    project_id = server_responses.SAMPLE_PROJECT["id"]

    mocked_responses.get(
        f"{BASE_URL}/v1/projects/{project_id}",
        json=server_responses.SAMPLE_PROJECT,
        status=200,
    )
    mocked_responses.get(
        f"{BASE_URL}/v1/projects/{project_id}/layers",
        json=[],
        status=200,
    )

    return {"api_key": api_key, "project_id": project_id}


@pytest.fixture
def pg_database():
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop()
