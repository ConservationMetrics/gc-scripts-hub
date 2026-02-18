import pytest
import responses


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps


@pytest.fixture
def comapeo_server_fixture(mocked_responses):
    """A mock CoMapeo Server for testing project count metrics."""

    server_url = "http://comapeo.example.org"
    access_token = "MapYourWorldTogether!"

    # Mock the projects endpoint with 3 projects
    mocked_responses.get(
        f"{server_url}/projects",
        json={
            "data": [
                {"projectId": "forest_expedition", "name": "Forest Expedition"},
                {"projectId": "river_mapping", "name": "River Mapping"},
                {"projectId": "wildlife_survey", "name": "Wildlife Survey"},
            ]
        },
        status=200,
    )

    return {
        "server_url": server_url,
        "access_token": access_token,
    }
