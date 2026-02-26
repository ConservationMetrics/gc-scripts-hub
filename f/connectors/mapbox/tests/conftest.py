from dataclasses import dataclass

import pytest
import responses

from f.connectors.mapbox.tests.assets import server_responses


@pytest.fixture
def mocked_responses():
    """
    Shared responses mock for Mapbox connector tests.

    Mirrors the pattern used in other connectors (for example, CoMapeo), so all
    tests use the same RequestsMock instance instead of the global decorator.
    """
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps


@dataclass
class MapboxTilesetSource:
    username: str
    access_token: str
    dataset_id: str


@pytest.fixture
def mapbox_tileset_source(mocked_responses) -> MapboxTilesetSource:
    """
    A mock Mapbox tileset source configuration and pre-registered responses.

    This fixture prepares a realistic Replace a tileset source interaction,
    registering the expected PUT request URL and response JSON on the shared
    `mocked_responses` instance.
    """
    username = "test-user"
    dataset_id = "hello-world"
    access_token = "pk.ey_test_public_token"

    url = server_responses.mapbox_tileset_source_url(
        username,
        dataset_id,
        access_token,
    )
    response_body = server_responses.mapbox_tileset_source_replace_response(
        username,
        dataset_id,
    )

    mocked_responses.put(url, json=response_body, status=200)

    return MapboxTilesetSource(
        username=username,
        access_token=access_token,
        dataset_id=dataset_id,
    )
