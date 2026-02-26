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
    tileset_id: str


@pytest.fixture
def mapbox_tileset_source(mocked_responses) -> MapboxTilesetSource:
    """
    A mock Mapbox tileset source configuration and pre-registered responses.

    Registers both the Replace (PUT) and Publish (POST) endpoints on the
    shared `mocked_responses` instance.
    """
    username = "test-user"
    tileset_id = "hello-world"
    access_token = "sk.ey_test_secret_token"

    # Register Replace tileset source endpoint
    replace_url = server_responses.mapbox_tileset_source_url(
        username, tileset_id, access_token
    )
    replace_body = server_responses.mapbox_tileset_source_replace_response(
        username, tileset_id
    )
    mocked_responses.put(replace_url, json=replace_body, status=200)

    # Register Publish tileset endpoint
    publish_url = server_responses.mapbox_publish_url(
        username, tileset_id, access_token
    )
    publish_body = server_responses.mapbox_publish_response(username, tileset_id)
    mocked_responses.post(publish_url, json=publish_body, status=200)

    return MapboxTilesetSource(
        username=username,
        access_token=access_token,
        tileset_id=tileset_id,
    )
