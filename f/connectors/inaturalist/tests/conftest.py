import json
import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import pytest
import responses
import testing.postgresql

from f.connectors.inaturalist.inaturalist_pull import BASE_URL
from f.connectors.inaturalist.tests.assets import server_responses

PROJECT_ID = server_responses.PROJECT_ID
USERNAME = server_responses.USERNAME

_MOCK_PHOTO_BYTES = b"fake-inaturalist-photo"


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


def _register_project_mock(rsps, project_id: str = PROJECT_ID):
    rsps.get(
        f"{BASE_URL}/projects/{project_id}",
        json=server_responses.project_metadata(),
        status=200,
    )


def _register_photo_mock(rsps):
    rsps.add_callback(
        responses.GET,
        re.compile(r"https://inaturalist-open-data\.s3\.amazonaws\.com/photos/"),
        callback=lambda _req: (200, {"Content-Type": "image/jpeg"}, _MOCK_PHOTO_BYTES),
    )


def _observations_callback(request):
    qs = parse_qs(urlparse(request.url).query)
    id_above = int(qs["id_above"][0]) if "id_above" in qs else None
    data = server_responses.observations_page(id_above=id_above)
    return (200, {}, json.dumps(data))


def _observations_paginated_callback(request):
    qs = parse_qs(urlparse(request.url).query)
    id_above = int(qs["id_above"][0]) if "id_above" in qs else None
    data = server_responses.observations_paginated(id_above=id_above, per_page=2)
    return (200, {}, json.dumps(data))


def _register_observations_mock(rsps, callback):
    rsps.add_callback(
        responses.GET,
        re.compile(rf"{re.escape(BASE_URL)}/observations"),
        callback=callback,
        content_type="application/json",
    )


def _disable_delays(monkeypatch):
    monkeypatch.setattr(
        "f.connectors.inaturalist.inaturalist_pull.time.sleep",
        lambda _s: None,
    )


@dataclass
class INaturalistProjectServer:
    project_id: str


@dataclass
class INaturalistUserServer:
    username: str


@pytest.fixture
def inaturalist_project_server(mocked_responses, monkeypatch):
    """Mock iNaturalist API returning the full 10-observation fixture in one page."""
    _disable_delays(monkeypatch)
    _register_project_mock(mocked_responses)
    _register_observations_mock(mocked_responses, _observations_callback)
    _register_photo_mock(mocked_responses)
    return INaturalistProjectServer(project_id=PROJECT_ID)


@pytest.fixture
def inaturalist_project_server_paginated(mocked_responses, monkeypatch):
    """Mock iNaturalist API returning pages of 2 observations via id_above."""
    _disable_delays(monkeypatch)
    monkeypatch.setattr("f.connectors.inaturalist.inaturalist_pull._PAGE_SIZE", 2)
    _register_project_mock(mocked_responses)
    _register_observations_mock(mocked_responses, _observations_paginated_callback)
    _register_photo_mock(mocked_responses)
    return INaturalistProjectServer(project_id=PROJECT_ID)


@pytest.fixture
def inaturalist_project_server_empty(mocked_responses):
    """Mock iNaturalist API returning zero project observations."""
    _register_project_mock(mocked_responses)
    mocked_responses.add_callback(
        responses.GET,
        re.compile(rf"{re.escape(BASE_URL)}/observations"),
        callback=lambda _req: (
            200,
            {},
            json.dumps(server_responses.observations_empty()),
        ),
        content_type="application/json",
    )
    return INaturalistProjectServer(project_id=PROJECT_ID)


@pytest.fixture
def inaturalist_user_server(mocked_responses, monkeypatch):
    """Mock iNaturalist API returning fixture observations for a username."""
    _disable_delays(monkeypatch)
    _register_observations_mock(mocked_responses, _observations_callback)
    _register_photo_mock(mocked_responses)
    return INaturalistUserServer(username=USERNAME)


@pytest.fixture
def inaturalist_user_server_empty(mocked_responses):
    """Mock iNaturalist API returning zero user observations."""
    mocked_responses.add_callback(
        responses.GET,
        re.compile(rf"{re.escape(BASE_URL)}/observations"),
        callback=lambda _req: (
            200,
            {},
            json.dumps(server_responses.observations_empty()),
        ),
        content_type="application/json",
    )
    return INaturalistUserServer(username=USERNAME)


@pytest.fixture
def pg_database():
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop()
