import json
from dataclasses import dataclass

import pytest
import responses
import testing.postgresql

from f.connectors.sensingclues.tests.assets import server_responses

FOCUS_BASE_URL = "https://focus.sensingclues.org/api/"
_LOGIN_URL = f"{FOCUS_BASE_URL}auth/login"
_FACETS_URL = f"{FOCUS_BASE_URL}search/all/facets"
_RESULTS_URL = f"{FOCUS_BASE_URL}search/all/results"

_USERNAME = "demo"
_PASSWORD = "demo"


def _request_json(request) -> dict:
    return json.loads(request.body) if request.body else {}


def _register_login(rsps, *, status: int = 200):
    if status == 200:
        rsps.post(_LOGIN_URL, json=server_responses.login_response(), status=200)
    else:
        rsps.post(_LOGIN_URL, json={"error": "Invalid credentials."}, status=status)


def _register_facets(rsps):
    rsps.post(_FACETS_URL, json=server_responses.facets_response(), status=200)


def _results_callback(request):
    body = _request_json(request)
    groups = body.get("filters", {}).get("dataSources") or []
    if isinstance(groups, str):
        groups = [groups]
    options = body.get("options") or {}
    start = int(options.get("start") or 1)
    page_length = int(options.get("pageLength") or 200)
    payload = server_responses.observations_page(groups, start=start, page_length=page_length)
    return (200, {}, json.dumps(payload))


def _register_results(rsps, callback=_results_callback):
    rsps.add_callback(
        responses.POST,
        _RESULTS_URL,
        callback=callback,
        content_type="application/json",
    )


@dataclass
class SensingCluesServer:
    username: str
    password: str
    group_identifier: str


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def sensingclues_server(mocked_responses):
    """Mock Focus returning Cluey-group observations (2 records, one page)."""
    _register_login(mocked_responses)
    _register_facets(mocked_responses)
    _register_results(mocked_responses)
    return SensingCluesServer(
        username=_USERNAME,
        password=_PASSWORD,
        group_identifier=server_responses.CLUEY_IDENTIFIER,
    )


@pytest.fixture
def sensingclues_server_paginated(mocked_responses, monkeypatch):
    """Mock Focus returning the 3-record Africa fixture in pages of 2."""
    monkeypatch.setattr(
        "f.connectors.sensingclues.sensingclues_observations._PAGE_LENGTH",
        2,
    )
    _register_login(mocked_responses)
    _register_facets(mocked_responses)
    _register_results(mocked_responses)
    return SensingCluesServer(
        username=_USERNAME,
        password=_PASSWORD,
        group_identifier=server_responses.AFRICA_IDENTIFIER,
    )


@pytest.fixture
def sensingclues_server_empty(mocked_responses):
    """Mock Focus returning zero observations."""
    _register_login(mocked_responses)
    _register_facets(mocked_responses)
    mocked_responses.add_callback(
        responses.POST,
        _RESULTS_URL,
        callback=lambda _req: (
            200,
            {},
            json.dumps(server_responses.observations_empty()),
        ),
        content_type="application/json",
    )
    return SensingCluesServer(
        username=_USERNAME,
        password=_PASSWORD,
        group_identifier=server_responses.CLUEY_IDENTIFIER,
    )


@pytest.fixture
def sensingclues_server_groups_only(mocked_responses):
    """Mock Focus login and group discovery without an observations endpoint."""
    _register_login(mocked_responses)
    _register_facets(mocked_responses)
    return SensingCluesServer(
        username=_USERNAME,
        password=_PASSWORD,
        group_identifier=server_responses.CLUEY_IDENTIFIER,
    )


@pytest.fixture
def sensingclues_server_unauthorized(mocked_responses):
    """Mock Focus rejecting login with HTTP 401."""
    _register_login(mocked_responses, status=401)
    return SensingCluesServer(
        username="bad",
        password="wrong",
        group_identifier=server_responses.CLUEY_IDENTIFIER,
    )


@pytest.fixture
def pg_database():
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop()
