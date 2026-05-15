import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
import responses
import testing.postgresql

from f.connectors.epicollect.tests.assets import server_responses

_BASE_URL = "https://five.epicollect.net"
_ASSETS = Path("f/connectors/epicollect/tests/assets")

PROJECT_SLUG = server_responses.PROJECT_SLUG
_CLIENT_ID = 7865
_CLIENT_SECRET = "test_client_secret"


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


def _register_token_mock(rsps):
    rsps.post(
        f"{_BASE_URL}/api/oauth/token",
        json=server_responses.token_response(),
        status=200,
    )


def _register_project_mock(rsps):
    rsps.get(
        f"{_BASE_URL}/api/export/project/{PROJECT_SLUG}",
        json=server_responses.project_metadata(),
        status=200,
    )


def _entries_callback(request):
    qs = parse_qs(urlparse(request.url).query)
    page = int(qs.get("page", [1])[0])
    data = server_responses.entries_page(page=page)
    return (200, {}, json.dumps(data))


def _entries_paginated_callback(request):
    qs = parse_qs(urlparse(request.url).query)
    page = int(qs.get("page", [1])[0])
    data = server_responses.entries_paginated(page=page, per_page=2)
    return (200, {}, json.dumps(data))


def _media_callback(request):
    qs = parse_qs(urlparse(request.url).query)
    media_type = qs.get("type", ["photo"])[0]
    fmt = qs.get("format", ["entry_original"])[0]

    if fmt == "project_thumb":
        return (200, {}, (_ASSETS / "mock_photo.jpg").read_bytes())
    if media_type == "photo":
        return (200, {}, (_ASSETS / "mock_photo.jpg").read_bytes())
    if media_type == "audio":
        return (200, {}, (_ASSETS / "mock_audio.mp4").read_bytes())
    if media_type == "video":
        return (200, {}, (_ASSETS / "mock_video.mp4").read_bytes())
    return (404, {}, b"")


def _register_media_mock(rsps):
    rsps.add_callback(
        responses.GET,
        re.compile(rf"{re.escape(_BASE_URL)}/api/export/media/{re.escape(PROJECT_SLUG)}"),
        callback=_media_callback,
    )


@dataclass
class EpiCollectServer:
    project_slug: str
    client_id: int
    client_secret: str


@pytest.fixture
def epicollect_server(mocked_responses):
    """Mock EpiCollect5 server returning a single page of one entry."""
    _register_token_mock(mocked_responses)
    _register_project_mock(mocked_responses)
    mocked_responses.add_callback(
        responses.GET,
        re.compile(rf"{re.escape(_BASE_URL)}/api/export/entries/{re.escape(PROJECT_SLUG)}"),
        callback=_entries_callback,
        content_type="application/json",
    )
    _register_media_mock(mocked_responses)

    return EpiCollectServer(
        project_slug=PROJECT_SLUG,
        client_id=_CLIENT_ID,
        client_secret=_CLIENT_SECRET,
    )


@pytest.fixture
def epicollect_server_paginated(mocked_responses):
    """Mock EpiCollect5 server returning two pages of two entries each."""
    _register_token_mock(mocked_responses)
    _register_project_mock(mocked_responses)
    mocked_responses.add_callback(
        responses.GET,
        re.compile(rf"{re.escape(_BASE_URL)}/api/export/entries/{re.escape(PROJECT_SLUG)}"),
        callback=_entries_paginated_callback,
        content_type="application/json",
    )
    _register_media_mock(mocked_responses)

    return EpiCollectServer(
        project_slug=PROJECT_SLUG,
        client_id=_CLIENT_ID,
        client_secret=_CLIENT_SECRET,
    )


@pytest.fixture
def pg_database():
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
