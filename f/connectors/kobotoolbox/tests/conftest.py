import re
from dataclasses import dataclass

import pytest
import responses
import testing.postgresql

from f.connectors.kobotoolbox.tests.assets import server_responses


@pytest.fixture
def mocked_responses():
    """responses.RequestsMock context, for testing code that makes HTTP requests."""
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def koboserver(mocked_responses):
    """A mock Kobo Server that you can use to provide survey responses"""

    @dataclass
    class KoboServer:
        account: dict
        form_id: str

    server_url = "http://kobotoolbox.example.org"
    form_id = "mimsyweretheborogoves"
    form_name = "Arboles"

    mocked_responses.get(
        f"{server_url}/api/v2/assets/{form_id}/",
        json=server_responses.kobo_form(server_url, form_id, form_name),
        status=200,
    )
    mocked_responses.get(
        f"{server_url}/api/v2/assets/{form_id}/data/",
        json=server_responses.kobo_form_submissions(server_url, form_id),
        status=200,
    )
    mocked_responses.get(
        re.compile(rf"{server_url}/api/v2/assets/{form_id}/data/\d+/attachments/\d+/?"),
        body=open("f/connectors/kobotoolbox/tests/assets/trees.png", "rb").read(),
        content_type="image/png",
        headers={"Content-Length": "3632"},
    )

    return KoboServer(dict(server_url=server_url, api_key="Callooh!Callay!"), form_id)


@pytest.fixture
def pg_database():
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
