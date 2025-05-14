import re
from dataclasses import dataclass

import pytest
import responses
import testing.postgresql

from f.connectors.kobotoolbox.tests.assets import server_responses

server_url = "http://kobotoolbox.example.org"
form_id = "mimsyweretheborogoves"
form_name = "Arboles"


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


def _register_common_mocks(rsps, form_id, metadata):
    rsps.get(
        f"{server_url}/api/v2/assets/{form_id}/",
        json=metadata,
        status=200,
    )
    rsps.get(
        f"{server_url}/api/v2/assets/{form_id}/data/",
        json=server_responses.kobo_form_submissions(server_url, form_id),
        status=200,
    )
    rsps.get(
        re.compile(rf"{server_url}/api/v2/assets/{form_id}/data/\d+/attachments/\d+/?"),
        body=open("f/connectors/kobotoolbox/tests/assets/trees.png", "rb").read(),
        content_type="image/png",
        headers={"Content-Length": "3632"},
    )


def _build_koboserver_fixture(metadata):
    @dataclass
    class KoboServer:
        account: dict
        form_id: str

    return KoboServer(dict(server_url=server_url, api_key="Callooh!Callay!"), form_id)


@pytest.fixture
def koboserver(mocked_responses):
    metadata = server_responses.kobo_form(server_url, form_id, form_name)

    _register_common_mocks(mocked_responses, form_id, metadata)

    return _build_koboserver_fixture(metadata)


@pytest.fixture
def koboserver_no_translations(mocked_responses):
    metadata = server_responses.kobo_form(server_url, form_id, form_name)
    metadata["content"]["translations"] = [None]

    _register_common_mocks(mocked_responses, form_id, metadata)

    return _build_koboserver_fixture(metadata)


@pytest.fixture
def pg_database():
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
