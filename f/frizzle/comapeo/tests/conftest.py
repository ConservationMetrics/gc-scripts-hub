import re
from dataclasses import dataclass

import pytest
import responses
import testing.postgresql

from f.frizzle.comapeo.tests.assets import server_responses
from f.frizzle.comapeo.comapeo_observations import comapeo_server


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def comapeoserver(mocked_responses):
    """A mock CoMapeo Server that you can use to provide projects and their observations"""

    @dataclass
    class CoMapeoServer:
        comapeo_server: dict
        comapeo_project_blocklist: list

    server_url = "http://comapeo.example.org"
    access_token = "MapYourWorldTogether!"
    comapeo_project_blocklist = ["river_mapping"]
    project_id = "forest_expedition"

    mocked_responses.get(
        f"{server_url}/projects",
        json=server_responses.comapeo_projects(server_url),
        status=200,
    )
    mocked_responses.get(
        f"{server_url}/projects/{project_id}/observations",
        json=server_responses.comapeo_project_observations(server_url, project_id),
        status=200,
    )
    mocked_responses.get(
        re.compile(rf"{server_url}/projects/{project_id}/attachments/.+/photo/.+"),
        body=open("f/frizzle/comapeo/tests/assets/capybara.jpg", "rb").read(),
        content_type="image/jpg",
        headers={"Content-Length": "11044"},
    )

    server: comapeo_server = dict(server_url=server_url, access_token=access_token)

    return CoMapeoServer(
        server,
        comapeo_project_blocklist,
    )


@pytest.fixture
def pg_database():
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
