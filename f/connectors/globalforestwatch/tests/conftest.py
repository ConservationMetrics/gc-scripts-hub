from dataclasses import dataclass

import pytest
import responses
import testing.postgresql

from f.connectors.globalforestwatch.tests.assets import server_responses


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def gfw_server(mocked_responses):
    """A mock GFW API that you can use to provide alerts data"""

    @dataclass
    class GFWAPI:
        gfw_api: dict

    mocked_responses.post(
        "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/latest/query",
        json=server_responses.gfw_integrated_alerts(),
        status=200,
    )

    gfw_api = dict(api_key="my-api-key")

    return GFWAPI(gfw_api)


@pytest.fixture
def pg_database():
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
