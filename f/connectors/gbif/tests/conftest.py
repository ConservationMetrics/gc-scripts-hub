from pathlib import Path

import pytest
import responses

from f.connectors.gbif.tests.assets import server_responses


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as mocked:
        yield mocked


@pytest.fixture
def archive_bytes():
    return (
        Path(__file__).parent / "assets" / "gbif-download-20260917.zip"
    ).read_bytes()


@pytest.fixture
def statistics_snapshot():
    return server_responses.statistics_snapshot()


@pytest.fixture
def pg_database(postgresql_factory):
    db = postgresql_factory()
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop()
