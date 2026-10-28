import json
import re

import pytest
import responses

from f.connectors.ebird.ebird_pull import _API_ROOT
from f.connectors.ebird.tests.assets.server_responses import (
    NOTABLE_OBSERVATIONS,
    RECENT_OBSERVATIONS,
)


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


def _register(rsps, payload, status=200):
    rsps.add(
        responses.GET,
        re.compile(rf"{re.escape(_API_ROOT)}/data/obs/"),
        json=payload,
        status=status,
    )


@pytest.fixture
def ebird_recent(mocked_responses):
    """Recent-observation fixture for region and nearby pulls."""
    _register(mocked_responses, RECENT_OBSERVATIONS)


@pytest.fixture
def ebird_notable(mocked_responses):
    """Notable-observation fixture (`detail=full`) for region and nearby pulls."""
    _register(mocked_responses, NOTABLE_OBSERVATIONS)


@pytest.fixture
def ebird_empty(mocked_responses):
    """Empty observation list."""
    _register(mocked_responses, [])


@pytest.fixture
def pg_database(postgresql_factory):
    db = postgresql_factory()
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop()
