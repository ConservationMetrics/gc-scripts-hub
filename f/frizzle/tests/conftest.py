import pytest
import responses
import testing.postgresql


@pytest.fixture
def mocked_responses():
    """responses.RequestsMock context, for testing code that makes HTTP requests."""
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def pg_database():
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop
