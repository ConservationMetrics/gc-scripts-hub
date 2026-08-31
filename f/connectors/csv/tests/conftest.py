import pytest


@pytest.fixture
def pg_database(postgresql_factory):
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = postgresql_factory()
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield dsn
    db.stop()
