import pytest


@pytest.fixture(scope="session")
def postgresql_factory():
    import testing.postgresql

    factory = testing.postgresql.PostgresqlFactory(
        cache_initialized_db=True, port=7654
    )
    try:
        yield factory
    finally:
        factory.clear_cache()
