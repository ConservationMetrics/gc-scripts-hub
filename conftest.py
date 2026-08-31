import pytest


@pytest.fixture(scope="session")
def postgresql_factory():
    """Provide a PostgreSQL factory shared for one pytest session.
    scope="session" means it persists for the full pytest run/process and the same instance can be referenced from multiple tests.
    That way we don't need to do the expensive re-initialization of postgres for every test module.
    The cache is cleared when the pytest process finishes.
    """
    import testing.postgresql

    factory = testing.postgresql.PostgresqlFactory(
        cache_initialized_db=True, port=7654
    )
    try:
        yield factory
    finally:
        factory.clear_cache()
