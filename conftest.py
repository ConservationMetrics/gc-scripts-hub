import pytest


@pytest.fixture(scope="session")
def postgresql_factory():
    """Provide a PostgreSQL factory shared for one pytest session.
    scope="session" means it persists for the full pytest run/process and the same instance can be referenced from multiple tests.
    That way we don't need to do the expensive re-initialization of postgres for every test module.
    The cache is cleared when the pytest process finishes.
    """
    import testing.postgresql

    # No explicit port: each instance binds to a random free port, so tox
    # environments running in parallel (tox -p) never collide on the same port.
    factory = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)
    try:
        yield factory
    finally:
        factory.clear_cache()
