import psycopg
import pytest
import responses
import testing.postgresql

from f.common_logic.db_operations import conninfo


@pytest.fixture(scope="function")
def _postgres_instance():
    """Spin up a temporary PostgreSQL instance for tests."""
    db = testing.postgresql.Postgresql(port=7654)
    try:
        yield db
    finally:
        db.stop()


@pytest.fixture
def pg_database(_postgres_instance):
    """Create a temporary PostgreSQL database with test data for metrics tests."""
    dsn = _postgres_instance.dsn()
    dsn["dbname"] = dsn.pop("database")

    # Create test tables and data
    conn_str = conninfo(dsn)
    with psycopg.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            # Create some test tables
            cursor.execute(
                "CREATE TABLE test_table_1 (id SERIAL PRIMARY KEY, data TEXT)"
            )
            cursor.execute(
                "CREATE TABLE test_table_2 (id SERIAL PRIMARY KEY, data TEXT)"
            )
            cursor.execute(
                "INSERT INTO test_table_1 (data) VALUES ('test1'), ('test2')"
            )
            cursor.execute(
                "INSERT INTO test_table_2 (data) VALUES ('test3'), ('test4'), ('test5')"
            )

            # Create view_config table for explorer metrics
            cursor.execute("""
                CREATE TABLE view_config (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    config JSONB
                )
            """)
            cursor.execute("""
                INSERT INTO view_config (name, config) VALUES
                ('view1', '{}'),
                ('view2', '{}'),
                ('view3', '{}')
            """)

            # Create Superset tables for testing
            cursor.execute("""
                CREATE TABLE dashboards (
                    id SERIAL PRIMARY KEY,
                    dashboard_title TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE slices (
                    id SERIAL PRIMARY KEY,
                    slice_name TEXT
                )
            """)
            cursor.execute("""
                INSERT INTO dashboards (dashboard_title) VALUES
                ('Dashboard 1'),
                ('Dashboard 2')
            """)
            cursor.execute("""
                INSERT INTO slices (slice_name) VALUES
                ('Chart 1'),
                ('Chart 2'),
                ('Chart 3'),
                ('Chart 4')
            """)

    return dsn


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps


@pytest.fixture
def comapeo_server_fixture(mocked_responses):
    """A mock CoMapeo Server for testing project count metrics."""

    server_url = "http://comapeo.example.org"
    access_token = "MapYourWorldTogether!"

    # Mock the projects endpoint with 3 projects
    mocked_responses.get(
        f"{server_url}/projects",
        json={
            "data": [
                {"projectId": "forest_expedition", "name": "Forest Expedition"},
                {"projectId": "river_mapping", "name": "River Mapping"},
                {"projectId": "wildlife_survey", "name": "Wildlife Survey"},
            ]
        },
        status=200,
    )

    return {
        "server_url": server_url,
        "access_token": access_token,
    }


@pytest.fixture
def auth0_server_fixture(mocked_responses):
    """A mock Auth0 server for testing Auth0 metrics."""

    domain = "your-tenant.us.auth0.com"

    # Mock Auth0 token endpoint (client credentials flow)
    mocked_responses.add(
        responses.POST,
        f"https://{domain}/oauth/token",
        json={
            "access_token": "test_access_token",
            "token_type": "Bearer",
            "expires_in": 86400,
        },
        status=200,
    )

    # Mock call 1: GET total users (per_page=1, include_totals=true)
    mocked_responses.add(
        responses.GET,
        f"https://{domain}/api/v2/users",
        json={
            "users": [{"user_id": "auth0|123"}],
            "total": 52,
        },
        status=200,
    )

    # Mock call 2: GET active users (search_engine=v3, with q parameter)
    mocked_responses.add(
        responses.GET,
        f"https://{domain}/api/v2/users",
        json={
            "users": [],
            "total": 25,
        },
        status=200,
    )

    # Mock call 3: GET logins count - first page (per_page=100, page=0, fields=logins_count)
    mocked_responses.add(
        responses.GET,
        f"https://{domain}/api/v2/users",
        json=[
            {"user_id": "auth0|1", "logins_count": 275},
            {"user_id": "auth0|2", "logins_count": 95},
            {"user_id": "auth0|3", "logins_count": 42},
        ],
        status=200,
    )

    return {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "domain": domain,
    }
