from unittest.mock import patch

import psycopg2
import pytest
import responses
import testing.postgresql

from f.common_logic.db_operations import conninfo
from f.metrics.guardianconnector.guardianconnector_metrics import (
    get_auth0_metrics,
    get_comapeo_metrics,
    get_directory_size,
    get_explorer_metrics,
    get_files_metrics,
    get_superset_metrics,
    get_warehouse_metrics,
    get_windmill_metrics,
    main,
)


def test_guardianconnector_metrics_structure(
    comapeo_server_fixture, pg_database, tmp_path
):
    """Test that the main function returns the correct nested structure."""

    # Create a temporary directory to simulate the datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)
    # Create a file large enough to show up in MB (1 MB)
    (comapeo_dir / "test_file.txt").write_bytes(b"x" * (1024 * 1024))

    result = main(
        comapeo_server_fixture, pg_database, str(datalake_root), "test", "test"
    )

    # Check top-level structure
    assert "comapeo" in result
    assert isinstance(result["comapeo"], dict)

    # Check CoMapeo metrics
    comapeo_metrics = result["comapeo"]
    assert "project_count" in comapeo_metrics
    assert comapeo_metrics["project_count"] == 3
    assert "data_size_mb" in comapeo_metrics
    assert comapeo_metrics["data_size_mb"] >= 1.0


def test_get_comapeo_metrics(comapeo_server_fixture, tmp_path):
    """Test the CoMapeo metrics function directly."""

    # Create a temporary directory to simulate the datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)
    (comapeo_dir / "test_file.txt").write_bytes(b"x" * (1024 * 1024))

    result = get_comapeo_metrics(comapeo_server_fixture, str(datalake_root))

    assert "project_count" in result
    assert result["project_count"] == 3
    assert "data_size_mb" in result
    assert result["data_size_mb"] >= 1.0


def test_project_count_empty(mocked_responses, pg_database, tmp_path):
    """Test that the script handles an empty project list."""

    server_url = "http://comapeo.example.org"
    access_token = "test_token"

    # Mock empty projects response
    mocked_responses.get(
        f"{server_url}/projects",
        json={"data": []},
        status=200,
    )

    comapeo = {
        "server_url": server_url,
        "access_token": access_token,
    }

    # Create a temporary directory to simulate the datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)

    result = main(
        comapeo,
        pg_database,
        str(datalake_root),
        guardianconnector_db="test",
        superset_db="test",
    )

    assert result["comapeo"]["project_count"] == 0
    assert "data_size_mb" in result["comapeo"]


def test_data_size_nonexistent_path(comapeo_server_fixture, pg_database):
    """Test that the script handles nonexistent data paths gracefully."""

    result = main(
        comapeo_server_fixture,
        pg_database,
        "/nonexistent/path",
        guardianconnector_db="test",
        superset_db="test",
    )

    assert "comapeo" in result
    assert result["comapeo"]["project_count"] == 3
    # Data size metric should not be present
    assert "data_size_mb" not in result["comapeo"]


def test_get_directory_size(tmp_path):
    """Test the directory size calculation function."""

    # Create a directory with known content
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    (test_dir / "file1.txt").write_bytes(b"x" * 1000)
    (test_dir / "file2.txt").write_bytes(b"y" * 2000)

    size = get_directory_size(str(test_dir))

    assert size is not None
    assert size > 3000  # At least the size of our files


def test_get_directory_size_nonexistent():
    """Test directory size function with nonexistent path."""

    size = get_directory_size("/nonexistent/path")

    assert size is None


@patch("f.metrics.guardianconnector.guardianconnector_metrics.subprocess.run")
def test_get_directory_size_subprocess_error(mock_run, tmp_path):
    """Test directory size function handles subprocess errors."""

    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()

    # Simulate subprocess error
    mock_run.side_effect = Exception("Command failed")

    size = get_directory_size(str(test_dir))

    assert size is None


@pytest.fixture
def pg_database():
    """Create a temporary PostgreSQL database for testing."""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")

    # Create test tables and data
    conn_str = conninfo(dsn)
    with psycopg2.connect(conn_str) as conn:
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
        conn.commit()

    yield dsn
    db.stop()


def test_get_warehouse_metrics(pg_database):
    """Test warehouse metrics collection."""

    metrics = get_warehouse_metrics(pg_database)

    assert "tables" in metrics
    assert "records" in metrics
    # Should have 5 tables (test_table_1, test_table_2, view_config, dashboards, slices)
    assert metrics["tables"] == 5
    # Should have 17 total records (2 + 3 + 3 + 2 + 4 + 3)
    assert metrics["records"] >= 14


def test_get_explorer_metrics(pg_database):
    """Test explorer metrics collection."""

    metrics = get_explorer_metrics(pg_database)

    assert "dataset_views" in metrics
    # Should have 3 records in view_config
    assert metrics["dataset_views"] == 3


def test_get_superset_metrics(pg_database):
    """Test Superset metrics collection."""

    metrics = get_superset_metrics(pg_database)

    assert "dashboards" in metrics
    assert "charts" in metrics
    # Should have 2 dashboards
    assert metrics["dashboards"] == 2
    # Should have 4 charts (slices)
    assert metrics["charts"] == 4


def test_guardianconnector_full_metrics(comapeo_server_fixture, pg_database, tmp_path):
    """Test the full metrics collection with all services."""

    # Create a temporary directory to simulate the datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)
    (comapeo_dir / "test_file.txt").write_bytes(b"x" * (1024 * 1024))

    # Pass the test database name for both guardianconnector_db and superset_db
    result = main(
        comapeo_server_fixture,
        pg_database,
        str(datalake_root),
        guardianconnector_db="test",
        superset_db="test",
    )

    # Check all services are present
    assert "comapeo" in result
    assert "warehouse" in result
    assert "explorer" in result
    assert "superset" in result

    # Check CoMapeo metrics
    assert result["comapeo"]["project_count"] == 3
    assert result["comapeo"]["data_size_mb"] >= 1.0

    # Check warehouse metrics
    assert result["warehouse"]["tables"] == 5
    assert result["warehouse"]["records"] >= 14

    # Check explorer metrics
    assert result["explorer"]["dataset_views"] == 3

    # Check Superset metrics
    assert result["superset"]["dashboards"] == 2
    assert result["superset"]["charts"] == 4

    # Check files metrics
    assert "files" in result
    assert "file_count" in result["files"]
    assert "data_size_mb" in result["files"]
    assert result["files"]["file_count"] >= 1  # At least the test file
    assert result["files"]["data_size_mb"] >= 1.0  # At least 1MB


def test_get_files_metrics(tmp_path):
    """Test files metrics collection."""
    # Create a test datalake structure
    datalake = tmp_path / "datalake"
    datalake.mkdir()

    # Create some test files
    (datalake / "file1.txt").write_bytes(b"x" * (1024 * 1024))  # 1MB
    (datalake / "file2.txt").write_bytes(b"y" * (512 * 1024))  # 512KB
    subdir = datalake / "subdir"
    subdir.mkdir()
    (subdir / "file3.txt").write_bytes(b"z" * (1024 * 1024))  # 1MB

    metrics = get_files_metrics(str(datalake))

    assert "file_count" in metrics
    assert "data_size_mb" in metrics
    assert metrics["file_count"] == 3
    assert metrics["data_size_mb"] >= 2.5  # At least 2.5MB


def test_get_windmill_metrics():
    """Test Windmill metrics collection using environment variables."""
    # Mock environment variables
    with patch.dict(
        "os.environ",
        {
            "WM_BASE_URL": "http://windmill.example.org",
            "WM_TOKEN": "test_token",
            "WM_WORKSPACE": "test_workspace",
        },
    ):
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.GET,
                "http://windmill.example.org/api/w/test_workspace/schedules/list",
                json=[
                    {"path": "f/example/script1", "schedule": "0 0 * * *"},
                    {"path": "f/example/script2", "schedule": "0 12 * * *"},
                    {"path": "f/example/script3", "schedule": "*/15 * * * *"},
                ],
                status=200,
            )

            metrics = get_windmill_metrics()

            assert "number_of_schedules" in metrics
            assert metrics["number_of_schedules"] == 3


def test_windmill_metrics_not_in_windmill():
    """Test that Windmill metrics are skipped when not running in Windmill."""
    # Ensure environment variables are not set
    with patch.dict("os.environ", {}, clear=True):
        metrics = get_windmill_metrics()

        # Should return empty dict when not in Windmill
        assert metrics == {}


def test_windmill_metrics_automatic(comapeo_server_fixture, pg_database, tmp_path):
    """Test that Windmill metrics are automatically collected when running in Windmill."""
    # Create test datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)
    (comapeo_dir / "test_file.txt").write_bytes(b"x" * (1024 * 1024))

    # Mock environment variables to simulate running in Windmill
    with patch.dict(
        "os.environ",
        {
            "WM_BASE_URL": "http://windmill.example.org",
            "WM_TOKEN": "test_token",
            "WM_WORKSPACE": "test_workspace",
        },
    ):
        with responses.RequestsMock() as rsps:
            # Mock Windmill schedules API
            rsps.add(
                responses.GET,
                "http://windmill.example.org/api/w/test_workspace/schedules/list",
                json=[{"path": "f/example/script1"}],
                status=200,
            )
            # Also need to mock CoMapeo API since comapeo_server_fixture is provided
            rsps.add(
                responses.GET,
                "http://comapeo.example.org/projects",
                json={
                    "data": [
                        {"projectId": "forest_expedition"},
                        {"projectId": "river_mapping"},
                    ]
                },
                status=200,
            )

            # Run without any windmill parameters
            result = main(comapeo_server_fixture, pg_database, str(datalake_root))

            # Windmill metrics should be automatically included
            assert "windmill" in result
            assert result["windmill"]["number_of_schedules"] == 1
            # Other metrics should also be present
            assert "comapeo" in result
            assert "files" in result


def test_get_auth0_metrics():
    """Test Auth0 metrics collection."""
    auth0_resource = {"token": "test_auth0_token"}
    auth0_domain = "your-tenant.us.auth0.com"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"https://{auth0_domain}/api/v2/users",
            json={
                "users": [{"user_id": "auth0|123"}],  # Actual users list
                "total": 150,  # Total count
            },
            status=200,
        )

        metrics = get_auth0_metrics(auth0_resource, auth0_domain)

        assert "users" in metrics
        assert metrics["users"] == 150


def test_auth0_metrics_optional(comapeo_server_fixture, tmp_path):
    """Test that Auth0 metrics are optional."""
    # Create test datalake
    datalake_root = tmp_path / "datalake"
    comapeo_dir = datalake_root / "comapeo"
    comapeo_dir.mkdir(parents=True)
    (comapeo_dir / "test_file.txt").write_bytes(b"x" * (1024 * 1024))

    # Run without Auth0 parameters
    result = main(comapeo_server_fixture, None, str(datalake_root))

    # Auth0 metrics should not be present
    assert "auth0" not in result
    # But other metrics should be present
    assert "comapeo" in result
    assert "files" in result


def test_no_parameters_provided(tmp_path):
    """Test that the script works when no parameters are provided."""
    # Create a datalake directory for files metrics
    datalake_root = tmp_path / "datalake"
    datalake_root.mkdir()
    (datalake_root / "test_file.txt").write_bytes(b"x" * (1024 * 1024))

    # Run with no CoMapeo, no db, no Windmill - only files metrics
    result = main(attachment_root=str(datalake_root))

    # Only files metrics should be present
    assert "files" in result
    assert result["files"]["file_count"] >= 1
    assert result["files"]["data_size_mb"] >= 1.0

    # All other metrics should not be present
    assert "comapeo" not in result
    assert "warehouse" not in result
    assert "explorer" not in result
    assert "superset" not in result
    assert "windmill" not in result

    # Result should not be empty
    assert len(result) == 1
